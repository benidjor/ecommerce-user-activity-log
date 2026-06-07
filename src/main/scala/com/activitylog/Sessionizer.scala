package com.activitylog

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

// object: Scala 싱글턴 객체(Python의 모듈 수준 함수와 유사).
// 인스턴스 생성 없이 Sessionizer.sessionize(df)로 바로 호출 가능.
object Sessionizer {

  // 세션 분리 임계값: 직전 이벤트와의 간격이 이 값 이상이면 새 세션 시작.
  // 300초 = 5분. Long 타입(L 접미사) — unix_timestamp() 결과가 Long이라 타입 일치.
  val GapThresholdSeconds: Long = 300L  // "5분 이상" = >= 300초

  def sessionize(df: DataFrame): DataFrame = {

    // ── 1단계: 동일초 tie 결정적 정렬 + 갭으로 새 세션 표시 ─────────────────────

    // Window spec: user_id별 파티션 내에서 이벤트를 시간 순으로 정렬하는 "창(window)" 정의.
    // partitionBy: Python pandas의 groupby와 유사 — user_id가 같은 행끼리 하나의 그룹으로 묶음.
    // orderBy: 파티션 내 정렬 기준.
    //   - event_time_utc: 기본 정렬(시간 순)
    //   - event_type, product_id: 동일 시각(타이브레이크) 시 결정적 정렬을 위한 보조 키.
    //     동일 시각 이벤트가 여러 건 있어도 항상 같은 순서가 나오도록 보장.
    val ordered = Window.partitionBy("user_id")
      .orderBy(col("event_time_utc"), col("event_type"), col("product_id"))

    // lag(col, 1).over(ordered): 각 행에서 "직전 행"의 event_time_utc 값을 가져옴.
    // Python pandas: df.groupby('user_id')['event_time_utc'].shift(1)과 유사.
    // 파티션 내 첫 번째 행은 직전이 없으므로 null 반환.
    val prev = lag(col("event_time_utc"), 1).over(ordered)

    // unix_timestamp(col): Timestamp 컬럼 → Unix epoch 초(Long) 변환.
    //   python: col.astype('int64') // 1e9 (나노초를 초로 변환) 와 유사.
    // gap: 현재 이벤트와 직전 이벤트 사이의 초 단위 시간 차이.
    val gap = unix_timestamp(col("event_time_utc")) - unix_timestamp(prev)

    // when(...).otherwise(...): SQL의 CASE WHEN ... THEN ... ELSE ... END와 동일.
    //   Python: np.where(condition, 1, 0) 또는 df['col'].apply(lambda x: 1 if ... else 0)
    // prev.isNull: 파티션 내 첫 번째 이벤트(직전 없음) → 무조건 새 세션 시작.
    // gap >= GapThresholdSeconds: 직전 이벤트와 간격이 300초 이상이면 새 세션 시작.
    // 결과: 새 세션 시작 행 = 1, 기존 세션 계속 = 0.
    val isNew = when(prev.isNull || gap >= GapThresholdSeconds, 1).otherwise(0)

    // rowsBetween(unboundedPreceding, currentRow): 파티션 시작부터 현재 행까지의 누적 범위 지정.
    //   unboundedPreceding = 파티션의 첫 번째 행부터.
    //   currentRow = 지금 처리 중인 행까지.
    //   Python pandas: expanding().sum() 또는 cumsum()의 window 버전.
    // ordered.rowsBetween: 앞서 정의한 정렬 순서(ordered)를 그대로 재사용.
    val cumulative = ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    // _is_new: 각 행이 새 세션 시작인지(1) 아닌지(0) 표시하는 임시 컬럼.
    // _seq: _is_new의 누적 합 → 세션 번호(세션이 바뀔 때마다 1씩 증가).
    //   예) [1, 0, 0, 1, 0] → [1, 1, 1, 2, 2] (같은 숫자 = 같은 세션)
    //   Python: (is_new == 1).cumsum() 과 동일 원리.
    val withSeq = df
      .withColumn("_is_new", isNew)
      .withColumn("_seq", sum(col("_is_new")).over(cumulative))

    // ── 2단계: 세션 시작시각(min) → 결정적 id ────────────────────────────────────

    // bySession: user_id + _seq(세션 번호) 조합으로 "하나의 세션" 그룹을 정의하는 윈도우.
    // 이 그룹 내에서 min(event_time_utc)를 구하면 세션의 첫 이벤트 시각을 얻는다.
    val bySession = Window.partitionBy(col("user_id"), col("_seq"))

    withSeq
      // _session_start: 세션 내 가장 이른 이벤트 시각(= 세션 시작 시각).
      // min().over(bySession): 파티션(세션) 내 최솟값을 모든 행에 동일하게 붙임.
      //   Python: df.groupby(['user_id','_seq'])['event_time_utc'].transform('min')
      .withColumn("_session_start", min(col("event_time_utc")).over(bySession))

      // session_id 생성: user_id + "_" + unix(세션 시작 시각) → 결정적(deterministic) id.
      // concat_ws("_", ...): "_"로 구분하여 여러 컬럼을 이어 붙임(Python: "_".join([...]) 유사).
      // col("user_id").cast("string"): Long 타입 user_id를 문자열로 변환.
      //   Python: str(user_id) 또는 df['user_id'].astype(str)
      // unix_timestamp(col("_session_start")).cast("string"): 세션 시작 Timestamp → epoch초 → 문자열.
      //   예) 2019-10-01 00:00:00 UTC → 1569888000 → "1569888000"
      // 최종 예) user_id=1, 세션시작=2019-10-01 00:00:00 → session_id = "1_1569888000"
      .withColumn("session_id",
        concat_ws("_", col("user_id").cast("string"),
          unix_timestamp(col("_session_start")).cast("string")))

      // 임시 컬럼 3개 제거: _is_new, _seq, _session_start.
      // drop: Python의 df.drop(columns=[...])와 동일.
      .drop("_is_new", "_seq", "_session_start")
  }
}
