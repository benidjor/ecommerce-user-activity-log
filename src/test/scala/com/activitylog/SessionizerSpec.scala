package com.activitylog

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, to_timestamp}
import org.scalatest.funsuite.AnyFunSuite

// AnyFunSuite: ScalaTest의 함수 기반 테스트 스타일(test("이름") { ... } 구문 사용).
// with SparkTestBase: 믹스인(mixin) — Python 다중 상속처럼 SparkSession 생명주기를 물려받는다.
class SessionizerSpec extends AnyFunSuite with SparkTestBase {

  // helper: (user_id, "yyyy-MM-dd HH:mm:ss") 행 목록 → event_time_utc 컬럼을 가진 DataFrame
  // (Long, String)*: 가변 인자(varargs). Python의 *args와 유사.
  //   호출 예) df((1L, "2019-10-01 00:00:00"), (1L, "2019-10-01 00:05:00"))
  private def df(rows: (Long, String)*): DataFrame = {
    // spark는 @transient var이라 클로저에 직접 캡처하면 직렬화 문제가 생긴다.
    // 로컬 val ss에 먼저 담고 import ss.implicits._로 DataFrame 인코더를 가져온다.
    val ss = spark
    import ss.implicits._   // toDF, as[T] 등 암묵적 변환(implicit conversion) 제공

    // rows.toDF: Seq[(Long, String)]을 ("user_id", "ts") 컬럼을 가진 DataFrame으로 변환.
    // withColumn: Python의 df.assign()과 유사. 새 컬럼을 추가(또는 기존 컬럼 덮어쓰기).
    // lit("view"): 리터럴(상수) 컬럼 생성. Python의 F.lit()과 동일.
    // to_timestamp: 문자열 → TimestampType 변환(SparkSession 타임존=UTC 기준 해석).
    rows.toDF("user_id", "ts")
      .withColumn("event_type", lit("view"))
      .withColumn("product_id", lit(1L))
      .withColumn("event_time_utc", to_timestamp(col("ts"), "yyyy-MM-dd HH:mm:ss"))
  }

  // sessionIds: sessionize 결과에서 session_id 컬럼 값을 순서대로 Seq[String]으로 수집.
  private def sessionIds(d: DataFrame): Seq[String] = {
    val ss = spark
    import ss.implicits._   // as[String] 타입 변환에 필요한 Encoder[String] 암묵적 제공

    // Sessionizer.sessionize(d): 세션화 로직 적용.
    // orderBy("event_time_utc"): 시간 순 정렬로 결과를 결정적으로 만든다.
    // .select("session_id").as[String]: DataFrame → Dataset[String] 타입 변환.
    // .collect().toSeq: 모든 행을 드라이버 메모리로 수집 → Scala Seq로 반환.
    Sessionizer.sessionize(d).orderBy("event_time_utc")
      .select("session_id").as[String].collect().toSeq
  }

  // ─── 경계 조건 테스트 6개 ───────────────────────────────────────────────────

  // [경계1] 정확히 5분(300초) 간격 → 새 세션 시작 (>=300s 조건의 경계값 테스트)
  test("exactly 5 minutes gap starts a new session (>=300s, boundary)") {
    val d = df((1L, "2019-10-01 00:00:00"), (1L, "2019-10-01 00:05:00"))
    assert(sessionIds(d).distinct.size == 2)
  }

  // [경계2] 4분 59초(299초) 간격 → 같은 세션 유지 (<300s 이므로 세션 분리 없음)
  test("less than 5 minutes stays same session") {
    val d = df((1L, "2019-10-01 00:00:00"), (1L, "2019-10-01 00:04:59"))
    assert(sessionIds(d).distinct.size == 1)
  }

  // [경계3] 동일 시각(간격=0) → 같은 세션 (gap=0 < 300, 분리 없음)
  test("same timestamp (gap=0) stays same session") {
    val d = df((1L, "2019-10-01 00:00:00"), (1L, "2019-10-01 00:00:00"))
    assert(sessionIds(d).distinct.size == 1)
  }

  // [경계4] 단일 이벤트 → 정확히 1개의 session_id
  test("single event yields exactly one session") {
    val d = df((1L, "2019-10-01 00:00:00"))
    assert(sessionIds(d).size == 1)
  }

  // [경계5] 자정 경계를 넘어도 gap < 5분이면 같은 세션
  // UTC 14:58 → 15:01 (KST 23:58 → 00:01 — 자정 넘어가는 시나리오)
  // gap = 3분 < 5분 → 같은 세션 id 유지
  test("session crossing midnight keeps one session id") {
    val d = df((1L, "2019-10-01 14:58:00"), (1L, "2019-10-01 15:01:00"))
    assert(sessionIds(d).distinct.size == 1)
  }

  // [경계6] session_id 결정성 검증: user_id + "_" + unix(세션 시작 시각)
  // 2019-10-01T00:00:00Z의 Unix epoch = 1569888000
  // 두 이벤트 gap = 2분 < 5분 → 하나의 세션, 세션 시작 = 첫 번째 이벤트 시각
  test("session id is deterministic = user_id + '_' + unix(session_start)") {
    val d = df((1L, "2019-10-01 00:00:00"), (1L, "2019-10-01 00:02:00"))
    val sid = sessionIds(d).head
    // java.time.Instant.parse: UTC 기준 ISO-8601 문자열 → epoch 초 변환(JVM 표준 라이브러리).
    val expected = "1_" + java.time.Instant.parse("2019-10-01T00:00:00Z").getEpochSecond
    assert(sid == expected)
  }
}
