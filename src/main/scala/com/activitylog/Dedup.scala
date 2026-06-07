package com.activitylog

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

// object: Scala의 싱글턴 객체(Python의 모듈 수준 함수/변수와 유사).
// 인스턴스 생성 없이 Dedup.dedup(df)로 바로 호출 가능.
object Dedup {

  // 자연키(natural key): 비즈니스 의미상 행을 유일하게 식별하는 컬럼 조합.
  // dedup은 이 4개 컬럼 값이 완전히 같은 행들을 "중복"으로 간주한다.
  private val key = Seq("user_id", "event_time", "event_type", "product_id")

  def dedup(df: DataFrame): DataFrame = {
    // 같은 자연키를 가진 중복 행이 여러 개일 때 어떤 1건을 남길지 결정하는 정렬 기준.
    // df.columns.contains: 실제 DataFrame에 해당 컬럼이 있을 때만 포함(없는 컬럼 참조 방지).
    // Python 예: [col(c).asc_nulls_last for c in tiebreak_cols if c in df.columns]
    val tiebreak = Seq("price", "brand", "category_id", "category_code", "user_session")
      .filter(df.columns.contains)               // DataFrame에 존재하는 컬럼만 사용
      .map(c => col(c).asc_nulls_last)           // 각 컬럼을 "오름차순, NULL은 뒤로" 정렬 객체로 변환

    // Window spec: 동일한 자연키 그룹 내에서 순위를 매기기 위한 윈도우 정의.
    // partitionBy: Python pandas의 groupby와 유사 — 자연키가 같은 행끼리 하나의 "파티션(그룹)"으로 묶음.
    // orderBy: 파티션 내 정렬 순서(여기서는 tiebreak 기준 오름차순).
    // key.map(col): _* 는 Seq를 가변인자(varargs)로 풀어서 넘기는 문법.
    //   Python 예: partitionBy(*[col(c) for c in key])
    val w = Window.partitionBy(key.map(col): _*).orderBy(tiebreak: _*)

    // row_number().over(w): 각 파티션(자연키 그룹) 내에서 1부터 순서대로 번호를 매김.
    //   정렬 기준(tiebreak)이 같으면 항상 동일 행이 1번 → 결정적(deterministic) dedup.
    // withColumn("_rn", ...): "_rn" 이름의 임시 컬럼 추가(Python: df.assign(_rn=...)).
    // filter(col("_rn") === 1): 각 그룹에서 1번 행(tiebreak 최솟값)만 남김.
    // drop("_rn"): 임시 컬럼 제거 후 반환.
    df.withColumn("_rn", row_number().over(w))
      .filter(col("_rn") === 1)
      .drop("_rn")
  }
}
