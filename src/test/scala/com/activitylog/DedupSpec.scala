package com.activitylog

import org.scalatest.funsuite.AnyFunSuite

// AnyFunSuite: ScalaTest의 함수형 테스트 스타일(test("이름") { ... } 형식).
// SparkTestBase: 공용 SparkSession을 주입하는 믹스인 trait.
class DedupSpec extends AnyFunSuite with SparkTestBase {
  test("removes natural-key duplicates deterministically, keeps one") {
    // @transient var인 spark를 로컬 val로 받아야 implicits 임포트가 직렬화 오류 없이 동작.
    val ss = spark
    // implicits: Seq(...).toDF("col1", "col2") 등 DSL 변환을 가능하게 하는 암묵적 변환 모음.
    import ss.implicits._

    // 같은 (user_id,event_time,event_type,product_id) 2건(price만 다름) → 1건
    // price: 9.0, 7.0 두 행 중 결정적으로 가장 작은 값(price asc) 1건이 남아야 함.
    val df = Seq(
      (1L, "2019-10-01 00:00:00 UTC", "view", 100L, 9.0),
      (1L, "2019-10-01 00:00:00 UTC", "view", 100L, 7.0),
      (1L, "2019-10-01 00:01:00 UTC", "view", 100L, 7.0)
    ).toDF("user_id", "event_time", "event_type", "product_id", "price")

    val out = Dedup.dedup(df)
    assert(out.count() == 2L)

    // $"event_time": implicits 임포트로 활성화되는 컬럼 선택 문법($"컬럼명" = Column 객체).
    // Python의 df["event_time"]과 동일 역할.
    val kept = out.filter($"event_time" === "2019-10-01 00:00:00 UTC")
      .select("price").as[Double].collect().toList
    // 결정적 선택: price asc → 7.0 유지
    assert(kept == List(7.0))
  }
}
