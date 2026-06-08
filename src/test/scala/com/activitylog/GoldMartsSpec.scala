package com.activitylog

import org.scalatest.funsuite.AnyFunSuite

// GoldMartsSpec: 작은 activity 픽스처를 temp view로 등록하고, sql/gold/*.sql을
//   그대로 실행해 각 마트의 핵심 로직을 검증한다(SoT 단일화).
class GoldMartsSpec extends AnyFunSuite with SparkTestBase {

  // fixtureActivity: 테스트용 activity temp view 등록.
  //   컬럼은 마트 SQL이 참조하는 것만(user_id, session_id, event_type, price, event_date).
  private def fixtureActivity(): Unit = {
    val ss = spark
    import ss.implicits._
    Seq(
      // (user, session, type, price, event_date)
      (1L, "s1", "view",     0.0, "2019-10-07"),
      (1L, "s1", "cart",     0.0, "2019-10-07"),
      (1L, "s1", "purchase", 10.0, "2019-10-07"),
      (2L, "s2", "view",     0.0, "2019-10-08"),
      (3L, "s3", "view",     0.0, "2019-10-14")  // 다음 주
    ).toDF("user_id", "session_id", "event_type", "price", "event_date")
      .createOrReplaceTempView("activity")
  }

  test("dim_date: event_date 범위를 빈 날 없이 연속 생성") {
    fixtureActivity()
    val rows = GoldMarts.build(spark, "sql/gold", "dim_date").collect()
    // 2019-10-07 ~ 2019-10-14 = 8일
    assert(rows.length == 8)
    // 첫 행 date_key = 2019-10-07
    assert(rows.head.getAs[String]("date_key") == "2019-10-07")
  }
}
