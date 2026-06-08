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

  test("fact_daily_activity: 일×타입 그레인, 가산 측정") {
    fixtureActivity()
    val ss = spark; import ss.implicits._
    val rows = GoldMarts.build(spark, "sql/gold", "fact_daily_activity")
      .as[(String, String, Long, Long, Long, Double)].collect().toList
    // 2019-10-07: view·cart·purchase 3행
    val d07 = rows.filter(_._1 == "2019-10-07")
    assert(d07.map(_._2).toSet == Set("view", "cart", "purchase"))
    // purchase 행: event_count=1, distinct_users=1, distinct_sessions=1, sum_price=10.0
    val purch = d07.find(_._2 == "purchase").get
    assert(purch._3 == 1L && purch._4 == 1L && purch._5 == 1L && purch._6 == 10.0)
  }

  test("mart_dau/wau/mau: 비가산 distinct 집계") {
    fixtureActivity()
    val ss = spark; import ss.implicits._
    // DAU: 10-07 user1, 10-08 user2, 10-14 user3 → 각 1
    val dau = GoldMarts.build(spark, "sql/gold", "mart_dau")
      .as[(String, Long, Long)].collect().map(r => (r._1, r._2)).toList
    assert(dau == List(("2019-10-07", 1L), ("2019-10-08", 1L), ("2019-10-14", 1L)))
    // WAU: 1주차(10-07~13) user1,2 → 2 / 2주차(10-14~) user3 → 1
    val wau = GoldMarts.build(spark, "sql/gold", "mart_wau")
      .as[(java.sql.Timestamp, Long, Long)].collect().map(_._2).toList
    assert(wau == List(2L, 1L))
    // MAU: 2019-10 한 달 user1,2,3 → 3
    val mau = GoldMarts.build(spark, "sql/gold", "mart_mau")
      .as[(String, Long)].collect().toList
    assert(mau == List(("2019-10", 3L)))
  }
}
