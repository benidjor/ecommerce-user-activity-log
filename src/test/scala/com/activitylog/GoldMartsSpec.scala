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

  test("mart_stickiness: DAU/MAU 비율") {
    fixtureActivity()
    val ss = spark; import ss.implicits._
    val rows = GoldMarts.build(spark, "sql/gold", "mart_stickiness")
      .as[(java.sql.Date, Long, Long, Double)].collect().toList
    // 2019-10-07: DAU 1 / MAU 3 = 0.333...
    val d07 = rows.find(_._1.toString == "2019-10-07").get
    assert(d07._2 == 1L && d07._3 == 3L)
    assert(math.abs(d07._4 - (1.0 / 3.0)) < 1e-9)
  }

  test("mart_funnel: 단계 distinct + 전환율, 상위 0이면 전환율 null") {
    fixtureActivity()
    val ss = spark; import ss.implicits._
    val rows = GoldMarts.build(spark, "sql/gold", "mart_funnel")
      .as[(java.sql.Timestamp, Long, Long, Long, Option[Double], Option[Double])].collect().toList
    // 1주차(10-07~13): view user1,2(2) cart user1(1) purchase user1(1)
    val w1 = rows.head
    assert(w1._2 == 2L && w1._3 == 1L && w1._4 == 1L)
    assert(math.abs(w1._5.get - 0.5) < 1e-9)   // cart/view = 1/2
    assert(math.abs(w1._6.get - 1.0) < 1e-9)   // purchase/cart = 1/1
    // 2주차(10-14~): view user3(1) cart 0 → cart_to_purchase = null(분모 0)
    val w2 = rows(1)
    assert(w2._3 == 0L && w2._6.isEmpty)
  }

  test("mart_cvr: 구매자/방문자 + WoW lag(첫 주 null)") {
    fixtureActivity()
    val ss = spark; import ss.implicits._
    val rows = GoldMarts.build(spark, "sql/gold", "mart_cvr")
      .as[(java.sql.Timestamp, Long, Long, Option[Double], Option[Double])].collect().toList
    // 1주차: visitors user1,2(2) purchasers user1(1) → CVR 0.5, WoW null(첫 주)
    val w1 = rows.head
    assert(w1._2 == 2L && w1._3 == 1L)
    assert(math.abs(w1._4.get - 0.5) < 1e-9)
    assert(w1._5.isEmpty)
    // 2주차: visitors user3(1) purchasers 0 → CVR 0.0, WoW = 0.0 - 0.5 = -0.5
    val w2 = rows(1)
    assert(math.abs(w2._4.get - 0.0) < 1e-9)
    assert(math.abs(w2._5.get - (-0.5)) < 1e-9)
  }

  test("mart_revenue: 구매 price 합 + AOV") {
    fixtureActivity()
    val ss = spark; import ss.implicits._
    val rows = GoldMarts.build(spark, "sql/gold", "mart_revenue")
      .as[(java.sql.Timestamp, Double, Long, Double)].collect().toList
    // 구매는 1주차 user1 purchase price=10.0 1건뿐
    assert(rows.length == 1)
    val w1 = rows.head
    // 부동소수는 형제 테스트와 동일하게 epsilon 비교(== 직접 비교 회피). 건수(_3)는 Long이라 ==.
    assert(math.abs(w1._2 - 10.0) < 1e-9 && w1._3 == 1L && math.abs(w1._4 - 10.0) < 1e-9)
  }
}
