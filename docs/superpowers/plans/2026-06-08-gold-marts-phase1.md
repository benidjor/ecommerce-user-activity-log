# Gold 마트 레이어 (Phase 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Silver `activity`(Hive External Table)에서 KPI용 Gold 차원·집계 fact·지표 마트를 SQL(SoT)+얇은 Scala 러너로 생성하고, parquet + Hive External Table(`gold_*`)로 노출한다.

**Architecture:** 각 마트의 SELECT 로직은 `sql/gold/<name>.sql` 한 곳에만 둔다(SoT). 얇은 러너 `GoldMarts`(Scala)가 그 파일을 읽어 `spark.sql`로 실행 → parquet 기록 → `gold_<name>` External Table 등록. 테스트는 **같은 sql 파일**을 작은 `activity` 픽스처에 대해 실행해 검증(WAU의 `.scala/.sql` 이중화를 반복하지 않음). 모든 마트는 `activity`만 상위 의존(마트 간 의존 없음 → 순서 무관·독립 테스트). DuckDB export·대시보드는 Phase 2(본 Phase 범위 밖).

**Tech Stack:** Scala 2.13 + Spark 4 SQL(Provided), ScalaTest(AnyFunSuite + 기존 `SparkTestBase`). 신규 라이브러리 없음. 산출물 parquet는 `output/gold/`(기존 `output/` gitignore 적용).

설계 근거: [2026-06-08-medallion-bi-dashboard-design.md](../specs/2026-06-08-medallion-bi-dashboard-design.md) §3.3·§4. 본 Phase는 그 §9의 Phase 1. **가드레일(§1.1)**: 제출용 WAU는 Hive `activity`(`sql/wau.sql`)가 정본 — Gold `mart_wau`는 대시보드용 추가본.

---

## File Structure

- `sql/gold/dim_date.sql` — 날짜 차원(activity 범위에서 생성).
- `sql/gold/fact_daily_activity.sql` — 집계 fact(일×이벤트타입).
- `sql/gold/mart_dau.sql` · `mart_wau.sql` · `mart_mau.sql` · `mart_stickiness.sql` — engagement 마트.
- `sql/gold/mart_funnel.sql` · `mart_cvr.sql` — conversion 마트.
- `sql/gold/mart_revenue.sql` — monetization 마트.
- `sql/gold/mart_retention.sql` — retention 코호트 마트.
- `src/main/scala/com/activitylog/GoldMarts.scala` — 러너(읽기·실행·기록·등록 + `main`).
- `src/test/scala/com/activitylog/GoldMartsSpec.scala` — 픽스처 기반 마트 검증.

각 `sql/gold/*.sql`은 **순수 SELECT**(materialization은 러너 담당)라, 테스트가 `activity` temp view에 그대로 실행 가능하다.

---

## Task 1: GoldMarts 러너 + dim_date (인프라 + 최소 마트)

**Files:**
- Create: `sql/gold/dim_date.sql`
- Create: `src/main/scala/com/activitylog/GoldMarts.scala`
- Test: `src/test/scala/com/activitylog/GoldMartsSpec.scala`

- [ ] **Step 1: dim_date SQL 작성**

`sql/gold/dim_date.sql`:

```sql
-- dim_date: activity의 event_date 최소~최대 범위로 날짜 차원 생성(빈 날 없이 연속)
-- dow: Spark dayofweek = 1(일)~7(토). is_weekend = 일/토.
WITH bounds AS (
  SELECT min(to_date(event_date)) AS d0, max(to_date(event_date)) AS d1 FROM activity
),
days AS (
  SELECT explode(sequence(d0, d1, interval 1 day)) AS d FROM bounds
)
SELECT
  date_format(d, 'yyyy-MM-dd')                              AS date_key,
  d                                                          AS date,
  date_trunc('week', d)                                      AS iso_week,
  date_format(d, 'yyyy-MM')                                  AS month,
  dayofweek(d)                                               AS dow,
  CASE WHEN dayofweek(d) IN (1, 7) THEN true ELSE false END  AS is_weekend
FROM days
ORDER BY d
```

- [ ] **Step 2: GoldMarts 러너 작성**

`src/main/scala/com/activitylog/GoldMarts.scala`:

```scala
package com.activitylog

import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.io.Source

// GoldMarts: Gold 마트 러너. sql/gold/<name>.sql(SoT)을 읽어 실행하고
//   parquet 기록 + External Table 등록까지 담당하는 얇은 드라이버.
object GoldMarts {

  // 생성 순서(모두 activity만 의존하므로 순서는 무관, 가독성용 정렬).
  val marts: Seq[String] = Seq(
    "dim_date", "fact_daily_activity",
    "mart_dau", "mart_wau", "mart_mau", "mart_stickiness",
    "mart_funnel", "mart_cvr", "mart_revenue", "mart_retention"
  )

  // readSql: sql/gold/<name>.sql 한 파일을 문자열로 읽는다.
  //   Source.fromFile: 파일을 열고, mkString으로 전체를 문자열화, finally로 닫음(누수 방지).
  def readSql(sqlDir: String, name: String): String = {
    val src = Source.fromFile(s"$sqlDir/$name.sql")
    try src.mkString finally src.close()
  }

  // build: 마트 SELECT를 activity(테이블/뷰)에 실행해 DataFrame 반환(테스트가 직접 호출).
  def build(spark: SparkSession, sqlDir: String, name: String): DataFrame =
    spark.sql(readSql(sqlDir, name))

  // runAll: 전체 마트를 실행 → parquet(단일 파일로 coalesce) 기록 → External Table 등록.
  //   마트는 수백 행이라 coalesce(1)로 소파일 1개. mode overwrite로 멱등.
  def runAll(spark: SparkSession, sqlDir: String, outBase: String): Unit = {
    marts.foreach { name =>
      val df   = build(spark, sqlDir, name)
      val path = s"$outBase/$name"
      df.coalesce(1).write.mode("overwrite").parquet(path)
      // USING parquet LOCATION: 데이터를 path에 둔 unmanaged(외부) 테이블. DROP해도 데이터 보존.
      spark.sql(s"DROP TABLE IF EXISTS gold_$name")
      spark.sql(s"CREATE TABLE gold_$name USING parquet LOCATION '$path'")
      spark.sql(s"REFRESH TABLE gold_$name")
      println(s"[OK] gold mart $name -> $path (rows=${df.count()})")
    }
  }

  // main: spark-submit 진입점. activity External Table이 메타스토어에 선등록돼 있어야 함.
  def main(args: Array[String]): Unit = {
    def arg(k: String, default: Option[String]): String = {
      val i = args.indexOf(k)
      if (i >= 0 && i + 1 < args.length) args(i + 1)
      else default.getOrElse(sys.error(s"$k required"))
    }
    val sqlDir  = arg("--sql-dir", Some("sql/gold"))
    val outBase = arg("--output", None)
    val spark   = SparkSessionFactory.create("gold-marts")
    try runAll(spark, sqlDir, outBase) finally spark.stop()
  }
}
```

- [ ] **Step 3: 실패 테스트 작성**

`src/test/scala/com/activitylog/GoldMartsSpec.scala`:

```scala
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
```

- [ ] **Step 4: 실패 확인**

Run: `sbt "testOnly com.activitylog.GoldMartsSpec"`
Expected: FAIL — `sql/gold/dim_date.sql`/`GoldMarts` 미작성 시 컴파일/실행 에러. (Step 1·2 작성 후엔 PASS)

- [ ] **Step 5: 통과 확인**

Run: `sbt "testOnly com.activitylog.GoldMartsSpec"`
Expected: PASS (1 test).

- [ ] **Step 6: 커밋**

```bash
git add sql/gold/dim_date.sql src/main/scala/com/activitylog/GoldMarts.scala src/test/scala/com/activitylog/GoldMartsSpec.scala
git commit -m "feat(gold): GoldMarts 러너 + dim_date 마트"
```

---

## Task 2: fact_daily_activity (집계 fact)

**Files:**
- Create: `sql/gold/fact_daily_activity.sql`
- Modify: `src/test/scala/com/activitylog/GoldMartsSpec.scala`

- [ ] **Step 1: SQL 작성**

`sql/gold/fact_daily_activity.sql`:

```sql
-- fact_daily_activity: 집계 fact. 그레인 = event_date × event_type.
-- 원자 fact는 Silver activity(약 9천만 행). 여기선 가산 측정 + 그 단위 distinct.
-- sum_price: 해당 타입의 price 합(매출 해석은 mart_revenue에서 purchase로 한정).
SELECT
  event_date,
  event_type,
  count(*)                   AS event_count,
  count(DISTINCT user_id)    AS distinct_users,
  count(DISTINCT session_id) AS distinct_sessions,
  sum(price)                 AS sum_price
FROM activity
GROUP BY event_date, event_type
ORDER BY event_date, event_type
```

- [ ] **Step 2: 실패 테스트 추가**

`GoldMartsSpec.scala`에 추가:

```scala
  test("fact_daily_activity: 일×타입 그레인, 가산 측정") {
    fixtureActivity()
    val ss = spark; import ss.implicits._
    val rows = GoldMarts.build(spark, "sql/gold", "fact_daily_activity")
      .as[(String, String, Long, Long, Long, Double)].collect().toList
    // 2019-10-07: view·cart·purchase 3행
    val d07 = rows.filter(_._1 == "2019-10-07")
    assert(d07.map(_._2).toSet == Set("view", "cart", "purchase"))
    // purchase 행: event_count=1, distinct_users=1, sum_price=10.0
    val purch = d07.find(_._2 == "purchase").get
    assert(purch._3 == 1L && purch._4 == 1L && purch._6 == 10.0)
  }
```

- [ ] **Step 3: 실패 확인**

Run: `sbt "testOnly com.activitylog.GoldMartsSpec"`
Expected: FAIL — `fact_daily_activity.sql` 미존재.

- [ ] **Step 4: 통과 확인**

Run: `sbt "testOnly com.activitylog.GoldMartsSpec"`
Expected: PASS (2 tests).

- [ ] **Step 5: 커밋**

```bash
git add sql/gold/fact_daily_activity.sql src/test/scala/com/activitylog/GoldMartsSpec.scala
git commit -m "feat(gold): fact_daily_activity 집계 fact"
```

---

## Task 3: Engagement 마트 (dau · wau · mau)

**Files:**
- Create: `sql/gold/mart_dau.sql`, `sql/gold/mart_wau.sql`, `sql/gold/mart_mau.sql`
- Modify: `src/test/scala/com/activitylog/GoldMartsSpec.scala`

- [ ] **Step 1: 세 SQL 작성**

`sql/gold/mart_dau.sql`:

```sql
-- mart_dau: 일별 활성 사용자/세션
SELECT event_date,
       count(DISTINCT user_id)    AS dau_users,
       count(DISTINCT session_id) AS dau_sessions
FROM activity
GROUP BY event_date
ORDER BY event_date
```

`sql/gold/mart_wau.sql`:

```sql
-- mart_wau: ISO 주(월요일)별 활성 사용자/세션. user+session을 한 행으로 통합.
-- 주의: 제출용 WAU 정본은 sql/wau.sql(Hive activity). 이 마트는 대시보드 서빙용 추가본.
SELECT date_trunc('week', to_date(event_date)) AS week_start,
       count(DISTINCT user_id)    AS wau_users,
       count(DISTINCT session_id) AS wau_sessions
FROM activity
GROUP BY 1
ORDER BY 1
```

`sql/gold/mart_mau.sql`:

```sql
-- mart_mau: KST 월별 활성 사용자(데이터 2개월 → 점 2~3개; 한계는 대시보드에 명시)
SELECT date_format(to_date(event_date), 'yyyy-MM') AS month,
       count(DISTINCT user_id) AS mau_users
FROM activity
GROUP BY 1
ORDER BY 1
```

- [ ] **Step 2: 실패 테스트 추가**

```scala
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
```

- [ ] **Step 3: 실패 확인**

Run: `sbt "testOnly com.activitylog.GoldMartsSpec"`
Expected: FAIL — 세 sql 미존재.

- [ ] **Step 4: 통과 확인**

Run: `sbt "testOnly com.activitylog.GoldMartsSpec"`
Expected: PASS (3 tests).

- [ ] **Step 5: 커밋**

```bash
git add sql/gold/mart_dau.sql sql/gold/mart_wau.sql sql/gold/mart_mau.sql src/test/scala/com/activitylog/GoldMartsSpec.scala
git commit -m "feat(gold): engagement 마트(dau/wau/mau)"
```

---

## Task 4: mart_stickiness (DAU/MAU)

**Files:**
- Create: `sql/gold/mart_stickiness.sql`
- Modify: `src/test/scala/com/activitylog/GoldMartsSpec.scala`

- [ ] **Step 1: SQL 작성**

`sql/gold/mart_stickiness.sql`:

```sql
-- mart_stickiness: 일별 DAU / 해당 월 MAU. nullif로 0 division 방지.
WITH dau AS (
  SELECT to_date(event_date) AS d,
         date_format(to_date(event_date), 'yyyy-MM') AS month,
         count(DISTINCT user_id) AS dau
  FROM activity GROUP BY 1, 2
),
mau AS (
  SELECT date_format(to_date(event_date), 'yyyy-MM') AS month,
         count(DISTINCT user_id) AS mau
  FROM activity GROUP BY 1
)
SELECT dau.d AS date, dau.dau, mau.mau,
       dau.dau / nullif(mau.mau, 0) AS stickiness
FROM dau JOIN mau USING (month)
ORDER BY dau.d
```

- [ ] **Step 2: 실패 테스트 추가**

```scala
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
```

- [ ] **Step 3: 실패 확인**

Run: `sbt "testOnly com.activitylog.GoldMartsSpec"`
Expected: FAIL — `mart_stickiness.sql` 미존재.

- [ ] **Step 4: 통과 확인**

Run: `sbt "testOnly com.activitylog.GoldMartsSpec"`
Expected: PASS (4 tests).

- [ ] **Step 5: 커밋**

```bash
git add sql/gold/mart_stickiness.sql src/test/scala/com/activitylog/GoldMartsSpec.scala
git commit -m "feat(gold): mart_stickiness(DAU/MAU)"
```

---

## Task 5: mart_funnel (단계 전환 + div-zero)

**Files:**
- Create: `sql/gold/mart_funnel.sql`
- Modify: `src/test/scala/com/activitylog/GoldMartsSpec.scala`

- [ ] **Step 1: SQL 작성**

`sql/gold/mart_funnel.sql`:

```sql
-- mart_funnel: 주별 단계(view→cart→purchase) distinct user + 단계 전환율.
-- nullif로 상위 단계 0일 때 division by zero 방지(전환율 null).
SELECT date_trunc('week', to_date(event_date)) AS week_start,
       count(DISTINCT CASE WHEN event_type = 'view'     THEN user_id END) AS users_view,
       count(DISTINCT CASE WHEN event_type = 'cart'     THEN user_id END) AS users_cart,
       count(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS users_purchase,
       count(DISTINCT CASE WHEN event_type = 'cart'     THEN user_id END)
         / nullif(count(DISTINCT CASE WHEN event_type = 'view' THEN user_id END), 0) AS view_to_cart,
       count(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END)
         / nullif(count(DISTINCT CASE WHEN event_type = 'cart' THEN user_id END), 0) AS cart_to_purchase
FROM activity
GROUP BY 1
ORDER BY 1
```

- [ ] **Step 2: 실패 테스트 추가**

```scala
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
```

- [ ] **Step 3: 실패 확인**

Run: `sbt "testOnly com.activitylog.GoldMartsSpec"`
Expected: FAIL — `mart_funnel.sql` 미존재.

- [ ] **Step 4: 통과 확인**

Run: `sbt "testOnly com.activitylog.GoldMartsSpec"`
Expected: PASS (5 tests).

- [ ] **Step 5: 커밋**

```bash
git add sql/gold/mart_funnel.sql src/test/scala/com/activitylog/GoldMartsSpec.scala
git commit -m "feat(gold): mart_funnel(단계 전환율 + div-zero 가드)"
```

---

## Task 6: mart_cvr (주별 CVR + 전주 대비)

**Files:**
- Create: `sql/gold/mart_cvr.sql`
- Modify: `src/test/scala/com/activitylog/GoldMartsSpec.scala`

- [ ] **Step 1: SQL 작성**

`sql/gold/mart_cvr.sql`:

```sql
-- mart_cvr: 주별 CVR = 구매자수 / 방문자수. WoW = 직전 주 대비 CVR 증감(lag).
WITH wk AS (
  SELECT date_trunc('week', to_date(event_date)) AS week_start,
         count(DISTINCT CASE WHEN event_type = 'view'     THEN user_id END) AS visitors,
         count(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS purchasers
  FROM activity GROUP BY 1
)
SELECT week_start, visitors, purchasers,
       purchasers / nullif(visitors, 0) AS cvr,
       (purchasers / nullif(visitors, 0))
         - lag(purchasers / nullif(visitors, 0)) OVER (ORDER BY week_start) AS cvr_wow_delta
FROM wk
ORDER BY week_start
```

- [ ] **Step 2: 실패 테스트 추가**

```scala
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
```

- [ ] **Step 3: 실패 확인**

Run: `sbt "testOnly com.activitylog.GoldMartsSpec"`
Expected: FAIL — `mart_cvr.sql` 미존재.

- [ ] **Step 4: 통과 확인**

Run: `sbt "testOnly com.activitylog.GoldMartsSpec"`
Expected: PASS (6 tests).

- [ ] **Step 5: 커밋**

```bash
git add sql/gold/mart_cvr.sql src/test/scala/com/activitylog/GoldMartsSpec.scala
git commit -m "feat(gold): mart_cvr(주별 CVR + WoW)"
```

---

## Task 7: mart_revenue (주별 매출 + AOV)

**Files:**
- Create: `sql/gold/mart_revenue.sql`
- Modify: `src/test/scala/com/activitylog/GoldMartsSpec.scala`

- [ ] **Step 1: SQL 작성**

`sql/gold/mart_revenue.sql`:

```sql
-- mart_revenue: 주별 결제금액(구매 이벤트의 price 합) + AOV(=결제금액/구매건수).
-- order_id 없어 구매 이벤트를 주문 proxy로 사용(한계는 대시보드에 명시).
SELECT date_trunc('week', to_date(event_date)) AS week_start,
       sum(price)                       AS revenue,
       count(*)                         AS purchases,
       sum(price) / nullif(count(*), 0) AS aov
FROM activity
WHERE event_type = 'purchase'
GROUP BY 1
ORDER BY 1
```

- [ ] **Step 2: 실패 테스트 추가**

```scala
  test("mart_revenue: 구매 price 합 + AOV") {
    fixtureActivity()
    val ss = spark; import ss.implicits._
    val rows = GoldMarts.build(spark, "sql/gold", "mart_revenue")
      .as[(java.sql.Timestamp, Double, Long, Double)].collect().toList
    // 구매는 1주차 user1 purchase price=10.0 1건뿐
    assert(rows.length == 1)
    val w1 = rows.head
    assert(w1._2 == 10.0 && w1._3 == 1L && w1._4 == 10.0)
  }
```

- [ ] **Step 3: 실패 확인**

Run: `sbt "testOnly com.activitylog.GoldMartsSpec"`
Expected: FAIL — `mart_revenue.sql` 미존재.

- [ ] **Step 4: 통과 확인**

Run: `sbt "testOnly com.activitylog.GoldMartsSpec"`
Expected: PASS (7 tests).

- [ ] **Step 5: 커밋**

```bash
git add sql/gold/mart_revenue.sql src/test/scala/com/activitylog/GoldMartsSpec.scala
git commit -m "feat(gold): mart_revenue(주별 매출 + AOV)"
```

---

## Task 8: mart_retention (주차 코호트 리텐션)

**Files:**
- Create: `sql/gold/mart_retention.sql`
- Modify: `src/test/scala/com/activitylog/GoldMartsSpec.scala`

- [ ] **Step 1: SQL 작성**

`sql/gold/mart_retention.sql`:

```sql
-- mart_retention: 주차 코호트 리텐션.
--   코호트 = 데이터 내 사용자의 '첫 활동 주'. week_offset = 활동 주 - 코호트 주(주 단위).
--   retention_rate = (코호트 중 offset주에 활동한 distinct user) / 코호트 크기.
-- 한계: 데이터 2개월(9주)이라 작은 삼각형. "신규"는 Oct 이전 이력 없어 '데이터 내 첫 활동'으로 정의.
WITH user_weeks AS (
  SELECT DISTINCT user_id, date_trunc('week', to_date(event_date)) AS wk FROM activity
),
cohort AS (
  SELECT user_id, min(wk) AS cohort_week FROM user_weeks GROUP BY user_id
),
cohort_size AS (
  SELECT cohort_week, count(*) AS cohort_users FROM cohort GROUP BY cohort_week
)
SELECT c.cohort_week,
       datediff(uw.wk, c.cohort_week) / 7                  AS week_offset,
       count(DISTINCT uw.user_id)                           AS active_users,
       cs.cohort_users,
       count(DISTINCT uw.user_id) * 1.0 / cs.cohort_users   AS retention_rate
FROM user_weeks uw
JOIN cohort      c  USING (user_id)
JOIN cohort_size cs USING (cohort_week)
GROUP BY c.cohort_week, datediff(uw.wk, c.cohort_week) / 7, cs.cohort_users
ORDER BY c.cohort_week, week_offset
```

- [ ] **Step 2: 실패 테스트 추가**

리텐션 전용 픽스처를 둔다(코호트 경계 명확화).

```scala
  test("mart_retention: 코호트 첫 주 기준 offset·retention_rate") {
    val ss = spark; import ss.implicits._
    // user1: 1주차·2주차 활동 / user2: 1주차만 / user3: 2주차 첫 활동
    Seq(
      (1L, "a", "view", 0.0, "2019-10-07"),
      (1L, "a", "view", 0.0, "2019-10-14"),
      (2L, "b", "view", 0.0, "2019-10-07"),
      (3L, "c", "view", 0.0, "2019-10-14")
    ).toDF("user_id", "session_id", "event_type", "price", "event_date")
      .createOrReplaceTempView("activity")

    val rows = GoldMarts.build(spark, "sql/gold", "mart_retention")
      .as[(java.sql.Timestamp, Int, Long, Long, Double)].collect().toList
    // 코호트 10-07(월): 크기 2(user1,2). offset0 active 2 → 1.0, offset1 active 1(user1) → 0.5
    val c1 = rows.filter(r => r._1.toString.startsWith("2019-10-07"))
    val o0 = c1.find(_._2 == 0).get; assert(o0._3 == 2L && math.abs(o0._5 - 1.0) < 1e-9)
    val o1 = c1.find(_._2 == 1).get; assert(o1._3 == 1L && math.abs(o1._5 - 0.5) < 1e-9)
    // 코호트 10-14(월): 크기 1(user3). offset0 active 1 → 1.0
    val c2 = rows.filter(r => r._1.toString.startsWith("2019-10-14"))
    assert(c2.length == 1 && c2.head._2 == 0 && math.abs(c2.head._5 - 1.0) < 1e-9)
  }
```

- [ ] **Step 3: 실패 확인**

Run: `sbt "testOnly com.activitylog.GoldMartsSpec"`
Expected: FAIL — `mart_retention.sql` 미존재.

- [ ] **Step 4: 통과 확인**

Run: `sbt "testOnly com.activitylog.GoldMartsSpec"`
Expected: PASS (8 tests).

- [ ] **Step 5: 커밋**

```bash
git add sql/gold/mart_retention.sql src/test/scala/com/activitylog/GoldMartsSpec.scala
git commit -m "feat(gold): mart_retention(주차 코호트)"
```

---

## Task 9: 전체 마트 실측 실행 + 검증 (실데이터)

**Files:**
- (실행만 — 산출물 `output/gold/`는 gitignore)

선행: Task 12 backfill의 `activity` 테이블이 메타스토어에 등록돼 있어야 한다(없으면 런북 [full-backfill.md](../../runbook/full-backfill.md) §3로 등록).

- [ ] **Step 1: event_type 실제 값 확인(SQL 가정 검증)**

Run:
```bash
spark-sql --conf spark.sql.session.timeZone=UTC -e "SELECT DISTINCT event_type FROM activity;"
```
Expected: `view`, `cart`, `remove_from_cart`, `purchase` 류. 퍼널/CVR SQL은 view/cart/purchase만 사용(remove_from_cart 무시) — 값이 다르면 `sql/gold/mart_funnel.sql`·`mart_cvr.sql`의 리터럴을 실제 값으로 맞추고 테스트 재실행.

- [ ] **Step 2: jar 빌드**

Run: `sbt package`
Expected: `target/scala-2.13/activity-log_2.13-0.1.0.jar` 갱신.

- [ ] **Step 3: Gold 마트 전체 실행**

Run:
```bash
spark-submit --class com.activitylog.GoldMarts --master "local[*]" --driver-memory 8g \
  --conf spark.sql.session.timeZone=UTC --conf spark.sql.shuffle.partitions=200 \
  --conf spark.driver.extraJavaOptions="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
  target/scala-2.13/activity-log_2.13-0.1.0.jar \
  --sql-dir "$(pwd)/sql/gold" --output "$(pwd)/output/gold"
```
Expected: 각 마트 `[OK] gold mart <name> -> ... (rows=N)` 10줄.

- [ ] **Step 4: mart_wau가 제출용 실측치와 일치하는지 회귀 검증**

Run:
```bash
spark-sql --conf spark.sql.session.timeZone=UTC -e "SELECT * FROM gold_mart_wau ORDER BY week_start;"
```
Expected: `wau_users` 첫 주(2019-09-30) 818388 등 — `results/wau_users.txt`와 일치(가드레일: 정본은 sql/wau.sql, 본 마트는 동일 수치여야 함).

- [ ] **Step 5: 마트 행수 sanity(집계 fact 확인)**

Run:
```bash
spark-sql --conf spark.sql.session.timeZone=UTC -e "SELECT count(*) FROM gold_fact_daily_activity; SELECT count(*) FROM gold_dim_date;"
```
Expected: fact는 수백(≈62×타입수), dim_date≈62. 9천만 행이 아님(집계 fact 확인).

- [ ] **Step 6: 커밋(코드 변경 시에만)**

event_type 보정 등 SQL 변경이 있었으면 커밋. 산출물(`output/gold/`)은 gitignore라 미커밋. 실행 절차 문서화는 Phase 2 런북에서.

---

## Self-Review 메모 (작성자 체크)

- **스펙 커버리지**: §4.1 차원+fact → Task 1·2. §4.2 비가산 마트(dau/wau/mau/stickiness/funnel/revenue/cvr/retention) → Task 3~8. §3.3 SQL SoT + 얇은 러너 → Task 1. §7 테스트(비가산성·퍼널·CVR div-zero·리텐션 코호트) → 각 Task 테스트로 커버.
- **가드레일(§1.1) 준수**: 제출용 WAU 정본은 `sql/wau.sql`(Hive activity), `mart_wau`는 대시보드용 추가본 — Task 9 Step 4에서 동일 수치 회귀 검증. Gold는 parquet+Hive로 등록(DuckDB는 Phase 2).
- **범위 밖(후속 Phase)**: DuckDB export·정적/Streamlit 대시보드(Phase 2~3), Airflow DAG·DailySplitter·Discord(Phase 4).
- **타입 일관성**: 러너 `build(spark, sqlDir, name)` 시그니처를 전 Task가 동일 사용. 마트는 모두 `activity`만 의존.
- **가정**: event_type 리터럴(view/cart/purchase)은 Task 9 Step 1에서 실데이터로 검증 후 필요 시 보정.
