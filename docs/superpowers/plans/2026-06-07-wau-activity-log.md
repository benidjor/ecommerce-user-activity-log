# 이커머스 Activity 로그 → Hive External Table + WAU 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 2019-Oct/Nov 이커머스 이벤트 CSV를 dedup·KST변환·5분갭 세션화하여 KST 일별 파티션 parquet(snappy)로 적재하고, Hive External Table로 노출한 뒤 WAU(user_id/session_id) 2종을 실측한다.

**Architecture:** 로컬 Spark(local 모드) + Scala + sbt. 순수 함수형 변환 단위(Schema/TimeUtils/Dedup/Sessionizer/WauQueries)를 작게 분리하고, 부수효과(원자적 파티션 쓰기·검증 게이트·_SUCCESS)는 PartitionWriter로 격리. 세션화는 결정적 `session_id`(=user_id+세션시작시각) + backward-only 윈도우. Backfill 모드(전역 세션화 1회)로 과제 완수, Incremental(전날 lookback)은 설계로 대응.

**Tech Stack:** Scala 2.13.14, Spark 4.1.2(브루 apache-spark와 정렬), sbt 1.12.x, ScalaTest 3.2.18, JDK 17(+add-opens), parquet+snappy, Hive(임베디드 Derby metastore) External Table.

**설계 근거 문서:** `docs/superpowers/specs/2026-06-07-wau-activity-log-design.md` (결정 로그 9개 + 핵심 로직 명세).

---

## 파일 구조

```
build.sbt                                  # 빌드 정의(Scala/Spark 버전, add-opens, assembly)
project/build.properties                   # sbt 버전
project/plugins.sbt                        # sbt-assembly
src/main/scala/com/activitylog/
  SparkSessionFactory.scala                # SparkSession 생성(UTC tz, hive 지원)
  Schema.scala                             # raw CSV 명시적 스키마 + 컬럼 상수
  TimeUtils.scala                          # " UTC" 파싱, KST 변환, event_date 파생
  Dedup.scala                              # 자연키 결정적 dedup
  Sessionizer.scala                        # 5분 갭 세션화, 결정적 session_id
  PartitionWriter.scala                    # 검증 게이트 + staging+rename + _SUCCESS
  WauQueries.scala                         # WAU user/session SQL
  ActivityPipeline.scala                   # read→dedup→tz→sessionize 조합
  Main.scala                               # CLI(--mode/--input/--output/--run-date)
src/test/scala/com/activitylog/
  SparkTestBase.scala                      # 테스트용 로컬 SparkSession(UTC)
  TimeUtilsSpec.scala
  DedupSpec.scala
  SessionizerSpec.scala                    # 핵심 TDD(경계 케이스)
  PartitionWriterSpec.scala
  WauSpec.scala
  ActivityPipelineSpec.scala
sql/create_external_table.sql              # 외부 테이블 DDL
README.md                                  # 마지막(AI 도구 활용 + 언어 사유)
```

---

## Task 0: 환경 설치 & 버전 정렬

**Files:** (없음 — 설치/검증만)

- [ ] **Step 1: sbt, apache-spark 설치**

Run:
```bash
brew install sbt apache-spark
```
Expected: 설치 완료(이미 있으면 "already installed").

- [ ] **Step 2: 버전 확인 및 정렬 메모**

Run:
```bash
sbt --version
spark-submit --version 2>&1 | grep -E "version|Scala" | head -5
/usr/bin/java -version 2>&1 | head -1
```
Expected: Spark 4.1.x, Scala 2.13.x, Java 17 확인.
**중요:** 출력된 Spark/Scala 버전이 4.1.2 / 2.13과 다르면, 이후 `build.sbt`의 `sparkVersion`/`scalaVersion`을 그 값으로 맞춘다(spark-submit 런타임과 일치시켜야 함).

---

## Task 1: sbt 프로젝트 골격 + 툴체인 검증 테스트

먼저 "Spark 테스트가 도는지"를 trivial 테스트로 확인해 툴체인(JDK17 add-opens 포함)을 검증한다.

**Files:**
- Create: `build.sbt`, `project/build.properties`, `project/plugins.sbt`
- Create: `src/test/scala/com/activitylog/SparkTestBase.scala`
- Create: `src/test/scala/com/activitylog/ToolchainSpec.scala`

- [ ] **Step 1: `project/build.properties`**

```
sbt.version=1.12.11
```

- [ ] **Step 2: `project/plugins.sbt`**

```scala
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.2.0")
```

- [ ] **Step 3: `build.sbt`**

```scala
ThisBuild / scalaVersion := "2.13.14"
ThisBuild / version      := "0.1.0"
ThisBuild / organization := "com.activitylog"

val sparkVersion = "4.1.2"  // Task 0에서 확인한 brew spark 버전과 일치시킬 것

// JDK 17에서 Spark 실행에 필요한 add-opens
val jvm17Opens = Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
)

lazy val root = (project in file("."))
  .settings(
    name := "activity-log",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql"  % sparkVersion % Provided,
      "org.apache.spark" %% "spark-hive" % sparkVersion % Provided,
      "org.scalatest"    %% "scalatest"  % "3.2.18"     % Test
    ),
    // provided 의존성을 테스트/실행 클래스패스에 포함
    Test / classpathConfiguration := Compile,
    Test / fork := true,
    Test / javaOptions ++= jvm17Opens,
    Test / parallelExecution := false,
    run / fork := true,
    run / javaOptions ++= jvm17Opens,
    // thin jar(sbt package)로 spark-submit. assembly는 README용 fat jar(선택)
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _ @ _*) => MergeStrategy.discard
      case _                            => MergeStrategy.first
    }
  )
```

- [ ] **Step 4: `SparkTestBase.scala` (테스트 공용 SparkSession)**

```scala
package com.activitylog

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkTestBase extends BeforeAndAfterAll { self: Suite =>
  @transient protected var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .appName("test")
      .master("local[2]")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    super.afterAll()
  }
}
```

- [ ] **Step 5: `ToolchainSpec.scala` (툴체인 검증 테스트 — 실패 먼저)**

```scala
package com.activitylog

import org.scalatest.funsuite.AnyFunSuite

class ToolchainSpec extends AnyFunSuite with SparkTestBase {
  test("spark session runs a trivial job on JDK17") {
    import spark.implicits._
    val df = Seq(1, 2, 3).toDF("n")
    assert(df.agg(org.apache.spark.sql.functions.sum("n")).first().getLong(0) == 6L)
  }
}
```

- [ ] **Step 6: 테스트 실행 → 통과 확인**

Run: `sbt test`
Expected: `ToolchainSpec` PASS. (실패 시 add-opens/버전 문제 → Task 0 버전 재정렬)

- [ ] **Step 7: 커밋**

```bash
git add build.sbt project/ src/test/scala/com/activitylog/SparkTestBase.scala src/test/scala/com/activitylog/ToolchainSpec.scala
git commit -m "chore: sbt+spark 골격 및 툴체인 검증 테스트"
```

---

## Task 2: Schema (raw CSV 명시적 스키마)

**Files:**
- Create: `src/main/scala/com/activitylog/Schema.scala`

- [ ] **Step 1: `Schema.scala` 작성**

```scala
package com.activitylog

import org.apache.spark.sql.types._

object Schema {
  // event_time은 " UTC" 접미사 때문에 String으로 읽고 이후 파싱(TimeUtils)
  val Raw: StructType = StructType(Seq(
    StructField("event_time",    StringType, nullable = true),
    StructField("event_type",    StringType, nullable = true),
    StructField("product_id",    LongType,   nullable = true),
    StructField("category_id",   LongType,   nullable = true),
    StructField("category_code", StringType, nullable = true),
    StructField("brand",         StringType, nullable = true),
    StructField("price",         DoubleType, nullable = true),
    StructField("user_id",       LongType,   nullable = true),
    StructField("user_session",  StringType, nullable = true)
  ))
}
```

- [ ] **Step 2: 컴파일 확인**

Run: `sbt compile`
Expected: SUCCESS.

- [ ] **Step 3: 커밋**

```bash
git add src/main/scala/com/activitylog/Schema.scala
git commit -m "feat: raw CSV 명시적 스키마"
```

---

## Task 3: TimeUtils (UTC 파싱 + KST + event_date) — TDD

**Files:**
- Create: `src/main/scala/com/activitylog/TimeUtils.scala`
- Test: `src/test/scala/com/activitylog/TimeUtilsSpec.scala`

- [ ] **Step 1: 실패 테스트 작성 (자정/월 경계 포함)**

```scala
package com.activitylog

import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite

class TimeUtilsSpec extends AnyFunSuite with SparkTestBase {
  test("UTC string parses and KST event_date crosses date at boundary") {
    import spark.implicits._
    // 2019-10-31 16:00:00 UTC == 2019-11-01 01:00:00 KST
    val df = Seq(
      ("2019-10-01 00:00:00 UTC"),
      ("2019-10-31 16:00:00 UTC")
    ).toDF("event_time")

    val out = TimeUtils.withKstColumns(df)
      .select(col("event_date"))
      .as[String].collect().toList

    assert(out == List("2019-10-01", "2019-11-01"))
  }
}
```

- [ ] **Step 2: 실패 확인**

Run: `sbt "testOnly com.activitylog.TimeUtilsSpec"`
Expected: FAIL ("not found: value TimeUtils" 또는 컴파일 에러).

- [ ] **Step 3: `TimeUtils.scala` 구현**

```scala
package com.activitylog

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object TimeUtils {
  // 전제: SparkSession의 spark.sql.session.timeZone = "UTC"
  // event_time "yyyy-MM-dd HH:mm:ss 'UTC'" → event_time_utc(instant),
  // event_time_kst(Asia/Seoul 벽시계), event_date(KST yyyy-MM-dd)
  def withKstColumns(df: DataFrame): DataFrame =
    df.withColumn("event_time_utc",
        to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss 'UTC'"))
      .withColumn("event_time_kst",
        from_utc_timestamp(col("event_time_utc"), "Asia/Seoul"))
      .withColumn("event_date",
        date_format(col("event_time_kst"), "yyyy-MM-dd"))
}
```

- [ ] **Step 4: 통과 확인**

Run: `sbt "testOnly com.activitylog.TimeUtilsSpec"`
Expected: PASS.

- [ ] **Step 5: 커밋**

```bash
git add src/main/scala/com/activitylog/TimeUtils.scala src/test/scala/com/activitylog/TimeUtilsSpec.scala
git commit -m "feat: UTC 파싱 + KST 변환 + event_date 파생 (TDD)"
```

---

## Task 4: Dedup (자연키 결정적) — TDD

**Files:**
- Create: `src/main/scala/com/activitylog/Dedup.scala`
- Test: `src/test/scala/com/activitylog/DedupSpec.scala`

- [ ] **Step 1: 실패 테스트 작성**

```scala
package com.activitylog

import org.scalatest.funsuite.AnyFunSuite

class DedupSpec extends AnyFunSuite with SparkTestBase {
  test("removes natural-key duplicates deterministically, keeps one") {
    import spark.implicits._
    // 같은 (user_id,event_time,event_type,product_id) 2건(price만 다름) → 1건
    val df = Seq(
      (1L, "2019-10-01 00:00:00 UTC", "view", 100L, 9.0),
      (1L, "2019-10-01 00:00:00 UTC", "view", 100L, 7.0),
      (1L, "2019-10-01 00:01:00 UTC", "view", 100L, 7.0)
    ).toDF("user_id", "event_time", "event_type", "product_id", "price")

    val out = Dedup.dedup(df)
    assert(out.count() == 2L)
    // 결정적 선택: price asc → 7.0 유지
    val kept = out.filter($"event_time" === "2019-10-01 00:00:00 UTC")
      .select("price").as[Double].collect().toList
    assert(kept == List(7.0))
  }
}
```

- [ ] **Step 2: 실패 확인**

Run: `sbt "testOnly com.activitylog.DedupSpec"`
Expected: FAIL.

- [ ] **Step 3: `Dedup.scala` 구현**

```scala
package com.activitylog

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Dedup {
  private val key = Seq("user_id", "event_time", "event_type", "product_id")

  def dedup(df: DataFrame): DataFrame = {
    val tiebreak = Seq("price", "brand", "category_id", "category_code", "user_session")
      .filter(df.columns.contains)
      .map(c => col(c).asc_nulls_last)
    val w = Window.partitionBy(key.map(col): _*).orderBy(tiebreak: _*)
    df.withColumn("_rn", row_number().over(w))
      .filter(col("_rn") === 1)
      .drop("_rn")
  }
}
```

- [ ] **Step 4: 통과 확인**

Run: `sbt "testOnly com.activitylog.DedupSpec"`
Expected: PASS.

- [ ] **Step 5: 커밋**

```bash
git add src/main/scala/com/activitylog/Dedup.scala src/test/scala/com/activitylog/DedupSpec.scala
git commit -m "feat: 자연키 결정적 dedup (TDD)"
```

---

## Task 5: Sessionizer (5분 갭, 결정적 id) — 핵심 TDD

**Files:**
- Create: `src/main/scala/com/activitylog/Sessionizer.scala`
- Test: `src/test/scala/com/activitylog/SessionizerSpec.scala`

- [ ] **Step 1: 경계 케이스 실패 테스트 작성**

```scala
package com.activitylog

import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite

class SessionizerSpec extends AnyFunSuite with SparkTestBase {
  import org.apache.spark.sql.functions.to_timestamp

  // helper: (user_id, "yyyy-MM-dd HH:mm:ss") rows with event_time_utc 컬럼
  private def df(rows: (Long, String)*) = {
    import spark.implicits._
    rows.toDF("user_id", "ts")
      .withColumn("event_type", org.apache.spark.sql.functions.lit("view"))
      .withColumn("product_id", org.apache.spark.sql.functions.lit(1L))
      .withColumn("event_time_utc", to_timestamp(col("ts"), "yyyy-MM-dd HH:mm:ss"))
  }

  private def sessionIds(d: org.apache.spark.sql.DataFrame): Seq[String] =
    Sessionizer.sessionize(d).orderBy("event_time_utc")
      .select("session_id").as(org.apache.spark.sql.Encoders.STRING)(spark.implicits.newStringEncoder)
      .collect().toSeq

  test("exactly 5 minutes gap starts a new session (>=300s, boundary)") {
    val d = df((1L, "2019-10-01 00:00:00"), (1L, "2019-10-01 00:05:00"))
    assert(sessionIds(d).distinct.size == 2)
  }

  test("less than 5 minutes stays same session") {
    val d = df((1L, "2019-10-01 00:00:00"), (1L, "2019-10-01 00:04:59"))
    assert(sessionIds(d).distinct.size == 1)
  }

  test("same timestamp (gap=0) stays same session") {
    val d = df((1L, "2019-10-01 00:00:00"), (1L, "2019-10-01 00:00:00"))
    assert(sessionIds(d).distinct.size == 1)
  }

  test("single event yields exactly one session") {
    val d = df((1L, "2019-10-01 00:00:00"))
    assert(sessionIds(d).size == 1)
  }

  test("session crossing midnight keeps one session id") {
    // 23:58 → 00:01 (gap 3분<5분) 같은 세션
    val d = df((1L, "2019-10-01 14:58:00"), (1L, "2019-10-01 15:01:00"))
    assert(sessionIds(d).distinct.size == 1)
  }

  test("session id is deterministic = user_id + '_' + unix(session_start)") {
    val d = df((1L, "2019-10-01 00:00:00"), (1L, "2019-10-01 00:02:00"))
    val sid = sessionIds(d).head
    val expected = "1_" + java.time.Instant.parse("2019-10-01T00:00:00Z").getEpochSecond
    assert(sid == expected)
  }
}
```

- [ ] **Step 2: 실패 확인**

Run: `sbt "testOnly com.activitylog.SessionizerSpec"`
Expected: FAIL.

- [ ] **Step 3: `Sessionizer.scala` 구현**

```scala
package com.activitylog

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Sessionizer {
  val GapThresholdSeconds: Long = 300L  // "5분 이상" = >= 300초

  def sessionize(df: DataFrame): DataFrame = {
    // 1단계: 동일초 tie 결정적 정렬 + 갭으로 새 세션 표시
    val ordered = Window.partitionBy("user_id")
      .orderBy(col("event_time_utc"), col("event_type"), col("product_id"))

    val prev   = lag(col("event_time_utc"), 1).over(ordered)
    val gap    = unix_timestamp(col("event_time_utc")) - unix_timestamp(prev)
    val isNew  = when(prev.isNull || gap >= GapThresholdSeconds, 1).otherwise(0)

    val cumulative = ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val withSeq = df
      .withColumn("_is_new", isNew)
      .withColumn("_seq", sum(col("_is_new")).over(cumulative))

    // 2단계: 세션 시작시각(min) → 결정적 id
    val bySession = Window.partitionBy(col("user_id"), col("_seq"))
    withSeq
      .withColumn("_session_start", min(col("event_time_utc")).over(bySession))
      .withColumn("session_id",
        concat_ws("_", col("user_id").cast("string"),
          unix_timestamp(col("_session_start")).cast("string")))
      .drop("_is_new", "_seq", "_session_start")
  }
}
```

- [ ] **Step 4: 통과 확인**

Run: `sbt "testOnly com.activitylog.SessionizerSpec"`
Expected: 6개 테스트 PASS.

- [ ] **Step 5: 커밋**

```bash
git add src/main/scala/com/activitylog/Sessionizer.scala src/test/scala/com/activitylog/SessionizerSpec.scala
git commit -m "feat: 5분 갭 세션화 + 결정적 session_id (핵심 TDD)"
```

---

## Task 6: PartitionWriter (검증 게이트 + staging+rename + _SUCCESS) — TDD

**Files:**
- Create: `src/main/scala/com/activitylog/PartitionWriter.scala`
- Test: `src/test/scala/com/activitylog/PartitionWriterSpec.scala`

- [ ] **Step 1: 실패 테스트 작성**

```scala
package com.activitylog

import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.funsuite.AnyFunSuite

class PartitionWriterSpec extends AnyFunSuite with SparkTestBase {
  test("validate rejects empty and null-key data") {
    import spark.implicits._
    val empty = Seq.empty[(Long, String)].toDF("user_id", "event_time_utc")
    assert(!PartitionWriter.validate(empty, 0L).ok)

    val nullKey = Seq((null.asInstanceOf[java.lang.Long], "2019-10-01"))
      .toDF("user_id", "event_time_utc")
    assert(!PartitionWriter.validate(nullKey, 1L).ok)
  }

  test("writePartition writes parquet, swaps atomically, creates _SUCCESS") {
    import spark.implicits._
    val base = java.nio.file.Files.createTempDirectory("activity").toString
    val df = Seq((1L, "2019-10-01 00:00:00")).toDF("user_id", "event_time_utc")

    PartitionWriter.writePartition(spark, df, base, "2019-10-01")

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    assert(fs.exists(new Path(s"$base/event_date=2019-10-01/_SUCCESS")))
    assert(!fs.exists(new Path(s"$base/_staging/event_date=2019-10-01")))
    val read = spark.read.parquet(s"$base/event_date=2019-10-01")
    assert(read.count() == 1L)
  }
}
```

- [ ] **Step 2: 실패 확인**

Run: `sbt "testOnly com.activitylog.PartitionWriterSpec"`
Expected: FAIL.

- [ ] **Step 3: `PartitionWriter.scala` 구현**

```scala
package com.activitylog

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

object PartitionWriter {
  final case class ValidationResult(ok: Boolean, message: String)

  /** 검증 게이트(최소 형태): 행수>0, 키 not null, 출력<=입력 */
  def validate(df: DataFrame, inputCount: Long): ValidationResult = {
    val cnt = df.count()
    if (cnt <= 0L) return ValidationResult(false, "row count is 0")
    val nullKeys = df.filter(col("user_id").isNull || col("event_time_utc").isNull).count()
    if (nullKeys > 0L) return ValidationResult(false, s"null key rows=$nullKeys")
    if (inputCount > 0L && cnt > inputCount)
      return ValidationResult(false, s"output($cnt) > input($inputCount)")
    ValidationResult(true, s"ok rows=$cnt")
  }

  /** 단일 event_date 파티션을 원자적으로 교체 후 _SUCCESS 마커 생성.
   *  df에 event_date 컬럼이 있으면 제거(파티션은 경로로 인코딩). */
  def writePartition(spark: SparkSession, df: DataFrame, baseDir: String, eventDate: String): Unit = {
    val fs       = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val staging  = new Path(s"$baseDir/_staging/event_date=$eventDate")
    val finalDir = new Path(s"$baseDir/event_date=$eventDate")

    if (fs.exists(staging)) fs.delete(staging, true) // 이전 크래시 찌꺼기 제거

    val toWrite = if (df.columns.contains("event_date")) df.drop("event_date") else df
    toWrite.write.option("compression", "snappy").mode("overwrite").parquet(staging.toString)

    if (fs.exists(finalDir)) fs.delete(finalDir, true)
    if (!fs.rename(staging, finalDir))
      throw new RuntimeException(s"atomic rename failed: $staging -> $finalDir")
    fs.create(new Path(finalDir, "_SUCCESS"), true).close()
  }
}
```

- [ ] **Step 4: 통과 확인**

Run: `sbt "testOnly com.activitylog.PartitionWriterSpec"`
Expected: PASS.

- [ ] **Step 5: 커밋**

```bash
git add src/main/scala/com/activitylog/PartitionWriter.scala src/test/scala/com/activitylog/PartitionWriterSpec.scala
git commit -m "feat: 검증 게이트 + 원자적 파티션 쓰기 + _SUCCESS (TDD)"
```

---

## Task 7: WauQueries (ISO 주, distinct) — TDD

**Files:**
- Create: `src/main/scala/com/activitylog/WauQueries.scala`
- Test: `src/test/scala/com/activitylog/WauSpec.scala`

- [ ] **Step 1: 실패 테스트 작성**

```scala
package com.activitylog

import org.scalatest.funsuite.AnyFunSuite

class WauSpec extends AnyFunSuite with SparkTestBase {
  test("WAU groups by ISO week (Monday) and counts distinct") {
    import spark.implicits._
    // 2019-10-07(월)~10-13(일) 같은 주, 10-14(월) 다음 주
    Seq(
      (1L, "s1", "2019-10-07"),
      (1L, "s1", "2019-10-08"), // 같은 user/session, 같은 주 → distinct 1
      (2L, "s2", "2019-10-13"),
      (1L, "s3", "2019-10-14")  // 다음 주
    ).toDF("user_id", "session_id", "event_date").createOrReplaceTempView("activity")

    val users = WauQueries.wauUsers(spark).as[(java.sql.Timestamp, Long)].collect()
      .map(_._2).toList
    assert(users == List(2L, 1L)) // 1주차 user 2명(1,2), 2주차 1명

    val sessions = WauQueries.wauSessions(spark).as[(java.sql.Timestamp, Long)].collect()
      .map(_._2).toList
    assert(sessions == List(2L, 1L)) // 1주차 session 2개(s1,s2), 2주차 1개(s3)
  }
}
```

- [ ] **Step 2: 실패 확인**

Run: `sbt "testOnly com.activitylog.WauSpec"`
Expected: FAIL.

- [ ] **Step 3: `WauQueries.scala` 구현**

```scala
package com.activitylog

import org.apache.spark.sql.{DataFrame, SparkSession}

object WauQueries {
  val wauUsersSql: String =
    """SELECT date_trunc('week', to_date(event_date)) AS week_start,
      |       COUNT(DISTINCT user_id) AS wau_users
      |FROM activity GROUP BY 1 ORDER BY 1""".stripMargin

  val wauSessionsSql: String =
    """SELECT date_trunc('week', to_date(event_date)) AS week_start,
      |       COUNT(DISTINCT session_id) AS wau_sessions
      |FROM activity GROUP BY 1 ORDER BY 1""".stripMargin

  def wauUsers(spark: SparkSession): DataFrame    = spark.sql(wauUsersSql)
  def wauSessions(spark: SparkSession): DataFrame = spark.sql(wauSessionsSql)
}
```

- [ ] **Step 4: 통과 확인**

Run: `sbt "testOnly com.activitylog.WauSpec"`
Expected: PASS.

- [ ] **Step 5: 커밋**

```bash
git add src/main/scala/com/activitylog/WauQueries.scala src/test/scala/com/activitylog/WauSpec.scala
git commit -m "feat: WAU(user/session) ISO 주 쿼리 (TDD)"
```

---

## Task 8: ActivityPipeline (조합) + SparkSessionFactory — TDD(통합)

**Files:**
- Create: `src/main/scala/com/activitylog/SparkSessionFactory.scala`
- Create: `src/main/scala/com/activitylog/ActivityPipeline.scala`
- Test: `src/test/scala/com/activitylog/ActivityPipelineSpec.scala`

- [ ] **Step 1: 실패 테스트 작성 (작은 CSV → 변환 결과 컬럼/세션 확인)**

```scala
package com.activitylog

import org.scalatest.funsuite.AnyFunSuite

class ActivityPipelineSpec extends AnyFunSuite with SparkTestBase {
  test("transform produces session_id, event_date, keeps original user_session, dedups") {
    import spark.implicits._
    val raw = Seq(
      ("2019-10-01 00:00:00 UTC", "view", 100L, 1L, "us-1"),
      ("2019-10-01 00:00:00 UTC", "view", 100L, 1L, "us-1"), // 중복
      ("2019-10-01 00:03:00 UTC", "view", 101L, 1L, "us-1"), // 같은 세션
      ("2019-10-01 00:10:00 UTC", "view", 102L, 1L, "us-2")  // 새 세션(>5분)
    ).toDF("event_time", "event_type", "product_id", "user_id", "user_session")

    val out = ActivityPipeline.transform(raw)
    assert(out.count() == 3L) // dedup으로 1건 제거
    assert(out.columns.contains("session_id"))
    assert(out.columns.contains("event_date"))
    assert(out.columns.contains("user_session")) // 원본 보존
    val sessions = out.select("session_id").as[String].distinct().count()
    assert(sessions == 2L)
  }
}
```

- [ ] **Step 2: 실패 확인**

Run: `sbt "testOnly com.activitylog.ActivityPipelineSpec"`
Expected: FAIL.

- [ ] **Step 3: `SparkSessionFactory.scala` 구현**

```scala
package com.activitylog

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {
  def create(appName: String): SparkSession =
    SparkSession.builder()
      .appName(appName)
      .master(sys.props.getOrElse("spark.master", "local[*]"))
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .enableHiveSupport()
      .getOrCreate()
}
```

- [ ] **Step 4: `ActivityPipeline.scala` 구현**

```scala
package com.activitylog

import org.apache.spark.sql.{DataFrame, SparkSession}

object ActivityPipeline {
  /** raw(또는 raw 스키마 DataFrame) → dedup → KST 컬럼 → 세션화 */
  def transform(raw: DataFrame): DataFrame = {
    val deduped = Dedup.dedup(raw)
    val timed   = TimeUtils.withKstColumns(deduped)
    Sessionizer.sessionize(timed)
  }

  /** CSV 경로(들)를 읽어 transform */
  def readAndTransform(spark: SparkSession, inputPaths: Seq[String]): DataFrame = {
    val raw = spark.read
      .option("header", "true")
      .schema(Schema.Raw)
      .csv(inputPaths: _*)
    transform(raw)
  }
}
```

- [ ] **Step 5: 통과 확인**

Run: `sbt "testOnly com.activitylog.ActivityPipelineSpec"`
Expected: PASS.

- [ ] **Step 6: 전체 테스트 회귀 확인 후 커밋**

Run: `sbt test`
Expected: 전 스펙 PASS.
```bash
git add src/main/scala/com/activitylog/SparkSessionFactory.scala src/main/scala/com/activitylog/ActivityPipeline.scala src/test/scala/com/activitylog/ActivityPipelineSpec.scala
git commit -m "feat: 파이프라인 조합 + SparkSession 팩토리 (통합 TDD)"
```

---

## Task 9: Main (CLI: backfill/incremental, write loop)

**Files:**
- Create: `src/main/scala/com/activitylog/Main.scala`

- [ ] **Step 1: `Main.scala` 구현**

```scala
package com.activitylog

import org.apache.spark.sql.functions.col

object Main {
  // 사용: --mode backfill|incremental --input <glob,...> --output <baseDir>
  //       [--run-date yyyy-MM-dd]  (incremental에서 대상일; lookback은 input에 전날 포함)
  private def arg(a: Array[String], k: String): Option[String] = {
    val i = a.indexOf(k); if (i >= 0 && i + 1 < a.length) Some(a(i + 1)) else None
  }

  def main(args: Array[String]): Unit = {
    val mode    = arg(args, "--mode").getOrElse("backfill")
    val inputs  = arg(args, "--input").getOrElse(sys.error("--input required")).split(",").toSeq
    val output  = arg(args, "--output").getOrElse(sys.error("--output required"))
    val runDate = arg(args, "--run-date")

    val spark = SparkSessionFactory.create(s"activity-$mode")
    try {
      val inputCount = spark.read.option("header","true").schema(Schema.Raw).csv(inputs: _*).count()
      val transformed = ActivityPipeline.readAndTransform(spark, inputs)

      // incremental: 대상일만 write (lookback 행은 문맥). backfill: 전체 event_date write.
      val toWrite = (mode, runDate) match {
        case ("incremental", Some(d)) => transformed.filter(col("event_date") === d)
        case _                        => transformed
      }

      val dates = toWrite.select("event_date").distinct()
        .as(org.apache.spark.sql.Encoders.STRING)(spark.implicits.newStringEncoder)
        .collect().toSeq.sorted

      dates.foreach { d =>
        val part = toWrite.filter(col("event_date") === d)
        val v = PartitionWriter.validate(part, inputCount)
        if (!v.ok) throw new RuntimeException(s"validation failed for $d: ${v.message}")
        PartitionWriter.writePartition(spark, part, output, d)
        println(s"[OK] wrote partition event_date=$d (${v.message})")
      }
      println(s"[DONE] mode=$mode partitions=${dates.size}")
    } finally {
      spark.stop()
    }
  }
}
```

- [ ] **Step 2: 컴파일 + 패키징 확인**

Run: `sbt package`
Expected: `target/scala-2.13/activity-log_2.13-0.1.0.jar` 생성.

- [ ] **Step 3: 커밋**

```bash
git add src/main/scala/com/activitylog/Main.scala
git commit -m "feat: CLI 엔트리(backfill/incremental) + 파티션 쓰기 루프"
```

---

## Task 10: External Table DDL

**Files:**
- Create: `sql/create_external_table.sql`

- [ ] **Step 1: DDL 작성**

```sql
-- 사용: spark-sql -f sql/create_external_table.sql 또는 spark.sql 로 실행
-- {{OUTPUT_DIR}} 를 실제 절대경로로 치환 (예: /Users/.../activity)
CREATE EXTERNAL TABLE IF NOT EXISTS activity (
  event_time      string,
  event_type      string,
  product_id      bigint,
  category_id     bigint,
  category_code   string,
  brand           string,
  price           double,
  user_id         bigint,
  user_session    string,
  event_time_utc  timestamp,
  event_time_kst  timestamp,
  session_id      string
)
PARTITIONED BY (event_date string)
STORED AS PARQUET
LOCATION '{{OUTPUT_DIR}}';

MSCK REPAIR TABLE activity;
```

- [ ] **Step 2: 커밋**

```bash
git add sql/create_external_table.sql
git commit -m "feat: Hive External Table DDL"
```

---

## Task 11: 샘플 End-to-End (Day 1 목표)

**Files:** (없음 — 실행/검증)

- [ ] **Step 1: 샘플 데이터 추출 (커밋 안 함)**

Run:
```bash
mkdir -p data/sample
head -200000 data/2019-Oct.csv > data/sample/oct_sample.csv
wc -l data/sample/oct_sample.csv
```
Expected: 약 20만 행.

- [ ] **Step 2: 샘플 backfill 실행**

Run:
```bash
sbt "runMain com.activitylog.Main --mode backfill --input data/sample/oct_sample.csv --output $(pwd)/output/activity"
```
Expected: `[OK] wrote partition ...` 여러 줄 + `[DONE]`. `output/activity/event_date=2019-10-01/_SUCCESS` 존재.

- [ ] **Step 3: 외부 테이블 등록 + WAU 확인 (spark-shell)**

Run:
```bash
OUT=$(pwd)/output/activity
spark-shell --conf spark.sql.session.timeZone=UTC <<EOF
spark.sql(s"""CREATE EXTERNAL TABLE IF NOT EXISTS activity (
  event_time string, event_type string, product_id bigint, category_id bigint,
  category_code string, brand string, price double, user_id bigint, user_session string,
  event_time_utc timestamp, event_time_kst timestamp, session_id string)
 PARTITIONED BY (event_date string) STORED AS PARQUET LOCATION '$OUT'""")
spark.sql("MSCK REPAIR TABLE activity")
spark.sql("SELECT date_trunc('week', to_date(event_date)) w, COUNT(DISTINCT user_id) FROM activity GROUP BY 1 ORDER BY 1").show(false)
spark.sql("SELECT date_trunc('week', to_date(event_date)) w, COUNT(DISTINCT session_id) FROM activity GROUP BY 1 ORDER BY 1").show(false)
:quit
EOF
```
Expected: 주별 WAU 2종 출력(샘플 기준 숫자). **end-to-end 1회 성공 확인 = Day 1 목표 달성.**

- [ ] **Step 4: 체크포인트 커밋(코드 변경 없으면 생략 가능)**

```bash
git add -A && git commit -m "chore: 샘플 end-to-end 검증" || echo "변경 없음"
```

---

## Task 12: 전체 Backfill + WAU 실측 (최종 1회)

**Files:** (없음 — 실행/검증)

- [ ] **Step 1: thin jar 빌드**

Run: `sbt package`
Expected: jar 생성.

- [ ] **Step 2: 전체 backfill (spark-submit, 메모리 상향)**

Run:
```bash
spark-submit \
  --class com.activitylog.Main \
  --master "local[*]" \
  --driver-memory 8g \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.driver.extraJavaOptions="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
  target/scala-2.13/activity-log_2.13-0.1.0.jar \
  --mode backfill \
  --input "$(pwd)/data/2019-Oct.csv,$(pwd)/data/2019-Nov.csv" \
  --output "$(pwd)/output/activity"
```
Expected: 전체 일자 파티션 생성 + `[DONE]`. (시간 소요 큼 — 1회만)

- [ ] **Step 3: WAU 2종 실측 (실제 숫자 기록)**

Run: Task 11 Step 3과 동일한 spark-shell 블록을 전체 `output/activity`에 대해 실행.
Expected: **user_id 기준 / session_id 기준 주별 WAU 실제 값**. 이 출력 텍스트와 사용 쿼리를 README/제출물에 그대로 기록(AI 주장 금지, 실행 결과만).

- [ ] **Step 4: 결과 캡처 저장**

Run:
```bash
mkdir -p results
# 위 show(false) 출력을 results/wau_users.txt, results/wau_sessions.txt 로 붙여넣기 저장
git add results/ && git commit -m "docs: 전체 WAU 실측 결과 + 쿼리"
```

---

## Task 13: (선택) Airflow DAG — 회사 스택 어필

**Files:**
- Create: `airflow/dags/activity_daily.py`

- [ ] **Step 1: DAG 작성 (BashOperator + catchup)**

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

PROJECT = "/ABSOLUTE/PATH/ecommerce-user-activity-log"  # 실제 절대경로로 치환
JAR = f"{PROJECT}/target/scala-2.13/activity-log_2.13-0.1.0.jar"
OUT = f"{PROJECT}/output/activity"

default_args = {"retries": 2, "retry_delay": timedelta(minutes=5), "depends_on_past": False}

with DAG(
    dag_id="activity_daily",
    start_date=datetime(2019, 10, 1),
    schedule="@daily",
    catchup=True,                  # catchup이 곧 backfill, 이후 매일 1 run
    max_active_runs=1,
    default_args=default_args,
    tags=["activity", "sessionization"],
) as dag:
    # 대상일 + 전날(lookback)을 input으로. {{ ds }}=대상일, {{ macros.ds_add(ds,-1) }}=전날
    run_job = BashOperator(
        task_id="spark_submit",
        bash_command=(
            f"spark-submit --class com.activitylog.Main --master 'local[*]' "
            f"--driver-memory 4g --conf spark.sql.session.timeZone=UTC {JAR} "
            f"--mode incremental --run-date {{{{ ds }}}} "
            f"--input {PROJECT}/data/{{{{ ds }}}}.csv,{PROJECT}/data/{{{{ macros.ds_add(ds,-1) }}}}.csv "
            f"--output {OUT}"
        ),
    )
    check_success = FileSensor(
        task_id="check_success",
        filepath=f"{OUT}/event_date={{{{ ds }}}}/_SUCCESS",
        mode="reschedule", poke_interval=30, timeout=600,
    )
    run_job >> check_success
```

- [ ] **Step 2: 커밋**

```bash
git add airflow/dags/activity_daily.py
git commit -m "feat(optional): Airflow daily DAG (catchup=backfill)"
```

> 주: 본 DAG는 일자별 입력 파일이 `data/yyyy-MM-dd.csv` 형태로 존재하는 프로덕션 가정. 과제 데이터(월 단위 CSV)에선 Task 12의 backfill로 충분하며, DAG는 "확장 가능성"을 보여주는 산출물.

---

## Task 14: README (AI 도구 활용 + 언어 사유 + 실행법 + WAU 결과)

**Files:**
- Create: `README.md`

- [ ] **Step 1: README 작성**

다음 섹션을 포함:
- **개요/아키텍처**: 데이터 흐름(설계 스펙 §2 요약).
- **실행법**: `sbt test`, 샘플 실행, 전체 backfill spark-submit, 외부 테이블 등록, WAU 쿼리.
- **WAU 결과**: Task 12에서 실측한 user_id/session_id 기준 주별 수치 + 사용 쿼리(그대로).
- **언어 선택(Scala) 사유**: Spark 네이티브, 타입 안전 Dataset, 윈도우/UDF 표현력, 관용성(설계 스펙 §6).
- **설계 결정 요약**: 설계 스펙 §7 결정 로그 표 인용(세션화 방식 A, 멱등 복구, ISO 주 등).
- **AI 도구 활용**:
  - *AI로 한 것*: 보일러플레이트 스캐폴딩(빌드 설정/테스트 골격/CLI 파싱), 설계 문서 초안화, 데이터 디자인 패턴 책 교차참조.
  - *직접 설계·검증한 것*: 숨은 결정 6+개 확정(세션 정의/타임존/주 경계/dedup/복구/자정 경계), 세션화 로직 TDD, **WAU 수치는 실제 Spark 실행 결과로만 검증**, 원본 user_session vs 생성 session_id 비교 검증.
  - *프롬프트 전략*: brainstorming으로 숨은 결정 선질문 → 설계 스펙 승인 → TDD로 핵심 로직 → 샘플 우선 end-to-end → 전체 1회 실측.
- **확장(프로덕션)**: Iceberg/Delta(ACID·타임트래블), Stateful Sessionizer(state store), 풀 dead-letter quarantine, SparkSubmitOperator.

- [ ] **Step 2: 커밋**

```bash
git add README.md
git commit -m "docs: README (AI 도구 활용/언어 사유/WAU 결과/실행법)"
```

---

## 자체 검토 (작성자 체크)

**스펙 커버리지** (설계 스펙 → 태스크):
- Hive table 제공(Spark App) → Task 8,9,10 ✅
- KST 일별 파티션 → Task 3(event_date), Task 6(쓰기) ✅
- 5분 갭 세션 분할/새 session_id → Task 5 ✅
- 재처리 후 parquet+snappy → Task 6 ✅
- External Table + 추가 기간 대응 → Task 10(DDL/MSCK), Task 9(--mode/--run-date) ✅
- 배치 장애 복구 → Task 6(staging+rename+검증게이트+_SUCCESS), Task 13(retry/알람은 DAG/README) ✅
- WAU user_id/session_id + 쿼리 → Task 7, Task 12(실측) ✅
- 언어 사유 → Task 14 ✅
- dedup(자연키) → Task 4 ✅
- 타임존 UTC→KST 1회 → Task 3 ✅

**플레이스홀더 스캔:** 코드 단계는 모두 실제 코드 포함. `{{OUTPUT_DIR}}`/`PROJECT` 절대경로/`results/*.txt` 붙여넣기는 환경 의존값으로 의도적 치환 지점(주석 명시).

**타입 일관성:** 컬럼명(`event_time_utc`, `event_time_kst`, `event_date`, `session_id`, `user_session`)이 Schema→TimeUtils→Sessionizer→PartitionWriter→DDL 전 구간 일치. `sessionize/transform/writePartition/validate` 시그니처가 호출부(Main/Pipeline)와 일치.

**알려진 리스크:**
- Spark 4.1.2/Scala 2.13 Maven 아티팩트 미존재 시 → Task 0에서 확인한 정확 버전으로 `build.sbt` 조정(필요시 4.0.x).
- JDK17 add-opens 누락 시 Spark 런타임 에러 → build.sbt Test/run javaOptions + spark-submit extraJavaOptions에 반영됨.
- Spark 4 ANSI 모드 기본 ON: 본 코드의 캐스트는 안전 범위.
