package com.activitylog

import org.scalatest.funsuite.AnyFunSuite

// AnyFunSuite: ScalaTest의 기본 테스트 클래스(Python의 unittest.TestCase와 유사).
// with SparkTestBase: 믹스인(mixin) — SparkSession 공유, beforeAll/afterAll 훅 포함.
class ActivityPipelineSpec extends AnyFunSuite with SparkTestBase {

  test("transform produces session_id, event_date, keeps original user_session, dedups") {
    // spark는 SparkTestBase에 @transient var로 선언된 SparkSession.
    // val ss = spark: 로컬 val에 담아야 클로저 직렬화 시 안전.
    //   (@transient var는 직렬화 제외 대상이라 람다/toDF 내부에서 직접 참조하면 오류 위험)
    val ss = spark
    // import ss.implicits._: Seq(...).toDF() 같은 DataFrame 변환 함수를 사용하려면 필수.
    //   Python에서는 spark.createDataFrame(...)를 직접 호출하지만,
    //   Scala에서는 이 import 이후에 Seq.toDF()가 암시적으로 활성화된다.
    import ss.implicits._

    // 테스트용 인라인 데이터: (event_time, event_type, product_id, user_id, user_session) 튜플.
    // Seq(...).toDF("col1", ...): Python의 spark.createDataFrame([...], schema) 와 동일.
    val raw = Seq(
      ("2019-10-01 00:00:00 UTC", "view", 100L, 1L, "us-1"),
      ("2019-10-01 00:00:00 UTC", "view", 100L, 1L, "us-1"), // 중복 — dedup으로 제거 대상
      ("2019-10-01 00:03:00 UTC", "view", 101L, 1L, "us-1"), // 같은 세션(3분 이내)
      ("2019-10-01 00:10:00 UTC", "view", 102L, 1L, "us-2")  // 새 세션(>5분 간격)
    ).toDF("event_time", "event_type", "product_id", "user_id", "user_session")

    // ActivityPipeline.transform: dedup → KST 컬럼 추가 → 세션화 의 조합.
    val out = ActivityPipeline.transform(raw)

    // 중복 1건 제거 → 4건에서 3건 남아야 함
    assert(out.count() == 3L)                              // dedup으로 1건 제거

    // 세션화 결과: session_id 컬럼 존재 여부 확인
    assert(out.columns.contains("session_id"))

    // KST 변환 결과: event_date 컬럼 존재 여부 확인
    assert(out.columns.contains("event_date"))

    // 설계 결정 1: 원본 user_session은 검증용으로 반드시 보존
    assert(out.columns.contains("user_session"))

    // 5분 간격 기준 세션 분리: user_id=1의 이벤트 3건 → 세션 1개,
    //                          user_id=1의 마지막 이벤트(10분 이후) → 세션 2개? 아니, user_id=1이 2개, user_id=1과 us-2...
    // 실제: user_id=1에서 00:00과 00:03은 같은 세션(1번), 00:10은 새 세션(2번) — 총 세션 2개
    val sessions = out.select("session_id").as[String].distinct().count()
    assert(sessions == 2L)
  }
}
