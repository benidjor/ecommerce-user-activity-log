package com.activitylog

import org.scalatest.funsuite.AnyFunSuite

// AnyFunSuite: ScalaTest 함수형 테스트 스타일(test("이름") { ... } 구문 사용).
// with SparkTestBase: 믹스인(mixin) — 공용 SparkSession 생명주기를 물려받는다.
class WauSpec extends AnyFunSuite with SparkTestBase {

  test("WAU groups by ISO week (Monday) and counts distinct") {
    // spark는 @transient var이라 클로저에서 직접 참조하면 직렬화 문제가 생길 수 있다.
    // val ss = spark : 로컬 val에 먼저 담아 클로저 캡처를 안전하게 한다.
    val ss = spark
    // import ss.implicits._ : toDF, as[T] 등 암묵적 변환(implicit conversion) 활성화.
    //   Python의 `from pyspark.sql import functions as F` 같은 역할.
    import ss.implicits._

    // 2019-10-07(월)~10-13(일) 같은 주, 10-14(월) 다음 주
    // Seq[(Long, String, String)].toDF(...) : 인라인 데이터를 DataFrame으로 변환.
    //   Python의 spark.createDataFrame([(1, "s1", "2019-10-07")], schema)와 동일.
    Seq(
      (1L, "s1", "2019-10-07"),
      (1L, "s1", "2019-10-08"), // 같은 user/session, 같은 주 → distinct 1
      (2L, "s2", "2019-10-13"),
      (1L, "s3", "2019-10-14")  // 다음 주
    ).toDF("user_id", "session_id", "event_date")
      // createOrReplaceTempView: DataFrame을 SQL로 쿼리할 수 있는 임시 뷰로 등록.
      //   Python의 df.createOrReplaceTempView("activity")와 동일.
      .createOrReplaceTempView("activity")

    // WauQueries.wauUsers(spark) : spark.sql(...)을 호출해 DataFrame 반환.
    // .as[(java.sql.Timestamp, Long)] : DataFrame → Dataset[(Timestamp, Long)] 타입 변환.
    //   SQL 결과의 (week_start, wau_users) 컬럼을 Scala 튜플 타입으로 매핑.
    // .collect() : 모든 행을 드라이버 메모리로 수집(Python의 .collect()와 동일).
    // .map(_._2) : 튜플의 두 번째 요소(wau_users)만 추출. Python의 [r[1] for r in ...]와 유사.
    // .toList : Array → List 변환
    val users = WauQueries.wauUsers(spark).as[(java.sql.Timestamp, Long)].collect()
      .map(_._2).toList
    // 1주차(10-07~10-13): user_id 1, 2 → distinct 2명
    // 2주차(10-14~): user_id 1 → distinct 1명
    assert(users == List(2L, 1L)) // 1주차 user 2명(1,2), 2주차 1명

    val sessions = WauQueries.wauSessions(spark).as[(java.sql.Timestamp, Long)].collect()
      .map(_._2).toList
    // 1주차(10-07~10-13): session_id s1, s2 → distinct 2개
    // 2주차(10-14~): session_id s3 → distinct 1개
    assert(sessions == List(2L, 1L)) // 1주차 session 2개(s1,s2), 2주차 1개(s3)
  }
}
