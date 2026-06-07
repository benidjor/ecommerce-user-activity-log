package com.activitylog

import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite

// AnyFunSuite: ScalaTest의 함수 스타일 테스트 클래스 (Python의 unittest.TestCase와 유사)
// with SparkTestBase: 믹스인(mixin) — Python의 다중 상속과 비슷하게 SparkSession을 주입받음
class TimeUtilsSpec extends AnyFunSuite with SparkTestBase {
  test("UTC string parses and KST event_date crosses date at boundary") {
    // val ss = spark 로 별도 변수에 할당하는 이유:
    //   SparkTestBase의 spark 필드가 @transient var 이므로 안정적 식별자(stable identifier)가 아님.
    //   Scala의 import는 안정적 식별자에만 가능하기 때문에 임시 val에 담아야 컴파일됨.
    val ss = spark
    import ss.implicits._  // Seq(...).toDF() 등 DataFrame 변환 헬퍼를 불러옴 (Python의 from pyspark.sql import functions 과 유사)

    // 2019-10-31 16:00:00 UTC == 2019-11-01 01:00:00 KST (UTC+9)
    // 자정 경계 테스트: 한국 시간 기준 날짜가 11-01로 넘어가는지 확인
    val df = Seq(
      ("2019-10-01 00:00:00 UTC"),
      ("2019-10-31 16:00:00 UTC")
    ).toDF("event_time")  // Seq[String]을 단일 컬럼 DataFrame으로 변환

    val out = TimeUtils.withKstColumns(df)
      .select(col("event_date"))  // col("event_date"): 컬럼 참조 (Python의 df["event_date"]와 동일)
      .as[String].collect().toList  // as[String]: Dataset[String]으로 형변환, collect: 전체 로컬 배열로 수집

    // List: Scala의 불변 순서 리스트 (Python의 list와 동일)
    assert(out == List("2019-10-01", "2019-11-01"))
  }
}
