package com.activitylog

import org.scalatest.funsuite.AnyFunSuite

// 툴체인 검증용 최소 테스트: sbt+Spark+JDK17(add-opens)이 실제로 도는지만 확인.
class ToolchainSpec extends AnyFunSuite with SparkTestBase {
  test("spark session runs a trivial job on JDK17") {
    // SparkTestBase의 spark는 @transient var(불안정 식별자)라 import 불가 →
    //   val로 한 번 담아야 import ss.implicits._ 가 컴파일됨.
    val ss = spark
    import ss.implicits._               // Seq(...).toDF() 같은 변환 헬퍼를 활성화
    val df = Seq(1, 2, 3).toDF("n")     // 정수 3개를 컬럼 "n"의 DataFrame으로
    // agg(sum("n")): 합계 집계 → first()로 첫 행 → getLong(0)으로 첫 컬럼을 Long으로 읽음.
    // 6L 의 L 접미사는 Long 리터럴(Python엔 없는 표기). 1+2+3 == 6 확인.
    assert(df.agg(org.apache.spark.sql.functions.sum("n")).first().getLong(0) == 6L)
  }
}
