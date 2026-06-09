package com.activitylog

import org.scalatest.funsuite.AnyFunSuite

// DailySplitterSpec: UTC 자정 경계 행이 올바른 KST event_date 폴더로 분할되는지 검증.
//   TimeUtilsSpec와 같은 경계 케이스(2019-10-31 16:00 UTC = 2019-11-01 KST)를 파일 분할 레벨에서 확인.
class DailySplitterSpec extends AnyFunSuite with SparkTestBase {
  test("rows are partitioned into KST event_date folders at the UTC midnight boundary") {
    val ss = spark
    import ss.implicits._

    // 임시 입력/출력 디렉터리(테스트 종료 후 OS가 정리)
    val tmp    = java.nio.file.Files.createTempDirectory("dsplit").toString
    val inDir  = s"$tmp/in"
    val outDir = s"$tmp/out"

    // Schema.Raw 9개 컬럼 CSV(헤더 포함) 작성.
    // 2019-10-01 00:00:00 UTC → KST 2019-10-01, 2019-10-31 16:00:00 UTC → KST 2019-11-01
    val rows = Seq(
      ("2019-10-01 00:00:00 UTC", "view", 1L, 10L, "c", "b", 1.0, 100L, "s1"),
      ("2019-10-31 16:00:00 UTC", "view", 2L, 20L, "c", "b", 2.0, 200L, "s2")
    ).toDF("event_time", "event_type", "product_id", "category_id",
           "category_code", "brand", "price", "user_id", "user_session")
    rows.write.option("header", "true").mode("overwrite").csv(inDir)

    DailySplitter.split(ss, Seq(inDir), outDir)

    // 출력 디렉터리가 실제로 생성됐는지 먼저 단언(없으면 listFiles가 null → NPE로 원인 불명확해지는 것 방지)
    val outFile = new java.io.File(outDir)
    assert(outFile.exists(), s"출력 디렉터리가 생성되지 않음: $outDir")

    // 출력 파티션 폴더명만 추려 정렬(event_date=... 디렉터리)
    val parts = outFile.listFiles()
      .filter(_.isDirectory).map(_.getName)
      .filter(_.startsWith("event_date=")).sorted.toList

    assert(parts == List("event_date=2019-10-01", "event_date=2019-11-01"))
  }
}
