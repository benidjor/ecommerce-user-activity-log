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
