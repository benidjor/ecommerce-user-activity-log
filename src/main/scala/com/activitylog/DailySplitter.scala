package com.activitylog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

// DailySplitter: 월 CSV를 읽어 KST event_date 기준으로 partitionBy 하여
//   data/daily/event_date=YYYY-MM-DD/ 로 분할 저장한다(Bronze 일별 랜딩, 스펙 §3.1).
//   Airflow 일별 run이 자기 날짜 폴더(event_date={{ds}})만 읽도록 사전 분할하는 용도.
object DailySplitter {

  // arg: CLI 인자 배열에서 키 다음 값을 Option으로 꺼낸다(Main.scala와 동일 관용구).
  private def arg(a: Array[String], k: String): Option[String] = {
    val i = a.indexOf(k)
    if (i >= 0 && i + 1 < a.length) Some(a(i + 1)) else None
  }

  // split: raw CSV → KST event_date 파생 → 원본 9개 컬럼 + event_date 선택 → event_date로 partitionBy 기록.
  //   파생 KST 컬럼(event_time_utc/kst)은 저장하지 않는다 — Bronze는 원본에 가깝게 두고,
  //   Silver(Main)가 Schema.Raw로 다시 읽어 자체적으로 KST를 재파생한다.
  def split(spark: SparkSession, inputs: Seq[String], output: String): Unit = {
    // Schema.Raw로 명시 스키마 강제(타입 추론 끔, Main과 동일).
    val raw = spark.read.option("header", "true").schema(Schema.Raw).csv(inputs: _*)
    // 원본 컬럼명 목록(9개) + event_date를 선택. fieldNames는 Schema.Raw 정의 순서.
    val originalCols = Schema.Raw.fieldNames.toSeq.map(col)
    val withDate = TimeUtils.withKstColumns(raw).select((originalCols :+ col("event_date")): _*)
    // partitionBy("event_date"): 경로에 event_date=YYYY-MM-DD/로 인코딩(파일엔 9개 컬럼만 남음).
    //   mode("overwrite"): 같은 출력 경로 재실행 시 깨끗이 덮어씀(데모 준비 멱등).
    withDate.write
      .partitionBy("event_date")
      .option("header", "true")
      .mode("overwrite")
      .csv(output)
  }

  // main: spark-submit 진입점.
  //   사용: --class com.activitylog.DailySplitter --input <월CSV,...> --output data/daily
  def main(args: Array[String]): Unit = {
    val inputs = arg(args, "--input").getOrElse(sys.error("--input required")).split(",").toSeq
    val output = arg(args, "--output").getOrElse(sys.error("--output required"))
    val spark  = SparkSessionFactory.create("daily-splitter")
    try split(spark, inputs, output) finally spark.stop()
  }
}
