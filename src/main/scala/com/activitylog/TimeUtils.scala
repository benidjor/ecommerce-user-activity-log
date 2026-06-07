package com.activitylog

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

// object: Scala의 싱글톤 객체 선언 — Python의 @staticmethod만 모아둔 클래스와 유사.
// 인스턴스화 없이 TimeUtils.withKstColumns(df) 형태로 바로 호출 가능.
object TimeUtils {
  // 전제: SparkSession의 spark.sql.session.timeZone = "UTC"
  // event_time "yyyy-MM-dd HH:mm:ss 'UTC'" → event_time_utc(instant),
  // event_time_kst(Asia/Seoul 벽시계), event_date(KST yyyy-MM-dd)

  // def withKstColumns(df: DataFrame): DataFrame
  //   Python의 def with_kst_columns(df: DataFrame) -> DataFrame: 과 동일한 함수 선언.
  //   Scala는 마지막 표현식이 반환값이므로 return 키워드 생략.
  def withKstColumns(df: DataFrame): DataFrame =
    // withColumn: 기존 DataFrame에 컬럼을 추가/교체해 새 DataFrame을 반환.
    //   Python의 df.withColumn("...", expr) 과 동일. 체이닝이 가능함.
    df.withColumn("event_time_utc",
        // to_timestamp: 문자열 → TimestampType 으로 파싱.
        // 포맷 문자열의 'UTC' (작은따옴표) 는 리터럴 텍스트를 의미 — 실제 " UTC" 접미사를 그대로 매칭.
        to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss 'UTC'"))
      .withColumn("event_time_kst",
        // from_utc_timestamp: UTC 기준 Timestamp 컬럼을 지정 타임존의 벽시계(wall-clock)로 변환.
        // Python의 df["event_time_utc"].dt.tz_localize("UTC").dt.tz_convert("Asia/Seoul") 에 해당.
        from_utc_timestamp(col("event_time_utc"), "Asia/Seoul"))
      .withColumn("event_date",
        // date_format: Timestamp → 지정 포맷 문자열로 변환.
        // KST 벽시계 기준으로 yyyy-MM-dd를 추출하므로 자정 경계가 한국 시각 기준으로 결정됨.
        date_format(col("event_time_kst"), "yyyy-MM-dd"))
}
