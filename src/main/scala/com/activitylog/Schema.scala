package com.activitylog

import org.apache.spark.sql.types._   // StructType/StructField/StringType 등 스키마 타입 모음

// object: 싱글톤(인스턴스 없이 Schema.Raw 로 접근). 상수 모음 용도.
object Schema {
  // val: 불변값(재할당 불가, Python의 상수 관례를 언어가 강제).
  // StructType(Seq(...)): 컬럼 정의 목록으로 만든 명시적 스키마.
  //   Seq는 Scala의 순서 컬렉션(Python list와 유사). schema inference를 끄고 이 스키마를 강제한다.
  // StructField(이름, 타입, nullable): 컬럼 한 개의 정의.
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
