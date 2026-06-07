package com.activitylog

import org.apache.spark.sql.{DataFrame, SparkSession}

// object: Scala 싱글턴 객체 — 인스턴스 생성 없이 ActivityPipeline.transform(df)로 바로 호출.
//   Python의 모듈 수준 함수와 동일한 개념.
object ActivityPipeline {

  /** raw(또는 raw 스키마 DataFrame) → dedup → KST 컬럼 → 세션화
   *
   *  처리 순서:
   *  1. Dedup.dedup(raw):         자연키 중복 제거 (설계 결정 5: 세션화 전에 수행)
   *  2. TimeUtils.withKstColumns: event_time_utc, event_time_kst, event_date 컬럼 추가
   *  3. Sessionizer.sessionize:   5분 갭 기준 세션화 → session_id 생성 (설계 결정 1, 2)
   *
   *  Python 예시로 풀면:
   *    df = dedup(raw)
   *    df = with_kst_columns(df)
   *    df = sessionize(df)
   *    return df
   */
  def transform(raw: DataFrame): DataFrame = {
    // 중복 제거 → KST 컬럼 추가 → 세션화의 순서로 함수를 합성(function composition).
    // Scala에서는 각 함수가 DataFrame을 받아 DataFrame을 반환하므로 체이닝이 가능하다.
    val deduped = Dedup.dedup(raw)           // 1단계: 자연키 기준 dedup
    val timed   = TimeUtils.withKstColumns(deduped)  // 2단계: UTC → KST 변환 컬럼 추가
    Sessionizer.sessionize(timed)            // 3단계: 5분 갭 세션화 + session_id 생성
  }

  /** CSV 경로(들)를 읽어 transform 수행
   *
   *  @param spark      사용할 SparkSession (테스트에서는 SparkTestBase의 세션, 실행 시에는 SparkSessionFactory.create)
   *  @param inputPaths CSV 파일 경로 목록 (여러 달치 CSV를 한 번에 처리 가능)
   *
   *  Python 예시:
   *    raw = spark.read.option("header", True).schema(Schema.Raw).csv(*inputPaths)
   *    return transform(raw)
   */
  def readAndTransform(spark: SparkSession, inputPaths: Seq[String]): DataFrame = {
    val raw = spark.read
      // header: CSV 첫 행을 컬럼명으로 사용(True). Python의 pd.read_csv(header=0)과 유사.
      .option("header", "true")
      // schema(Schema.Raw): CSV 읽기 시 스키마 추론(inferSchema)을 끄고 명시적 스키마를 강제.
      //   스키마 추론은 느리고 타입이 틀릴 수 있어 프로덕션에서는 명시적 스키마가 권장됨.
      .schema(Schema.Raw)
      // csv(inputPaths: _*): Seq[String]을 가변인자(varargs)로 펼쳐서 전달.
      //   ": _*" 는 "이 Seq의 모든 원소를 varargs 인자로 풀어라"는 Scala 문법.
      //   Python의 func(*list) 언패킹과 완전히 동일한 개념.
      //   여러 경로를 넘기면 Spark가 파일들을 하나의 DataFrame으로 합쳐서 읽는다.
      .csv(inputPaths: _*)
    // 읽은 raw DataFrame을 transform 파이프라인에 통과시켜 최종 결과 반환.
    transform(raw)
  }
}
