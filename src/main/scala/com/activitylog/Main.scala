package com.activitylog

import org.apache.spark.sql.functions.col

// object Main: Scala에서 "object"는 싱글턴(한 번만 생성되는 인스턴스).
//   def main(args: Array[String]): Unit 이 있으면 JVM 진입점(Python의 if __name__ == "__main__": 과 동일).
//   spark-submit으로 실행 시 이 main이 호출된다.
object Main {

  // 사용: --mode backfill|incremental --input <glob,...> --output <baseDir>
  //       [--run-date yyyy-MM-dd]  (incremental에서 대상일; lookback은 input에 전날 포함)

  // Array[String]: Scala에서 JVM 배열 타입. 여기서는 CLI 인자 배열(Python의 sys.argv와 유사).
  //   Array는 고정 크기, Seq는 가변 크기 컬렉션 — 내부 로직에서는 Seq로 변환해서 사용.
  // Option[String]: 값이 있을 수도(Some("값")), 없을 수도(None) 있는 컨테이너.
  //   Python의 Optional[str]과 개념상 동일하며, None 체크를 타입으로 강제하는 Scala 관용구.
  private def arg(a: Array[String], k: String): Option[String] = {
    // a.indexOf(k): 배열에서 키(예: "--mode")의 위치(인덱스)를 반환. 없으면 -1.
    //   Python의 list.index(k)와 비슷하나, 없을 때 예외 대신 -1을 반환한다.
    val i = a.indexOf(k)
    // i >= 0: 키를 찾았고, i + 1 < a.length: 키 다음에 값이 실제로 존재하는지 확인.
    // Some(a(i + 1)): 값이 있으면 Some으로 감싸서 반환(Python: return a[i+1]).
    // else None: 키가 없거나 값이 없으면 None 반환(Python: return None).
    if (i >= 0 && i + 1 < a.length) Some(a(i + 1)) else None
  }

  // main: JVM 진입점. args는 CLI 인자 배열(예: ["--mode", "backfill", "--input", "data/*.csv", ...]).
  //   Unit: Scala에서 반환값이 없음을 나타냄(Python의 None / void와 동일).
  def main(args: Array[String]): Unit = {
    // arg(...).getOrElse("backfill"): Option이 Some이면 내부 값, None이면 기본값 반환.
    //   Python: arg(args, "--mode") or "backfill" 와 동일한 동작.
    val mode    = arg(args, "--mode").getOrElse("backfill")

    // sys.error("메시지"): JVM을 즉시 RuntimeException으로 종료(Python의 raise RuntimeError("...")).
    //   getOrElse(sys.error(...)): --input이 없으면 에러를 던지는 "필수 인자 검증" 패턴.
    val inputs  = arg(args, "--input").getOrElse(sys.error("--input required"))
      // .split(","): 쉼표로 구분된 경로 문자열을 배열로 나눔(Python의 str.split(",")).
      // .toSeq: Array를 Scala의 불변 시퀀스(Seq)로 변환 — readAndTransform 인자 타입이 Seq[String]이므로.
      .split(",").toSeq
    val output  = arg(args, "--output").getOrElse(sys.error("--output required"))
    // runDate: Option[String] — incremental 모드에서만 사용하는 선택적 인자.
    //   값이 없으면 None(backfill 모드에서는 무시됨).
    val runDate = arg(args, "--run-date")

    // SparkSessionFactory.create: 로컬 SparkSession 생성(설계 결정 8: local 모드).
    //   s"activity-$mode": Scala 문자열 보간(Python의 f"activity-{mode}"와 동일).
    val spark = SparkSessionFactory.create(s"activity-$mode")

    // import spark.implicits._: SparkSession의 암묵적 변환(implicit conversion) 임포트.
    //   이 import가 있어야 DataFrame → Dataset[String] 변환(.as[String])이 컴파일된다.
    //   spark가 일반 val(로컬 참조)이므로 안전하게 임포트 가능.
    //   (테스트에서 "@transient var spark"를 "val ss = spark"로 로컬화하는 것과 같은 이유)
    import spark.implicits._

    // try { } finally { }: 예외가 발생해도 finally 블록은 반드시 실행됨.
    //   Python의 try/finally와 완전히 동일한 구조.
    //   여기서는 파이프라인이 실패해도 spark.stop()으로 세션을 정상 종료하기 위해 사용.
    try {
      // 입력 CSV를 먼저 raw 스키마로 읽어 전체 행 수를 계산 → validate의 기준값(inputCount).
      //   Schema.Raw: 명시적 스키마(타입 추론 없음, 설계 결정 5 보조).
      //   .count(): Action — 실제로 CSV를 읽어 행 수를 반환(lazy evaluation 종료 시점).
      val inputCount = spark.read.option("header","true").schema(Schema.Raw).csv(inputs: _*).count()

      // ActivityPipeline.readAndTransform: CSV 읽기 → dedup → KST 변환 → 세션화 전 과정.
      //   결과 DataFrame에는 event_date(KST 날짜), session_id 등이 포함됨.
      val transformed = ActivityPipeline.readAndTransform(spark, inputs)

      // incremental: 대상일만 write (lookback 행은 세션화 문맥용). backfill: 전체 event_date write.
      // (mode, runDate) match: Python의 match/case 또는 if-elif 체인과 유사한 패턴 매칭.
      //   ("incremental", Some(d)): mode가 "incremental"이고 runDate가 Some일 때만 필터링.
      //   _: 나머지 모든 경우(backfill 또는 runDate 없는 incremental) → 전체 반환.
      val toWrite = (mode, runDate) match {
        case ("incremental", Some(d)) => transformed.filter(col("event_date") === d)
        case _                        => transformed
      }

      // .select("event_date").distinct(): event_date 컬럼만 추출 후 중복 제거.
      //   Python: df[["event_date"]].drop_duplicates() 와 동일.
      // .as[String]: DataFrame → Dataset[String] 타입 변환.
      //   import spark.implicits._ 로 활성화된 Encoder[String]을 암묵적으로 사용.
      //   Python의 .rdd.map(lambda r: r[0]) 처럼 단일 값 타입 시퀀스로 변환하는 개념.
      // .collect(): Spark 분산 Dataset을 드라이버 메모리로 수집 → Array[String].
      //   Python의 .collect() 또는 list(rdd.collect())와 동일.
      // .toSeq.sorted: Array를 Seq로 변환 후 날짜 오름차순 정렬(문자열 비교, ISO 날짜는 사전순=시간순).
      val dates = toWrite.select("event_date").distinct()
        .as[String]
        .collect().toSeq.sorted

      // dates.foreach { d => ... }: Seq의 각 원소에 대해 블록을 실행.
      //   Python의 for d in dates: 와 완전히 동일한 순차 반복.
      dates.foreach { d =>
        // 해당 event_date에 해당하는 행만 필터링(파티션 단위 처리).
        val part = toWrite.filter(col("event_date") === d)
        // PartitionWriter.validate: 행수>0, 키 not null, 출력<=입력 세 가지 검증.
        //   inputCount: 위에서 계산한 원본 전체 행 수(파티션별 검증의 상한선).
        val v = PartitionWriter.validate(part, inputCount)
        // v.ok가 false이면 RuntimeException을 던져 파이프라인 중단(fail-fast).
        //   s"...": Scala 문자열 보간(f-string과 동일).
        if (!v.ok) throw new RuntimeException(s"validation failed for $d: ${v.message}")
        // 검증 통과 시 해당 파티션을 staging+rename 원자 교체로 기록(설계 결정 6).
        PartitionWriter.writePartition(spark, part, output, d)
        println(s"[OK] wrote partition event_date=$d (${v.message})")
      }
      println(s"[DONE] mode=$mode partitions=${dates.size}")
    } finally {
      // finally: 정상 종료·예외 어느 경우에도 반드시 실행.
      //   spark.stop(): SparkContext와 모든 실행자를 정상 종료(리소스 해제).
      //   Python의 spark.stop() / with SparkSession(...) as spark: 컨텍스트 매니저 종료와 동일.
      spark.stop()
    }
  }
}
