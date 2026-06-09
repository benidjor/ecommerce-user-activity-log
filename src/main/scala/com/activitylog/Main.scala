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
    // --mode 오타 방지: 허용값이 아니면 즉시 중단(조용히 backfill로 빠지는 것 방지).
    //   require(조건, 메시지): 조건이 false면 IllegalArgumentException(Python의 assert와 유사).
    require(Set("backfill", "incremental").contains(mode), s"invalid --mode: $mode (backfill|incremental)")

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
      // raw CSV를 명시적 스키마로 1회 정의(이후 count·transform이 이 lineage를 공유).
      //   Schema.Raw: 타입 추론 없이 명시 스키마 강제(설계 결정 5 보조).
      val raw = spark.read.option("header", "true").schema(Schema.Raw).csv(inputs: _*)

      // rawCount: 전역 sanity("행 폭증 방지")의 기준값. dedup/세션화는 행을 늘리지 않는다.
      //   .count(): Action — 실제 스캔 1회.
      val rawCount = raw.count()

      // ActivityPipeline.transform: dedup → KST 변환 → 세션화. raw를 재사용하므로 CSV를 또 읽지 않는다.
      //   결과 DataFrame에는 event_date(KST 날짜), session_id 등이 포함됨.
      val transformed = ActivityPipeline.transform(raw)

      // 요구사항 4 "추가 기간 처리": 새 기간이 들어오면 그 날짜만 incremental로 적재하고,
      //   이후 sql/create_external_table.sql의 MSCK REPAIR로 새 파티션을 등록한다.
      //   기존 파티션은 건드리지 않고 새 event_date만 덧붙임. session_id가 결정적이라 backfill과 결과가 같음(멱등).
      // incremental: 대상일만 write (lookback 행은 세션화 문맥용). backfill: 전체 event_date write.
      // (mode, runDate) match: Python의 match/case 또는 if-elif 체인과 유사한 패턴 매칭.
      val toWrite = (mode, runDate) match {
        case ("incremental", Some(d)) => transformed.filter(col("event_date") === d)
        case _                        => transformed
      }

      // persist: 파티션별 write 루프에서 transform 파이프라인이 매번 재계산되는 것을 막는다.
      //   (파티션 수만큼 전체 재실행 → 캐시 1회로 축소. 메모리 부족 시 디스크로 spill.)
      //   Python(pandas)엔 없는 개념 — Spark는 lazy라 action마다 lineage를 다시 실행한다.
      toWrite.persist()
      try {
        // toWrite.count(): persist 구체화 + 전역 sanity. 변환 후 행수가 raw보다 많으면 로직 오류(폭증).
        //   설계 스펙 §검증게이트 ③("출력행수가 입력 대비 합리적")을 전역 1회로 복원.
        val outCount = toWrite.count()
        if (outCount > rawCount)
          throw new RuntimeException(s"row explosion: output($outCount) > input($rawCount)")

        // event_date 목록(정렬) — 파티션 단위로 순회.
        //   .as[String]: implicits로 DataFrame → Dataset[String]. .toSeq.sorted: ISO 날짜는 사전순=시간순.
        val dates = toWrite.select("event_date").distinct()
          .as[String]
          .collect().toSeq.sorted

        dates.foreach { d =>
          // 해당 event_date 행만 필터(persist된 toWrite에서 읽으므로 재계산 없음).
          val part = toWrite.filter(col("event_date") === d)
          // 파티션 단위 검증(행수>0·키 not null). 실패 시 fail-fast.
          val v = PartitionWriter.validate(part)
          if (!v.ok) throw new RuntimeException(s"validation failed for $d: ${v.message}")
          // 검증 통과 시 staging+rename 원자 교체로 기록(설계 결정 6).
          PartitionWriter.writePartition(spark, part, output, d)
          println(s"[OK] wrote partition event_date=$d (${v.message})")
        }
        println(s"[DONE] mode=$mode partitions=${dates.size}")
      } finally {
        // 캐시 해제(메모리/디스크 정리). 정상·예외 모두 실행.
        toWrite.unpersist()
      }
    } finally {
      // spark.stop(): SparkContext와 모든 실행자를 정상 종료(리소스 해제).
      //   Python의 spark.stop() / with 컨텍스트 매니저 종료와 동일.
      spark.stop()
    }
  }
}
