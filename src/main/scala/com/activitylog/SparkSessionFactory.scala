package com.activitylog

import org.apache.spark.sql.SparkSession

// object: Scala 싱글턴 객체 — Python의 모듈처럼 인스턴스 생성 없이 바로 호출 가능.
//   SparkSessionFactory.create("appName") 형태로 사용.
object SparkSessionFactory {

  // def create(appName: String): SparkSession
  //   Python: def create(app_name: str) -> SparkSession: 과 동일한 함수 선언.
  //   반환 타입은 ": SparkSession"으로 명시. Scala는 마지막 표현식이 반환값(return 생략).
  def create(appName: String): SparkSession = {
    // SparkSession.builder(): Python의 SparkSession.builder 빌더 패턴과 동일.
    val spark = SparkSession.builder()
      // appName: Spark UI/로그에 표시될 애플리케이션 이름.
      .appName(appName)
      // master: 실행 모드 지정.
      //   sys.props.getOrElse: Java 시스템 프로퍼티에서 "spark.master" 값을 읽고,
      //   없으면 기본값 "local[*]"(로컬 모드, 가용 코어 전부 사용)을 반환.
      //   Python: os.environ.get("SPARK_MASTER", "local[*]")와 유사한 동작.
      //   spark-submit 시 -Dspark.master=yarn 등으로 외부 주입 가능.
      .master(sys.props.getOrElse("spark.master", "local[*]"))
      // 세션 타임존을 UTC로 고정: 날짜/시간 함수가 항상 UTC 기준으로 동작.
      //   TimeUtils.withKstColumns가 "UTC → KST 변환"을 1회 수행하므로 이 설정이 전제.
      .config("spark.sql.session.timeZone", "UTC")
      // Hive External Table 지원 활성화:
      //   SparkSession에 Hive 메타스토어 연동 기능(enableHiveSupport)을 추가.
      //   실제 별도 Hive 서버 없이 임베디드 Derby metastore를 사용(설계 결정 7).
      //   이 설정 없이는 spark.sql("SHOW TABLES") 등 Hive DDL을 사용할 수 없음.
      .enableHiveSupport()
      // getOrCreate: 이미 동일한 이름의 세션이 있으면 재사용, 없으면 새로 생성.
      //   Python과 동일한 패턴 — 중복 세션 방지.
      .getOrCreate()

    // getOrCreate가 기존 세션을 재사용하면 위 .config(timeZone)가 무시될 수 있다.
    //   결정적 session_id가 UTC 기준 unix_timestamp에 의존하므로 런타임에 UTC를 재확인한다.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark
  }
}
