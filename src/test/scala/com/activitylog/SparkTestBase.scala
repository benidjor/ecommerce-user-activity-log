package com.activitylog

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

// trait: 재사용 가능한 믹스인(Python의 다중 상속/Mixin과 유사). 테스트 스펙들이
//   "with SparkTestBase"로 섞어 쓰면 공용 SparkSession을 물려받는다.
// self: Suite => : 자기 타입(self-type) 선언 — 이 trait는 ScalaTest Suite에
//   섞일 때만 쓸 수 있다는 제약(BeforeAndAfterAll 훅을 안전하게 호출하기 위함).
trait SparkTestBase extends BeforeAndAfterAll { self: Suite =>
  // @transient: 직렬화 대상에서 제외(Spark가 객체를 워커로 보낼 때 세션은 보내지 않음).
  // var: 재할당 가능한 변수(Python 일반 변수). _ 는 "타입 기본값(null)으로 초기화".
  @transient protected var spark: SparkSession = _

  // beforeAll: 스펙 내 모든 테스트 실행 전 1회 호출(Python unittest의 setUpClass와 유사).
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .appName("test")
      .master("local[2]")                          // 로컬 모드, 스레드 2개로 실행
      .config("spark.sql.session.timeZone", "UTC")  // 세션 타임존을 UTC로 고정(TimeUtils 전제)
      .config("spark.sql.shuffle.partitions", "4")  // 테스트라 셔플 파티션을 작게
      .config("spark.ui.enabled", "false")          // 테스트 중 웹 UI 비활성
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")          // 로그 소음 줄이기
  }

  // afterAll: 모든 테스트 종료 후 1회 호출 — 세션을 닫아 리소스 정리(tearDownClass와 유사).
  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    super.afterAll()
  }
}
