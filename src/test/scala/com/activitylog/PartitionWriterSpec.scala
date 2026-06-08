package com.activitylog

// Hadoop FileSystem API: HDFS·로컬 FS를 동일 인터페이스로 다루는 추상 클래스(Python의 os/pathlib 대응)
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.funsuite.AnyFunSuite

class PartitionWriterSpec extends AnyFunSuite with SparkTestBase {

  test("validate rejects empty and null-key data") {
    // spark는 @transient var 이므로 람다/클로저 안에서 직접 캡처하면
    // 직렬화 오류 가능 → val로 로컬 참조를 만들어서 implicits를 안전하게 임포트
    val ss = spark
    import ss.implicits._

    // 빈 DataFrame: 행 수 0 → validate가 ok=false를 반환해야 한다
    val empty = Seq.empty[(Long, String)].toDF("user_id", "event_time_utc")
    assert(!PartitionWriter.validate(empty).ok)

    // user_id=null 행: 키 not-null 검증에서 걸려야 한다
    val nullKey = Seq((null.asInstanceOf[java.lang.Long], "2019-10-01"))
      .toDF("user_id", "event_time_utc")
    assert(!PartitionWriter.validate(nullKey).ok)
  }

  test("validate passes clean non-empty data") {
    val ss = spark
    import ss.implicits._
    // 행수>0, 키 not null → ok=true (단일 집계 경로 검증)
    val clean = Seq((1L, "2019-10-01 00:00:00"), (2L, "2019-10-01 00:01:00"))
      .toDF("user_id", "event_time_utc")
    assert(PartitionWriter.validate(clean).ok)
  }

  test("writePartition writes parquet, swaps atomically, creates _SUCCESS") {
    val ss = spark
    import ss.implicits._

    // 임시 디렉터리를 base로 사용(테스트 격리)
    val base = java.nio.file.Files.createTempDirectory("activity").toString
    val df   = Seq((1L, "2019-10-01 00:00:00")).toDF("user_id", "event_time_utc")

    PartitionWriter.writePartition(spark, df, base, "2019-10-01")

    // Hadoop FileSystem: SparkContext의 hadoopConfiguration으로 로컬 FS 인스턴스 획득
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    // _SUCCESS 마커가 최종 파티션 경로 안에 있어야 한다
    assert(fs.exists(new Path(s"$base/event_date=2019-10-01/_SUCCESS")))

    // staging 디렉터리는 rename 후 남아있으면 안 된다
    assert(!fs.exists(new Path(s"$base/_staging/event_date=2019-10-01")))

    // 실제 parquet 파일을 읽어서 행 수 확인
    val read = spark.read.parquet(s"$base/event_date=2019-10-01")
    assert(read.count() == 1L)
  }
}
