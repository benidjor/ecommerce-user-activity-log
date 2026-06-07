package com.activitylog

// Hadoop FileSystem: HDFS·로컬·S3 등 모든 파일시스템을 단일 인터페이스로 다루는 추상 클래스.
//   Python의 os.path / pathlib.Path 역할을 하지만 분산 스토리지까지 포괄한다.
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

// object: Scala의 싱글턴 객체(Python의 @staticmethod 묶음과 유사).
//   인스턴스 없이 PartitionWriter.validate(...) 형태로 바로 호출한다.
object PartitionWriter {

  // final case class: 불변(immutable) 값 객체(Python의 @dataclass(frozen=True)와 유사).
  //   final = 상속 금지. case = toString·equals·copy 자동 생성. Pattern matching에도 활용.
  final case class ValidationResult(ok: Boolean, message: String)

  /** 검증 게이트(최소 형태): 행수>0, 키 not null, 출력<=입력
   *
   *  파이프라인에서 파티션을 실제로 쓰기 전에 호출한다.
   *  세 가지 조건 중 하나라도 어긋나면 ok=false를 즉시 반환(early return).
   */
  def validate(df: DataFrame, inputCount: Long): ValidationResult = {
    val cnt = df.count()

    // Scala에서 `return`은 메서드를 즉시 탈출(Python의 early return과 동일).
    // 단, Scala 스타일 가이드에서는 일반적으로 지양하지만 여기서는
    // 조건이 순서대로 실패를 가르는 검증 게이트이므로 명시적으로 사용한다.
    if (cnt <= 0L) return ValidationResult(false, "row count is 0")

    // 키 컬럼(user_id, event_time_utc) 중 하나라도 null인 행을 집계
    val nullKeys = df.filter(col("user_id").isNull || col("event_time_utc").isNull).count()
    if (nullKeys > 0L) return ValidationResult(false, s"null key rows=$nullKeys")

    // 출력 행 수가 입력보다 많으면 로직 오류(dedup 후 오히려 증가한 경우 등)
    if (inputCount > 0L && cnt > inputCount)
      return ValidationResult(false, s"output($cnt) > input($inputCount)")

    ValidationResult(true, s"ok rows=$cnt")
  }

  /** 단일 event_date 파티션을 원자적으로 교체 후 _SUCCESS 마커 생성.
   *
   *  흐름: staging 경로에 parquet 쓰기 → 기존 final 경로 삭제 → rename → _SUCCESS 생성.
   *  df에 event_date 컬럼이 있으면 제거(파티션은 경로명으로 인코딩되므로 중복).
   */
  def writePartition(spark: SparkSession, df: DataFrame, baseDir: String, eventDate: String): Unit = {
    // FileSystem.get(conf): hadoopConfiguration으로 현재 스토리지(로컬 FS / HDFS)에
    //   맞는 FileSystem 구현체를 가져온다(Python의 open/os 모듈이 OS를 추상화하는 것과 유사).
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    // Path: Hadoop의 경로 객체. java.io.File / pathlib.Path의 Hadoop 버전.
    val staging  = new Path(s"$baseDir/_staging/event_date=$eventDate")
    val finalDir = new Path(s"$baseDir/event_date=$eventDate")

    // 이전 크래시로 staging이 남아있으면 제거(두 번째 인수 true = 재귀 삭제)
    // fs.delete(path, recursive): Python의 shutil.rmtree와 유사
    if (fs.exists(staging)) fs.delete(staging, true)

    // event_date 컬럼이 있으면 제거(파티션 경로에 이미 인코딩됨, 중복 저장 방지)
    val toWrite = if (df.columns.contains("event_date")) df.drop("event_date") else df

    // df.write.option(...).mode("overwrite").parquet(path):
    //   Python의 df.to_parquet(path, compression='snappy') 에 해당.
    //   mode("overwrite") = 대상이 이미 있어도 덮어씀(staging이므로 안전).
    toWrite.write
      .option("compression", "snappy")
      .mode("overwrite")
      .parquet(staging.toString)

    // 기존 final 파티션이 있으면 삭제 후 staging을 rename → 원자적 교체
    if (fs.exists(finalDir)) fs.delete(finalDir, true)

    // fs.rename(src, dst): 로컬 FS에서는 atomic move, HDFS에서도 메타데이터 교체로 동작.
    //   Python의 os.rename / pathlib.Path.rename과 유사하나 반환값(Boolean)으로 성공 여부 확인 필요.
    if (!fs.rename(staging, finalDir))
      throw new RuntimeException(s"atomic rename failed: $staging -> $finalDir")

    // _SUCCESS 마커 파일 생성: Hadoop 에코시스템에서 파티션 완료를 나타내는 관례적 파일.
    //   fs.create(path, overwrite=true): 빈 파일 생성 후 .close()로 즉시 닫는다.
    //   Python의 open(path, 'w').close()와 동일한 패턴.
    fs.create(new Path(finalDir, "_SUCCESS"), true).close()
  }
}
