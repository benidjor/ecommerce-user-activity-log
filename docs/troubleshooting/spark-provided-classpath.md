# 트러블슈팅: Spark `Provided` 의존성과 클래스패스

`spark-sql`·`spark-hive`를 `Provided` scope로 선언한 결과, sbt의 `test`·`run` 런타임 클래스패스에서 Spark가 빠져 두 군데서 다른 증상으로 막힘.
같은 뿌리 (Provided)에서 나온 두 사례를 함께 정리함.

## 배경: 왜 `Provided`이고, 무엇이 문제인가

`build.sbt`에서 Spark를 `Provided`로 선언함.

```scala
"org.apache.spark" %% "spark-sql"  % sparkVersion % Provided,
"org.apache.spark" %% "spark-hive" % sparkVersion % Provided,
```

- 실행 모델이 **spark-submit + thin jar** (설계 결정 8)
  spark-submit이 클러스터/런타임에 Spark를 이미 제공하므로, 우리 jar에 Spark를 넣으면 버전 충돌·용량 낭비가 됨.
- `Provided`는 "컴파일 시점엔 필요하지만 런타임은 환경이 제공한다"는 뜻.
  그래서 `sbt package`는 Spark가 빠진 thin jar를 만듦 (의도된 동작)
- **부작용**: `Provided`는 sbt의 `Compile` classpath에만 들어가고, `Test`·`Runtime` classpath에는 자동으로 들어가지 않음.
  그래서 `sbt test`와 `sbt run` 양쪽에서 Spark 클래스를 못 찾음.
  아래 두 사례가 이 부작용.

> sbt classpath 용어: `managedClasspath`는 라이브러리 의존성으로 받은 JAR 묶음, `unmanagedClasspath`는 수동으로 더한 항목. `Compile / managedClasspath`에는 `Provided` JAR이 포함됨.

## 1. 테스트에서 Spark 클래스 누락

`Provided`라 테스트 실행 시 Spark가 클래스패스에 없음.

### 증상

테스트 컴파일/실행 시 Spark 클래스 `NoClassDefFoundError` (예: `org/apache/spark/sql/SparkSession`)

### 원인

- `Provided` 의존성이 `Test` 런타임 classpath에 포함되지 않음.
- 계획 원안의 `Test / classpathConfiguration := Compile`은 sbt 1.12.x에서 이 의도를 달성하지 못함 (해당 설정의 동작이 바뀜/무력화)

### 해결

`Compile`의 managed classpath (= `Provided` JAR 포함)를 `Test`의 unmanaged classpath에 더함.

```scala
Test / unmanagedClasspath ++= (Compile / managedClasspath).value
```

이러면 테스트 JVM이 Spark JAR을 볼 수 있음.
`ToolchainSpec` (사소한 Spark job)으로 이 경로가 살아있는지 회귀 감지함.

### 재발 방지

- `build.sbt`에 고정. 새 spec도 이 설정 위에서 동작.

## 2. `sbt runMain`으로 앱 실행 실패

`run`/`runMain`도 `Provided`가 빠지므로 같은 이유로 실패함.

### 증상

```bash
sbt "runMain com.activitylog.Main --mode backfill --input ... --output ..."
```
```
java.lang.NoClassDefFoundError: org/apache/spark/sql/SparkSession$
  at com.activitylog.SparkSessionFactory$.create(SparkSessionFactory.scala:14)
```

### 원인

§1과 동일한 뿌리. `run` 런타임 classpath에 `Provided` (Spark)가 없음.
**`run`은 일부러 보강하지 않음** — 이 앱의 실행 수단은 sbt가 아니라 spark-submit이기 때문 (테스트만 보강한 이유도 이것)

### 해결

설계 실행 모델대로 **spark-submit + thin jar**로 실행함.
spark-submit이 런타임에 Spark를 제공하므로 `Provided`가 정확히 맞아떨어짐.

```bash
sbt package   # thin jar 빌드
spark-submit \
  --class com.activitylog.Main \
  --master "local[*]" \
  --driver-memory 4g \
  --conf spark.sql.session.timeZone=UTC \
  target/scala-2.13/activity-log_2.13-0.1.0.jar \
  --mode backfill --input <csv> --output <dir>
```

### 재발 방지

- 실행은 항상 spark-submit. `sbt`는 빌드·테스트 전용이라는 점을 README·런북에 명시함.
- (대안: `run`에도 `unmanagedClasspath`를 더하면 sbt run이 되지만, 실행 수단을 spark-submit으로 일원화하려고 일부러 하지 않음.)

## 요약

| 사례 | 어디서 | 해결 |
|---|---|---|
| 테스트 classpath 누락 | `sbt test` | `Test / unmanagedClasspath ++= (Compile / managedClasspath).value` |
| 실행 classpath 누락 | `sbt runMain` | spark-submit + thin jar로 실행 |

## 관련

- 모듈 접근 (`--add-opens`)·JDK 버전은 [jdk-toolchain.md](jdk-toolchain.md)
