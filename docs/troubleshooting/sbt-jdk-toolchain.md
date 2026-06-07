# 트러블슈팅: sbt + JDK 툴체인

Task 1~7을 진행하며 sbt·JDK·Spark 4 조합에서 반복적으로 부딪힌 환경·빌드 이슈와, 계획서 원안 코드가 실제 sbt 1.12.x / Scala 2.13.17 / Spark 4.1.2에서 동작하지 않아 교정한 사항을 한곳에 모은다.

각 항목은 증상·원인·해결·재발 방지 순으로 적는다. 모두 재발 가능성이 있어 커밋 본문(5~15줄)을 넘어 별도 문서로 보존한다.

## 1. Homebrew sbt가 JDK 26을 선택해 Spark 초기화 실패

sbt를 그냥 실행하면 런처가 시스템 최신 JDK(26)를 골라 Spark 4가 뜨지 못한다.

- **증상** — `sbt test` 시 `ExceptionInInitializerError`(내부적으로 `jdk.internal.ref.Cleaner` 관련)로 Spark 세션 생성이 실패.
- **원인** — Homebrew sbt 런처가 기본/최신 JDK를 선택. Spark 4는 JDK 17/21 대상이며 그보다 높은 JDK에서 내부 API 접근이 막힌다.
- **해결** — 프로젝트 루트 `.sbtopts`에 `-java-home <JDK17 home>`을 고정한다. 1회성으론 `JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt ...`도 가능.
- **재발 방지** — `.sbtopts`는 머신 종속 절대경로라 `.gitignore`로 로컬 전용 처리(커밋 금지). JDK17 요구는 `CLAUDE.md`·계획서에 문서화. 다른 머신은 `/usr/libexec/java_home -v 17`로 경로를 다시 확인해 `.sbtopts`를 재생성한다.

## 2. sbt 1.12.x에서 Provided 의존성이 테스트 클래스패스에서 누락

`spark-sql`·`spark-hive`를 `Provided`로 두면 테스트가 Spark 클래스를 못 찾는다.

- **증상** — 테스트 컴파일/실행 시 Spark 클래스 `NoClassDefFoundError`.
- **원인** — 계획 원안의 `Test / classpathConfiguration := Compile`가 sbt 1.12.x에서 Provided 의존성을 테스트 클래스패스에 포함시키지 못한다(설정 동작 변화).
- **해결** — `Test / unmanagedClasspath ++= (Compile / managedClasspath).value`로 Provided JAR을 테스트 클래스패스에 명시적으로 더한다.
- **재발 방지** — `build.sbt`에 반영. `ToolchainSpec`(사소한 Spark job)이 회귀를 즉시 잡는다.

## 3. `import spark.implicits._` 컴파일 실패 (`@transient var`)

테스트 베이스의 `spark`를 그대로 import 경로로 쓰면 컴파일이 안 된다.

- **증상** — `import spark.implicits._`에서 "stable identifier required" 류 컴파일 에러.
- **원인** — `SparkTestBase`의 `spark`가 `@transient var`라 Scala가 요구하는 안정 식별자(stable identifier)가 아니다. Scala의 `import`는 안정 식별자에만 가능하다.
- **해결** — `val ss = spark; import ss.implicits._`로 지역 `val`에 담아 import.
- **재발 방지** — 모든 spec에서 이 패턴을 사용. `SparkTestBase`/`ToolchainSpec` 주석에도 이유를 명시했다.

## 4. 계획서 인코더 표현이 Scala 2.13 / Spark 4에서 비호환

세션화 테스트의 원안 인코더 표현이 컴파일되지 않았다.

- **증상** — `SessionizerSpec`의 `.as(Encoders.STRING)(spark.implicits.newStringEncoder)`가 컴파일 실패.
- **원인** — `Dataset.as[U]`는 타입 파라미터 + implicit `Encoder[U]` 형태라, `Encoder` 값을 일반 인자로 넘기는 표현이 시그니처와 맞지 않는다.
- **해결** — `import ss.implicits._` 후 `.as[String]`로 단순화한다(결과 동일).
- **재발 방지** — 테스트에서는 표준 implicits 기반 `.as[T]`를 사용한다.

## 요약

| # | 한 줄 요약 | 고정 위치 |
|---|---|---|
| 1 | JDK 26 자동 선택 → `.sbtopts`로 JDK17 고정 | `.sbtopts`(gitignore) |
| 2 | Provided가 test 클래스패스 누락 → `unmanagedClasspath ++= managedClasspath` | `build.sbt` |
| 3 | `@transient var` spark는 import 불가 → `val ss = spark` | 전 spec |
| 4 | `.as(Encoders.STRING)(...)` 비호환 → `.as[String]` | `SessionizerSpec` |
