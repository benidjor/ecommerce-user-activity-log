# 트러블슈팅: JDK 툴체인 (버전 선택 + 모듈 접근)

Spark 4는 JDK 17/21을 대상으로 하고, JDK 17+의 모듈 캡슐화 때문에 일부 내부 API에 `--add-opens`가 필요함.
빌드/테스트가 JDK17에서 결정적으로 돌도록 맞추는 과정에서 부딪힌 두 가지를 정리함.

## 1. sbt 런처가 JDK 26을 골라 Spark 초기화 실패

`sbt`를 그냥 실행하면 런처가 시스템 최신 JDK (26)를 선택해 Spark 4가 뜨지 못함.

### 증상

`sbt test` (Spark 세션 생성 시점)에서 다음과 같이 실패함.

```
java.lang.ExceptionInInitializerError
  ...
Caused by: ... (jdk.internal.ref.Cleaner 등 내부 API 접근 실패)
```

같은 코드가 JDK17에서는 정상 동작함.

### 원인

- Homebrew로 설치한 sbt 런처가 JVM을 직접 고르며, 설치된 **최신 JDK (26)**를 선택함.
- Spark 4.x의 지원 JDK는 17/21.
  그보다 높은 JDK에서는 일부 내부 API가 제거·차단되어 초기화가 실패함.
- 이건 "버전 선택" 문제라, `build.sbt`의 `--add-opens` (아래 §2)로는 해결되지 않음 — 애초에 잘못된 JDK가 선택되기 때문.

### 해결

프로젝트 루트 `.sbtopts`에 sbt가 쓸 JDK를 JDK17로 고정함.

```
-java-home /Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home
```

경로는 머신마다 다르므로 아래로 확인함.

```bash
/usr/libexec/java_home -v 17
```

1회성으로는 환경변수로도 됨.

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt test
```

### 재발 방지

- `.sbtopts`는 **머신 종속 절대경로**라 저장소 포터빌리티 (다른 사람의 clone)를 깨므로 `.gitignore`에 넣고 **커밋하지 않음**.
  JDK17 요구사항은 `CLAUDE.md`·계획서·README에 문서로 남김.
- 다른 머신/CI에서는 해당 OS의 JDK17 경로로 `.sbtopts`를 새로 만듦.
- `spark-submit`은 Homebrew apache-spark가 가져온 JDK21 런타임을 쓰며 Spark 4를 지원하므로 별도 고정이 필요 없음 (JDK17 고정은 빌드·테스트 전용)

## 2. JDK 17+ 모듈 캡슐화로 Spark 리플렉션 차단 (`--add-opens`)

올바른 JDK (17/21)를 써도, JDK 9+의 모듈 시스템이 내부 패키지를 막아 Spark가 리플렉션으로 접근하는 일부 API가 거부됨.

### 증상

`--add-opens` 없이 테스트/실행하면 다음과 같은 오류로 실패함.

```
java.lang.reflect.InaccessibleObjectException:
  Unable to make ... accessible: module java.base does not "opens java.nio" to unnamed module
```

(Spark가 `sun.nio.ch`·`java.nio` 등에 리플렉션 접근할 때 발생.)

### 원인

- JDK 9의 모듈 시스템 (JPMS)이 `java.base`의 내부 패키지를 기본적으로 캡슐화함.
- Spark는 성능을 위해 이 내부 클래스들에 리플렉션으로 접근하는데, 모듈이 열려 있지 않으면 `InaccessibleObjectException`이 발생함.
- `--add-opens=<module>/<package>=ALL-UNNAMED`는 해당 패키지를 "이름 없는 모듈 (우리 앱)"에 열어주는 JVM 플래그.

### 해결

`build.sbt`에 필요한 `--add-opens` 묶음을 정의하고 테스트·실행 JVM에 적용함.
**JVM 옵션은 fork된 (별도) JVM에만 적용되므로 `fork := true`가 함께 필요함.**

```scala
val jvm17Opens = Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  // ... (java.io/java.net/java.util/sun.security.action 등)
)
// ...
Test / fork := true,
Test / javaOptions ++= jvm17Opens,
run  / fork := true,
run  / javaOptions ++= jvm17Opens,
```

`spark-submit`로 실행할 때는 같은 플래그를 conf로 넘김 (Spark 4 스크립트가 기본 add-opens를 일부 설정하지만, 명시하면 확실함)

```bash
spark-submit --conf spark.driver.extraJavaOptions="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" ...
```

### 재발 방지

- `build.sbt`의 `jvm17Opens`로 고정.
  `fork := true`와 한 쌍으로 둠 (fork 안 하면 javaOptions가 무시됨)
- 새 `InaccessibleObjectException`이 뜨면 메시지에 나온 `module/package`를 `jvm17Opens`에 추가함.

## 관련

- 실행 (spark-submit) 시의 클래스패스 문제는 [spark-provided-classpath.md](spark-provided-classpath.md)
