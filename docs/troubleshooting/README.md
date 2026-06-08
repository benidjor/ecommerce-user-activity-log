# 트러블슈팅 모음

구현 중 부딪힌 환경·빌드·코드 이슈를 주제별로 정리한다. 각 문서는 증상·원인·해결·재발 방지 순서로 작성한다(컨벤션 §3).

| 주제 | 다루는 문제 |
|---|---|
| [jdk-toolchain.md](jdk-toolchain.md) | sbt 런처의 JDK 26 선택, JDK 17+ 모듈 접근(`--add-opens`) |
| [spark-provided-classpath.md](spark-provided-classpath.md) | `Provided` Spark가 test·run classpath에서 빠지는 문제 |
| [scala-spark-test-idioms.md](scala-spark-test-idioms.md) | `@transient var` implicits, `Dataset.as[T]` 인코더 표현 |
| [hive-external-table.md](hive-external-table.md) | spark-shell 카탈로그, `MSCK REPAIR` 파티션 인식 |
| [spark-sql-driver-memory-oom.md](spark-sql-driver-memory-oom.md) | 전체 데이터 WAU의 `COUNT(DISTINCT session_id)` OOM, `--driver-memory` |
