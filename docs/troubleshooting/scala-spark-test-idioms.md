# 트러블슈팅: Scala/Spark 테스트 컴파일 관용구

계획서 원안의 테스트 코드가 Scala 2.13 / Spark 4에서 컴파일되지 않아 교정한 두 가지.
둘 다 "Scala 문법 규칙" 때문이며 로직 (검증 내용)과는 무관함.
Python만 쓰던 사람이 가장 헷갈리는 부분이라 자세히 적음.

## 1. `import spark.implicits._`가 컴파일 안 됨 (`@transient var`)

테스트 베이스의 `spark` 필드를 그대로 import 경로로 쓰면 막힘.

### 증상

```scala
class FooSpec extends AnyFunSuite with SparkTestBase {
  test("...") {
    import spark.implicits._   // ← 컴파일 에러
    Seq(1,2,3).toDF("n")
  }
}
```
```
error: stable identifier required, but spark found.
```

### 원인

- `SparkTestBase`의 `spark`는 재할당 가능한 `var` (생명주기 관리 때문)
  ```scala
  @transient protected var spark: SparkSession = _
  ```
- Scala에서 `import x._` (멤버 일괄 임포트)의 `x`는 **안정 식별자 (stable identifier)** 여야 함.
  안정 식별자는 값이 바뀌지 않는 `val`·`object`·패키지 등.
- `var`는 언제든 다른 객체로 바뀔 수 있어 "안정적"이지 않음.
  그래서 `import spark.implicits._`처럼 `var`를 임포트 경로로 쓸 수 없음.
- `@transient`/`var`인 이유: 테스트 트레이트가 `beforeAll`에서 세션을 만들어 할당하고 `afterAll`에서 정리 (`stop`)하려면 재할당이 필요하고, Spark가 객체를 직렬화할 때 세션은 제외해야 (`@transient`) 하기 때문.

### `implicits._`가 왜 필요한가

`import spark.implicits._`는 `Seq(...).toDF()`, `ds.as[String]` 같은 변환을 가능하게 하는 **암묵적 변환 (implicit conversion)·Encoder**를 스코프로 들여옴.
Python엔 없는 개념으로, "이 스코프에서만 켜지는 자동 변환 규칙"이라고 보면 됨.
그래서 import가 꼭 필요함.

### 해결

지역 `val`에 한 번 담아 그 `val`을 임포트함.

```scala
val ss = spark          // 안정 식별자(val)로 캡처
import ss.implicits._    // OK
Seq(1,2,3).toDF("n")
```

### 재발 방지

- 모든 spec에서 이 패턴 사용. `SparkTestBase`·`ToolchainSpec` 주석에 이유를 적어둠.
- 실제 앱 코드처럼 `spark`가 일반 `val`이면 (예: `val spark = SparkSessionFactory.create(...)`) `import spark.implicits._`가 바로 됨 — `var`일 때만 생기는 문제.

## 2. 계획서의 인코더 표현이 Scala 2.13/Spark 4에서 비호환

세션화 테스트 원안의 인코더 표현이 컴파일되지 않음.

### 증상

```scala
Sessionizer.sessionize(d).select("session_id")
  .as(org.apache.spark.sql.Encoders.STRING)(spark.implicits.newStringEncoder)  // ← 컴파일 실패
```

### 원인

- `Dataset.as`의 시그니처는 **타입 파라미터 + implicit Encoder**.
  ```scala
  def as[U : Encoder]: Dataset[U]   // = def as[U](implicit e: Encoder[U]): Dataset[U]
  ```
- 즉 `.as[String]`처럼 **타입을 적고**, `Encoder[String]`은 컴파일러가 implicit으로 주입하게 해야 함.
- 원안처럼 `Encoder` 값을 일반 인자로 넘기는 `.as(Encoders.STRING)(...)` 형태는 이 시그니처와 맞지 않아 컴파일되지 않음.

### 해결

implicits를 import하고 타입 파라미터로 호출함 (결과는 동일)

```scala
val ss = spark
import ss.implicits._
val ids: Seq[String] =
  Sessionizer.sessionize(d).select("session_id").as[String].collect().toSeq
```

### 재발 방지

- 테스트에서는 표준 implicits 기반 `.as[T]`를 사용함 (§1과 같은 `val ss = spark` 패턴 위에서)

## 관련

- 이 implicits/Encoder가 동작하려면 Spark가 클래스패스에 있어야 함 → [spark-provided-classpath.md](spark-provided-classpath.md)
