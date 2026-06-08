# 트러블슈팅: Scala/Spark 테스트 컴파일 관용구

계획서 원안의 테스트 코드가 Scala 2.13 / Spark 4에서 컴파일되지 않아 교정한 두 가지. 둘 다 "Scala 문법 규칙" 때문이며 로직(검증 내용)과는 무관하다. Python만 쓰던 사람이 가장 헷갈리는 부분이라 자세히 적는다.

## 1. `import spark.implicits._`가 컴파일 안 됨 (`@transient var`)

테스트 베이스의 `spark` 필드를 그대로 import 경로로 쓰면 막힌다.

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

- `SparkTestBase`의 `spark`는 재할당 가능한 `var`다(생명주기 관리 때문).
  ```scala
  @transient protected var spark: SparkSession = _
  ```
- Scala에서 `import x._`(멤버 일괄 임포트)의 `x`는 **안정 식별자(stable identifier)** 여야 한다. 안정 식별자는 값이 바뀌지 않는 `val`·`object`·패키지 등이다.
- `var`는 언제든 다른 객체로 바뀔 수 있어 "안정적"이지 않다. 그래서 `import spark.implicits._`처럼 `var`를 임포트 경로로 쓸 수 없다.
- `@transient`/`var`인 이유: 테스트 트레이트가 `beforeAll`에서 세션을 만들어 할당하고 `afterAll`에서 정리(`stop`)하려면 재할당이 필요하고, Spark가 객체를 직렬화할 때 세션은 제외해야(`@transient`) 하기 때문.

### `implicits._`가 왜 필요한가

`import spark.implicits._`는 `Seq(...).toDF()`, `ds.as[String]` 같은 변환을 가능하게 하는 **암묵적 변환(implicit conversion)·Encoder**를 스코프로 들여온다. Python엔 없는 개념으로, "이 스코프에서만 켜지는 자동 변환 규칙"이라고 보면 된다. 그래서 import가 꼭 필요하다.

### 해결

지역 `val`에 한 번 담아 그 `val`을 임포트한다.

```scala
val ss = spark          // 안정 식별자(val)로 캡처
import ss.implicits._    // OK
Seq(1,2,3).toDF("n")
```

### 재발 방지

- 모든 spec에서 이 패턴 사용. `SparkTestBase`·`ToolchainSpec` 주석에 이유를 적어둠.
- 실제 앱 코드처럼 `spark`가 일반 `val`이면(예: `val spark = SparkSessionFactory.create(...)`) `import spark.implicits._`가 바로 된다 — `var`일 때만 생기는 문제다.

## 2. 계획서의 인코더 표현이 Scala 2.13/Spark 4에서 비호환

세션화 테스트 원안의 인코더 표현이 컴파일되지 않았다.

### 증상

```scala
Sessionizer.sessionize(d).select("session_id")
  .as(org.apache.spark.sql.Encoders.STRING)(spark.implicits.newStringEncoder)  // ← 컴파일 실패
```

### 원인

- `Dataset.as`의 시그니처는 **타입 파라미터 + implicit Encoder**다.
  ```scala
  def as[U : Encoder]: Dataset[U]   // = def as[U](implicit e: Encoder[U]): Dataset[U]
  ```
- 즉 `.as[String]`처럼 **타입을 적고**, `Encoder[String]`은 컴파일러가 implicit으로 주입하게 해야 한다.
- 원안처럼 `Encoder` 값을 일반 인자로 넘기는 `.as(Encoders.STRING)(...)` 형태는 이 시그니처와 맞지 않아 컴파일되지 않는다.

### 해결

implicits를 import하고 타입 파라미터로 호출한다(결과는 동일).

```scala
val ss = spark
import ss.implicits._
val ids: Seq[String] =
  Sessionizer.sessionize(d).select("session_id").as[String].collect().toSeq
```

### 재발 방지

- 테스트에서는 표준 implicits 기반 `.as[T]`를 사용한다(§1과 같은 `val ss = spark` 패턴 위에서).

## 3. SQL 결과 타입과 `.as[T]` 인코더 불일치 (`CANNOT_UP_CAST`)

Gold 마트(Phase 1) `mart_retention` 테스트에서, 계획서 SQL을 그대로 `.as[(..., Int, ..., Double)]`로 디코딩하니 분석 단계에서 죽었다. SQL 산술의 **결과 타입**이 테스트 튜플 타입과 달랐기 때문이다.

### 증상

```
org.apache.spark.sql.AnalysisException: [CANNOT_UP_CAST_DATATYPE]
Cannot up cast week_offset from "DOUBLE" to "INT".
Cannot up cast retention_rate from "DECIMAL(38,16)" to "DOUBLE".
```

### 원인

Spark SQL 산술의 타입 규칙이 Postgres/MySQL 직관과 다르다.

- `datediff(...) / 7` — Spark의 `/`는 **실수 나눗셈**이라 피연산자가 정수여도 결과는 `DOUBLE`. "정수 나눗셈이라 INT"라는 가정이 틀림.
- `count(...) * 1.0 / cohort` — 리터럴 `1.0`이 `DECIMAL`이라 `정수 * DECIMAL / 정수` = `DECIMAL`. (`정수 / 정수`는 이미 `DOUBLE`이라, 비율을 원하면 `*1.0`이 오히려 DECIMAL을 만든다.)
- `.as[T]`의 Encoder는 **넓히는(up-cast) 변환만** 허용한다. `DOUBLE→INT`(축소)·`DECIMAL→DOUBLE`(비호환)은 막혀 컴파일이 아니라 쿼리 분석 단계에서 실패한다.

### 해결

테스트 튜플 타입을 바꾸기보다, **SQL이 의도한 타입을 내도록** `CAST`로 고정한다(의도가 코드에 드러나고 마트 스키마도 깔끔).

```sql
-- 주 오프셋은 정수(주 경계가 7일 배수라 무손실). SELECT와 GROUP BY에 동일 표현식을 둬야 그룹핑이 맞음.
CAST(datediff(uw.wk, c.cohort_week) / 7 AS INT)            AS week_offset,
-- 비율은 실수. 정수/정수가 이미 DOUBLE이므로 분자만 캐스팅(또는 그대로 둬도 DOUBLE).
CAST(count(DISTINCT uw.user_id) AS DOUBLE) / cs.cohort_users AS retention_rate
```

### 재발 방지

- 마트 SQL을 추가할 때, 테스트가 `.as[(...)]`로 받을 컬럼은 **SQL 쪽 타입을 먼저 확정**한다. `/`=DOUBLE, `*1.0`=DECIMAL, `datediff`=INT, `count`=BIGINT를 기억.
- `CAST(... AS INT)`를 `GROUP BY`에도 쓸 땐 `SELECT`와 **동일 표현식**이어야 그룹핑이 일치한다.

## 관련

- 이 implicits/Encoder가 동작하려면 Spark가 클래스패스에 있어야 한다 → [spark-provided-classpath.md](spark-provided-classpath.md).
- Gold 마트 실행·검증 절차 → [../runbook/gold-marts.md](../runbook/gold-marts.md).
