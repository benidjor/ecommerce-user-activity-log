# 트러블슈팅: Hive External Table 등록·조회

샘플 E2E (Task 11)에서 적재한 parquet을 외부 테이블로 노출하고 WAU를 조회하는 과정에서 부딪힌 카탈로그·파티션 인식 이슈.

## 1. spark-shell 기본 카탈로그가 Hive가 아님

`spark-shell`에서 `CREATE EXTERNAL TABLE`·`MSCK REPAIR`가 기대대로 동작하지 않음.

### 증상

`spark-shell`에서 외부 테이블을 만들고 `MSCK REPAIR TABLE`을 해도 파티션이 인식되지 않거나, Hive DDL이 in-memory 카탈로그에서 어색하게 동작함.

### 원인

- `spark-shell`은 기본적으로 **in-memory 카탈로그**를 쓸 수 있음.
  `MSCK REPAIR`·`EXTERNAL TABLE`·파티션 메타데이터는 **Hive 카탈로그** (임베디드 Derby metastore)가 있어야 제대로 동작함.
- `spark-sql` (SQL 전용 CLI)은 기본이 Hive 카탈로그라 이 문제가 없음.

### 해결

- **권장**: 순수 SQL은 `spark-sql -f <파일>`로 실행 (기본 Hive 카탈로그, heredoc 불필요)
  ```bash
  spark-sql --conf spark.sql.session.timeZone=UTC -f sql/wau.sql
  ```
- `spark-shell`을 꼭 써야 하면 Hive 카탈로그를 명시.
  ```bash
  spark-shell --conf spark.sql.catalogImplementation=hive ...
  ```

### 재발 방지

- 런북·README는 SQL 실행에 `spark-sql -f`를 표준으로 둠.
  `spark-shell`은 DataFrame/Scala 디버깅 용도로만.

## 2. `MSCK REPAIR` 전에는 파티션이 보이지 않음

테이블을 만들어도 데이터가 0건처럼 보임.

### 증상

`CREATE EXTERNAL TABLE` 직후 `SELECT`하면 행이 0건.

### 원인

- 외부 테이블은 `LOCATION`만 가리킬 뿐, `event_date=...` 디렉터리 (파티션)를 자동으로 알지 못함.
- 파티션을 메타스토어에 등록해야 쿼리에 포함됨.

### 해결

```sql
MSCK REPAIR TABLE activity;   -- LOCATION 아래 파티션 디렉터리를 스캔해 등록
```

`sql/create_external_table.sql`에 DDL과 함께 포함됨.
**새 날짜 파티션이 추가될 때마다 (증분 적재 후) 다시 실행**해야 함.

### 재발 방지

- 적재 → `MSCK REPAIR` → 조회 순서를 런북에 고정.
  프로덕션에서는 `ALTER TABLE ... ADD PARTITION` 또는 카탈로그 자동 등록 (Glue 등)으로 대체.

## 관련

- 실행 절차 전체는 [런북: 샘플 E2E](../runbook/sample-e2e.md)
- `LOCATION` 절대경로·`{{OUTPUT_DIR}}` 치환은 런북 §2 참조.
