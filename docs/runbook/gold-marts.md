# 런북: Gold 마트 생성 + 검증 (Phase 1 / Task 9)

Silver `activity`(Hive External Table)에서 KPI Gold 마트 10종을 생성해 parquet + `gold_*` External Table로 노출하고, 제출용 WAU 정본과의 회귀를 검증하는, 실제로 실행·검증된 절차다. 적재(backfill)는 [full-backfill.md](full-backfill.md)가 선행이고, 본 런북은 그 위에서 마트만 만든다.

> 실측일 2026-06-08 기준. 산출: 마트 10종, `gold_mart_wau`가 정본 `results/wau_users.txt`·`wau_sessions.txt`와 9개 ISO 주 전부 1:1 일치.

## 0. 선행조건 — `activity` 등록 확인

`GoldMarts`는 메타스토어에 등록된 `activity` 테이블을 읽는다. 없으면 [full-backfill.md](full-backfill.md) §2~3으로 적재·등록한다.

```bash
spark-sql --conf spark.sql.session.timeZone=UTC -e "SHOW TABLES;"   # activity 보여야 함
```

## 1. event_type 가정 검증 (실행 전 가드레일)

퍼널·CVR 마트(`mart_funnel`·`mart_cvr`)는 `event_type` 리터럴 `view`/`cart`/`purchase`를 쓴다. 실데이터 값이 다르면 두 SQL의 리터럴을 맞추고 테스트를 재실행해야 한다.

```bash
spark-sql --driver-memory 8g \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.driver.extraJavaOptions="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED" \
  -e "SELECT DISTINCT event_type FROM activity;"
```

실측 결과: `view` / `cart` / `purchase` 3종(`remove_from_cart` 없음). 가정 일치 → 리터럴 보정 불필요.

## 2. thin jar 빌드

```bash
sbt package   # target/scala-2.13/activity-log_2.13-0.1.0.jar
```

## 3. Gold 마트 전체 실행

```bash
spark-submit \
  --class com.activitylog.GoldMarts \
  --master "local[*]" \
  --driver-memory 8g \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.driver.extraJavaOptions="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED" \
  target/scala-2.13/activity-log_2.13-0.1.0.jar \
  --sql-dir "$(pwd)/sql/gold" --output "$(pwd)/output/gold"
```

옵션 의미:

- `--driver-memory 8g` — 마트의 `COUNT(DISTINCT user_id/session_id)`가 전체 `activity`(약 9천만 행)를 집계하므로 WAU 조회와 같은 힙이 필요(부족 시 OOM, [spark-sql-driver-memory-oom.md](../troubleshooting/spark-sql-driver-memory-oom.md)).
- `--add-opens=...sun.util.calendar=ALL-UNNAMED` — 마트가 `date`/`timestamp` 컬럼(`dim_date`, `week_start`)을 기록·집계할 때 필요. nio 묶음만으로는 `IllegalAccessException`([jdk-toolchain.md](../troubleshooting/jdk-toolchain.md) §2).
- `--sql-dir`/`--output` — SoT SQL 디렉터리와 parquet 출력 베이스(`output/`은 gitignore).

기대 출력(마트당 1줄, 총 10줄):

```
[OK] gold mart dim_date -> .../output/gold/dim_date (rows=62)
[OK] gold mart fact_daily_activity -> .../output/gold/fact_daily_activity (rows=186)
[OK] gold mart mart_dau -> .../output/gold/mart_dau (rows=62)
[OK] gold mart mart_wau -> .../output/gold/mart_wau (rows=9)
[OK] gold mart mart_mau -> .../output/gold/mart_mau (rows=3)
[OK] gold mart mart_stickiness -> .../output/gold/mart_stickiness (rows=62)
[OK] gold mart mart_funnel -> .../output/gold/mart_funnel (rows=9)
[OK] gold mart mart_cvr -> .../output/gold/mart_cvr (rows=9)
[OK] gold mart mart_revenue -> .../output/gold/mart_revenue (rows=9)
[OK] gold mart mart_retention -> .../output/gold/mart_retention (rows=45)
```

## 4. 검증

### 4.1. 가드레일 회귀 — `gold_mart_wau` = 정본 WAU

`gold_mart_wau`는 대시보드용 추가본이고, 제출 정본은 `sql/wau.sql`(Hive `activity`)이다. 둘은 같은 주 경계 정의(`date_trunc('week', to_date(event_date))`)를 쓰므로 수치가 일치해야 한다.

```bash
spark-sql --driver-memory 8g \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.driver.extraJavaOptions="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED" \
  -e "SELECT * FROM gold_mart_wau ORDER BY week_start;"
```

`results/wau_users.txt`·`wau_sessions.txt`와 9주 전부 1:1 일치(예: 2019-09-30 → users 818,388 / sessions 1,570,536, 2019-11-11 → users 1,543,309 / sessions 4,754,423).

### 4.2. 행수 sanity — 집계 fact 확인

```bash
spark-sql --conf spark.sql.session.timeZone=UTC \
  --conf spark.driver.extraJavaOptions="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED" \
  -e "SELECT count(*) FROM gold_fact_daily_activity; SELECT count(*) FROM gold_dim_date;"
```

`gold_fact_daily_activity`=186(62일×3타입), `gold_dim_date`=62. 9천만 행이 아니라 집계(수백 행)임을 확인 — 원자 fact는 Silver, Gold는 집계라는 모델 정합.

## 운영 메모

- **멱등** — 러너가 마트별 `overwrite` + `DROP/CREATE TABLE`이라 재실행 안전. 마트가 서로 독립(모두 `activity`만 의존)이라 일부만 다시 만들어도 됨.
- **롤백** — `gold_*`는 unmanaged 외부 테이블이라 DROP해도 `output/gold/` 데이터는 보존. `activity` 코어에는 영향 없음.
- **데이터 미커밋** — `output/gold/`는 gitignore(데이터 커밋 금지 규칙).
- **DuckDB/대시보드** — Gold parquet를 DuckDB로 export하고 정적 HTML/Streamlit이 읽는 단계는 Phase 2~3.
