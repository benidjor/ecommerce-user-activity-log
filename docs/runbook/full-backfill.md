# 런북: 전체 (Oct+Nov) Backfill + WAU 실측 (Task 12)

전체 원본 CSV (`2019-Oct.csv` 5.3G + `2019-Nov.csv` 8.4G)를 적재해 외부 테이블로 노출하고 WAU 2종을 **실측**하는, 실제로 실행·검증된 절차.
절차 자체는 [sample-e2e.md](sample-e2e.md)와 동일하고 **(1) 입력을 전체 CSV로, (2) 드라이버 메모리를 8g로** 키운 것만 다름.
각 옵션의 의미는 sample-e2e 런북을 참고함.

> 실측일 2026-06-08 기준.
> 적재 결과: **파티션 62개 (2019-10-01 ~ 2019-12-01 KST)**.
> 12-01 파티션은 11-30 UTC 말 이벤트가 KST (+9h)로 넘어간 정상 산출물.

## 1. thin jar 빌드

```bash
sbt package   # target/scala-2.13/activity-log_2.13-0.1.0.jar
```

## 2. 전체 적재 (backfill)

샘플과 달리 드라이버 메모리를 8g로 키움 (세션화 window 셔플 + 전체 persist)

```bash
rm -rf output/activity   # 멱등이라 생략 가능(깨끗한 재적재용)
spark-submit \
  --class com.activitylog.Main \
  --master "local[*]" \
  --driver-memory 8g \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.driver.extraJavaOptions="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
  target/scala-2.13/activity-log_2.13-0.1.0.jar \
  --mode backfill \
  --input "$(pwd)/data/2019-Oct.csv,$(pwd)/data/2019-Nov.csv" \
  --output "$(pwd)/output/activity"
```

sample-e2e와 다른 점만:

- `--driver-memory 8g` — 전체 데이터의 세션화 셔플·`persist`·행폭증 검증을 감당할 힙 (4g는 부족할 수 있음)
- `--conf spark.sql.shuffle.partitions=200` — 세션화·집계 셔플을 200개로 분할 (기본보다 명시)
- `--input "...Oct.csv,...Nov.csv"` — 쉼표로 두 파일을 한 번에 (코드가 `split(",")`로 처리함)

기대 출력 (마지막 라인):

```
[OK] wrote partition event_date=2019-12-01 (ok rows=587484)
[DONE] mode=backfill partitions=62
```

완료 확인:

```bash
ls -d output/activity/event_date=* | wc -l        # 62
find output/activity -name _SUCCESS | wc -l        # 62 (파티션마다 마커)
ls -d output/activity/*_staging* 2>/dev/null || echo "staging 잔여 없음(원자 교체 정상)"
```

## 3. 외부 테이블 등록

sample-e2e와 동일 (placeholder 치환 → `spark-sql -f`)

```bash
sed "s|{{OUTPUT_DIR}}|$(pwd)/output/activity|" sql/create_external_table.sql > /tmp/ddl.sql
spark-sql \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.driver.extraJavaOptions="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
  -f /tmp/ddl.sql
```

등록 확인:

```bash
spark-sql --conf spark.sql.session.timeZone=UTC -e "SHOW PARTITIONS activity;" | grep -c "event_date="   # 62
```

## 4. WAU 조회 (driver-memory 8g 필수)

> **주의** — WAU 조회에도 `--driver-memory 8g`가 필요함.
> 기본 메모리로 돌리면 `COUNT(DISTINCT session_id)`(고카디널리티)가 메모리 부족 (OOM)으로 실패함.
> 상세: [spark-sql-driver-memory-oom.md](../troubleshooting/spark-sql-driver-memory-oom.md)

```bash
spark-sql \
  --driver-memory 8g \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.driver.extraJavaOptions="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
  -f sql/wau.sql
```

실측 결과 (2026-06-08, 9개 ISO 주). 전문은 `results/wau_users.txt`·`results/wau_sessions.txt`:

| week_start (KST, 월요일) | WAU users | WAU sessions |
|---|---:|---:|
| 2019-09-30 | 818,388 | 1,570,536 |
| 2019-10-07 | 1,057,958 | 2,154,180 |
| 2019-10-14 | 1,090,898 | 2,257,214 |
| 2019-10-21 | 1,093,146 | 2,153,837 |
| 2019-10-28 | 1,054,722 | 2,115,233 |
| 2019-11-04 | 1,321,141 | 2,751,842 |
| 2019-11-11 | 1,543,309 | 4,754,423 |
| 2019-11-18 | 1,376,755 | 2,876,494 |
| 2019-11-25 | 1,176,254 | 2,376,156 |

- 매주 sessions > users (사용자당 세션 ≥ 1)로 정합.
- 11-11 시작 주에 세션이 4.75M로 급증 — 원본 행수 급증 (11/15~17)과 일치.
- **경계 주의**: 첫 주 (09-30)는 데이터가 10-01부터, 마지막 주 (11-25)는 12-01 파티션까지만 있어 **둘 다 부분 주**(전체 7일이 아님)

## 5. 추가 기간 처리 (incremental)

요구사항 4의 "추가 기간 처리 대응"은 incremental 모드로 처리.
External Table라 데이터 파일과 테이블 정의가 분리돼 있어, 새 기간은 **새 `event_date=` 파티션을 쓰고 등록만** 하면 됨 (테이블 재생성·전체 재적재 불필요)

동작:

- `--mode incremental --run-date D` — 변환은 입력 전체에 적용하되, **기록은 `event_date == D` 파티션만** 함 (`Main.scala`의 `(mode, runDate) match`)
- **전날 (D-1) lookback** — 입력에 D-1을 함께 넣어 D 첫 이벤트의 직전 갭(세션 경계)을 정확히 계산. D-1 행은 기록 대상에서 제외되므로 D 파티션만 멱등 overwrite.
- `session_id`가 결정적이라 incremental 결과는 backfill과 동일 (멱등). 두 방식이 같은 결과를 내는지 실측한 근거 (D=2019-10-03, 양방향 `EXCEPT ALL`=0): [results/backfill_incremental_equiv.txt](../../results/backfill_incremental_equiv.txt)

절차:

```bash
# 1) 새 날짜만 적재 (예: 2019-12-01). 입력에는 대상일 + 전날(lookback)이 포함되게 구성.
spark-submit --class com.activitylog.Main --master "local[*]" --driver-memory 8g \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.driver.extraJavaOptions="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
  target/scala-2.13/activity-log_2.13-0.1.0.jar \
  --mode incremental --run-date 2019-12-01 --input "$(pwd)/data/2019-Dec.csv" --output "$(pwd)/output/activity"

# 2) 새 event_date= 파티션을 메타스토어에 등록 (새 날짜가 늘 때마다 재실행)
spark-sql --conf spark.sql.session.timeZone=UTC -e "MSCK REPAIR TABLE activity;"

# 3) 등록 확인
spark-sql --conf spark.sql.session.timeZone=UTC -e "SHOW PARTITIONS activity;"
```

- Airflow `activity_daily` DAG가 바로 이 흐름 (일별 incremental + repair)을 `@daily catchup`으로 자동화함 → [airflow.md](airflow.md)

## 운영 메모

[sample-e2e.md](sample-e2e.md)의 운영 메모 (멱등 재실행·부분 실패 복구·완료 감지·gitignore)와 동일.
