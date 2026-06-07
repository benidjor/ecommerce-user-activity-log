# 런북: 샘플 End-to-End 적재 + 외부 테이블 + WAU

샘플 CSV를 적재해 Hive External Table로 노출하고 WAU를 조회하는, **실제로 실행·검증된** 절차다. 전체 backfill(Task 12)도 입력 경로만 바꾸면 동일하다.

실행 모델은 설계 결정 8에 따라 **spark-submit + thin jar**다. 적재는 spark-submit, SQL(테이블 등록·WAU)은 `spark-sql -f <파일>`로 한다. `sbt runMain`은 Spark 의존성이 `Provided`라 실패하므로 쓰지 않는다([spark-provided-classpath](../troubleshooting/spark-provided-classpath.md) 참조).

## 사전 준비: thin jar 빌드

```bash
sbt package   # target/scala-2.13/activity-log_2.13-0.1.0.jar
```

## 1. 적재 (backfill)

샘플은 Oct CSV의 첫 20만 행을 쓴다.

```bash
mkdir -p data/sample
head -200000 data/2019-Oct.csv > data/sample/oct_sample.csv

rm -rf output/activity   # 깨끗한 재적재(멱등하므로 생략 가능)
spark-submit \
  --class com.activitylog.Main \
  --master "local[*]" \
  --driver-memory 4g \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.driver.extraJavaOptions="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
  target/scala-2.13/activity-log_2.13-0.1.0.jar \
  --mode backfill \
  --input "$(pwd)/data/sample/oct_sample.csv" \
  --output "$(pwd)/output/activity"
```

기대 출력:

```
[OK] wrote partition event_date=2019-10-01 (ok rows=199853)
[DONE] mode=backfill partitions=1
```

완료는 파티션별 `_SUCCESS` 마커로 확인한다.

```bash
ls output/activity/event_date=2019-10-01/_SUCCESS
```

## 2. 외부 테이블 등록 (spark-sql)

DDL은 `sql/create_external_table.sql`에 있고 `LOCATION`이 `{{OUTPUT_DIR}}` placeholder다. 실제 절대경로로 치환해 실행한다.

```bash
sed "s|{{OUTPUT_DIR}}|$(pwd)/output/activity|" sql/create_external_table.sql > /tmp/ddl.sql
spark-sql --conf spark.sql.session.timeZone=UTC \
  --conf spark.driver.extraJavaOptions="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
  -f /tmp/ddl.sql
```

> `spark-sql`은 기본이 Hive catalog라 `CREATE EXTERNAL TABLE`·`MSCK REPAIR`가 동작한다. (spark-shell로 할 때는 `--conf spark.sql.catalogImplementation=hive`가 필요 — [hive-external-table](../troubleshooting/hive-external-table.md) 참조)
> `MSCK REPAIR`는 새 날짜 파티션이 늘 때마다 다시 실행해야 인식된다(증분 적재 후에도 호출).

## 3. WAU 조회 (spark-sql)

쿼리는 `sql/wau.sql`(user/session 2종)에 있다.

```bash
spark-sql --conf spark.sql.session.timeZone=UTC -f sql/wau.sql
```

샘플 실측 결과(첫 20만 행, 전부 2019-10-01 → ISO 주 2019-09-30 월요일):

```
2019-09-30 00:00:00    38439     # wau_users
2019-09-30 00:00:00    47823     # wau_sessions
```

## 운영 메모

- **멱등 재실행** — 같은 날짜를 다시 적재하면 `PartitionWriter`가 staging+rename으로 해당 파티션만 교체한다. 다른 날짜는 안전. 실패 후 재실행은 같은 명령을 다시 돌리면 된다.
- **부분 실패 복구** — staging 도중 죽어도 final 파티션은 직전 상태를 유지한다. 재실행 시 이전 staging 잔여는 자동 삭제된다.
- **완료 감지** — `event_date=<날짜>/_SUCCESS` 존재로 판단(Airflow `FileSensor`가 이를 사용).
- **로컬 산출물** — `output/`, `metastore_db/`, `derby.log`, `spark-warehouse/`는 gitignore 대상(커밋 금지).
