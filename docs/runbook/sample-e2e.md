# 런북: 샘플 End-to-End 적재 + 외부 테이블 + WAU

샘플 CSV를 적재해 Hive External Table로 노출하고 WAU를 조회하는, **실제로 실행·검증된** 절차.
전체 backfill (Task 12)도 입력 경로만 바꾸면 동일함.

실행 모델은 설계 결정 8에 따라 **spark-submit + thin jar**.
적재는 spark-submit, SQL (테이블 등록·WAU)은 `spark-sql -f <파일>`로 함.
`sbt runMain`은 Spark 의존성이 `Provided`라 실패하므로 쓰지 않음 ([spark-provided-classpath](../troubleshooting/spark-provided-classpath.md) 참조)

> 명령 안에서 `\` (백슬래시)는 "다음 줄에 이어짐"을 뜻함.
> 여러 줄 명령은 통째로 복사해 실행함.
> `\`로 이어지는 줄은 끝에 주석을 달 수 없어, 각 옵션 설명은 코드 블록 **아래 목록**에 둠.

```
사전 준비          1. 적재                   2. 외부 테이블 등록     3. WAU 조회
thin jar 빌드  ──▶  spark-submit backfill  ──▶  spark-sql DDL      ──▶  spark-sql wau.sql
```

## 사전 준비: thin jar 빌드

```bash
sbt package   # Spark 제외 thin jar 생성 → target/scala-2.13/activity-log_2.13-0.1.0.jar
```

## 1. 적재 (backfill)

샘플은 Oct CSV의 첫 20만 행을 씀.

```bash
mkdir -p data/sample                                          # 샘플 폴더 생성(-p: 있어도 에러 없음)
head -200000 data/2019-Oct.csv > data/sample/oct_sample.csv   # 앞 20만 행만 잘라 저장(헤더 포함)

rm -rf output/activity   # 이전 출력 정리(멱등이라 생략 가능; 깨끗한 재적재용)
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

각 옵션 설명:

- `--class com.activitylog.Main` — jar 안에서 실행할 메인 클래스 (진입점)
- `--master "local[*]"` — 로컬 모드로 실행, `*`는 가용 CPU 코어 전부 사용.
- `--driver-memory 4g` — 드라이버 (=로컬 실행 JVM) 힙 메모리 4GB.
- `--conf spark.sql.session.timeZone=UTC` — 세션 타임존을 UTC로 고정 (`TimeUtils`가 UTC → KST 변환을 전제)
- `--conf spark.driver.extraJavaOptions="--add-opens=..."` — JDK 17+의 막힌 내부 모듈을 열어 Spark 리플렉션을 허용 ([jdk-toolchain](../troubleshooting/jdk-toolchain.md) §2)
- `target/.../activity-log_2.13-0.1.0.jar` — 실행할 thin jar (여기까지가 spark-submit 인자)
- `--mode backfill` / `--input` / `--output` — **앱 자체 인자** (jar 뒤에 옴). 전량 적재 모드, 입력 CSV, 출력 베이스 경로.

기대 출력:

```
[OK] wrote partition event_date=2019-10-01 (ok rows=199853)
[DONE] mode=backfill partitions=1
```

완료는 파티션별 `_SUCCESS` 마커로 확인함.

```bash
ls output/activity/event_date=2019-10-01/_SUCCESS   # 존재하면 그 파티션 적재 완료
```

## 2. 외부 테이블 등록 (spark-sql)

DDL은 `sql/create_external_table.sql`에 있고 `LOCATION`이 `{{OUTPUT_DIR}}` placeholder.
실제 절대경로로 치환해 실행함.

```bash
sed "s|{{OUTPUT_DIR}}|$(pwd)/output/activity|" sql/create_external_table.sql > /tmp/ddl.sql   # placeholder를 실제 경로로 치환
spark-sql \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.driver.extraJavaOptions="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
  -f /tmp/ddl.sql
```

각 옵션 설명:

- `sed "s|찾을것|바꿀것|" 파일` — 텍스트 치환. 여기선 `{{OUTPUT_DIR}}`를 현재 경로 (`$(pwd)/output/activity`)로 바꿔 `/tmp/ddl.sql`에 저장. 구분자로 `/` 대신 `|`를 써서 경로의 `/`와 충돌을 피함.
- `spark-sql` — SQL 전용 CLI (기본 Hive catalog). `--conf`는 spark-submit과 동일.
- `-f /tmp/ddl.sql` — 이 SQL 파일을 실행 (`CREATE EXTERNAL TABLE` + `MSCK REPAIR`)

> `spark-sql`은 기본이 Hive catalog라 `CREATE EXTERNAL TABLE`·`MSCK REPAIR`가 동작함. (spark-shell로 할 때는 `--conf spark.sql.catalogImplementation=hive`가 필요 — [hive-external-table](../troubleshooting/hive-external-table.md) 참조)
> `MSCK REPAIR`는 새 날짜 파티션이 늘 때마다 다시 실행해야 인식됨 (증분 적재 후에도 호출)

## 3. WAU 조회 (spark-sql)

쿼리는 `sql/wau.sql` (user/session 2종)에 있음.

```bash
spark-sql --conf spark.sql.session.timeZone=UTC -f sql/wau.sql   # WAU 2종 조회(테이블 등록 후 실행)
```

샘플 실측 결과 (첫 20만 행, 전부 2019-10-01 → ISO 주 2019-09-30 월요일):

```
2019-09-30 00:00:00    38439     # 1행: wau_users (주 시작일 \t 고유 user_id 수)
2019-09-30 00:00:00    47823     # 2행: wau_sessions (주 시작일 \t 고유 session_id 수)
```

## 운영 메모

- **멱등 재실행** — 같은 날짜를 다시 적재하면 `PartitionWriter`가 staging+rename으로 해당 파티션만 교체함. 다른 날짜는 안전. 실패 후 재실행은 같은 명령을 다시 돌리면 됨.
- **부분 실패 복구** — staging 도중 중단돼도 final 파티션은 직전 상태를 유지함. 재실행 시 이전 staging 잔여는 자동 삭제됨.
- **완료 감지** — `event_date=<날짜>/_SUCCESS` 존재로 판단 (Airflow `FileSensor`가 이를 사용)
- **로컬 산출물** — `output/`, `metastore_db/`, `derby.log`, `spark-warehouse/`는 gitignore 대상 (커밋 금지)
