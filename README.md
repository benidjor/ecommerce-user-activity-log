# 이커머스 사용자 Activity 로그 → Hive External Table + WAU

2019년 10~11월 이커머스 이벤트 CSV(약 13.7GB)를 **dedup → KST 변환 → 5분 갭 세션화**하여 **KST 일별 파티션 parquet(snappy)**로 적재하고, **Hive External Table**로 노출한 뒤 **WAU(user_id 기준 / session_id 기준) 2종**을 실제 Spark 실행으로 실측한다. (DE 사전 과제)

- 언어/실행: **Scala + sbt + 로컬 Spark(local 모드)**, `spark-submit`로 thin jar 실행.
- 카탈로그: 고전적 **Hive External Table + Spark 임베디드 Derby 메타스토어**(별도 서비스 불필요).
- 설계 근거 전문: [설계 스펙](docs/superpowers/specs/2026-06-07-wau-activity-log-design.md) · [구현 계획](docs/superpowers/plans/2026-06-07-wau-activity-log.md)

---

## 1. 아키텍처 / 데이터 흐름

```
raw CSV(UTC)  ──read──▶  Spark App (Scala)
2019-Oct.csv             1) DEDUP   자연키(user_id,event_time,event_type,product_id) 1건 유지
2019-Nov.csv             2) TZ      UTC→KST 1회 변환 → event_time_kst, event_date(파생)
                         3) SESSIONIZE  Window(user_id, event_time_utc)
                                    gap≥5분 → 새 세션, session_id = user_id + "_" + unix(세션시작)
                         4) WRITE   parquet+snappy → _staging/event_date=D/
                                    ▼ (검증 게이트 통과 시에만)
                         5) ATOMIC SWAP  rename _staging → event_date=D/
                         6) MARK    _SUCCESS (맨 마지막)
                                    ▼
        Hive External Table `activity` (PARTITIONED BY event_date, STORED AS PARQUET)
                                    ▼  (소비자는 _SUCCESS 있는 파티션만 읽음)
        WAU 쿼리 (ISO week 월요일 시작, KST) — COUNT(DISTINCT user_id) / COUNT(DISTINCT session_id)
```

핵심 설계 포인트:

- **파티션 = 이벤트의 `event_date`(KST)** (세션 단위 아님). 세션이 자정을 넘으면 같은 `session_id`가 두 파티션에 나뉘어 존재(정상).
- **세션화 정렬은 원본 `event_time_utc` 기준**, 파티션용 `event_date`만 KST에서 파생 → 단일 변환 지점으로 일관성 확보.
- **`session_id`는 결정적**(`min(세션시작시각)` 기반) → backfill 전역 세션화와 incremental(전날 lookback) 재계산 결과가 동일(멱등).

출력 스키마는 [설계 스펙 §3](docs/superpowers/specs/2026-06-07-wau-activity-log-design.md) 참고.

---

## 2. 실행법

상세·검증된 절차는 런북에 있다. 아래는 요약이다.

- 샘플(Oct 20만 행) end-to-end: [docs/runbook/sample-e2e.md](docs/runbook/sample-e2e.md)
- 전체(Oct+Nov) backfill + WAU 실측: [docs/runbook/full-backfill.md](docs/runbook/full-backfill.md)

```bash
# 0. 테스트 (14건/7 suites 통과 — 세션화 경계 케이스 중심)
sbt test

# 1. thin jar 빌드
sbt package   # target/scala-2.13/activity-log_2.13-0.1.0.jar

# 2. 전체 backfill 적재 (driver-memory 8g)
spark-submit --class com.activitylog.Main --master "local[*]" --driver-memory 8g \
  --conf spark.sql.session.timeZone=UTC --conf spark.sql.shuffle.partitions=200 \
  --conf spark.driver.extraJavaOptions="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
  target/scala-2.13/activity-log_2.13-0.1.0.jar \
  --mode backfill --input "$(pwd)/data/2019-Oct.csv,$(pwd)/data/2019-Nov.csv" --output "$(pwd)/output/activity"

# 3. 외부 테이블 등록
sed "s|{{OUTPUT_DIR}}|$(pwd)/output/activity|" sql/create_external_table.sql > /tmp/ddl.sql
spark-sql --conf spark.sql.session.timeZone=UTC -f /tmp/ddl.sql

# 4. WAU 조회 (driver-memory 8g 필수 — COUNT(DISTINCT session_id) OOM 방지)
spark-sql --driver-memory 8g --conf spark.sql.session.timeZone=UTC \
  --conf spark.sql.shuffle.partitions=200 -f sql/wau.sql
```

> JDK: `sbt test`는 JDK17, `spark-submit`/`spark-sql`은 brew JDK21. 둘 다 Spark 4 지원이며 `--add-opens`가 필요하다([docs/troubleshooting/jdk-toolchain.md](docs/troubleshooting/jdk-toolchain.md)).

---

## 3. WAU 실측 결과

**전체 Oct+Nov backfill(파티션 62개, 2019-10-01~2019-12-01 KST) 기준, 실제 `spark-sql` 실행 결과.** AI 주장이 아니라 실행 출력만 기록한다. 전문: [results/wau_users.txt](results/wau_users.txt) · [results/wau_sessions.txt](results/wau_sessions.txt).

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

사용 쿼리([sql/wau.sql](sql/wau.sql)):

```sql
-- 주 경계 = ISO week(월요일 시작), KST event_date 기준
SELECT date_trunc('week', to_date(event_date)) AS week_start,
       COUNT(DISTINCT user_id) AS wau_users
FROM activity GROUP BY 1 ORDER BY 1;

SELECT date_trunc('week', to_date(event_date)) AS week_start,
       COUNT(DISTINCT session_id) AS wau_sessions
FROM activity GROUP BY 1 ORDER BY 1;
```

해석:

- `session_id`에 user_id가 포함되므로 매주 **sessions ≥ users**(주별 활성 세션 수로 해석). 실측도 전 주에서 성립한다.
- 블랙프라이데이 주(11-11~)에 세션이 4.75M로 급증 — 원본 행수 급증(11/15~17)과 일치한다.
- 첫 주(09-30)는 데이터가 10-01부터, 마지막 주(11-25)는 12-01 파티션까지만 있어 **둘 다 부분 주**다(전체 7일 아님).

---

## 4. 언어 선택 (Scala) 사유

- **Spark 네이티브 언어** — 최신 API/성능을 1급으로 지원하고, `Dataset`의 타입 안정성을 활용할 수 있다.
- **세션화 로직 표현력** — 윈도우 함수(`lag`/`sum over`)·UDF 등 핵심 로직을 타입 안전하게 표현한다.
- **대안 비교** — Java도 가능하나 Spark 관용 표현·간결성에서 Scala 우위. PySpark는 과제 요구상 제외.

> **대시보드 레이어는 Python(경계 명시).** 파이프라인 본체·`GoldMarts` 등 **Spark Application은 Scala**로 유지한다. Phase 2의 마트 export(`dashboard/export_duckdb.py`)·정적 빌드(`dashboard/build.py`)·CI는 Spark 외 서빙/오케스트레이션이라 Python(duckdb/pandas/jinja2)을 쓴다. 제출용 WAU 정본은 여전히 Hive `activity`의 `sql/wau.sql`이며, 대시보드의 `marts.duckdb`는 Gold 서빙 사본이다(Hive 대체 아님).

---

## 5. 설계 결정 요약 (결정 로그)

전체 근거는 [설계 스펙 §7](docs/superpowers/specs/2026-06-07-wau-activity-log-design.md)에 있다. 핵심만 추리면:

| # | 결정 | 선택 |
|---|---|---|
| 1 | session_id | 원본 `user_session` 무시, 결정적 id 직접 생성(`user_id + "_" + unix(세션시작)`). 원본은 검증용 보존 |
| 2 | 자정/파티션 경계 | 방식 A — 이벤트 `event_date`(KST) 기준 파티션 + backward-only 세션화. backfill=전역 1회, 증분=전날 lookback |
| 3 | WAU 주 경계 | ISO week(월요일 시작), KST. `date_trunc('week', to_date(event_date))` |
| 4 | 타임존 | 적재 시 UTC→KST 1회 변환(`event_time_kst`·`event_date` 파생), 원본 UTC 보존(세션 정렬 기준) |
| 5 | dedup | 자연키 1건 유지, **세션화 전** 수행(갭 계산 왜곡 방지). 실측 중복률 ≈0.06% |
| 6 | 장애 복구 | staging+rename 원자 교체 + 검증 게이트 + 파티션별 `_SUCCESS` + 멱등 overwrite. retry/알람은 Airflow 콜백+Discord. checkpoint 미사용 |
| 7 | 테이블 | Hive External Table + 임베디드 Derby 메타스토어. Iceberg/Delta는 "프로덕션 확장" |
| 8 | 실행 모델 | Backfill 1회로 과제 완수, Incremental은 설계로 대응(`--mode`/`--run-date`) |
| 9 | 언어 | Scala + sbt + 로컬 Spark(local 모드), spark-submit thin jar |

---

## 6. AI 도구 활용

과제 요구사항에 따라 **무엇을 AI로, 무엇을 직접** 했는지와 프롬프트 전략을 명시한다.

### 6.1. 사용 도구

- **방법론** — `superpowers`(brainstorming · test-driven-development · verification-before-completion) + `karpathy-guidelines`(Think · Simplicity · Surgical · Goal).
- **설계/계획** — `superpowers:writing-plans`로 설계 스펙 + 구현 계획을 로컬 산출.
- **구현** — `superpowers:subagent-driven-development`로 계획을 Task 단위 실행(태스크마다 구현 → spec 리뷰 → code quality 리뷰 게이트).
- **검증** — `superpowers:verification-before-completion`(WAU는 실제 Spark 실행 결과로만 보고).
- **의도적 미사용** — ultraplan(웹 Claude Code+GitHub 필요, 가치 중복) · Ouroboros(외부·무거움). 검토 후 과제 규모상 로컬 도구로 대체.
- **대시보드(Phase 2)**: DuckDB(임베디드 파일) + pandas + Jinja2 정적 빌드, Chart.js(CDN), GitHub Actions → GitHub Pages. (Spark 외 서빙 레이어 — Python)

### 6.2. 역할 분담

- **AI로 한 것** — 빌드/테스트 골격·CLI 파싱 등 보일러플레이트 스캐폴딩, 설계 문서 초안화, 데이터 디자인 패턴 책 교차참조, 트러블슈팅·런북 문서화.
- **직접 설계·검증한 것** — 숨은 결정 9개 확정(세션 정의·타임존·주 경계·dedup·복구·자정 경계 등), 세션화 로직 TDD(경계 케이스), **WAU 수치 실측 검증**, 원본 `user_session` vs 생성 `session_id` 비교 검증.

### 6.3. 프롬프트 전략

`brainstorming`으로 숨은 결정 선질문 → 설계 스펙 승인 → `TDD`로 핵심 로직(세션화) → **샘플 우선** end-to-end 1회 → **전체 1회 실측**. 각 코드 태스크는 karpathy 4원칙(불확실하면 질문, 최소 코드, 수술적 변경, 검증까지 루프)을 적용했다.

---

## 7. 알려진 한계 · 프로덕션 확장

| 항목 | 현재 | 확장 방향 |
|---|---|---|
| 파티션 overwrite | 파티션별 staging+rename 루프(원자성·검증 게이트 우선) | `partitionOverwriteMode=dynamic`은 코드는 줄지만 비원자·검증 약화 → 미채택 |
| 쓰기 원자성 | rename은 로컬/HDFS에서 원자적 | S3는 copy+delete라 비원자 → **Iceberg/Delta committer**로 승급 |
| WAU 쿼리 이중화 | `WauQueries.scala`(테스트용) + `sql/wau.sql`(실측용)에 동일 쿼리 | SoT 이원화 → 한쪽을 다른 쪽에서 로드해 단일화 여지 |
| `price` 타입 | `double`(현 과제는 집계에 price 미사용) | 금액 정밀도가 필요하면 `Decimal`로(부동소수 오차 회피) |
| dedup 결정성 | 자연키 1건 유지 + 안정 정렬 `row_number`로 결정 | 완전 동일 행은 어느 것이 남아도 결과 동일(무해) |
| 검증 게이트 ④ | "결과 파티션 날짜==기대"는 Main이 `event_date` 필터 후 파티션별 write라 구조적으로 보장 → 별도 단언 생략 | — |
| `ActivityPipeline.readAndTransform` | 미사용(Main은 raw 재사용 위해 `transform(raw)` 직접 호출) | 정리 후보 |
| 입력 견고성 | PERMISSIVE 파싱 + 손상 행 카운트 | 풀 dead-letter quarantine + replay |
| 세션화 | backfill 전역 / incremental 전날 lookback | 초장시간 세션 대비 Stateful Sessionizer(state store) |
| 오케스트레이션 | (선택) Airflow BashOperator + spark-submit, `catchup=True`=backfill | SparkSubmitOperator |

---

## 8. 프로젝트 구조 / 문서 맵

```
src/main/scala/com/activitylog/   # 파이프라인: Schema, Dedup, TimeUtils, Sessionizer,
                                  #   ActivityPipeline, PartitionWriter, WauQueries, Main
src/test/scala/com/activitylog/   # 테스트(세션화 경계 케이스 중심)
sql/                              # create_external_table.sql, wau.sql
results/                          # WAU 실측치(커밋됨)
docs/specs·plans                  # 설계 스펙 / 구현 계획
docs/runbook                      # 실행 절차(sample-e2e, full-backfill)
dashboard/                        # Phase 2 서빙: export_duckdb.py(Gold→marts.duckdb), build.py+templates/(정적 index.html). 실행: docs/runbook/dashboard.md
docs/troubleshooting              # 환경·빌드·실행 이슈 모음
docs/conventions                  # 커밋·PR 컨벤션(SoT)
```
