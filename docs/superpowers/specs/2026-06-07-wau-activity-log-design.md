# 설계 스펙 — 이커머스 사용자 Activity 로그 → Hive External Table + WAU

- 작성일: 2026-06-07
- 대상 데이터: `2019-Oct.csv`(5.3GB), `2019-Nov.csv`(8.4GB) — Kaggle ecommerce behavior (multi-category store)
- 언어: **Scala** (Spark Application), 빌드: **sbt**, 실행: 로컬 Spark(local 모드)
- 산출물: KST 일별 파티션 Hive **External Table** + WAU 2종(user_id / session_id)

---

## 1. 목표 / 요구사항 매핑

| 요구사항 | 본 설계의 충족 방식 |
|---|---|
| activity 로그를 Hive table로 제공 (Spark App) | Scala Spark App → parquet/snappy → **Hive External Table** |
| KST 기준 daily partition | `event_date`(KST yyyy-MM-dd) 파티션 |
| user_id 내 5분 갭 세션 분할, 새 session_id | Window(lag→gap≥5분→누적합) + **결정적 session_id** |
| 재처리 후 parquet, snappy | 파티션 단위 멱등 overwrite, parquet+snappy |
| External Table, 추가 기간 대응 | 날짜 파라미터화 + `ADD PARTITION`/`MSCK REPAIR` |
| 배치 장애 복구 장치 | **staging정리+rename 원자 교체 + 검증 게이트 + `_SUCCESS` 마커 + 멱등 재시도 + Discord 알람** |
| WAU(user_id 기준 / session_id 기준) + 쿼리 | ISO주(월요일 시작, KST) 기준 2종 쿼리 + 실측 |
| 언어(Scala/Java) 선택 사유 | README에 기술 (아래 §6) |

---

## 2. 데이터 흐름도 (텍스트)

```
                          ┌─────────────────────────────────────────────────────────┐
  raw CSV (UTC)           │  Spark App (Scala)  ── 파라미터: --run-date 또는 --range.   │
  2019-Oct.csv ──┐        │                                                         │
  2019-Nov.csv ──┴──read──▶  1) READ  대상일 + 전날 lookback (방식 A, backward-only).   │
                          │  2) DEDUP 자연키(user_id,event_time,event_type,product)   │
                          │  3) TZ    UTC→KST 1회 변환 → event_time_kst, event_date   │
                          │  4) SESSIONIZE  Window(user_id, event_time)             │
                          │        gap=lag차, gap≥5분→새세션, 누적합→session_seq         │
                          │        session_id = user_id + "_" + unix(session_start) │
                          │  5) FILTER event_date == 대상일 (lookback은 문맥만)         │
                          └───────────────────────┬─────────────────────────────────┘
                                                  ▼
                             6) WRITE parquet+snappy →  .../activity/_staging/event_date=D/
                                                        ▼ (전체 성공 시에만)
                             7) ATOMIC SWAP  rename _staging → .../activity/event_date=D/
                                                        ▼
                             8) MARK  _SUCCESS 파일 생성 (맨 마지막)
                                                        ▼
                          ┌───────────────────────────────────────────────────────────────┐
                          │  Hive External Table  `activity`  (LOCATION = .../activity)   │
                          │  STORED AS PARQUET, PARTITIONED BY (event_date)               │
                          │  새 파티션 인식: MSCK REPAIR TABLE / ALTER TABLE ADD PARTITION   │
                          └───────────────────────────────┬───────────────────────────────┘
                                                           ▼  (소비자는 _SUCCESS 있는 파티션만 읽음)
                          ┌──────────────────────────────────────────────────────────────┐
                          │  WAU 쿼리 (ISO week, 월요일 시작, KST)                           │
                          │   6-a) user_id 기준    : COUNT(DISTINCT user_id)   per week   │
                          │   6-b) session_id 기준 : COUNT(DISTINCT session_id) per week  │
                          └──────────────────────────────────────────────────────────────┘

                          [오케스트레이션 — 선택/어필] Airflow @daily, catchup=True (start=2019-10-01)
                             task1: BashOperator → spark-submit --run-date {{ ds }}
                             task2: _SUCCESS 확인(FileSensor)
                             → catchup이 곧 Backfill, 이후 매일 1 run = 정상 운영 (본체 완성·검증 후 추가)
```

---

## 3. 출력 스키마 (`activity` External Table)

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `event_time` (코드: `event_time_utc`) | timestamp | **원본 UTC** (감사·재현용 보존, 세션화 정렬 기준) |
| `event_time_kst` | timestamp | UTC→KST 1회 변환 (파생) |
| `event_type` | string | view/cart/purchase 등 |
| `product_id` | long | |
| `category_id` | long | |
| `category_code` | string | nullable |
| `brand` | string | nullable |
| `price` | double | |
| `user_id` | long | |
| `user_session` | string | **원본 세션(검증용 참고 컬럼, 미사용)** |
| `session_id` | string | **생성**: `user_id + "_" + unix(session_start)` (결정적) |
| `event_date` | string (**partition**) | KST 기준 `yyyy-MM-dd` |

> 파티션 = **이벤트의 event_date(KST)**. 세션이 자정을 넘으면 같은 `session_id`가 두 파티션에 나뉘어 존재(정상). 세션 단위 파티션 아님.

---

## 4. 핵심 로직 명세

### 4.1 세션화 (TDD 대상)
정렬·세션화·`session_id` 산출은 **원본 `event_time_utc`(단조증가)** 기준. 파티션용 `event_date`만 KST에서 파생(§4.6).
```
// 1단계: 갭으로 새 세션 경계 표시
W      = Window.partitionBy(user_id).orderBy(event_time_utc, event_type, product_id)  // 동일초 tie 결정적 정렬
prev   = lag(event_time_utc) over W
gap    = unix_timestamp(event_time_utc) - unix_timestamp(prev)    // 초 단위 차 (timestamp 직접 빼기 불가)
is_new = (prev is null) OR (gap >= 300)         // "5분 이상" = ≥300초 → 새 세션 (경계 포함)
seq    = sum(is_new) over W rows(unbounded preceding → current)   // user 내 세션 시퀀스

// 2단계: 세션 시작시각 → 결정적 id (별도 윈도우 단계, seq 확정 후)
Wg            = Window.partitionBy(user_id, seq)
session_start = min(event_time_utc) over Wg     // 행 순서와 무관하게 결정적
session_id    = concat(user_id, "_", unix_timestamp(session_start))
```
- **결정성**: `session_start`가 min 기반이라 동일초 tie·재실행·lookback 재계산에도 동일 id 산출. 2차 정렬키(`event_type, product_id`)로 tie 정렬도 고정.
- **TDD 경계 케이스**: 정확히 5분(=경계 포함→새 세션) / 동일초(gap=0→같은 세션) / 단일 이벤트(세션 1개) / 자정 통과(같은 id 유지) / dedup 후 갭 정상 / 사용자·이벤트 각 1개.

### 4.2 자정 경계 = 방식 A (Bounded Lookback) — 2가지 실행 모드
- **Backfill 모드 (과제 최종 실행)**: 전체 범위(Oct+Nov)를 한 DataFrame으로 읽어 **user_id 전역 세션화**. 윈도우가 user_id로 모으므로 자정·월 경계가 자연 해소 → **lookback 불필요·정확**. 세션화 후 `event_date`로 분배 저장.
- **Incremental 모드 (추가 기간 대응)**: 대상일 D + 전날(D-1)을 함께 읽어 세션화 → `event_date==D`만 write. D-1은 **읽기만**(수정 안 함) → forward-dependency 없음, 날짜별 독립 재실행.
- **두 모드 결과 동일**: `session_id`가 결정적이라 backfill 산출 id와 incremental(lookback) 재계산 id가 일치(멱등).
- **한계(문서화)**: incremental에서 lookback(전날) 범위를 넘는 초장시간 세션은 시작점 오인 가능 → 현실 세션 길이상 무시. 확장 시 책 Stateful Sessionizer(state store)로 승급.

### 4.3 dedup
- 키: `(user_id, event_time, event_type, product_id)` 동일 → 1건만 유지.
- **결정적 선택**: 키 동일·나머지 컬럼(price/brand 등) 상이 시, 안정 정렬 후 `row_number()=1` 채택(또는 전체 컬럼 `dropDuplicates`)으로 재실행 동일성 보장.
- **세션화 전** 수행(중복이 갭 계산을 왜곡하지 않도록). 실측 중복률 ≈ 0.06%(완전동일 0.058% / 자연키 0.062%).

### 4.4 장애 복구 / 안정성

**(1) 쓰기 안전 (write-path)**
- **staging 정리**: 잡 시작 시 `_staging/event_date=D/` 잔여물 삭제(이전 크래시 찌꺼기 제거) 후 새로 씀 → 재처리 오염 방지.
- **원자 교체**: staging에 완전히 쓴 뒤 `rename`으로 최종 파티션 교체(부분 쓰기로 인한 깨진 파티션 방지). 순수 parquet `.mode(overwrite)`는 비트랜잭셔널. 로컬/HDFS는 rename 원자적, **S3는 비원자(copy+delete) → 프로덕션은 Iceberg/Delta committer로 승급**.
- **검증 게이트 (Audit-Write-Audit-Publish, 책 Ch9) — core, 최소 형태**: 교체 *전에* staging sanity 단언 **3~5개**만 — ①행수>0 ②키(`user_id`,`event_time`) not null ③출력 행수가 입력(dedup 후) 대비 합리적 범위 ④결과 파티션 날짜==기대. **실패 시 교체 중단 + 알람** → "성공적으로 쓰레기 발행" 방지. (품질 프레임워크(GE/Deequ)는 과잉 → 미사용)

**(2) 완료 신호 & 관측**
- **`_SUCCESS` 마커**: 파티션별로 **맨 마지막**에 생성(Readiness Marker). 소비자는 마커 있는 파티션만 읽음.
- **run 메타데이터 로깅**: run_date, 입력행수, 중복제거수, 출력행수, 세션수, 소요시간, status → 디버깅·알람 본문 재료.

**(3) 재시도 & 멱등성**
- **멱등 단위 = 하루(event_date)**: 결정적 `session_id` + 파티션 overwrite → 재실행해도 동일 결과.
- **재시도**: 일시적 오류(I/O·OOM)는 Airflow `retries`/`retry_delay`로 자동 backoff. 영구 오류는 알람 후 수동 개입.
- **실패 격리(선택)**: 범위 처리 시 per-date try → 한 일자 실패가 다른 일자를 막지 않고 실패 일자만 수집·보고.

**(4) 알림 & 재처리 (Discord)**
- **성공/실패 알람**: Airflow `on_success_callback`/`on_failure_callback`(또는 잡 드라이버 try/finally)에서 **Discord 웹훅** 호출 → run 메타데이터 + 실패 시 에러 요약·로그/Airflow run 딥링크.
- **재처리 트리거**: 커스텀 Discord 버튼/봇은 **만들지 않음**(상시 봇+인터랙션 엔드포인트+API 연동 = 과한 인프라). 대신 알람 딥링크 → **Airflow 네이티브 Clear/Retry**(또는 `spark-submit --run-date D` 재실행)로 재처리 — 멱등 설계라 안전. (원클릭 봇은 가능하나 범위 초과 → README 언급만)

**(5) 입력 견고성 (Dead-Letter — 책 Ch3) — core, 경량 형태**
- 손상 CSV 행: **PERMISSIVE 파싱 + 손상 행 카운트 + 임계 초과 시 알람** → 행 하나가 전체 배치를 죽이지 않음(FAILFAST 금지). "추가 기간 처리 대응"상 미래 더러운 데이터 대비.
- **풀 quarantine 저장소 + replay 파이프라인은 미구현**(정제된 일회성 데이터엔 과잉) → README "확장"으로 언급.

**(6) 미사용**
- **checkpoint**: 스트리밍 전용. 배치는 파티션 구조로 복구 단위가 명확(책 Ch3).

### 4.5 WAU (ISO week, 월요일 시작, KST)
```sql
-- 주 식별자 = 그 주의 월요일 날짜 (연말/연초 경계 자동 안전, year+weeknum 조합 불필요)
-- event_date(STRING, KST yyyy-MM-dd) → to_date → 주 시작(월요일)
SELECT date_trunc('week', to_date(event_date)) AS week_start,
       COUNT(DISTINCT user_id)    AS wau_users          -- 6-a
FROM activity GROUP BY 1 ORDER BY 1;

SELECT date_trunc('week', to_date(event_date)) AS week_start,
       COUNT(DISTINCT session_id) AS wau_sessions       -- 6-b
FROM activity GROUP BY 1 ORDER BY 1;
```
- `date_trunc('week')`는 Spark에서 **월요일 시작**(ISO 부합).
- **주 경계 의미**: 파티션이 event_date(KST)라, 일→월 주 경계를 넘는 세션/사용자는 두 주 모두에서 활동 → 각 주에 1회씩 집계(해당 주에 실제 활동했으므로 의도된 동작).
- **session WAU 해석**: `session_id`에 user_id가 포함되므로 distinct 세션 수 ≥ distinct 사용자 수. "주별 활성 세션 수"로 해석.
- 수치는 **AI 주장 금지** — 실제 Spark 실행 결과로만 보고(사용 쿼리 동봉).

### 4.6 입력 파싱 · 스키마 · 쓰기 세부
- **명시적 스키마**: 14GB에 inferSchema(2-pass)는 비용 큼 → `StructType`로 스키마 코드 명시, `header=true`.
- **event_time 파싱**: 원본은 `"2019-10-01 00:00:00 UTC"`(접미사 ` UTC`) → `to_timestamp(event_time, "yyyy-MM-dd HH:mm:ss 'UTC'")` (리터럴 UTC 처리). 결과 = UTC 기준 `event_time_utc`.
- **KST 파생**: `event_time_kst = from_utc_timestamp(event_time_utc, 'Asia/Seoul')` (KST=UTC+9, DST 없음) → `event_date = date_format(event_time_kst, 'yyyy-MM-dd')`.
- **타입**: `product_id`/`category_id`/`user_id` = long, `price` = double, 나머지 string.
- **쓰기**: `format("parquet").option("compression","snappy")`. `event_date` 기준 `repartition`/`coalesce`로 파티션당 파일 수 제어(소파일 난립 방지).

---

## 5. 실행 모델 / 환경

- **재처리(reprocessing) 정의**: raw 이벤트를 dedup·KST변환·세션부여로 **재가공**한 결과를 parquet+snappy 파티션으로 저장하는 것. 동일 일자 재실행 시 **멱등 overwrite**(§4.4).
- **개발**: 샘플(하루치/소량)로 end-to-end 1회 성공 우선(Day 1 목표).
- **최종**: 전체(Oct+Nov) **Backfill 1회** → WAU 실측 1회.
- **추가 기간 대응**: `--run-date`/`--range` 파라미터 + 파티션 멱등 overwrite + `ADD PARTITION`. (실 데이터가 더 안 와도 *능력*으로 구현·설명)
- **테이블**: 고전적 Hive External Table + **Spark 임베디드 Hive 메타스토어(Derby)** → 별도 Metastore 서비스 불필요(로컬 경량). Iceberg/Delta는 README "프로덕션 업그레이드"로만 언급.
- **오케스트레이션(선택/어필)**: 본체 완성·검증 후 **Airflow + BashOperator(spark-submit)** 얇은 DAG. `catchup=True`가 Backfill을 겸함. SparkSubmitOperator 대비 설치·설정 0 + 명령 투명(설명 용이).
- **데이터 미커밋**: 대용량 CSV는 `.gitignore`.

---

## 6. 언어 선택 (Scala) 사유 — README 초안용 골자
- Spark 네이티브 언어 → 최신 API/성능 1급 지원, Dataset/타입 안정성.
- 윈도우 함수·UDF 등 세션화 로직을 타입 안전하게 표현.
- (대안 Java도 허용되나, Spark 관용 표현·간결성에서 Scala 우위. PySpark는 요구사항상 제외.)

---

## 7. 결정 로그 (숨은 결정 + 근거)

| # | 결정 | 선택 | 근거 / 출처 |
|---|---|---|---|
| 1 | session_id 처리 | **직접 생성 + 원본 user_session 보존(방법 C)** | 원본 세션이 5분룰 미준수(샘플 검증: 내부 갭>5분 12.7%, 전환의 45.2%가 <5분). 보존은 검증 목적(책 근거 아님) |
| 2 | 자정 경계 | **방식 A: event_date 파티션 + 결정적 id + 전날 lookback** | 이벤트 단위 파티셔닝 → 세션화가 backward-only가 되어 forward-dependency 회피. 책 Sessionization(Ch5)의 session window=5분 gap |
| 3 | WAU 주 경계 | **ISO week(월요일 시작), KST** | 국제 표준. 주 식별자=월요일 날짜(`date_trunc('week')`)라 연말/연초 경계 자동 안전(year+weeknum 조합 불필요) |
| 4 | 타임존 변환 | **적재 시 UTC→KST 1회, 원본 UTC 보존** | 단일 변환 지점 → 세션/파티션/WAU 일관, 감사·재현성 |
| 5 | dedup | **자연키 1건 유지, 세션화 전** | 동일 이벤트 노이즈 제거(≈0.06%), 갭 계산 왜곡 방지. 책 Windowed Deduplicator(Ch3) |
| 6 | 장애 복구 | **staging정리+rename 원자 교체 + 검증 게이트 + `_SUCCESS` + 멱등 재시도 + Discord 알람(Airflow 콜백); 커스텀 봇·checkpoint 미사용** | 책: Checkpointer 스트림 전용(Ch3), Data Overwrite "save mode 비트랜잭셔널"(Ch4), Readiness Marker(Ch2), Audit-Write-Audit-Publish(Ch9), Dead-Letter(Ch3), Observability(Ch10) |
| 7 | 테이블/카탈로그 | **Hive External Table + 임베디드 metastore**; Iceberg는 README | "Hive external table" 요구 정확 부합 + 로컬 경량. Iceberg+Hive카탈로그는 설정·버전 마찰 큼 |
| 8 | 실행 모델 | **Backfill 1회로 과제 완수, Incremental은 설계로 대응** | 과거 2개월 고정 데이터. Airflow는 어필용 선택(catchup=backfill) |
| 9 | 언어 | **Scala + sbt + 로컬 Spark** | Spark 네이티브, 타입 안전, 관용 표현 |

---

## 8. 개발 순서 (요약)
1. sbt + Spark(local) 환경 세팅, 샘플 데이터 추출.
2. **세션화 TDD**(경계 케이스 실패테스트 → 통과).
3. 파이프라인 보일러플레이트(read→dedup→tz→sessionize→write) 최소·또렷하게.
4. staging+rename+`_SUCCESS` 복구 장치, External Table 등록.
5. 샘플 end-to-end 1회 성공 확인(Day 1 목표).
6. 전체 Backfill 1회 → WAU 2종 **실측**(쿼리 동봉).
7. (선택) Airflow BashOperator DAG 추가.
8. README("AI 도구 활용" 포함) 작성.
