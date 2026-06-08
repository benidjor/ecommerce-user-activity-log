# 설계 스펙 — 메달리온 파이프라인 + Airflow + BI 대시보드 (Gold 서빙 확장)

기존 WAU 파이프라인(Bronze→Silver) 위에 **Gold 마트 레이어 + Airflow 일별 오케스트레이션 + BI 대시보드 서빙 + Discord 알람**을 얹어, 면접관이 "원본 → 정제 → 집계 → DB → 대시보드"가 매일 흐르는 파이프라인을 **실제로 실행·열람**할 수 있게 한다.

상위 프로젝트 스펙: [2026-06-07-wau-activity-log-design.md](2026-06-07-wau-activity-log-design.md). 본 스펙은 그 위의 **증분 설계**이며, 기존 결정(KST 1회 변환·결정적 session_id·Hive External Table·멱등 복구)을 그대로 계승한다.

---

## 1. 목표 / 요구사항 매핑

| 요구사항(원과제) | 본 설계에서의 충족 |
|---|---|
| Hive External Table 노출 | Silver = `activity`(기존). Gold 마트도 parquet + External Table로 노출 |
| 추가 기간 처리 대응 | Airflow `@daily` + `catchup=True` + `--mode incremental` → 일별 증분이 곧 추가 기간 처리 |
| WAU(user/session) | Gold `mart_wau` + 대시보드 |

| 확장 목표(사용자 비전) | 충족 |
|---|---|
| Bronze→Silver→Gold→DB→대시보드 일별 흐름 어필 | Airflow DAG가 5단계를 매일 실행, UI 그래프로 시연 |
| Gold 차원 모델(스타 스키마) | `dim_date`(+선택 `dim_event_type`) + `fact_daily_activity` + 비가산 지표 마트 |
| 마트를 DB 적재 후 대시보드 연결 | Gold → Postgres 적재 → Metabase(라이브) |
| 면접관 마찰 0 열람 | 정적 HTML → GitHub Pages(항상 떠 있음, 비용 0) |
| Discord 알람(원설계 복원) | DAG `on_success`/`on_failure` 콜백 → Discord 웹훅 |

---

## 2. 아키텍처 (메달리온 + 일별 흐름)

```
                 ┌──────────── Airflow DAG  activity_daily (@daily, catchup=True) ────────────┐
                 │                                                                            │
 [Bronze]        │  task0(1회·DAG 외)  월 CSV → 일별 분할  data/daily/event_date=YYYY-MM-DD/   │
 raw CSV(UTC) ───┼──▶ task1 Silver  spark-submit --mode incremental --run-date {{ds}}          │
 2019-Oct/Nov    │        → activity (Hive External Table, parquet, KST 일별 파티션)            │
                 │    ──▶ task2 gate   FileSensor _SUCCESS                                      │
 [Silver]        │    ──▶ task3 Gold   spark-sql sql/gold/*.sql                                 │
 activity table  │        → dim_date · fact_daily_activity · mart_*  (parquet + External Table) │
                 │    ──▶ task4 Load   Gold 마트 → Postgres (Spark JDBC write)                  │
 [Gold]          │    ──▶ task5 Publish 정적 HTML 렌더 → dashboard/index.html → GitHub Pages    │
 마트(parquet+DB)│                                                                            │
                 │  on_success / on_failure ─────────▶ Discord 웹훅(run 메타데이터·에러 딥링크)  │
                 └────────────────────────────────────────────────────────────────────────────┘
                                    │                                   │
                       ┌────────────┘                                   └────────────┐
              [서빙 A · 정적/항상]                                      [서빙 B · 라이브/어필]
              GitHub Pages (정적 HTML, Chart.js)                       docker-compose: Postgres + Metabase
              면접관 URL 클릭, 비용 0                                   면접관(또는 본인) `up` → 라이브 필터
```

서빙은 **하이브리드**: A(정적)가 항상 보이는 기본, B(라이브)가 "진짜 스택" 어필.

---

## 3. 컴포넌트 (단위별 책임 · 인터페이스 · 의존)

각 단위는 단일 책임을 가지며 독립적으로 실행·검증 가능하다.

### 3.1 DailySplitter (Bronze 일별 랜딩)
- **무엇**: 월 CSV를 읽어 KST `event_date`로 `partitionBy` 하여 `data/daily/event_date=YYYY-MM-DD/`로 분할 저장(일별 Bronze 랜딩).
- **인터페이스**: `spark-submit --class com.activitylog.DailySplitter --input <월CSV,...> --output data/daily`
- **의존**: 기존 `Schema.Raw`, `TimeUtils`(KST 파생 재사용 — 타임존 로직 중복 금지). 출력은 CSV(헤더 포함).
- **근거**: Airflow 일별 run이 `--input data/daily/event_date={{ds}}/`로 자기 날짜만 읽게 함(1-5 사전 분할). **KST 기준 분할**이라 파이프라인 파티션과 정합.
- **실행 빈도**: 1회(데모 준비). gitignore 대상.

### 3.2 Silver (기존 파이프라인 재사용)
- 변경 없음. Airflow가 `--mode incremental --run-date {{ds}}`로 호출. 입력은 `data/daily`의 당일 + 전날(lookback) 폴더.

### 3.3 Gold 마트 (차원 모델 + 지표 마트)
- **무엇**: `activity`(Silver)에서 집계해 Gold 테이블 생성. parquet 저장 + Hive External Table 등록(메달리온 일관).
- **인터페이스**: `sql/gold/*.sql`을 `spark-sql -f`로 실행(설명 가능성 위해 SQL 우선). 드라이버는 얇게.
- **의존**: `activity` 테이블.
- **모델**(§4 상세): `dim_date`, `fact_daily_activity`(가산 측정), 비가산 지표 마트(`mart_wau`/`mart_mau`/`mart_stickiness`/`mart_retention`).

### 3.4 DB Load (Gold → Postgres)
- **무엇**: Gold 마트(수백 행)를 Postgres 테이블로 적재(라이브 BI 소스).
- **인터페이스**: Spark JDBC write(`overwrite`). 대상 = docker-compose Postgres.
- **의존**: Gold parquet 마트, Postgres 기동.
- **주의**: 정적 대시보드는 Postgres에 의존하지 않음(§3.5) → DB 다운과 무관하게 정적은 항상 빌드 가능.

### 3.5 정적 대시보드 빌드 (서빙 A)
- **무엇**: Gold 마트(**parquet 직접 읽기**)를 JSON으로 임베드한 **단일 자기완결 HTML** 생성 → GitHub Pages 발행.
- **인터페이스**: `dashboard/build.py`(parquet→DuckDB/pandas→Jinja2 템플릿→`dashboard/index.html`). 차트는 Chart.js(또는 ECharts) vendored/CDN.
- **의존**: Gold parquet(Postgres 불필요 — 빌드 시점 정적 스냅샷).
- **언어 선택(의도)**: 정적 HTML 생성은 Python(pandas/jinja2)이 가장 단순·유지 용이(사용자 Python 배경). 파이프라인 본체는 Scala 유지 → 빌드 툴만 Python.
- **발행**: `index.html`을 GitHub Pages 소스(예: `gh-pages` 브랜치 또는 `/docs`)로 게시. DAG는 산출물 생성까지, 게시는 얇은 단계(수동 push 또는 GitHub Action) — DAG가 git push 하지 않음.

### 3.6 라이브 스택 (서빙 B, docker-compose)
- **무엇**: `docker/docker-compose.yml`로 Postgres + Metabase(+ 선택 Airflow) 기동. Metabase가 Postgres 마트를 쿼리해 라이브 대시보드 제공.
- **인터페이스**: `docker-compose up` → `localhost:3000`(Metabase). 대시보드 정의는 스크린샷 + (가능 시) export json 동봉.
- **의존**: Postgres 마트(§3.4).
- **위치**: 어필·선택. 면접관이 직접 `up` 하거나 본인이 호스팅. 미기동이어도 정적(A)으로 열람 보장.

### 3.7 Discord 알람
- **무엇**: DAG run 성공/실패를 Discord 웹훅으로 통지(run 메타데이터·실패 시 에러 요약·Airflow run 딥링크).
- **인터페이스**: `on_success_callback`/`on_failure_callback` → `airflow/callbacks/discord.py`. 웹훅 URL은 Airflow Variable/환경변수(시크릿 미커밋).
- **근거**: 원설계 §4.4(4)·결정 로그 #6 복원. 커스텀 봇·버튼은 미구현(과한 인프라) 방침 유지.

---

## 4. Gold 데이터 모델 (스타 스키마 + 비가산 마트)

**핵심 모델링 원칙(정직성)**: count-distinct 지표(WAU/MAU/리텐션)는 **비가산(non-additive)** — 일별 distinct를 합쳐 주별 distinct를 만들 수 없다. 따라서 두 부류로 나눈다.

### 4.1 차원 + 가산 팩트 (스타)
- **`dim_date`** — 날짜 차원. `date_key(yyyy-MM-dd)`, `iso_week(월요일)`, `month`, `dow`, `is_weekend`. SQL `sequence`로 생성.
- **`dim_event_type`**(선택) — view/cart/purchase 등 퍼널 차원.
- **`fact_daily_activity`** — 그레인 = `event_date × event_type`. 측정: `event_count`, `distinct_users`(그 날·타입 distinct), `distinct_sessions`, `revenue(sum price where purchase)`. **가산 측정**(event_count·revenue)은 주/월 재집계 가능.

### 4.2 비가산 지표 마트 (Silver 직접 집계)
- **`mart_dau`** — `event_date`별 distinct user/session. (일 그레인 distinct는 fact에서 읽어도 됨)
- **`mart_wau`** — ISO주별 distinct user/session. (기존 `sql/wau.sql` 승계)
- **`mart_mau`** — KST 월별 distinct user. (데이터 2개월 → 점 2~3개, 한계 명시)
- **`mart_stickiness`** — DAU/MAU(또는 DAU/WAU) 비율.
- **`mart_funnel`** — 단계별(view→cart→purchase) distinct user·전환율(기간 전체 + 주별).
- **`mart_revenue`** — 주별 결제금액, AOV(=결제금액/구매수).
- **`mart_cvr`** — 주별 CVR(구매자/방문자) + 전주 대비(WoW) 증감.
- **`mart_retention`** — 주차 코호트 리텐션(코호트 = 데이터 내 첫 활동 주). 9주 → 작은 삼각형, 한계 명시.

### 4.3 지표 정의·한계 (대시보드/README 동봉)
- "활성" = 해당 기간 ≥1 이벤트.
- **신규 유저 불가**: Oct 이전 이력 없음 → "신규" 미정의(제외).
- MAU·stickiness·리텐션: 2개월 데이터라 표본 얇음 → 그래프에 명시.
- AOV: order_id 없음 → 구매 이벤트를 주문 proxy로 사용(명시).

---

## 5. KPI 카탈로그 (대시보드 구성)

테마별 그룹(타일 난립 방지):

| 그룹 | KPI | 산출 소스 |
|---|---|---|
| Engagement | DAU, WAU(user/session), MAU, Stickiness | mart_dau/wau/mau/stickiness |
| Conversion | 퍼널(view→cart→purchase), 주별 CVR + 전주 대비 | mart_funnel, mart_cvr |
| Monetization | 주별 결제금액, AOV | mart_revenue |
| Retention | 주차 코호트 리텐션 | mart_retention |

차트 형태(정적): DAU 라인(62점), WAU/세션 라인, 퍼널 바, CVR 라인+WoW, 리텐션 히트맵(삼각), 매출 바.

---

## 6. 오류 처리 / 안정성

- **멱등**: Gold 마트는 Silver에서 매 run 전체 재계산 후 overwrite(마트가 작아 비용 무시). Silver는 기존 파티션 멱등.
- **순서 보장**: DAG 의존(`task1 >> task2 >> task3 >> task4 >> task5`). gate(FileSensor) 통과해야 Gold 진행.
- **DB/라이브 분리**: 정적(A)은 parquet만 의존 → Postgres·Metabase 다운과 독립. 면접관 열람 가용성 우선.
- **알람**: 실패 시 Discord로 즉시 통지(어느 task·어떤 run). 재처리는 Airflow 네이티브 Clear/Retry(멱등이라 안전).
- **시크릿**: Discord 웹훅·DB 비밀번호는 환경변수/Airflow Variable. 커밋 금지.

---

## 7. 테스트 전략

- **DailySplitter**: 소량 픽스처로 KST 경계(UTC 자정 전후 행이 올바른 날짜 폴더로) 단위 테스트.
- **Gold SQL**: 작은 합성 `activity` 픽스처로 각 마트의 핵심 케이스 검증 — 비가산성(주별 distinct ≠ 일별 합), 퍼널 단조성, CVR 분모 0 처리, 리텐션 코호트 경계. (기존 Spark 테스트 베이스 재사용)
- **정적 빌드**: 마트 JSON이 HTML에 임베드됐는지·차트 데이터 행수 일치 스모크 테스트.
- **DAG**: import 무오류 + 의존 그래프 형태(`airflow dags test` 1일 dry-run).
- **수치는 실제 실행 결과로만**(기존 불변 규칙 계승).

---

## 8. 결정 로그 (본 확장)

| # | 결정 | 선택 | 근거 |
|---|---|---|---|
| A | 일별 입력 | 월 CSV → KST 일별 분할(DailySplitter) | Airflow run-per-day 모델 전제. 데모를 실제로 일별 구동 |
| B | Gold 모델 | 경량 스타(dim_date+fact) + 비가산 지표 마트 분리 | count-distinct 비가산성 정직 반영. dim_user 등 거대 차원은 과설계라 제외 |
| C | Gold 저장 | parquet + Hive External Table, 그리고 Postgres 적재 | 메달리온 일관(parquet) + 라이브 BI 소스(Postgres) 양립 |
| D | 서빙 | 하이브리드 — 정적(GitHub Pages) 기본 + 라이브(docker-compose Metabase) 어필 | 면접관 마찰 0·비용 0 보장 + "진짜 스택" 어필 동시 |
| E | 정적 빌드 언어 | Python(pandas/jinja2) | HTML 생성 최단경로, 사용자 Python 배경. 파이프라인은 Scala 유지 |
| F | DB | Postgres | 표준 BI 백엔드, Metabase 즉시 연동. 마트가 작아 DuckDB도 가능하나 라이브 우선 |
| G | 알람 | Discord 웹훅(Airflow 콜백), 커스텀 봇 미구현 | 원설계 §4.4(4) 복원, 과한 인프라 회피 |

---

## 9. 구현 단계 (Phase — 각 Phase는 독립 산출물 + 별도 PR)

스코프가 크므로 단계화한다. 각 Phase 종료 시점에 동작하는 산출물이 남는다.

- **Phase 0 — DailySplitter**: 월→일 분할. 산출: `data/daily/` 일별 Bronze. (DAG의 전제)
- **Phase 1 — Gold 마트**: `sql/gold/*.sql` + `dim_date`/`fact`/`mart_*` parquet + External Table. 산출: 쿼리 가능한 지표 마트(인프라 0). *여기까지만으로도 WAU·DAU 등 실측 테이블 확보.*
- **Phase 2 — 정적 대시보드**: `dashboard/build.py` → `index.html` → GitHub Pages. 산출: 면접관 열람 URL.
- **Phase 3 — Airflow DAG**: Bronze→Silver→Gold→publish 오케스트레이션 + Discord 콜백. 산출: 일별 흐름 시연.
- **Phase 4 — 라이브 스택**: `docker-compose`(Postgres+Metabase) + DB Load(task4). 산출: 라이브 BI 어필.

각 Phase는 갱신된 main에서 분기 → PR(base=main) → squash 머지(스택 금지, 기존 규칙).

---

## 10. 범위 밖 (Out of Scope)

- 커스텀 Discord 재처리 봇/버튼(원설계 방침 유지).
- `dim_user`/`dim_product` 풀 스타(애드혹 제품 분석 필요 시 확장 — README 언급).
- Iceberg/Delta, Stateful Sessionizer, 풀 dead-letter quarantine(상위 스펙의 프로덕션 확장 그대로).
- 실시간/스트리밍.
