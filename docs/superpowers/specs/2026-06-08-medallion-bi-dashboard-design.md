# 설계 스펙 — 메달리온 파이프라인 + Airflow + BI 대시보드 (Gold 서빙 확장)

기존 WAU 파이프라인(Bronze→Silver) 위에 **Gold 마트 레이어 + Airflow 일별 오케스트레이션 + BI 대시보드 서빙 + Discord 알람**을 얹어, "원본 → 정제 → 집계 → DB → 대시보드"가 매일 흐르는 파이프라인을 **실제로 실행·열람**할 수 있게 한다.

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
| Bronze→Silver→Gold→DB→대시보드 일별 흐름 어필 | Airflow DAG가 단계를 매일 실행, UI 그래프로 시연 |
| Gold 차원 모델(스타 스키마) | `dim_date`(+선택 `dim_event_type`) + `fact_daily_activity` + 비가산 지표 마트 |
| 마트를 DB로 노출 후 대시보드 연결 | Gold → **DuckDB 파일(repo 커밋, 패턴 3)** → 정적·Streamlit 둘 다 SQL로 읽기 |
| 면접관 마찰 0 열람(맥북 꺼져도) | 정적 HTML → GitHub Pages(즉시·항상 ON) + Streamlit Cloud(인터랙티브) |
| Discord 알람(원설계 복원) | DAG `on_success`/`on_failure` 콜백 → Discord 웹훅 |

### 1.1 요구사항 가드레일 (확장이 원과제를 침범하지 않도록)

본 확장(Gold·DuckDB·대시보드)은 원과제 요구사항을 **추가**할 뿐 대체하지 않는다. 아래를 불변으로 지킨다.

- **"Hive External Table" 요구 = Silver `activity`** (parquet + Hive metastore). DuckDB는 Hive가 아니며, 요구를 대체하지 않는 **Gold 서빙 사본**일 뿐이다.
- **제출용 WAU(요구 7) = Hive `activity`에서 계산** — `sql/wau.sql` on `activity` → `results/`. 대시보드가 DuckDB에서 읽는 WAU는 *서빙 표시*이지 제출물이 아니다.
- **Spark Application 언어 제약** — 파이프라인·`DailySplitter`·`GoldMarts`는 Scala. DuckDB export·정적 빌드·Streamlit·Airflow는 Spark 외 서빙/오케스트레이션이라 Python 허용(README에 경계 명시).
- 데이터 흐름: Bronze→Silver(Hive)→Gold(parquet+Hive `gold_*`)까지 Hive 일관, **DuckDB는 맨 끝 대시보드용 사본**.

---

## 2. 아키텍처 (메달리온 + 일별 흐름)

```
                 ┌──────────── Airflow DAG  activity_daily (@daily, catchup=True) ───────────────┐
                 │                                                                               │
 [Bronze]        │  task0(1회·DAG 외)  월 CSV → 일별 분할  data/daily/event_date=YYYY-MM-DD/         │
 raw CSV(UTC) ───┼──▶ task1 Silver  spark-submit --mode incremental --run-date {{ds}}            │
 2019-Oct/Nov    │        → activity (Hive External Table, parquet, KST 일별 파티션)                │
                 │    ──▶ task2 gate   FileSensor _SUCCESS                                       │
 [Silver]        │    ──▶ task3 Gold   spark-sql sql/gold/*.sql                                  │
 activity table  │        → dim_date · fact_daily_activity · mart_*  (parquet + External Table)  │
                 │    ──▶ task4 Export Gold 마트 → DuckDB 파일 (dashboard/marts.duckdb)            │
 [Gold]          │    ──▶ task5 Build  정적 HTML 렌더 → dashboard/index.html                       │
 마트(parquet+    │                                                                               │
   DuckDB 파일)   │  on_success / on_failure ─────────▶ Discord 웹훅(run 메타데이터·에러 딥링크)         │
                 └───────────────────────────────────────────────────────────────────────────────┘
                          │ (산출물을 repo에 커밋 — 얇은 단계, DAG가 push 안 함)
                          ▼
                   marts.duckdb + index.html  (repo)
                       │                                   │
          ┌────────────┘                                   └────────────┐
   [서빙 A · 정적/항상 ON]                                    [서빙 B · 인터랙티브]
   GitHub Pages (정적 HTML, Chart.js)                       Streamlit Cloud (Python 앱)
   marts.duckdb 임베드 → 즉시 표시, 콜드스타트 0              marts.duckdb를 SQL로 읽음 → 필터·드릴다운
   면접관 URL 클릭, 비용 0                                    맥북 꺼져도 OK(콜드스타트 ~30–60s)
```

서빙은 **하이브리드**: A(정적)가 즉시·항상 보이는 폴백, B(Streamlit)가 인터랙티브 어필. **둘 다 repo에 커밋된 `marts.duckdb`(패턴 3)를 읽음** — 호스팅 DB·시크릿 불필요.

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

### 3.4 Mart Export (Gold → DuckDB 파일, 패턴 3)
- **무엇**: Gold 마트(수백 행)를 **임베디드 DuckDB 파일** `dashboard/marts.duckdb`로 export(서빙 A·B 공통 소스).
- **인터페이스**: parquet 마트 → DuckDB 테이블 적재. DuckDB는 서버 없는 파일 DB라 호스팅·시크릿 불필요.
- **의존**: Gold parquet 마트.
- **근거(패턴 3)**: 클라우드 Streamlit은 로컬 Postgres를 못 읽음 → 마트가 작아 **DB 파일을 repo에 커밋**해 앱과 함께 배포. "Streamlit이 DB를 SQL로 읽는다" 경험은 유지하되 호스팅 DB 제거.
- **커밋 정책**: 데모 서빙용은 **최종 마트 스냅샷 1개**를 커밋(매 일별 run마다 커밋해 git 비대화하지 않음). 일별 흐름은 DAG로 시연.

### 3.5 정적 대시보드 빌드 (서빙 A)
- **무엇**: `marts.duckdb`를 읽어 JSON으로 임베드한 **단일 자기완결 HTML** 생성 → GitHub Pages 발행.
- **인터페이스**: `dashboard/build.py`(duckdb→pandas→Jinja2 템플릿→`dashboard/index.html`). 차트는 Chart.js(또는 ECharts) vendored/CDN. 데이터가 HTML에 구워지므로 **열람 시 즉시 표시·콜드스타트 0**.
- **의존**: `marts.duckdb`(빌드 시점 정적 스냅샷).
- **언어 선택(의도)**: 대시보드는 Spark Application이 아니므로 과제 언어 제약(Scala/Java) 밖 → Python(duckdb/pandas/jinja2)이 가장 단순·유지 용이(사용자 Python 배경). 파이프라인 본체는 Scala 유지.
- **발행**: `index.html`을 GitHub Pages 소스(예: `gh-pages` 브랜치 또는 `/docs`)로 게시. DAG는 산출물 생성까지, 게시는 얇은 단계(수동 push 또는 GitHub Action) — DAG가 git push 하지 않음.

### 3.6 Streamlit 앱 (서빙 B, 인터랙티브)
- **무엇**: `marts.duckdb`를 SQL로 읽어 인터랙티브 대시보드(필터·드릴다운) 제공. **Streamlit Community Cloud**에 public repo에서 배포 → 면접관 웹 열람(맥북 꺼져도 OK).
- **인터페이스**: `dashboard/streamlit_app.py`(Python). `duckdb.connect("marts.duckdb")` 또는 `st.connection`. 차트는 Streamlit 위젯/Plotly.
- **의존**: 커밋된 `marts.duckdb`(§3.4). 호스팅 DB·시크릿 불필요(파일 DB라 시크릿 없음).
- **운영 노트**: 무료 티어는 유휴 시 슬립 → 클릭 시 **콜드스타트 ~30–60s**(앱 의존성 최소화로 단축). 슬립/장애 시 **정적(A)이 즉시 폴백**이라 열람 보장.
- **언어**: Streamlit은 Python 전용(Scala 불가). 대시보드 레이어라 제약 밖.

### 3.7 Discord 알람
- **무엇**: DAG run 성공/실패를 Discord 웹훅으로 통지(run 메타데이터·실패 시 에러 요약·Airflow run 딥링크).
- **인터페이스**: `on_success_callback`/`on_failure_callback` → `airflow/callbacks/discord.py`. 웹훅 URL은 Airflow Variable/환경변수(시크릿 미커밋).
- **근거**: 원설계 §4.4(4)·결정 로그 #6 복원. 커스텀 봇·버튼은 미구현(과한 인프라) 방침 유지.

---

## 4. Gold 데이터 모델 (스타 스키마 + 비가산 마트)

**원자 fact는 Silver, Gold는 집계 fact (행 수 주의)**: 원자(transaction) fact — 이벤트 1건=1행, 약 9천만 행 — 는 **Silver `activity`**가 이미 보유한다. Gold는 이를 복제하지 않고 **거친 그레인으로 미리 집계한 aggregate fact**만 둔다. 그래서 `COUNT(DISTINCT)`·`SUM`·`GROUP BY`로 9천만 행이 **하루당/주당 한 줄**로 접혀 Gold 전체가 **수백 행**이 된다(이 작음이 패턴 3 = DuckDB 파일 커밋 서빙의 근거). 유저/상품 단위 fact는 폭증하므로 제외(결정 B).

**비가산성**: count-distinct 지표(WAU/MAU/리텐션)는 **비가산(non-additive)** — 일별 distinct를 합쳐 주별 distinct를 만들 수 없다. 따라서 가산 fact와 비가산 마트를 나눈다.

### 4.1 차원 + 가산 팩트 (스타)
- **`dim_date`** — 날짜 차원. `date_key(yyyy-MM-dd)`, `iso_week(월요일)`, `month`, `dow`, `is_weekend`. SQL `sequence`로 생성. (~62행)
- **`dim_event_type`**(선택) — view/cart/purchase 등 퍼널 차원. (~5행)
- **`fact_daily_activity`** — **집계 fact**(원자 fact 아님). 그레인 = `event_date × event_type`(≈62×5 = **~300행**). 측정: `event_count`, `distinct_users`(그 날·타입 distinct), `distinct_sessions`, `revenue(sum price where purchase)`. **가산 측정**(event_count·revenue)은 주/월 재집계 가능. (원자 단위가 필요하면 `fact_events`≈Silver 복제 — 범위 밖)

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
- **순서 보장**: DAG 의존(`task1 Silver >> task2 gate >> task3 Gold >> task4 Export >> task5 Build`). gate(FileSensor) 통과해야 Gold 진행.
- **서빙 폴백**: 정적(A)·Streamlit(B) 모두 커밋된 `marts.duckdb`만 의존(호스팅 DB 없음). Streamlit 슬립/장애 시 정적(A)이 즉시 폴백 → 면접관 열람 가용성 우선.
- **알람**: 실패 시 Discord로 즉시 통지(어느 task·어떤 run). 재처리는 Airflow 네이티브 Clear/Retry(멱등이라 안전).
- **시크릿**: Discord 웹훅 URL은 환경변수/Airflow Variable, 커밋 금지. (DuckDB 파일 DB라 DB 비밀번호 없음 — 시크릿 표면 최소.)

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
| C | Gold 저장 | parquet + Hive External Table, 그리고 DuckDB 파일 export | 메달리온 일관(parquet) + 서빙용 임베디드 DB 파일(패턴 3) |
| D | 서빙 | 하이브리드 — 정적(GitHub Pages, 즉시·항상 ON) + Streamlit Cloud(인터랙티브) | 면접관 마찰 0·비용 0·맥북 꺼져도 OK. 정적이 콜드스타트 폴백 |
| E | 대시보드 언어 | Python(duckdb/pandas/jinja2 + Streamlit) | 대시보드는 Spark Application 아님 → 제약(Scala/Java) 밖. Streamlit은 Python 전용. 파이프라인은 Scala 유지 |
| F | DB(서빙) | **임베디드 DuckDB 파일(repo 커밋, 패턴 3)** | 클라우드 Streamlit이 로컬 DB 못 읽음 + 마트가 작음 → 파일 DB가 무료·무호스팅·무시크릿. "SQL로 읽힘" 유지 |
| G | 알람 | Discord 웹훅(Airflow 콜백), 커스텀 봇 미구현 | 원설계 §4.4(4) 복원, 과한 인프라 회피 |
| H | 콜드스타트 대응 | 정적(A)을 즉시 폴백으로 상시 유지, 면접 후 단순화 | 콜드스타트는 고정 성질(통제 불가)이라 폴백 유지. 면접 후 Chart.js 제거는 선택적 cleanup |

---

## 9. 구현 단계 (Phase — 각 Phase는 독립 산출물 + 별도 PR)

스코프가 크므로 단계화한다. 각 Phase 종료 시점에 동작하는 산출물이 남는다.

- **Phase 0 — DailySplitter**: 월→일 분할. 산출: `data/daily/` 일별 Bronze. (DAG의 전제)
- **Phase 1 — Gold 마트**: `sql/gold/*.sql` + `dim_date`/`fact`/`mart_*` parquet + External Table. 산출: 쿼리 가능한 지표 마트(인프라 0). *여기까지만으로도 WAU·DAU 등 실측 테이블 확보.*
- **Phase 2 — Mart Export + 정적 대시보드**: `marts.duckdb` export + `dashboard/build.py` → `index.html` → GitHub Pages. 산출: 면접관 즉시 열람 URL.
- **Phase 3 — Streamlit 앱**: `dashboard/streamlit_app.py`(DuckDB 읽기) → Streamlit Cloud 배포. 산출: 인터랙티브 웹 대시보드.
- **Phase 4 — Airflow DAG**: Bronze→Silver→Gold→export→build 오케스트레이션 + Discord 콜백. 산출: 일별 흐름 시연(어필의 핵심).

각 Phase는 갱신된 main에서 분기 → PR(base=main) → squash 머지(스택 금지, 기존 규칙). Phase 1까지는 인프라 0, Phase 2~3이 서빙, Phase 4가 오케스트레이션 캡스톤.

---

## 10. 범위 밖 (Out of Scope)

- 커스텀 Discord 재처리 봇/버튼(원설계 방침 유지).
- **서버형 BI 스택(Metabase/Superset + 호스팅 Postgres)** — 무료·항상 ON 요건과 안 맞아 제외. 프로덕션 확장으로 README 언급(호스팅 DB + BI 툴).
- `dim_user`/`dim_product` 풀 스타(애드혹 제품 분석 필요 시 확장 — README 언급).
- Iceberg/Delta, Stateful Sessionizer, 풀 dead-letter quarantine(상위 스펙의 프로덕션 확장 그대로).
- 실시간/스트리밍.

---

## 11. Phase 2 결정 보강 (Mart Export + 정적 대시보드 — 2026-06-09 브레인스토밍)

§3.4·§3.5·§9의 Phase 2를 구현하기 직전, 스펙이 열어둔 갈림길을 좁힌 결정이다. 본 §11은 §3·§9의 **증분**이며 충돌 없이 계승한다.

### 11.1 데이터 흐름 (책임 경계)

```
output/gold/*/*.parquet  (Phase 1 산출 — Spark/Scala가 생성)
        │  ① export_duckdb.py  (로컬 1회, Python+duckdb)
        ▼
dashboard/marts.duckdb   ← repo 커밋(패턴3·결정 F, 수백 행). Phase 3 Streamlit과 공유하는 단일 서빙 소스
        │  ② build.py  (로컬 & CI, duckdb→pandas→Jinja2)
        ▼
dashboard/index.html     ← 생성물(gitignore). 마트 데이터는 JSON으로 임베드, Chart.js는 CDN
        │  ③ .github/workflows/pages.yml  (main push 시 build→deploy)
        ▼
GitHub Pages URL         ← 면접관 즉시 열람, 콜드스타트 0
```

- **CI는 Spark/parquet 불필요**: 커밋된 `marts.duckdb`만 읽어 `build.py` 실행 → HTML 배포. export(①)는 데이터 갱신 시 로컬 실행이며, Phase 4 Airflow DAG가 이를 흡수한다.

### 11.2 결정 (이번 회차에서 확정)

| # | 결정 | 선택 | 근거 |
|---|---|---|---|
| P2-1 | Pages 발행 | **GitHub Actions → Pages**(`actions/upload-pages-artifact`+`deploy-pages`) | main push마다 자동 빌드·배포, 항상-ON URL. `docs/`는 스펙 마크다운으로 점유돼 Pages 소스로 부적합 → main/docs 깨끗 유지. 사용자는 repo 설정에서 Pages source=Actions만 1회 켬 |
| P2-2 | 차트 전달 | **Chart.js CDN** | 단순·작은 HTML. 데이터(JSON)는 HTML에 임베드돼 콜드스타트 0 유지. 면접관 브라우저는 온라인이라 CDN 충분 |
| P2-3 | `index.html` | **커밋 안 함**(생성물, gitignore) | CI가 매 push 빌드/배포 → 생성물 중복 커밋·diff churn 제거. 로컬은 `build.py` 실행해 확인 |
| P2-4 | `marts.duckdb` | **커밋함**(패턴3·결정 F 재확인) | 수백 행 서빙 사본 + CI 빌드 입력 + Phase 3 Streamlit 공유. `data/*.csv` 커밋금지와 구분되는 의도적 예외 |
| P2-5 | 문서 산출 | **본 §11 보강 + 계획서**(별도 Phase 2 스펙 미작성) | §3·§9가 이미 상세 → 중복 스펙 회피(과한설계 지양) |

### 11.3 컴포넌트 (Phase 2 산출 파일)

| 파일 | 책임 | 의존 |
|---|---|---|
| `dashboard/export_duckdb.py` | `output/gold/*` parquet → `marts.duckdb`(마트당 1테이블) | duckdb |
| `dashboard/build.py` | `marts.duckdb` → pandas → Jinja2 → `index.html`(마트 JSON 임베드) | duckdb·pandas·jinja2 |
| `dashboard/templates/index.html.j2` | KPI 그룹(§5: Engagement/Conversion/Monetization/Retention) 레이아웃 + Chart.js(CDN) | — |
| `dashboard/requirements.txt` | duckdb·pandas·jinja2·pytest 버전 고정 | — |
| `.github/workflows/pages.yml` | main push → pip install → build.py → upload-pages-artifact → deploy-pages | `marts.duckdb`(커밋) |
| `dashboard/tests/test_build.py` | pytest 스모크 | export·build |

### 11.4 테스트 (TDD, pytest — 수치 단언 금지)

- **export**: 합성 parquet 픽스처 → `marts.duckdb`에 기대 테이블 존재 + 테이블 행수 = 입력 행수.
- **build**: `index.html` 생성됨 + 임베드 JSON의 마트별 행수 = duckdb 행수 + KPI 섹션 4개 존재.
- **수치는 실데이터(`marts.duckdb`)로만 보고** — 테스트는 구조·정합성만 단언하고 특정 WAU 값은 단언하지 않는다(불변 규칙 §7 계승).

### 11.5 언어 경계 (README 명시)

export/build/CI/테스트는 **Python**(대시보드 레이어 — Spark Application 아님, 결정 E 허용). 파이프라인 본체·`GoldMarts`는 Scala 유지. Python 코드에도 한국어 설명 주석을 단다(사용자 Python 배경).

### 11.6 Phase 2 범위 밖

Streamlit 앱 → Phase 3. Airflow DAG/Discord → Phase 4. 이번 회차는 export + 정적 서빙(Pages)까지만.
