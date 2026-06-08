# Mart Export + 정적 대시보드 (Phase 2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Phase 1 Gold parquet 마트를 `marts.duckdb`로 export하고, `build.py`로 데이터를 임베드한 정적 `index.html`을 만들어 GitHub Actions로 GitHub Pages에 배포한다.

**Architecture:** `output/gold/*` parquet → `export_duckdb.py` → `dashboard/marts.duckdb`(repo 커밋) → `build.py`(duckdb→pandas→Jinja2) → `dashboard/site/index.html`(생성물, 미커밋) → `.github/workflows/pages.yml`이 main push마다 빌드·배포. CI는 Spark/parquet 없이 커밋된 `marts.duckdb`만 읽는다.

**Tech Stack:** Python 3.9+(로컬)/3.11(CI), duckdb, pandas, jinja2, pytest, Chart.js(CDN), GitHub Actions(`upload-pages-artifact`/`deploy-pages`).

**근거 스펙:** `docs/superpowers/specs/2026-06-08-medallion-bi-dashboard-design.md` §3.4·§3.5·§5·§9·**§11**(Phase 2 결정 보강).

---

## 불변 가드레일 (모든 Task 공통)

- **언어 경계**: export/build/CI/테스트는 Python(대시보드 레이어). 파이프라인 본체·`GoldMarts`는 Scala 유지. README에 명시.
- **수치 = 실데이터만**: 대시보드는 `marts.duckdb`(=gold 실측)에서만 렌더. 테스트는 구조·정합성만 단언하고 특정 WAU 값은 단언 금지.
- **커밋 정책**: `marts.duckdb`는 커밋(서빙 사본·CI 입력). `dashboard/site/`·`dashboard/.venv/`는 gitignore.
- **Python에도 한국어 설명 주석**(사용자 Python 배경).
- **커밋 트레일러**: 각 커밋 끝에 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

## 마트 스키마 참조 (export/build에서 사용하는 정확한 컬럼)

`sql/gold/*.sql`에서 확정된 출력 컬럼(= `output/gold/<table>/*.parquet`의 컬럼):

| 테이블 | 컬럼 |
|---|---|
| `dim_date` | `date_key, date, iso_week, month, dow, is_weekend` |
| `fact_daily_activity` | `event_date, event_type, event_count, distinct_users, distinct_sessions, sum_price` |
| `mart_dau` | `event_date, dau_users, dau_sessions` |
| `mart_wau` | `week_start, wau_users, wau_sessions` |
| `mart_mau` | `month, mau_users` |
| `mart_stickiness` | `date, dau, mau, stickiness` |
| `mart_funnel` | `week_start, users_view, users_cart, users_purchase, view_to_cart, cart_to_purchase` |
| `mart_revenue` | `week_start, revenue, purchases, aov` |
| `mart_cvr` | `week_start, visitors, purchasers, cvr, cvr_wow_delta`(첫 행 `cvr_wow_delta`=NULL) |
| `mart_retention` | `cohort_week, week_offset, active_users, cohort_users, retention_rate` |

KPI 그룹(§5): **Engagement**(dau/wau/mau/stickiness) · **Conversion**(funnel/cvr) · **Monetization**(revenue) · **Retention**(retention).

## 파일 구조 (생성/수정 대상)

```
dashboard/
  requirements.txt            # Task 1 (생성)
  export_duckdb.py            # Task 2 (생성) — parquet → marts.duckdb
  build.py                    # Task 3~5 (생성) — duckdb → index.html
  templates/index.html.j2     # Task 4 (생성) — Jinja2 템플릿 + Chart.js
  marts.duckdb                # Task 2 (생성·커밋) — 실데이터 export 산출
  site/index.html             # build 산출물 (gitignore)
  .venv/                      # 로컬 venv (gitignore)
  tests/
    conftest.py               # Task 2 (생성) — dashboard/를 import 경로에 추가
    test_export.py            # Task 2
    test_build.py             # Task 3~5
.github/workflows/pages.yml   # Task 6 (생성)
.gitignore                    # Task 1 (수정) — dashboard/site/, dashboard/.venv/ 추가
README.md                     # Task 7 (수정) — 언어 경계·도구·구조
docs/runbook/dashboard.md     # Task 7 (생성)
```

---

## Task 1: Python 환경 · requirements · gitignore

대시보드 레이어의 Python 의존성과 venv, gitignore를 준비한다. (인프라 Task — 테스트 없음)

**Files:**
- Create: `dashboard/requirements.txt`
- Modify: `.gitignore`

- [ ] **Step 1: requirements.txt 작성**

`dashboard/requirements.txt`:
```
# 대시보드 레이어 전용 Python 의존성(파이프라인 본체와 분리 — Spark 외 서빙 레이어).
# 버전은 Python 3.9(로컬)~3.11(CI) 모두에서 wheel 제공 확인된 조합.
duckdb==1.1.3
pandas==2.2.3
jinja2==3.1.5
pytest==8.3.4
```

- [ ] **Step 2: .gitignore에 생성물·venv 추가**

`.gitignore`의 `# === IDE / OS ===` 줄 바로 위에 아래 블록을 추가:
```
# === 대시보드(Phase 2) 생성물 / 로컬 venv ===
# marts.duckdb는 서빙 사본이라 의도적으로 커밋(패턴3). 아래만 무시.
dashboard/site/
dashboard/.venv/
```

- [ ] **Step 3: venv 생성 + 설치**

Run:
```bash
python3 -m venv dashboard/.venv
dashboard/.venv/bin/pip install --upgrade pip
dashboard/.venv/bin/pip install -r dashboard/requirements.txt
```
Expected: `Successfully installed duckdb-1.1.3 jinja2-3.1.5 pandas-2.2.3 pytest-8.3.4 ...`(의존 numpy 등 포함).

- [ ] **Step 4: 설치 검증**

Run:
```bash
dashboard/.venv/bin/python -c "import duckdb, pandas, jinja2; print(duckdb.__version__, pandas.__version__, jinja2.__version__)"
```
Expected: `1.1.3 2.2.3 3.1.5`

- [ ] **Step 5: Commit**

```bash
git add dashboard/requirements.txt .gitignore
git commit -F - <<'EOF'
chore(dashboard): Phase 2 Python 의존성·gitignore 추가

대시보드 레이어 전용 requirements(duckdb·pandas·jinja2·pytest)와
생성물(dashboard/site/)·venv(dashboard/.venv/) gitignore.
marts.duckdb는 서빙 사본이라 의도적으로 커밋 대상에서 제외하지 않음.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Task 2: export_duckdb.py — parquet → marts.duckdb

`output/gold/<table>/*.parquet`를 읽어 마트당 테이블 1개로 `marts.duckdb`를 만든다.

**Files:**
- Create: `dashboard/export_duckdb.py`
- Create: `dashboard/tests/conftest.py`
- Create: `dashboard/tests/test_export.py`
- Generate+commit: `dashboard/marts.duckdb`

- [ ] **Step 1: import 경로 conftest 작성**

`dashboard/tests/conftest.py`:
```python
# pytest가 dashboard/의 top-level 모듈(export_duckdb, build)을 import할 수 있게
# dashboard/ 디렉토리를 sys.path에 추가한다(플랫 import 유지 — 패키지화 불필요).
import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
```

- [ ] **Step 2: 실패 테스트 작성**

`dashboard/tests/test_export.py`:
```python
# export_marts: gold parquet 디렉토리 → marts.duckdb. 합성 parquet 픽스처로
# (1) 마트별 테이블 생성 (2) 행수 = 입력 행수 를 검증한다(수치 자체는 단언 안 함).
import duckdb

from export_duckdb import export_marts


def _write_parquet(con, sql, path):
    # duckdb COPY로 합성 parquet 생성(pyarrow 불필요).
    con.execute(f"COPY ({sql}) TO '{path}' (FORMAT PARQUET)")


def test_export_creates_tables_with_matching_row_counts(tmp_path):
    gold = tmp_path / "gold"
    (gold / "mart_dau").mkdir(parents=True)
    (gold / "mart_wau").mkdir(parents=True)
    w = duckdb.connect()
    _write_parquet(
        w,
        "SELECT * FROM (VALUES ('2019-10-07', 5, 7), ('2019-10-08', 6, 9)) "
        "AS t(event_date, dau_users, dau_sessions)",
        gold / "mart_dau" / "part-0.parquet",
    )
    _write_parquet(
        w,
        "SELECT * FROM (VALUES (DATE '2019-10-07', 100, 120)) "
        "AS t(week_start, wau_users, wau_sessions)",
        gold / "mart_wau" / "part-0.parquet",
    )
    w.close()

    db = tmp_path / "marts.duckdb"
    tables = export_marts(gold, db)

    assert set(tables) == {"mart_dau", "mart_wau"}
    con = duckdb.connect(str(db), read_only=True)
    assert con.execute("SELECT count(*) FROM mart_dau").fetchone()[0] == 2
    assert con.execute("SELECT count(*) FROM mart_wau").fetchone()[0] == 1
    con.close()
```

- [ ] **Step 3: 테스트 실패 확인**

Run: `dashboard/.venv/bin/pytest dashboard/tests/test_export.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'export_duckdb'`

- [ ] **Step 4: export_duckdb.py 구현**

`dashboard/export_duckdb.py`:
```python
# export_duckdb: Gold parquet 마트(output/gold/<table>/*.parquet)를 읽어
# 서빙용 임베디드 DB 파일 dashboard/marts.duckdb로 적재한다(마트당 테이블 1개).
# CI는 Spark/parquet 없이 이 커밋된 .duckdb만 읽으므로, 이 스크립트는
# 데이터 갱신 시 로컬에서 1회 실행한다(Phase 4 Airflow DAG가 이를 흡수 예정).
import pathlib

import duckdb


def export_marts(gold_dir, db_path):
    """gold_dir 하위의 각 마트 디렉토리를 marts.duckdb 테이블로 적재.

    반환: 적재된 테이블 이름 목록(정렬).
    CREATE OR REPLACE라 재실행해도 멱등.
    """
    gold_dir = pathlib.Path(gold_dir)
    db_path = pathlib.Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    tables = []
    # 하위 디렉토리(=마트) 중 parquet 파일이 있는 것만 적재. _SUCCESS 등은 무시.
    for sub in sorted(gold_dir.iterdir()):
        if not sub.is_dir():
            continue
        if not list(sub.glob("*.parquet")):
            continue
        table = sub.name
        con.execute(
            f"CREATE OR REPLACE TABLE {table} AS "
            f"SELECT * FROM read_parquet('{sub}/*.parquet')"
        )
        tables.append(table)
    con.close()
    return sorted(tables)


if __name__ == "__main__":
    names = export_marts("output/gold", "dashboard/marts.duckdb")
    print(f"exported {len(names)} marts -> dashboard/marts.duckdb: {names}")
```

- [ ] **Step 5: 테스트 통과 확인**

Run: `dashboard/.venv/bin/pytest dashboard/tests/test_export.py -v`
Expected: PASS (`1 passed`)

- [ ] **Step 6: 실데이터로 marts.duckdb 생성**

Run (Phase 1 산출 `output/gold/*`가 로컬에 있어야 함):
```bash
dashboard/.venv/bin/python dashboard/export_duckdb.py
```
Expected: `exported 10 marts -> dashboard/marts.duckdb: ['dim_date', 'fact_daily_activity', 'mart_cvr', 'mart_dau', 'mart_funnel', 'mart_mau', 'mart_retention', 'mart_revenue', 'mart_stickiness', 'mart_wau']`

검증:
```bash
dashboard/.venv/bin/python -c "import duckdb; c=duckdb.connect('dashboard/marts.duckdb', read_only=True); print([r[0] for r in c.execute('SHOW TABLES').fetchall()]); print('mart_wau rows:', c.execute('SELECT count(*) FROM mart_wau').fetchone()[0])"
```
Expected: 10개 테이블 목록 + `mart_wau rows: 9`(Oct/Nov 9주).

- [ ] **Step 7: Commit (코드 + 실데이터 duckdb)**

```bash
git add dashboard/export_duckdb.py dashboard/tests/conftest.py dashboard/tests/test_export.py dashboard/marts.duckdb
git commit -F - <<'EOF'
feat(dashboard): Gold parquet → marts.duckdb export

export_marts가 output/gold/<table>/*.parquet를 마트당 테이블로 적재
(CREATE OR REPLACE 멱등). CI는 Spark 없이 이 커밋된 duckdb만 읽음.
합성 parquet 픽스처로 테이블 생성·행수 일치 검증. 실데이터 export로
10개 마트 marts.duckdb 동봉(서빙 사본·패턴3).

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Task 3: build.py — load_marts (duckdb → JSON-able records)

`marts.duckdb`의 각 테이블을 날짜는 `yyyy-MM-dd` 문자열, NULL은 `None`으로 직렬화한 레코드 dict로 변환한다.

**Files:**
- Create: `dashboard/build.py`
- Create: `dashboard/tests/test_build.py`

- [ ] **Step 1: 실패 테스트 작성**

`dashboard/tests/test_build.py`:
```python
# build.py 단위 테스트. 합성 marts.duckdb 픽스처로 (1) 날짜→문자열 직렬화
# (2) NULL→None (3) 마트별 행수 일치 를 검증(특정 수치는 단언 안 함).
import json
import re

import duckdb

from build import load_marts


def test_load_marts_serializes_dates_and_nulls(tmp_path):
    db = tmp_path / "m.duckdb"
    con = duckdb.connect(str(db))
    # mart_cvr: 첫 행 cvr_wow_delta=NULL(실제 마트와 동일한 구조).
    con.execute(
        "CREATE TABLE mart_cvr AS SELECT * FROM (VALUES "
        "(DATE '2019-10-07', 100, 5, 0.05, NULL), "
        "(DATE '2019-10-14', 120, 9, 0.075, 0.025)) "
        "AS t(week_start, visitors, purchasers, cvr, cvr_wow_delta)"
    )
    con.close()

    marts = load_marts(db)

    assert marts["mart_cvr"][0]["week_start"] == "2019-10-07"
    assert marts["mart_cvr"][0]["cvr_wow_delta"] is None
    assert marts["mart_cvr"][1]["cvr_wow_delta"] == 0.025
    assert len(marts["mart_cvr"]) == 2
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `dashboard/.venv/bin/pytest dashboard/tests/test_build.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'build'`

- [ ] **Step 3: build.py에 load_marts 구현**

`dashboard/build.py`(이번 Task에서는 load_marts만; render/build는 Task 4·5에서 추가):
```python
# build.py: marts.duckdb를 읽어 정적 대시보드 HTML(index.html)을 생성한다.
# duckdb → pandas → (날짜 문자열화·NULL 정규화) → Jinja2 템플릿에 JSON 임베드.
# 차트는 Chart.js(CDN), 데이터는 페이지에 구워져 콜드스타트 0.
import json
import pathlib

import duckdb
import pandas as pd


def load_marts(db_path):
    """marts.duckdb의 모든 테이블을 {테이블명: [레코드dict, ...]}로 반환.

    날짜/타임스탬프 컬럼은 'yyyy-MM-dd' 문자열로, NULL/NaN은 None으로 정규화한다
    (Chart.js 라벨·JSON 임베드에 적합한 형태).
    """
    con = duckdb.connect(str(db_path), read_only=True)
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    out = {}
    for t in tables:
        df = con.execute(f"SELECT * FROM {t}").df()
        for col in df.columns:
            # date/timestamp 컬럼은 날짜 문자열로(예: week_start, date, cohort_week).
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime("%Y-%m-%d")
        # to_json이 NaN→null 처리 → 다시 파싱해 None이 든 순수 레코드 리스트로.
        out[t] = json.loads(df.to_json(orient="records"))
    con.close()
    return out
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `dashboard/.venv/bin/pytest dashboard/tests/test_build.py -v`
Expected: PASS (`1 passed`)

- [ ] **Step 5: Commit**

```bash
git add dashboard/build.py dashboard/tests/test_build.py
git commit -F - <<'EOF'
feat(dashboard): load_marts(duckdb→정규화 레코드)

marts.duckdb의 각 테이블을 레코드 dict로 변환. 날짜는 yyyy-MM-dd
문자열, NULL/NaN은 None으로 정규화(Chart.js·JSON 임베드용).
mart_cvr의 NULL cvr_wow_delta 직렬화·행수로 검증.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Task 4: 템플릿 + render() — KPI 4섹션 + Chart.js

데이터를 임베드하고 4개 KPI 섹션·차트를 그리는 단일 자기완결 HTML 템플릿과 렌더 함수.

**Files:**
- Create: `dashboard/templates/index.html.j2`
- Modify: `dashboard/build.py`(render 추가)
- Modify: `dashboard/tests/test_build.py`(render 테스트 추가)

- [ ] **Step 1: 실패 테스트 추가**

`dashboard/tests/test_build.py` 끝에 추가:
```python
from build import render


def test_render_embeds_marts_and_kpi_sections():
    marts = {"mart_dau": [{"event_date": "2019-10-07", "dau_users": 5, "dau_sessions": 7}]}
    html = render(marts, "dashboard/templates/index.html.j2")

    # 데이터가 JSON 스크립트 블록에 임베드됐는지.
    assert 'id="marts-data"' in html
    assert '"mart_dau"' in html
    # KPI 4섹션 DOM 존재.
    assert 'id="section-engagement"' in html
    assert 'id="section-conversion"' in html
    assert 'id="section-monetization"' in html
    assert 'id="section-retention"' in html
    # 차트 라이브러리는 CDN.
    assert "cdn.jsdelivr.net/npm/chart.js" in html
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `dashboard/.venv/bin/pytest dashboard/tests/test_build.py::test_render_embeds_marts_and_kpi_sections -v`
Expected: FAIL — `ImportError: cannot import name 'render'`

- [ ] **Step 3: 템플릿 작성**

`dashboard/templates/index.html.j2`:
```html
<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>이커머스 Activity 대시보드 — Gold 마트</title>
<!-- 차트는 Chart.js CDN. 데이터(JSON)는 아래 marts-data 블록에 임베드 → 콜드스타트 0. -->
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<style>
 body{font-family:system-ui,-apple-system,sans-serif;margin:0;background:#f7f7f9;color:#1c1c28}
 header{background:#1c1c28;color:#fff;padding:20px 28px}
 header h1{margin:0;font-size:20px}
 header p{margin:6px 0 0;font-size:13px;color:#b8b8c8}
 main{max-width:1100px;margin:0 auto;padding:24px}
 section{background:#fff;border:1px solid #e6e6ee;border-radius:10px;padding:18px 20px;margin-bottom:22px}
 section h2{margin:0 0 4px;font-size:16px}
 section .note{margin:0 0 14px;font-size:12px;color:#7a7a8c}
 .grid{display:grid;grid-template-columns:1fr 1fr;gap:18px}
 .card{min-width:0}
 .card h3{margin:0 0 8px;font-size:13px;color:#444}
 canvas{max-height:260px}
 table.ret{border-collapse:collapse;font-size:12px}
 table.ret td,table.ret th{border:1px solid #e6e6ee;padding:4px 8px;text-align:center}
 footer{max-width:1100px;margin:0 auto;padding:0 24px 40px;font-size:12px;color:#7a7a8c}
</style>
</head>
<body>
<header>
  <h1>이커머스 Activity 대시보드 — Gold 마트</h1>
  <p>Silver activity → Gold 마트(parquet) → marts.duckdb. 수치는 실제 집계 결과(2019 Oct/Nov).</p>
</header>
<main>
  <section id="section-engagement">
    <h2>Engagement</h2>
    <p class="note">DAU·WAU·MAU·Stickiness. MAU·Stickiness는 데이터 2개월이라 표본이 얇음(점 2~3개).</p>
    <div class="grid">
      <div class="card"><h3>DAU (일별 활성 user/session)</h3><canvas id="c-dau"></canvas></div>
      <div class="card"><h3>WAU (주별 활성 user/session)</h3><canvas id="c-wau"></canvas></div>
      <div class="card"><h3>MAU (월별 활성 user)</h3><canvas id="c-mau"></canvas></div>
      <div class="card"><h3>Stickiness (DAU/MAU)</h3><canvas id="c-stick"></canvas></div>
    </div>
  </section>
  <section id="section-conversion">
    <h2>Conversion</h2>
    <p class="note">퍼널은 가장 최근 주 기준(주별 distinct는 비가산이라 합산 안 함). CVR = 구매자/방문자.</p>
    <div class="grid">
      <div class="card"><h3>퍼널 (최근 주: view→cart→purchase)</h3><canvas id="c-funnel"></canvas></div>
      <div class="card"><h3>주별 CVR + 전주 대비(WoW)</h3><canvas id="c-cvr"></canvas></div>
    </div>
  </section>
  <section id="section-monetization">
    <h2>Monetization</h2>
    <p class="note">매출 = 구매 이벤트 price 합. order_id 없어 구매 이벤트를 주문 proxy로 사용. AOV = 매출/구매건수.</p>
    <div class="grid">
      <div class="card"><h3>주별 매출</h3><canvas id="c-rev"></canvas></div>
      <div class="card"><h3>주별 AOV</h3><canvas id="c-aov"></canvas></div>
    </div>
  </section>
  <section id="section-retention">
    <h2>Retention</h2>
    <p class="note">주차 코호트(데이터 내 첫 활동 주). 2개월(9주)이라 작은 삼각형. 색이 진할수록 잔존율 높음.</p>
    <div id="ret-table"></div>
  </section>
</main>
<footer>생성: build.py(duckdb→pandas→Jinja2). 차트: Chart.js CDN. 데이터는 이 페이지에 임베드됨.</footer>

<!-- 마트 데이터: JSON 블록으로 임베드(스크립트 파싱 안정 + 추출 용이). -->
<script id="marts-data" type="application/json">{{ marts_json | safe }}</script>
<script>
const MARTS = JSON.parse(document.getElementById('marts-data').textContent);

// 공통 헬퍼: 색 팔레트 / 차트 옵션 / 라인·바 생성.
function palette(i){return ['#5b8def','#27ae60','#e67e22','#9b59b6'][i % 4];}
function opt(){return {responsive:true,plugins:{legend:{labels:{boxWidth:12,font:{size:11}}}},
  scales:{x:{ticks:{font:{size:10}}},y:{ticks:{font:{size:10}}}}};}
function lineChart(id, labels, series){
  const el=document.getElementById(id); if(!el) return;
  new Chart(el,{type:'line',data:{labels:labels,datasets:series.map((s,i)=>(
    {label:s.label,data:s.data,borderColor:palette(i),backgroundColor:palette(i),tension:.2}))},options:opt()});
}
function barChart(id, labels, series){
  const el=document.getElementById(id); if(!el) return;
  new Chart(el,{type:'bar',data:{labels:labels,datasets:series.map((s,i)=>(
    {label:s.label,data:s.data,backgroundColor:palette(i)}))},options:opt()});
}

// Engagement
if(MARTS.mart_dau){const d=MARTS.mart_dau;
  lineChart('c-dau',d.map(r=>r.event_date),
    [{label:'users',data:d.map(r=>r.dau_users)},{label:'sessions',data:d.map(r=>r.dau_sessions)}]);}
if(MARTS.mart_wau){const d=MARTS.mart_wau;
  lineChart('c-wau',d.map(r=>r.week_start),
    [{label:'users',data:d.map(r=>r.wau_users)},{label:'sessions',data:d.map(r=>r.wau_sessions)}]);}
if(MARTS.mart_mau){const d=MARTS.mart_mau;
  barChart('c-mau',d.map(r=>r.month),[{label:'MAU',data:d.map(r=>r.mau_users)}]);}
if(MARTS.mart_stickiness){const d=MARTS.mart_stickiness;
  lineChart('c-stick',d.map(r=>r.date),[{label:'stickiness',data:d.map(r=>r.stickiness)}]);}

// Conversion
if(MARTS.mart_funnel && MARTS.mart_funnel.length){
  const last=MARTS.mart_funnel[MARTS.mart_funnel.length-1];
  barChart('c-funnel',['view','cart','purchase'],
    [{label:last.week_start,data:[last.users_view,last.users_cart,last.users_purchase]}]);}
if(MARTS.mart_cvr){const d=MARTS.mart_cvr; const el=document.getElementById('c-cvr');
  if(el) new Chart(el,{data:{labels:d.map(r=>r.week_start),datasets:[
    {type:'line',label:'CVR',data:d.map(r=>r.cvr),borderColor:palette(0),backgroundColor:palette(0),tension:.2},
    {type:'bar',label:'WoW Δ',data:d.map(r=>r.cvr_wow_delta),backgroundColor:palette(2)}]},options:opt()});}

// Monetization
if(MARTS.mart_revenue){const d=MARTS.mart_revenue;
  barChart('c-rev',d.map(r=>r.week_start),[{label:'revenue',data:d.map(r=>r.revenue)}]);
  lineChart('c-aov',d.map(r=>r.week_start),[{label:'AOV',data:d.map(r=>r.aov)}]);}

// Retention: 코호트 × week_offset 삼각 테이블(셀=잔존율). Chart.js 매트릭스 플러그인 대신 CSS 음영.
(function(){
  const d=MARTS.mart_retention; const host=document.getElementById('ret-table'); if(!d || !host) return;
  const cohorts=[...new Set(d.map(r=>r.cohort_week))].sort();
  const offsets=[...new Set(d.map(r=>r.week_offset))].sort((a,b)=>a-b);
  const idx={}; d.forEach(r=>{idx[r.cohort_week+'|'+r.week_offset]=r.retention_rate;});
  let html='<table class="ret"><tr><th>코호트 / 주차</th>'+offsets.map(o=>'<th>+'+o+'</th>').join('')+'</tr>';
  cohorts.forEach(c=>{
    html+='<tr><th>'+c+'</th>'+offsets.map(o=>{
      const v=idx[c+'|'+o];
      if(v==null) return '<td></td>';
      const a=Math.max(0,Math.min(1,v));
      return '<td style="background:rgba(39,174,96,'+a.toFixed(2)+')">'+(v*100).toFixed(0)+'%</td>';
    }).join('')+'</tr>';
  });
  html+='</table>'; host.innerHTML=html;
})();
</script>
</body>
</html>
```

- [ ] **Step 4: build.py에 render 추가**

`dashboard/build.py`의 import 블록에 추가:
```python
from jinja2 import Environment, FileSystemLoader
```
파일 끝(load_marts 아래)에 추가:
```python
def render(marts, template_path):
    """마트 dict를 JSON으로 임베드한 정적 HTML 문자열을 반환."""
    template_path = pathlib.Path(template_path)
    env = Environment(
        loader=FileSystemLoader(str(template_path.parent)),
        autoescape=False,  # JSON/JS는 우리가 통제(|safe). 데이터에 </script> 없음.
    )
    tmpl = env.get_template(template_path.name)
    return tmpl.render(marts_json=json.dumps(marts, ensure_ascii=False))
```

- [ ] **Step 5: 테스트 통과 확인**

Run: `dashboard/.venv/bin/pytest dashboard/tests/test_build.py -v`
Expected: PASS (`2 passed`)

- [ ] **Step 6: Commit**

```bash
git add dashboard/build.py dashboard/templates/index.html.j2 dashboard/tests/test_build.py
git commit -F - <<'EOF'
feat(dashboard): KPI 4섹션 템플릿 + render()

Engagement/Conversion/Monetization/Retention 4섹션 단일 자기완결
HTML 템플릿. 데이터는 marts-data JSON 블록에 임베드, 차트는 Chart.js
CDN. 리텐션은 매트릭스 플러그인 대신 CSS 음영 삼각 테이블. render가
마트 dict를 JSON 임베드해 HTML 반환. 섹션·CDN·임베드 존재로 검증.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Task 5: build() — site/index.html 생성

load_marts + render를 묶어 `dashboard/site/index.html`을 쓰는 엔트리포인트.

**Files:**
- Modify: `dashboard/build.py`(build + `__main__`)
- Modify: `dashboard/tests/test_build.py`(build 테스트 추가)

- [ ] **Step 1: 실패 테스트 추가**

`dashboard/tests/test_build.py` 끝에 추가:
```python
from build import build


def test_build_writes_html_with_matching_row_counts(tmp_path):
    db = tmp_path / "m.duckdb"
    con = duckdb.connect(str(db))
    con.execute(
        "CREATE TABLE mart_dau AS SELECT * FROM (VALUES "
        "('2019-10-07', 5, 7), ('2019-10-08', 6, 9)) "
        "AS t(event_date, dau_users, dau_sessions)"
    )
    con.close()

    out = tmp_path / "site" / "index.html"
    build(db, "dashboard/templates/index.html.j2", out)

    html = out.read_text(encoding="utf-8")
    # 임베드 JSON 블록 추출 → 마트 행수가 duckdb 행수와 일치.
    m = re.search(
        r'<script id="marts-data" type="application/json">(.*?)</script>',
        html,
        re.S,
    )
    data = json.loads(m.group(1))
    assert len(data["mart_dau"]) == 2
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `dashboard/.venv/bin/pytest dashboard/tests/test_build.py::test_build_writes_html_with_matching_row_counts -v`
Expected: FAIL — `ImportError: cannot import name 'build'`

- [ ] **Step 3: build + __main__ 구현**

`dashboard/build.py` 끝에 추가:
```python
def build(db_path="dashboard/marts.duckdb",
          template_path="dashboard/templates/index.html.j2",
          out_path="dashboard/site/index.html"):
    """marts.duckdb → 정적 index.html. 산출 경로(pathlib.Path) 반환."""
    marts = load_marts(db_path)
    html = render(marts, template_path)
    out = pathlib.Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    return out


if __name__ == "__main__":
    p = build()
    print(f"built {p}")
```

- [ ] **Step 4: 전체 테스트 통과 확인**

Run: `dashboard/.venv/bin/pytest dashboard/tests -v`
Expected: PASS (`4 passed` — export 1 + build 3)

- [ ] **Step 5: 실데이터로 빌드 + 육안 확인**

Run:
```bash
dashboard/.venv/bin/python dashboard/build.py
```
Expected: `built dashboard/site/index.html`

검증(임베드 마트 키 확인 + 브라우저로 열기):
```bash
dashboard/.venv/bin/python -c "import re,json,pathlib; h=pathlib.Path('dashboard/site/index.html').read_text(); d=json.loads(re.search(r'marts-data\" type=\"application/json\">(.*?)</script>', h, re.S).group(1)); print('tables:', sorted(d)); print('mart_wau rows:', len(d['mart_wau']))"
open dashboard/site/index.html
```
Expected: 10개 테이블 키 + `mart_wau rows: 9`. 브라우저에 4섹션 차트·리텐션 테이블 표시(생성물은 gitignore라 커밋 안 함).

- [ ] **Step 6: Commit**

```bash
git add dashboard/build.py dashboard/tests/test_build.py
git commit -F - <<'EOF'
feat(dashboard): build()로 site/index.html 생성

load_marts+render을 묶어 dashboard/site/index.html 출력(생성물·gitignore).
임베드 JSON 행수=duckdb 행수로 스모크 검증. 실데이터 빌드로 4섹션
대시보드 육안 확인.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Task 6: GitHub Actions → Pages 배포 워크플로

main push 시 커밋된 `marts.duckdb`로 빌드해 Pages에 배포한다. (CI/CD 설정 — 로컬 TDD 불가, YAML 파싱 검증 + push 후 Actions 확인)

**Files:**
- Create: `.github/workflows/pages.yml`

- [ ] **Step 1: 워크플로 작성**

`.github/workflows/pages.yml`:
```yaml
# Phase 2 정적 대시보드 배포: main push마다 커밋된 marts.duckdb로 build.py를
# 실행해 dashboard/site/를 GitHub Pages에 배포. Spark/parquet 불필요.
# 사전 1회: repo Settings → Pages → Source = "GitHub Actions"로 설정해야 함.
name: Deploy dashboard to Pages

on:
  push:
    branches: [main]
    paths:
      - 'dashboard/**'
      - '.github/workflows/pages.yml'
  workflow_dispatch:

permissions:
  contents: read
  pages: write
  id-token: write

concurrency:
  group: pages
  cancel-in-progress: true

jobs:
  build-deploy:
    runs-on: ubuntu-latest
    environment:
      name: github-pages
      url: ${{ steps.deploy.outputs.page_url }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - name: Install deps
        run: pip install -r dashboard/requirements.txt
      - name: Build static dashboard
        run: python dashboard/build.py
      - name: Upload Pages artifact
        uses: actions/upload-pages-artifact@v3
        with:
          path: dashboard/site
      - name: Deploy to Pages
        id: deploy
        uses: actions/deploy-pages@v4
```

- [ ] **Step 2: YAML 파싱 검증**

Run(런타임에 pyyaml 없으면 venv에 임시 설치 후 확인):
```bash
dashboard/.venv/bin/pip install pyyaml -q && \
dashboard/.venv/bin/python -c "import yaml; d=yaml.safe_load(open('.github/workflows/pages.yml')); print('jobs:', list(d['jobs'])); print('steps:', len(d['jobs']['build-deploy']['steps']))"
```
Expected: `jobs: ['build-deploy']` / `steps: 6`
(pyyaml은 검증 전용 — requirements.txt에 넣지 않음. 빌드는 pyyaml 불필요.)

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/pages.yml
git commit -F - <<'EOF'
ci(dashboard): GitHub Actions → Pages 배포 워크플로

main push(dashboard/** 변경) 시 커밋된 marts.duckdb로 build.py 실행 →
dashboard/site를 Pages artifact로 배포. Spark/parquet 불필요.
사전 1회 repo Settings에서 Pages Source=GitHub Actions 설정 필요.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

> **수동 1회 설정(머지 후, 사용자 작업):** GitHub repo → Settings → Pages → Build and deployment → Source를 **GitHub Actions**로 변경. 이후 main에 dashboard 변경이 push되면 자동 배포되고 `https://<user>.github.io/<repo>/`에서 열람.

---

## Task 7: 문서화 — README 언어 경계 + 대시보드 런북

언어 경계(Python 허용)·도구·구조를 README에 반영하고, 대시보드 실행 절차 런북을 추가한다.

**Files:**
- Modify: `README.md`(§4 언어 경계 노트, §6.1 도구, §8 구조)
- Create: `docs/runbook/dashboard.md`

- [ ] **Step 1: README §4에 언어 경계 노트 추가**

`README.md`의 `## 4. 언어 선택 (Scala) 사유` 섹션 본문 끝에 아래 문단을 추가(기존 문장은 수정하지 말 것 — Surgical):
```markdown

> **대시보드 레이어는 Python(경계 명시).** 파이프라인 본체·`GoldMarts` 등 **Spark Application은 Scala**로 유지한다. Phase 2의 마트 export(`dashboard/export_duckdb.py`)·정적 빌드(`dashboard/build.py`)·CI는 Spark 외 서빙/오케스트레이션이라 Python(duckdb/pandas/jinja2)을 쓴다. 제출용 WAU 정본은 여전히 Hive `activity`의 `sql/wau.sql`이며, 대시보드의 `marts.duckdb`는 Gold 서빙 사본이다(Hive 대체 아님).
```

- [ ] **Step 2: README §6.1 사용 도구에 대시보드 스택 한 줄 추가**

`### 6.1. 사용 도구` 목록(불릿)의 끝에 추가:
```markdown
- **대시보드(Phase 2)**: DuckDB(임베디드 파일) + pandas + Jinja2 정적 빌드, Chart.js(CDN), GitHub Actions → GitHub Pages. (Spark 외 서빙 레이어 — Python)
```

- [ ] **Step 3: README §8 구조 맵에 dashboard 추가**

`## 8. 프로젝트 구조 / 문서 맵` 안의 구조 목록에 아래 항목을 적절한 위치(예: sql/ 항목 근처)에 추가:
```markdown
- `dashboard/` — Phase 2 서빙: `export_duckdb.py`(Gold→`marts.duckdb`), `build.py`+`templates/`(정적 `index.html`). 실행: [docs/runbook/dashboard.md](docs/runbook/dashboard.md)
```

- [ ] **Step 4: 대시보드 런북 작성**

`docs/runbook/dashboard.md`:
```markdown
# 런북 — Phase 2 Mart Export + 정적 대시보드

Gold parquet 마트를 `marts.duckdb`로 export하고 정적 `index.html`을 빌드·열람하는 절차.
근거: 설계 스펙 §3.4·§3.5·§11. 파이프라인은 Scala, 본 레이어는 Python(언어 경계).

## 0. 전제
- Phase 1 산출물 `output/gold/*`(parquet)이 로컬에 존재(없으면 `docs/runbook/gold-marts` 또는 GoldMarts 실행 선행).
- Python 3.9+.

## 1. venv 준비(최초 1회)
```bash
python3 -m venv dashboard/.venv
dashboard/.venv/bin/pip install --upgrade pip
dashboard/.venv/bin/pip install -r dashboard/requirements.txt
```

## 2. 테스트
```bash
dashboard/.venv/bin/pytest dashboard/tests -v   # 4 passed
```

## 3. Export (Gold parquet → marts.duckdb)
```bash
dashboard/.venv/bin/python dashboard/export_duckdb.py
# exported 10 marts -> dashboard/marts.duckdb: [...]
```
`marts.duckdb`는 서빙 사본이라 **커밋**한다(패턴3).

## 4. 정적 빌드 + 열람
```bash
dashboard/.venv/bin/python dashboard/build.py   # built dashboard/site/index.html
open dashboard/site/index.html
```
`dashboard/site/`는 생성물이라 gitignore. 데이터는 HTML에 임베드되어 콜드스타트 0.

## 5. 배포(GitHub Pages)
- main에 `dashboard/**` 변경이 push되면 `.github/workflows/pages.yml`이 빌드·배포.
- **최초 1회**: repo Settings → Pages → Source = **GitHub Actions**.
- 공개 URL: `https://<user>.github.io/<repo>/`.

## 트러블슈팅
- `ModuleNotFoundError`: venv 활성 경로(`dashboard/.venv/bin/...`)로 실행했는지 확인.
- 빈 차트: `output/gold/*` 부재 → export가 0행. Phase 1 산출 먼저 생성.
- Pages 404: Settings의 Source가 GitHub Actions인지, 워크플로 성공했는지 Actions 탭 확인.
```

- [ ] **Step 5: Commit**

```bash
git add README.md docs/runbook/dashboard.md
git commit -F - <<'EOF'
docs(dashboard): 언어 경계·도구·구조 + Phase 2 런북

README §4에 대시보드 레이어 Python 경계 명시(Spark Application은 Scala
유지, WAU 정본은 Hive activity). §6.1 도구·§8 구조에 dashboard 추가.
docs/runbook/dashboard.md에 venv·export·build·Pages 배포 절차.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Task 8: 검증 + 브랜치 마감(PR)

전체 검증 후 PR을 연다. (REQUIRED SUB-SKILL: superpowers:verification-before-completion, 이어 superpowers:finishing-a-development-branch)

- [ ] **Step 1: 전체 테스트 재실행**

Run: `dashboard/.venv/bin/pytest dashboard/tests -v`
Expected: `4 passed`

- [ ] **Step 2: export + build 재실행(실데이터)**

Run:
```bash
dashboard/.venv/bin/python dashboard/export_duckdb.py
dashboard/.venv/bin/python dashboard/build.py
```
Expected: 10 마트 export + `built dashboard/site/index.html`. `open dashboard/site/index.html`로 4섹션 차트·리텐션 테이블 육안 확인.

- [ ] **Step 3: 커밋 트리·상태 확인**

Run: `git status && git log --oneline origin/main..HEAD`
Expected: 워킹트리 clean(`dashboard/site/` 미추적·무시됨), Task 1~7 커밋 7개, `dashboard/marts.duckdb` 추적됨.

- [ ] **Step 4: push + PR 생성**

```bash
git push -u origin feat/mart-export-dashboard
```
PR 본문은 컨벤션 7섹션 + 새 포맷(엔대시 1–N, 명사형 불릿, 한 줄 한 문장). 코드블록 포함이라 `--body-file` 사용. Claude 서명 금지(§2.7). 예:
```bash
gh pr create --base main --head feat/mart-export-dashboard \
  --title "feat(dashboard): Phase 2 Mart Export + 정적 대시보드" \
  --body-file docs/superpowers/plans/_pr-body-phase2.md   # 작성자가 7섹션 본문 작성
```
Expected: PR(base=main) 생성. CI(pages.yml)는 머지 후 main에서 동작.

> **머지 후(사용자):** Settings → Pages → Source = GitHub Actions 1회 설정 → 공개 URL 확인.

---

## Self-Review (이 계획 ↔ 스펙 §11 대조)

- **§11.1 데이터 흐름**: Task 2(export)·5(build)·6(CI) 커버. CI가 커밋 duckdb만 읽음(Task 6) — `output/` gitignore와 정합. ✅
- **§11.2 결정**: P2-1 Actions→Pages(Task 6), P2-2 Chart.js CDN(Task 4 템플릿), P2-3 index.html 미커밋(Task 1 gitignore `dashboard/site/`), P2-4 marts.duckdb 커밋(Task 2 Step 7), P2-5 별도 스펙 없이 본 계획. ✅
- **§11.3 컴포넌트 6파일**: requirements(T1)·export_duckdb(T2)·build(T3-5)·templates(T4)·pages.yml(T6)·tests(T2-5) 모두 Task 보유. ✅
- **§11.4 테스트**: export 행수(T2)·load 날짜·NULL(T3)·render 섹션·CDN(T4)·build 행수(T5). 수치 단언 없음. ✅
- **§11.5 언어 경계**: README §4 노트(T7). Python 주석 전 파일 반영. ✅
- **§5 KPI 4그룹**: 템플릿 4섹션(T4)에 dau/wau/mau/stickiness/funnel/cvr/revenue/aov/retention 차트. ✅
- **타입 일관성**: 함수 시그니처 `export_marts(gold_dir, db_path)`·`load_marts(db_path)`·`render(marts, template_path)`·`build(db_path, template_path, out_path)` 전 Task 동일. 임베드 추출 패턴(`marts-data` 스크립트 블록)도 T4 템플릿↔T5 테스트 일치. ✅

**스펙 대비 의도적 정련(계획 단계 결정):**
- 산출 경로를 `dashboard/index.html` → **`dashboard/site/index.html`**로 둠: `upload-pages-artifact`가 디렉토리를 사이트 루트로 올리므로, `.py`와 섞이지 않게 전용 `site/` 폴더 사용(깨끗한 Pages 아티팩트). gitignore는 `dashboard/site/`.
- 리텐션은 Chart.js 매트릭스 플러그인 대신 **CSS 음영 HTML 테이블**: CDN 의존 1개로 유지(Simplicity), 삼각 코호트가 테이블로 더 읽힘.
