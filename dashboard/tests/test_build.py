# build.py 단위 테스트. 합성 marts.duckdb 픽스처로 (1) 날짜→문자열 직렬화
# (2) NULL→None (3) 마트별 행수 일치 를 검증(특정 수치는 단언 안 함).
import json
import re

import duckdb

from build import build
from build import load_marts
from build import render


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
