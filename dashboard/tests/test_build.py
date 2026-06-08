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
