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
    out = {}
    # with 블록: 조회 중 예외가 나도 커넥션을 확실히 닫는다(export_duckdb.py와 동일 패턴).
    with duckdb.connect(str(db_path), read_only=True) as con:
        tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
        for t in tables:
            df = con.execute(f"SELECT * FROM {t}").df()
            for col in df.columns:
                # date/timestamp 컬럼은 날짜 문자열로(예: week_start, date, cohort_week).
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].dt.strftime("%Y-%m-%d")
            # to_json이 NaN→null 처리 → 다시 파싱해 None이 든 순수 레코드 리스트로.
            out[t] = json.loads(df.to_json(orient="records"))
    return out
