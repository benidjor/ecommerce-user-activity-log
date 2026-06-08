# build.py: marts.duckdb를 읽어 정적 대시보드 HTML(index.html)을 생성한다.
# duckdb → pandas → (날짜 문자열화·NULL 정규화) → Jinja2 템플릿에 JSON 임베드.
# 차트는 Chart.js(CDN), 데이터는 페이지에 구워져 콜드스타트 0.
import json
import pathlib

import duckdb
import pandas as pd
from jinja2 import Environment, FileSystemLoader


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


def render(marts, template_path):
    """마트 dict를 JSON으로 임베드한 정적 HTML 문자열을 반환."""
    template_path = pathlib.Path(template_path)
    env = Environment(
        loader=FileSystemLoader(str(template_path.parent)),
        autoescape=False,  # JSON/JS는 우리가 통제(|safe). </script> 시퀀스는 아래에서 치환해 차단.
    )
    tmpl = env.get_template(template_path.name)
    # JSON을 <script> 블록에 임베드하므로 "</script>" 시퀀스를 차단(표준 하드닝).
    # "</" → "<\/" 치환은 JSON.parse가 동일하게 되돌리므로 데이터는 변하지 않는다.
    marts_json = json.dumps(marts, ensure_ascii=False).replace("</", "<\\/")
    return tmpl.render(marts_json=marts_json)
