# export_duckdb: Gold parquet 마트(output/gold/<table>/*.parquet)를 읽어
# 서빙용 임베디드 DB 파일 dashboard/marts.duckdb로 적재한다(마트당 테이블 1개).
# output/gold 하위 전체(dim_date·fact_daily_activity·mart_*)를 적재한다 —
# 정적 대시보드는 mart_*만 쓰지만, 서빙 완전성·Phase 3 Streamlit 재사용을 위해 Gold 전부 보존.
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

    tables = []
    # with 블록: 적재 중 예외가 나도 커넥션을 확실히 닫는다(파일 핸들 누수 방지).
    with duckdb.connect(str(db_path)) as con:
        for sub in sorted(gold_dir.iterdir()):
            if not sub.is_dir():
                continue
            if not list(sub.glob("*.parquet")):
                continue
            # table = sub.name은 Gold 마트 디렉토리명(snake_case 고정)이라 identifier 주입 위험 없음.
            table = sub.name
            con.execute(
                f"CREATE OR REPLACE TABLE {table} AS "
                f"SELECT * FROM read_parquet('{sub}/*.parquet')"
            )
            tables.append(table)
    return sorted(tables)


if __name__ == "__main__":
    names = export_marts("output/gold", "dashboard/marts.duckdb")
    print(f"exported {len(names)} marts -> dashboard/marts.duckdb: {names}")
