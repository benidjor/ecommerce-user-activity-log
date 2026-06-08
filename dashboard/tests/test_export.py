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
    writer = duckdb.connect()
    _write_parquet(
        writer,
        "SELECT * FROM (VALUES ('2019-10-07', 5, 7), ('2019-10-08', 6, 9)) "
        "AS t(event_date, dau_users, dau_sessions)",
        gold / "mart_dau" / "part-0.parquet",
    )
    _write_parquet(
        writer,
        "SELECT * FROM (VALUES (DATE '2019-10-07', 100, 120)) "
        "AS t(week_start, wau_users, wau_sessions)",
        gold / "mart_wau" / "part-0.parquet",
    )
    writer.close()

    db = tmp_path / "marts.duckdb"
    tables = export_marts(gold, db)

    assert set(tables) == {"mart_dau", "mart_wau"}
    con = duckdb.connect(str(db), read_only=True)
    assert con.execute("SELECT count(*) FROM mart_dau").fetchone()[0] == 2
    assert con.execute("SELECT count(*) FROM mart_wau").fetchone()[0] == 1
    con.close()
