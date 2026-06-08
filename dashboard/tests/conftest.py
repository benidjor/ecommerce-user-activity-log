# pytest가 dashboard/의 top-level 모듈(export_duckdb, build)을 import할 수 있게
# dashboard/ 디렉토리를 sys.path에 추가한다(플랫 import 유지 — 패키지화 불필요).
import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
