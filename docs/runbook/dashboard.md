# 런북 — Phase 2 Mart Export + 정적 대시보드

Gold parquet 마트를 `marts.duckdb`로 export하고 정적 `index.html`을 빌드·열람하는 절차.
근거: 설계 스펙 §3.4·§3.5·§11. 파이프라인은 Scala, 본 레이어는 Python(언어 경계).

## 0. 전제
- Phase 1 산출물 `output/gold/*`(parquet)이 로컬에 존재(없으면 GoldMarts 실행 선행).
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
- `ModuleNotFoundError`: venv 경로(`dashboard/.venv/bin/...`)로 실행했는지 확인.
- 빈 차트: `output/gold/*` 부재 → export가 0행. Phase 1 산출 먼저 생성.
- Pages 404: Settings의 Source가 GitHub Actions인지, 워크플로 성공했는지 Actions 탭 확인.
