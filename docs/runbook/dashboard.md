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

최초 1회 설정은 **순서가 중요**하다(공개 → 소스 → 머지).

1. **repo 공개** — Settings → General → Danger Zone → Change visibility → Public. (무료 플랜은 public repo에서만 Pages 동작 — private면 "Upgrade or make this repository public"만 뜸.)
2. **Pages 소스 지정** — Settings → Pages → Source = **GitHub Actions**. (이걸 먼저 안 켜면 머지돼도 `deploy-pages` 단계가 실패.)
3. **머지/푸시** — main에 `dashboard/**` 변경이 들어가면 `.github/workflows/pages.yml`이 빌드·배포.
4. 공개 URL: `https://<user>.github.io/<repo>/`(반영까지 ~1분).

이후엔 main push마다 자동 재배포. 자세한 실패 패턴은 [트러블슈팅 문서](../troubleshooting/dashboard-build-deploy.md) 참고.

## 트러블슈팅
- `ModuleNotFoundError`: venv 경로(`dashboard/.venv/bin/...`)로 실행했는지 확인.
- 빈 차트: `output/gold/*` 부재 → export가 0행. Phase 1 산출 먼저 생성.
- Pages 설정 화면이 "Upgrade or make public"만 표시: repo가 private. public 전환 필요(§5-1).
- Pages 404 / 배포 실패: Source가 GitHub Actions인지, 워크플로가 초록인지 Actions 탭 확인.
- 자세한 빌드·배포 이슈(public 요건·duckdb 비결정성·Chart.js 함정): [docs/troubleshooting/dashboard-build-deploy.md](../troubleshooting/dashboard-build-deploy.md).
