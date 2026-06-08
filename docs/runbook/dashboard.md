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
4. 공개 URL: `https://benidjor.github.io/ecommerce-user-activity-log/`(반영까지 ~1분).

이후엔 main push마다 자동 재배포. 자세한 실패 패턴은 [트러블슈팅 문서](../troubleshooting/dashboard-build-deploy.md) 참고.

## 6. 지표 정의 & 데이터 경계 주의

대시보드 설명 시 자주 묻는 지표 정의·데이터 경계 효과를 정리한다. 모든 수치는 `marts.duckdb` 실측.

### 6.1. 지표 정의
- DAU/WAU — 일·주별 distinct **user**와 distinct **session** 2종(`mart_dau`·`mart_wau`).
- MAU — 월별 distinct **user**만(`mart_mau`, Monthly Active Users 관례). 월별 세션은 미산출(필요 시 mart 확장).
- Stickiness — 일별 활성 user ÷ 그 달 활성 user(`mart_stickiness`). 분자·분모 모두 **user 기준**(세션 아님).
- CVR — 주별 구매자 수 ÷ 방문자 수(`mart_cvr`). 전주 대비(WoW)는 위치 기반 lag(연속 9주라 정확).
- 퍼널 — 단계(view/cart/purchase)별 그 주 distinct user를 **독립 집계**(`mart_funnel`). 엄격한 부분집합이 아니라, cart 없이 구매한 사용자 때문에 전환율이 100%를 넘는 주가 있음.
- ARPPU — 주별 매출 ÷ 구매자 수(`mart_revenue.revenue` ÷ `mart_cvr.purchasers`). **order_id가 없어 진짜 AOV(주문 1건당)는 산출 불가** → 구매자 1인당 결제액으로 대체.
- Retention — 코호트(첫 활동 주)별 경과 주차 distinct user 비율(`mart_retention`).

### 6.2. 데이터 경계 효과(버그 아님, 정상)
- KST 파티션 경계 — 원본은 UTC 2019-10~11. KST(+9h) 변환으로 2019-11-30 밤 활동 일부가 **2019-12-01 KST**로 넘어와 부분 일(약 8.6만 user)이 생김. 2019-12은 이 하루뿐이라 DAU=MAU → Stickiness=1.0. Stickiness 차트는 이 점을 제외해 그림.
- WAU 첫 주 — ISO 주(월요일 시작)라 첫 점이 **2019-09-30(월)**. 데이터는 2019-10-01부터라 첫 주는 2019-10-01~2019-10-06만 든 부분 주.

### 6.3. 렌더 검증(선택)
헤드리스 Chrome로 차트까지 렌더된 스크린샷 확인:
```bash
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" --headless=new --disable-gpu \
  --window-size=1400,3000 --virtual-time-budget=9000 \
  --screenshot=/tmp/dash.png "file://$(pwd)/dashboard/site/index.html"
```

## 트러블슈팅
- `ModuleNotFoundError`: venv 경로(`dashboard/.venv/bin/...`)로 실행했는지 확인.
- 빈 차트: `output/gold/*` 부재 → export가 0행. Phase 1 산출 먼저 생성.
- Pages 설정 화면이 "Upgrade or make public"만 표시: repo가 private. public 전환 필요(§5-1).
- Pages 404 / 배포 실패: Source가 GitHub Actions인지, 워크플로가 초록인지 Actions 탭 확인.
- 자세한 빌드·배포 이슈(public 요건·duckdb 비결정성·Chart.js 함정): [docs/troubleshooting/dashboard-build-deploy.md](../troubleshooting/dashboard-build-deploy.md).
