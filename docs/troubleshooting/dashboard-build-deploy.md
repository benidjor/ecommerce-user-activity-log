# 트러블슈팅: 대시보드 빌드 & GitHub Pages 배포 (Phase 2)

정적 대시보드(`dashboard/build.py` → `index.html` → GitHub Pages)를 만들고 배포하는 과정에서 부딪힌 빌드·배포 함정을 정리한다. 근거: 설계 스펙 §11, 런북 `docs/runbook/dashboard.md`.

## 1. private repo라 GitHub Pages가 안 켜짐

### 증상

Settings → Pages에 설정 UI 대신 **"Upgrade or make this repository public to enable Pages"** 배너만 뜨고 Source 드롭다운이 없다.

### 원인

- GitHub 무료 플랜에서 **Pages는 public repo에서만** 동작한다(private Pages는 Pro/Enterprise 유료).
- 본 프로젝트는 비공개로 개발하다 Phase 2 서빙 단계에서 처음 공개가 필요해졌다.

### 해결

- repo를 public으로 전환(Settings → General → Danger Zone → Change visibility → Public).
- 전환 전 점검: 추적되는 시크릿·원본 데이터 없음 확인(`data/*.csv`는 gitignore, `marts.duckdb`는 집계 수치뿐). 포트폴리오라 공개가 애초 의도.

## 2. 공개·소스 설정 순서를 어기면 배포 실패

### 증상

머지(=main push)는 됐는데 Pages 배포 워크플로의 `actions/deploy-pages` 단계가 실패하거나, URL이 404.

### 원인

- `deploy-pages`는 **Pages가 "GitHub Actions" 소스로 활성화돼 있어야** 동작한다.
- repo 공개 → Pages Source 지정 → 머지 순서를 지키지 않으면, 워크플로가 먼저 돌며 배포 대상이 없다.

### 해결

- 순서 고정: ① repo 공개 → ② Settings → Pages → Source = **GitHub Actions** → ③ main에 `dashboard/**` 머지.
- 순서를 어겼다면 ②를 켠 뒤 Actions 탭 → 해당 워크플로 → **Re-run jobs**(또는 `workflow_dispatch` 수동 실행).

## 3. marts.duckdb가 재-export마다 "변경됨"으로 잡힘

### 증상

데이터 변화가 없는데 `export_duckdb.py`를 다시 돌리면 `git status`에 `dashboard/marts.duckdb`가 `M`(modified)으로 뜬다.

### 원인

- DuckDB 파일 포맷은 **바이트 단위로 결정적이지 않다**(내부 페이지 배치·메타데이터가 매 생성마다 달라짐). 데이터(테이블·행)는 동일해도 파일 바이트가 바뀐다.

### 해결

- **데이터가 실제로 바뀐 경우에만** `marts.duckdb`를 커밋한다(검증 목적 재-export 산물은 `git checkout dashboard/marts.duckdb`로 되돌림).
- 무결성은 바이트가 아니라 내용으로 확인: `SHOW TABLES` 개수 + 핵심 마트 행수(예: `mart_wau` 9행).

## 4. Chart.js 4.x 혼합(line+bar) 차트가 렌더 안 됨

### 증상

CVR 차트(라인 CVR + 막대 WoW)만 그려지지 않는다. 다른 단일 타입 차트는 정상.

### 원인

- Chart.js 4.x는 `new Chart(el, {data, options})`에 **top-level `type`이 필수**다. dataset별 `type`만 지정하고 config 최상위 `type`을 빼면 컨트롤러 등록에 실패한다.
- 단위 테스트가 HTML 문자열만 검증(JS 미실행)이라 이 버그를 못 잡았다 — 실브라우저에서만 드러남.

### 해결

- 혼합 차트는 최상위에 fallback `type`을 두고 dataset에서 override: `new Chart(el, {type:'bar', data:{datasets:[{type:'line',...},{type:'bar',...}]}})`.

## 5. <script> JSON 임베드의 `</script>` 취약점

### 증상

(현재 데이터에선 미발생, 잠재) 마트에 문자열 컬럼이 추가되고 값에 `</script>`가 들어가면 임베드 블록이 조기 종료돼 페이지가 깨진다.

### 원인

- `<script id="marts-data" type="application/json">{{ json }}</script>` 블록은 HTML 파서가 `</script>` 시퀀스에서 무조건 종료한다. `json.dumps`는 `</`를 이스케이프하지 않는다.

### 해결

- 임베드 직전 `json.dumps(...).replace("</", "<\\/")` 표준 하드닝. `\/`는 JSON.parse가 `/`로 되돌리므로 데이터는 불변. (현재 마트는 날짜·숫자뿐이라 무해하나 방어층으로 적용.)

## 6. Actions "Node.js 20 actions are deprecated" 경고

### 증상

배포는 성공(초록)인데 annotation에 `Node.js 20 actions are deprecated` 경고 1건.

### 원인

- 사용한 액션(checkout/setup-python/upload-pages-artifact/deploy-pages)의 현행 메이저가 Node 20 런타임에서 돈다. GitHub의 향후 Node 24 전환 사전 공지일 뿐, 실패가 아니다.

### 해결

- **현재 조치 불필요**(배포 정상). GitHub가 Node 24 기반 새 메이저를 내면 그때 액션 버전만 올린다.
