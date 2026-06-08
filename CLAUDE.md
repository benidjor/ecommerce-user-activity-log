# 프로젝트: 이커머스 사용자 Activity 로그 → Hive External Table + WAU

## 무엇을 만드는가
2019-Oct/Nov 이커머스 이벤트 CSV를 dedup·KST변환·5분갭 세션화하여 KST 일별 파티션
parquet(snappy)로 적재하고, Hive External Table로 노출한 뒤 WAU(user_id/session_id) 2종을
실측한다. (백패커/아이디어스 DE 사전 과제)

## 핵심 문서 (먼저 읽을 것)
- 설계 스펙(결정 로그 9개 + 핵심 로직 명세): `docs/superpowers/specs/2026-06-07-wau-activity-log-design.md`
- 구현 계획(Task 0~14, 완전한 코드/명령/기대결과): `docs/superpowers/plans/2026-06-07-wau-activity-log.md`
- (확장) 메달리온 BI 대시보드 설계 스펙: `docs/superpowers/specs/2026-06-08-medallion-bi-dashboard-design.md`
- (확장) Gold 마트 Phase 1 계획: `docs/superpowers/plans/2026-06-08-gold-marts-phase1.md`

## 진행 방식
- main에 직접 구현 금지, 태스크마다 커밋.
- **PR 스택 금지**: 각 Task/Phase를 **main에서 분기 → PR(base=main) → squash 머지 → 다음은 갱신된 main에서 분기**. 직전 브랜치 base 금지(스택 머지 사고 이력).
- 구현은 `superpowers:subagent-driven-development`로 계획을 Task 1부터 실행
  (태스크마다 구현 서브에이전트 → spec 리뷰 → code quality 리뷰).
- 각 태스크는 TDD(실패 테스트 먼저). 계획서의 코드/명령을 그대로 사용.
- 진행 상태는 Task 리스트로 추적(Task 0 환경설치는 완료됨).
- 실행은 **순차**. 병렬 worktree(Task 2~7만 독립)도 가능하나 태스크가 작아(5~15분) 머지 오버헤드가 이득을 상회 → **Simplicity First로 순차 채택**. (Task 8~14는 의존성상 본래 순차)

## 환경 (확인됨 2026-06-07)
- Spark 4.1.2 / Scala 2.13.17 / sbt(brew) 설치 완료.
- `sbt test`는 JDK 17, `spark-submit`은 brew JDK 21 — 둘 다 Spark 4 지원, JDK add-opens 필요(build.sbt/extraJavaOptions에 반영).
- 실행 모델: 개발은 샘플 우선(Day1 end-to-end), 전체(Oct+Nov) backfill은 최종 1회만.

## 핵심 설계 결정 (한 줄 요약 — 전체 근거는 설계 스펙 §7 결정 로그)
1. **session_id**: 원본 `user_session` 무시, 직접 생성. 결정적 `id = user_id + "_" + unix(세션시작시각)`. 원본은 검증용 컬럼으로 보존.
2. **자정/파티션 경계 = 방식 A**: 파티션은 **이벤트의 `event_date`(KST)** 기준(세션 단위 아님). 세션화는 backward-only(직전 이벤트만). backfill=전역 세션화 1회, 증분=전날(D-1) lookback. 결정적 id라 두 모드 결과 동일.
3. **WAU 주 경계**: ISO week(월요일 시작), KST. `week_start = date_trunc('week', to_date(event_date))`.
4. **타임존**: 원본 `event_time`(UTC) 보존 + 적재 시 KST **1회** 변환 → `event_time_kst`, `event_date` 파생. (세션 정렬은 UTC 기준)
5. **dedup**: 자연키 `(user_id,event_time,event_type,product_id)` 1건 유지, **세션화 전** 수행.
6. **장애복구**: staging+rename 원자 교체 + 검증게이트(최소 단언) + 파티션별 `_SUCCESS` + 멱등 overwrite(단위=하루). retry/알람은 Airflow 콜백+Discord 웹훅. **checkpoint 미사용**(스트림 전용). 커스텀 재실행 버튼 대신 **Airflow 네이티브 Retry**.
7. **테이블**: 고전적 Hive External Table + Spark **임베디드 Derby metastore**(별도 서비스 X). Iceberg/Delta는 README "프로덕션 확장"으로만.
8. **언어**: Scala + sbt + 로컬 Spark(local 모드). spark-submit로 thin jar 실행.
9. **오케스트레이션**: Airflow는 **선택(어필용)** — 본체 완성·검증 후 BashOperator+spark-submit, `catchup=True`가 backfill 겸함.

## 확장: 메달리온 BI 대시보드 (코어 완료 후 진행)
원과제 코어(WAU+Hive External Table, Task 1~12) 완료 후, Gold 마트·Airflow 일별 오케스트레이션·BI 대시보드·Discord 알람을 얹는다(스펙 2026-06-08, Phase 0~4).
- **진행 상태(2026-06-09)**: Phase 1(Gold 마트)·Phase 2(Mart Export+정적 대시보드) 완료·배포 — 라이브 <https://benidjor.github.io/ecommerce-user-activity-log/>. 지표 정의·데이터 경계는 `docs/runbook/dashboard.md` §6. 다음 = Phase 3(Streamlit) → Phase 4(Airflow+Discord). Phase 0(DailySplitter)은 Phase 4 전제라 미진행.
- **요구사항 가드레일(불변)**: "Hive External Table" 요구 = **Silver `activity`**. DuckDB는 Gold 서빙 사본(Hive 대체 아님). 제출용 WAU는 Hive `activity`(`sql/wau.sql`) 정본. **Spark Application은 전부 Scala**(파이프라인·DailySplitter·GoldMarts); 대시보드(정적/Streamlit)·Airflow·DuckDB export는 Spark 외라 **Python 허용**(README에 경계 명시).
- **서빙**: 정적 HTML(GitHub Pages) + Streamlit Cloud, 둘 다 repo 커밋된 DuckDB 파일(패턴3) 읽기. Metabase/Postgres는 무료·항상ON 불충족이라 범위 밖.
- **Gold 모델**: 경량 스타(`dim_date`+집계 fact) + count-distinct 비가산 지표 마트. **원자 fact는 Silver(약 9천만 행), Gold는 집계(수백 행)**. SQL은 `sql/gold/*.sql`(SoT) + 얇은 `GoldMarts` 러너(WAU의 .scala/.sql 이중화 반복 안 함).

## AI 도구 조합 (근거: `wau-ai-tooling-strategy.html` §04, line 689 "최소 구성=superpowers+karpathy")
- **방법론(필수)**: superpowers(`brainstorming`·`test-driven-development`·`verification-before-completion`) + karpathy-guidelines(Think·Simplicity·Surgical·Goal).
- **설계/계획**: `superpowers:writing-plans` 로 스펙+계획서 로컬 산출(ultraplan/Ouroboros·Seed의 대체 — 결과물 동일).
- **구현**: `superpowers:subagent-driven-development`(태스크별 리뷰 게이트). workflow codegen보다 설명 가능성 우선이라 이를 선택.
- **검증**: 기본 `verification-before-completion`(WAU 실측). **Task 12에서 선택적으로 `workflow` verify**(두 WAU 적대적 교차검증, opt-in="ultracode"/"워크플로우로 WAU 교차검증"). 사용 시 README "사용 도구"에 workflow 추가.
- **의도적 미사용**: ultraplan(웹 Claude Code+GitHub 필요, 가치 중복), Ouroboros(외부·무거움) → README엔 "검토 후 로컬·과제 규모상 제외"로 기술해 성숙도 어필.

## 구현 4원칙 (karpathy-guidelines — 모든 코드 태스크에 적용)
1. **Think Before Coding**: 가정은 명시하고 불확실하면 질문. 해석이 여럿이면 **조용히 고르지 말고** 제시. 더 단순한 길이 있으면 말할 것. (도구·설계 선택도 임의로 바꾸지 말고 알릴 것)
2. **Simplicity First**: 요청을 푸는 최소 코드만. 투기적 추상화·미요청 설정·불가능 시나리오용 예외처리 금지. "시니어가 과하다 할까?" → 그러면 단순화.
3. **Surgical Changes**: 건드릴 것만 수정. 인접 코드·주석·포맷 임의 개선/리팩터 금지. 기존 스타일 준수. 내 변경이 만든 orphan(미사용 import 등)만 정리. 모든 변경 라인은 요청에 직접 연결돼야.
4. **Goal-Driven Execution**: 성공기준을 먼저 정의하고 검증될 때까지 루프. "검증"=테스트 통과/실제 실행 결과. (세션화=경계 케이스 TDD, WAU=실제 Spark 실행)

## 불변 규칙
- **WAU 수치는 AI 주장 금지** — 반드시 실제 Spark 실행 결과로만 보고(verification-before-completion). 사용 쿼리 동봉.
- **데이터 커밋 금지**: `data/*.csv`는 `.gitignore` 적용됨.
- 인터뷰에서 라인 단위 설명이 가능해야 하므로 **과한 추상화 금지**(최소·또렷한 구조).
- 항상 한국어 존댓말.
