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
- **진행 상태(2026-06-09)**: Phase 1(Gold 마트)·Phase 2(Mart Export+정적 대시보드) 완료·배포 — 라이브 <https://benidjor.github.io/ecommerce-user-activity-log/>. 지표 정의·데이터 경계는 `docs/runbook/dashboard.md` §6. **Phase 3(Streamlit)은 보류**(세 목적—데이터 유동성·실무 어필·장애/복구 가시화—은 Airflow UI+Discord가 충족, 정적과 같은 스냅샷 읽어 중복 → 선택적 폴리시로 남김; 스펙 §12.0). **Phase 0(DailySplitter, PR #32)·Phase 4(Airflow DAG+Discord, PR #33) 완료·머지**(2026-06-09). 핵심 결정: BashOperator+spark-submit, `airflow standalone`(SQLite+SequentialExecutor), 장애 2모드 시연(자동 retry 복구·검증 게이트 실패 후 멱등 복구), @daily catchup(2019-10-01~12-01 KST, 데모 10-01~10-07). 라이브 실구동 시연(7일 catchup·장애 2모드)·캡처 README §8, Discord 임베드 카드. 구동 절차 `docs/runbook/airflow.md`, 로컬 구동 함정(macOS setproctitle fork·standalone PATH·Discord 403·@daily backfill·clear≠재실행) `docs/troubleshooting/airflow-local-macos.md`, 개념(단일 파이프라인·두 driver) `docs/notes/single-pipeline-two-drivers.md`. WAU 재실행으로 §3 수치 재확인(backfill≡incremental 동치 실측). 코어 원과제(요구 1~8 + AI 가이드) 전부 충족.
- **요구사항 가드레일(불변)**: "Hive External Table" 요구 = **Silver `activity`**. DuckDB는 Gold 서빙 사본(Hive 대체 아님). 제출용 WAU는 Hive `activity`(`sql/wau.sql`) 정본. **Spark Application은 전부 Scala**(파이프라인·DailySplitter·GoldMarts); 대시보드(정적/Streamlit)·Airflow·DuckDB export는 Spark 외라 **Python 허용**(README에 경계 명시).
- **서빙**: 정적 HTML(GitHub Pages) + Streamlit Cloud, 둘 다 repo 커밋된 DuckDB 파일(패턴3) 읽기. Metabase/Postgres는 무료·항상ON 불충족이라 범위 밖.
- **Gold 모델**: 경량 스타(`dim_date`+집계 fact) + count-distinct 비가산 지표 마트. **원자 fact는 Silver(약 9천만 행), Gold는 집계(수백 행)**. SQL은 `sql/gold/*.sql`(SoT) + 얇은 `GoldMarts` 러너(WAU의 .scala/.sql 이중화 반복 안 함).

## AI 도구 조합
- 사용·미사용 도구와 선택 근거(방법론·설계·구현·검증)는 `docs/ai-tooling.md` 참고. 최소 구성 = superpowers + karpathy-guidelines.

## 구현 4원칙 (karpathy-guidelines — 모든 코드 태스크에 적용)
- **Think Before Coding · Simplicity First · Surgical Changes · Goal-Driven Execution**. 상세 지침은 `andrej-karpathy-skills:karpathy-guidelines` 스킬을 호출해 따른다.

## 불변 규칙
- **WAU 수치는 AI 주장 금지** — 반드시 실제 Spark 실행 결과로만 보고(verification-before-completion). 사용 쿼리 동봉.
- **데이터 커밋 금지**: `data/*.csv`는 `.gitignore` 적용됨.
- 인터뷰에서 라인 단위 설명이 가능해야 하므로 **과한 추상화 금지**(최소·또렷한 구조).
- 항상 한국어 존댓말.
