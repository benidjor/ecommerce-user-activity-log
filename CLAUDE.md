# 프로젝트: 이커머스 사용자 Activity 로그 → Hive External Table + WAU

## 무엇을 만드는가
2019-Oct/Nov 이커머스 이벤트 CSV를 dedup·KST변환·5분갭 세션화하여 KST 일별 파티션
parquet(snappy)로 적재하고, Hive External Table로 노출한 뒤 WAU(user_id/session_id) 2종을
실측한다. (백패커/아이디어스 DE 사전 과제)

## 핵심 문서 (먼저 읽을 것)
- 설계 스펙(결정 로그 9개 + 핵심 로직 명세): `docs/superpowers/specs/2026-06-07-wau-activity-log-design.md`
- 구현 계획(Task 0~14, 완전한 코드/명령/기대결과): `docs/superpowers/plans/2026-06-07-wau-activity-log.md`

## 진행 방식
- 작업 브랜치: `feat/activity-log` (main에 직접 구현 금지). 태스크마다 커밋.
- 구현은 `superpowers:subagent-driven-development`로 계획을 Task 1부터 실행
  (태스크마다 구현 서브에이전트 → spec 리뷰 → code quality 리뷰).
- 각 태스크는 TDD(실패 테스트 먼저). 계획서의 코드/명령을 그대로 사용.
- 진행 상태는 Task 리스트로 추적(Task 0 환경설치는 완료됨).

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

## 불변 규칙
- **WAU 수치는 AI 주장 금지** — 반드시 실제 Spark 실행 결과로만 보고(verification-before-completion). 사용 쿼리 동봉.
- **데이터 커밋 금지**: `data/*.csv`는 `.gitignore` 적용됨.
- 인터뷰에서 라인 단위 설명이 가능해야 하므로 **과한 추상화 금지**(최소·또렷한 구조).
- 항상 한국어 존댓말.
