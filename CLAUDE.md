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

## 불변 규칙
- **WAU 수치는 AI 주장 금지** — 반드시 실제 Spark 실행 결과로만 보고(verification-before-completion). 사용 쿼리 동봉.
- **데이터 커밋 금지**: `data/*.csv`는 `.gitignore` 적용됨.
- 세션 ID는 원본 `user_session`을 쓰지 않고 **직접 생성**(5분 갭, 결정적 id). 원본은 검증용 참고 컬럼으로만 보존.
- 인터뷰에서 라인 단위 설명이 가능해야 하므로 **과한 추상화 금지**(최소·또렷한 구조).
- 항상 한국어 존댓말.
