# AI 도구 조합 (사용·미사용·근거)

이 프로젝트에서 사용한 AI 도구와 활용 범위·선택 근거. README "사용 도구" 절의 SoT.
(근거: `wau-ai-tooling-strategy.html` §04, line 689 "최소 구성=superpowers+karpathy")

- **방법론(필수)**: superpowers(`brainstorming`·`test-driven-development`·`verification-before-completion`) + karpathy-guidelines(Think·Simplicity·Surgical·Goal).
- **설계/계획**: `superpowers:writing-plans` 로 스펙+계획서 로컬 산출(ultraplan/Ouroboros·Seed의 대체 — 결과물 동일).
- **구현**: `superpowers:subagent-driven-development`(태스크별 리뷰 게이트). workflow codegen보다 설명 가능성 우선이라 이를 선택.
- **검증**: 기본 `verification-before-completion`(WAU 실측). **Task 12에서 선택적으로 `workflow` verify**(두 WAU 적대적 교차검증, opt-in="ultracode"/"워크플로우로 WAU 교차검증"). 사용 시 README "사용 도구"에 workflow 추가.
- **의도적 미사용**: ultraplan(웹 Claude Code+GitHub 필요, 가치 중복), Ouroboros(외부·무거움) → README엔 "검토 후 로컬·과제 규모상 제외"로 기술해 성숙도 어필.
