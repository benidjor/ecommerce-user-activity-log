<!--
  컨벤션 SoT: docs/conventions/2026-06-07-commit-pr-convention.md
  - 각 헤더 아래는 소제목 성격의 한 문장(도입) → 이후 불릿으로 세부를 명사형으로.
  - 헤더 번호는 점 구분(점 개수 = 깊이 − 1). 두 단어 결합은 `&`로 통일.
  - 코드 블록 안 `#` 주석은 한국어. PR에 Claude 서명 금지.
  - Squash merge → 이 본문이 곧 main 단일 커밋 본문.
-->

## 1. 배경 & 목적

### 1.1. 배경

<!-- 어떤 단계(plan의 Day / Task)인지 + 직전 단계의 부족함 -->

### 1.2. 기대 효과

<!-- 머지 후 변화 + 다음 Day의 전제 + SLO / 비용 / 운영 영향 -->

## 2. 의사결정 & Trade-off

<!--
  의사결정 / 대안 검토 / 채택 사유 / 원안 대비 달라진 점 표 / review Important fix.
  diff가 보여주는 자명한 narrative는 제외.
  큰 트러블슈팅은 docs/portfolio/troubleshooting/ 로 archive 후 link + 한 줄 요약.
-->

## 3. 변경 사항

<!-- 파일 단위 narrative. 무엇이 바뀌었는지 불릿. -->

## 4. 검증

<!-- 명령어 + 결과 발췌. 코드 블록 `#` 주석은 한국어. row count / SLO / 샘플 등 실제 증거. -->

```bash
# 예: sbt test
```

## 5. 장애 시나리오 & 롤백 전략

<!--
  머지 후 잘못되면 어떤 형태로 잘못될 수 있는가 + 롤백 방법.
  데이터 손실 / 멱등성 / schema / SLO / 비용 / 보안 / 계층 의존 중 해당하는 것만.
  없으면 아래 한 줄.
-->

잠재 위험 없음. `git revert`로 롤백 가능.

## 6. 체크리스트

- [ ] atomicity (1 PR = 1 논리 단위)
- [ ] secrets 미포함
- [ ] tests + lint 통과
- [ ] SoT 일관 (docs ↔ 코드)
- [ ] commit 컨벤션 준수
- [ ] PR 크기 가이드 (≤200 LOC 이상적)

## 7. 레퍼런스

<!-- plan / spec / 이전 PR / runbook / 메모리 link -->
