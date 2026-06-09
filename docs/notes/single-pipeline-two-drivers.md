# 단일 파이프라인, 두 실행 방식 — DailySplitter와 Airflow의 관계

> Phase 0(DailySplitter)·Phase 4(Airflow)를 얹으면서 헷갈리기 쉬운 점을 정리한 참고 문서다.
> "기존 vs Airflow 두 개의 파이프라인이 생기는 것 아닌가?", "Airflow가 기존을 대체하나?"에 대한 답.

## 핵심 한 줄

**파이프라인은 하나다.** 같은 변환 코어를 (1) 손으로 `backfill`, (2) Airflow로 일별 자동 — **두 방식(driver)으로 돌리는 것**이며, `DailySplitter`는 (2)에서만 필요한 **입력 랜딩(Bronze) 추가**다. Silver/Gold는 하나의 같은 테이블이라, Airflow는 "결과를 대체"하는 게 아니라 "그 결과를 만드는 실행을 자동화·통합"한다.

## 1. 기존 파이프라인엔 별도 Bronze 단계가 없다 (오해 바로잡기)

기존 파이프라인의 "일별 파티션 분리"는 **Silver를 쓸 때** 일어난다 — `Main`이 월 CSV를 통째로 읽어 변환한 뒤, `PartitionWriter`가 `output/activity/event_date=YYYY-MM-DD/`로 날짜별로 기록한다(`Main.scala`의 write 루프, `PartitionWriter.writePartition`). 즉 **일별 파티셔닝 = Silver 출력 단계**이고, 물리적으로 분리된 Bronze 폴더는 존재하지 않았다.

`DailySplitter`가 새로 만드는 것은 **입력(Bronze) 쪽의 일별 랜딩**(`data/daily/`)이고, 이건 Silver의 일별 파티셔닝을 *대신*하는 게 아니라 그 **앞에 새로 추가**되는 단계다. Silver의 날짜별 기록은 두 경로 모두에서 `Main` 안에서 그대로 일어난다.

## 2. 질문별 답

**Q1. 기존 파이프라인 흐름은?** — "월 CSV → 일별 분리 → Bronze"라는 중간 단계는 **없다**. 정확히는:
`월 CSV(원천=raw) → Main[dedup→KST→세션화 + event_date별 Silver 기록] → Silver → Gold → 대시보드`. 일별 파티셔닝은 Silver 쓰기에서 발생.

**Q2. DailySplitter가 기존의 일별 분리를 대신하나?** — 아니오.
- 기존 일별 파티셔닝 = Silver 출력(`output/activity`), `Main` 안에서. Airflow 경로에서도 **그대로** 일어남.
- DailySplitter 일별 파티셔닝 = 입력 Bronze(`data/daily`), **변환 없음**(dedup·세션화 안 함, 행 그대로).
- 레이어가 다름 → 대체가 아니라 **앞단 추가**.

**Q3. 두 경로 차이가 DailySplitter 유무인가?** — 데이터 레이어상 신규 컴포넌트는 맞지만 그게 전부는 아님. 차이 3가지:
1. Bronze 단계 `DailySplitter` 추가.
2. `Main` 호출 모드 — 기존 `backfill`(월 전체 한 방) ↔ Airflow `incremental`(`--run-date {{ds}}` 일별 + 전날 lookback).
3. 오케스트레이션 래퍼 — Airflow DAG(catchup·retry·MSCK repair·gate·Discord).
- **변환 코드(dedup/KST/세션화, Gold, WAU)는 동일.**

**Q4. Silver/Gold DB가 동일한가?** — 네. 둘 다 같은 `output/activity`(Silver=Hive `activity`)·`output/gold`(Gold)에 씀(DAG의 `--output` 경로 동일). 테이블은 하나뿐.

**Q5. 그러면 Airflow가 기존을 대체하나?** — 층위를 나눠야 정확하다.
- **데이터 산출물**: Silver/Gold가 테이블 하나뿐이라 "두 벌 공존"이 아님. 어느 driver로 돌리든 같은 테이블을 채움.
- **실행 주체**: Airflow가 전 과정(silver→…→build)을 자동으로 돌릴 수 있으니, *실행 driver로서는* 대체 가능 — Airflow를 돌리면 수동 실행을 또 할 필요 없음.
- **하지만 "갈아끼움"은 아님**: Airflow는 새 변환 코드가 아니라 기존과 같은 `spark-submit`/python을 호출. 설계 결정 9대로 코어 제출 정본(Hive `activity` + `sql/wau.sql`)은 그대로, Airflow는 "선택(어필용)" 상위 자동화 레이어.

**Q6. 백필하려면 Silver/Gold를 미리 지워야 하나?** — 아니오(설계상 불필요).
- Silver: `PartitionWriter`가 staging→rename 원자 교체 + 일 단위 멱등 overwrite(설계 결정 6). 같은 날 재실행 시 그 파티션만 덮어씀.
- Gold: 전체 재계산 후 overwrite(멱등).
- 날짜 집합이 동일(Oct+Nov)이라 stale 파티션도 안 생김 → 그냥 다시 돌리면 됨.
- (예외: 런북 §5(b)에서 폴더를 옮겼다 복구하고 재실행하는 건 *장애 재현* 절차이지 일반 백필이 아님.) 멱등 설계 자체가 "수동 삭제 없이 재실행"을 위한 것.

## 3. 도식

### 기존(수동) — Bronze 레이어 없음, 일별 분리는 Silver 쓰기에서

```
data/2019-Oct.csv ┐  (원천 CSV = raw, 물리적 Bronze 없음)
data/2019-Nov.csv ┘
        │   spark-submit Main --mode backfill --input data/*.csv
        ▼
   ┌──────────────────────────────────────────────────┐
   │ Main: dedup → KST → 세션화                          │
   │   └ PartitionWriter: event_date별 Silver 기록  ◀── 일별 분리는 "여기" │
   └──────────────────────────────────────────────────┘
        ▼  output/activity/event_date=YYYY-MM-DD/   ← Silver (Hive activity)
        ▼  GoldMarts → output/gold/                  ← Gold (마트)
        ▼  export_duckdb.py → marts.duckdb → build.py → 정적 대시보드
```

### Airflow(Phase 4) — 앞에 ★Bronze 추가, 나머지는 같은 코드

```
data/2019-Oct.csv ┐
data/2019-Nov.csv ┘
        │   ★ DailySplitter (신규, 변환 없음 = 행 그대로 event_date로만 분리)
        ▼
   data/daily/event_date=YYYY-MM-DD/   ◀── ★ 새로 생기는 Bronze 일별 랜딩
        │   (DAG가 날짜별로) Main --mode incremental --run-date {{ds}}  (+전날 lookback)
        ▼
   ┌──────────────────────────────────────────────────┐
   │ Main: dedup → KST → 세션화   ← 기존과 동일 코드        │
   │   └ PartitionWriter: 그날 event_date Silver 기록  ◀── 일별 분리는 여전히 "여기" │
   └──────────────────────────────────────────────────┘
        ▼  output/activity/...   ← 같은 Silver (같은 Hive activity)
        ▼  MSCK repair → gate(_SUCCESS) → GoldMarts → output/gold (같은 Gold)
        ▼  export → build (같은 대시보드)
   └─ 전체를 Airflow DAG가 catchup·retry·Discord로 자동 오케스트레이션 ─┘
```

## 4. 한눈에 비교

| 항목 | 기존(수동) | Airflow(Phase 4) |
|---|---|---|
| 입력 Bronze(`data/daily`) | ❌ 없음 | ★ DailySplitter로 생성 |
| 변환 코드(dedup/KST/세션화/Gold) | 동일 | 동일(재사용) |
| Silver 일별 파티셔닝 | `Main` 안 `PartitionWriter` | 동일 |
| `Main` 호출 | `backfill`(한 방) | `incremental`(일별 catchup) |
| Silver/Gold 산출물 | `output/activity`·`output/gold` | 같은 경로 |
| 오케스트레이션 | 손으로 명령 실행 | DAG(자동·retry·게이트·알림) |
| 위치 | 코어 제출 정본 | 선택(어필용) 상위 레이어 |

## 결론

"기존 vs Airflow 두 파이프라인"이 아니라, **같은 변환 코어를 (1) 손으로 backfill, (2) Airflow로 일별 자동 — 두 방식으로 돌리는 것**이며, `DailySplitter`는 (2)에서만 필요한 입력 랜딩(Bronze) 추가다. Silver/Gold는 하나의 같은 테이블이라, Airflow는 "결과를 대체"하는 게 아니라 "그 결과를 만드는 실행을 자동화·통합"한다.

---

참고: 코드 위치는 `src/main/scala/com/activitylog/Main.scala`(write 루프)·`PartitionWriter.scala`(staging→rename·`_SUCCESS`)·`DailySplitter.scala`. 라인 번호는 변경될 수 있으니 심볼로 찾을 것. 관련 문서 — 설계 스펙 §12, 런북 `docs/runbook/airflow.md`, README §8.
