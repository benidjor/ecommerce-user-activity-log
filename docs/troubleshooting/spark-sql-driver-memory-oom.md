# 트러블슈팅: spark-sql 고카디널리티 COUNT(DISTINCT) OOM

전체 (Oct+Nov, 약 9천만 행) 데이터에서 WAU를 조회할 때, `spark-sql` CLI 기본 드라이버 메모리로는 `COUNT(DISTINCT session_id)`가 OutOfMemoryError로 실패함.
샘플 (20만 행)에서는 재현되지 않아 전체 backfill (Task 12)에서 처음 부딪힌 문제.

## 증상

`sql/wau.sql`을 메모리 옵션 없이 실행하면 **첫 쿼리 (`COUNT(DISTINCT user_id)`)는 9주치 결과를 출력**하고, **두 번째 쿼리 (`COUNT(DISTINCT session_id)`)에서 실패함.**

```bash
spark-sql --conf spark.sql.session.timeZone=UTC -f sql/wau.sql
# ... user WAU 9행 정상 출력 후 ...
# ERROR: java.lang.OutOfMemoryError: Java heap space
#   ... Job aborted due to stage failure ...
# 종료코드 52
```

## 원인

- `spark-sql` CLI는 `--driver-memory`를 주지 않으면 **드라이버 힙이 기본값 (~1g 미만)**으로 시작됨.
  local 모드에서는 드라이버 JVM이 곧 실행자라, 이 힙이 집계 작업 전체의 메모리 한도.
- `session_id`는 결정적 생성 id (`user_id + "_" + unix(세션시작)`, 설계 결정 1)라 **카디널리티가 `user_id`보다 훨씬 높음** (주당 사용자 ~80만~150만 vs 세션 ~150만~470만)
  `COUNT(DISTINCT)`는 distinct 집합을 메모리에 들고 집계하므로, 카디널리티가 높을수록 힙을 많이 씀.
- 그래서 user WAU는 통과하고 session WAU에서만 실패함.
  샘플 (20만 행)은 distinct 규모가 작아 기본 힙으로도 충분.

## 해결

적재 (backfill)와 동일하게 **`--driver-memory 8g`를 `spark-sql`에도 부여** (셔플 파티션도 맞춤)
재실행하면 두 쿼리 모두 정상 종료.

```bash
spark-sql \
  --driver-memory 8g \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.driver.extraJavaOptions="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
  -f sql/wau.sql
# 종료코드 0, user 9행 + session 9행 = 18행 정상
```

## 재발 방지

- 전체 데이터 WAU 조회는 런북 [full-backfill.md](../runbook/full-backfill.md)의 `--driver-memory 8g` 명령을 그대로 사용.
  메모리가 더 부족하면 8g → 12g로 올림 (머신 RAM 한도 내에서)
- **프로덕션 확장**: 정확한 distinct가 필수가 아니면 `approx_count_distinct` (HyperLogLog)로 메모리를 크게 줄일 수 있음.
  클러스터 환경에서는 executor 메모리·파티션 수로 분산해 단일 드라이버 힙 의존을 없앰.
