-- dim_date: activity의 event_date 최소~최대 범위로 날짜 차원 생성(빈 날 없이 연속)
-- dow: Spark dayofweek = 1(일)~7(토). is_weekend = 일/토.
-- 용도: Phase 2 대시보드의 날짜축(빈 날 채움·요일/주/월 라벨)용 standalone 차원.
--   현재 마트들과 직접 조인하지 않으며, 서빙 단계(DuckDB/대시보드)에서 사용한다.
WITH bounds AS (
  SELECT min(to_date(event_date)) AS d0, max(to_date(event_date)) AS d1 FROM activity
),
days AS (
  SELECT explode(sequence(d0, d1, interval 1 day)) AS d FROM bounds
)
SELECT
  date_format(d, 'yyyy-MM-dd')                              AS date_key,
  d                                                          AS date,
  date_trunc('week', d)                                      AS iso_week,
  date_format(d, 'yyyy-MM')                                  AS month,
  dayofweek(d)                                               AS dow,
  CASE WHEN dayofweek(d) IN (1, 7) THEN true ELSE false END  AS is_weekend
FROM days
ORDER BY d
