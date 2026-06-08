-- fact_daily_activity: 집계 fact. 그레인 = event_date × event_type.
-- 원자 fact는 Silver activity(약 9천만 행). 여기선 가산 측정 + 그 단위 distinct.
-- sum_price: 해당 타입의 price 합(매출 해석은 mart_revenue에서 purchase로 한정).
SELECT
  event_date,
  event_type,
  count(*)                   AS event_count,
  count(DISTINCT user_id)    AS distinct_users,
  count(DISTINCT session_id) AS distinct_sessions,
  sum(price)                 AS sum_price
FROM activity
GROUP BY event_date, event_type
ORDER BY event_date, event_type
