-- mart_cvr: 주별 CVR = 구매자수 / 방문자수. WoW = 직전 주 대비 CVR 증감(lag).
-- 주의: lag는 위치 기반(직전 '행')이라 데이터에 빈 주가 있으면 캘린더상 직전 주와 다를 수 있다.
--   본 데이터(Oct/Nov 9주 연속)는 빈 주가 없어 행=주가 성립하므로 직전 주 비교가 정확하다.
WITH wk AS (
  SELECT date_trunc('week', to_date(event_date)) AS week_start,
         count(DISTINCT CASE WHEN event_type = 'view'     THEN user_id END) AS visitors,
         count(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS purchasers
  FROM activity GROUP BY 1
)
SELECT week_start, visitors, purchasers,
       purchasers / nullif(visitors, 0) AS cvr,
       (purchasers / nullif(visitors, 0))
         - lag(purchasers / nullif(visitors, 0)) OVER (ORDER BY week_start) AS cvr_wow_delta
FROM wk
ORDER BY week_start
