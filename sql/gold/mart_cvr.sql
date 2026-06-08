-- mart_cvr: 주별 CVR = 구매자수 / 방문자수. WoW = 직전 주 대비 CVR 증감(lag).
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
