-- mart_stickiness: 일별 DAU / 해당 월 MAU. nullif로 0 division 방지.
WITH dau AS (
  SELECT to_date(event_date) AS d,
         date_format(to_date(event_date), 'yyyy-MM') AS month,
         count(DISTINCT user_id) AS dau
  FROM activity GROUP BY 1, 2
),
mau AS (
  SELECT date_format(to_date(event_date), 'yyyy-MM') AS month,
         count(DISTINCT user_id) AS mau
  FROM activity GROUP BY 1
)
SELECT dau.d AS date, dau.dau, mau.mau,
       dau.dau / nullif(mau.mau, 0) AS stickiness
FROM dau JOIN mau USING (month)
ORDER BY dau.d
