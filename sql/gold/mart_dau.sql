-- mart_dau: 일별 활성 사용자/세션
SELECT event_date,
       count(DISTINCT user_id)    AS dau_users,
       count(DISTINCT session_id) AS dau_sessions
FROM activity
GROUP BY event_date
ORDER BY event_date
