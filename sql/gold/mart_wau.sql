-- mart_wau: ISO 주(월요일)별 활성 사용자/세션. user+session을 한 행으로 통합.
-- 주의: 제출용 WAU 정본은 sql/wau.sql(Hive activity). 이 마트는 대시보드 서빙용 추가본.
SELECT date_trunc('week', to_date(event_date)) AS week_start,
       count(DISTINCT user_id)    AS wau_users,
       count(DISTINCT session_id) AS wau_sessions
FROM activity
GROUP BY 1
ORDER BY 1
