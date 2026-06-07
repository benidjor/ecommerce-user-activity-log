-- WAU(주간 활성) 2종: user_id 기준 / session_id 기준
-- 사용: 먼저 sql/create_external_table.sql 로 activity 테이블을 등록한 뒤
--   spark-sql --conf spark.sql.session.timeZone=UTC -f sql/wau.sql
-- 주 경계: ISO week(월요일 시작), KST event_date 기준 (설계 결정 3)

-- 1) user_id 기준 WAU
SELECT date_trunc('week', to_date(event_date)) AS week_start,
       COUNT(DISTINCT user_id) AS wau_users
FROM activity
GROUP BY 1 ORDER BY 1;

-- 2) session_id 기준 WAU
SELECT date_trunc('week', to_date(event_date)) AS week_start,
       COUNT(DISTINCT session_id) AS wau_sessions
FROM activity
GROUP BY 1 ORDER BY 1;
