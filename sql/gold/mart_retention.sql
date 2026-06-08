-- mart_retention: 주차 코호트 리텐션.
--   코호트 = 데이터 내 사용자의 '첫 활동 주'. week_offset = 활동 주 - 코호트 주(주 단위).
--   retention_rate = (코호트 중 offset주에 활동한 distinct user) / 코호트 크기.
-- 한계: 데이터 2개월(9주)이라 작은 삼각형. "신규"는 Oct 이전 이력 없어 '데이터 내 첫 활동'으로 정의.
WITH user_weeks AS (
  SELECT DISTINCT user_id, date_trunc('week', to_date(event_date)) AS wk FROM activity
),
cohort AS (
  SELECT user_id, min(wk) AS cohort_week FROM user_weeks GROUP BY user_id
),
cohort_size AS (
  SELECT cohort_week, count(*) AS cohort_users FROM cohort GROUP BY cohort_week
)
-- week_offset: 활동 주 - 코호트 주를 7로 나눈 주 단위 오프셋(정수).
--   Spark의 '/'는 실수 나눗셈(DOUBLE 반환)이라 CAST(... AS INT)로 정수화한다
--   (주 경계가 7일 간격이라 항상 7의 배수 → 정수 변환에 손실 없음).
--   SELECT와 GROUP BY가 동일 표현식이어야 그룹핑이 맞으므로 두 곳 모두 CAST.
SELECT c.cohort_week,
       CAST(datediff(uw.wk, c.cohort_week) / 7 AS INT)      AS week_offset,
       count(DISTINCT uw.user_id)                           AS active_users,
       cs.cohort_users,
       -- retention_rate: 비율(0~1). Spark에선 정수/정수가 이미 DOUBLE이라 캐스트 없이도
       --   동작하지만, (1) 결과가 실수 비율임을 명시하고 (2) DECIMAL을 만드는 정수*1.0/정수
       --   패턴을 피하려는 의도로 분자를 DOUBLE로 명시 캐스팅한다.
       CAST(count(DISTINCT uw.user_id) AS DOUBLE) / cs.cohort_users AS retention_rate
FROM user_weeks uw
JOIN cohort      c  USING (user_id)
JOIN cohort_size cs USING (cohort_week)
GROUP BY c.cohort_week, CAST(datediff(uw.wk, c.cohort_week) / 7 AS INT), cs.cohort_users
ORDER BY c.cohort_week, week_offset
