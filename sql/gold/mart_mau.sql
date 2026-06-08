-- mart_mau: KST 월별 활성 사용자(데이터 2개월 → 점 2~3개; 한계는 대시보드에 명시)
SELECT date_format(to_date(event_date), 'yyyy-MM') AS month,
       count(DISTINCT user_id) AS mau_users
FROM activity
GROUP BY 1
ORDER BY 1
