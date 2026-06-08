-- mart_revenue: 주별 결제금액(구매 이벤트의 price 합) + AOV(=결제금액/구매건수).
-- order_id 없어 구매 이벤트를 주문 proxy로 사용(한계는 대시보드에 명시).
SELECT date_trunc('week', to_date(event_date)) AS week_start,
       sum(price)                       AS revenue,
       count(*)                         AS purchases,
       sum(price) / nullif(count(*), 0) AS aov
FROM activity
WHERE event_type = 'purchase'
GROUP BY 1
ORDER BY 1
