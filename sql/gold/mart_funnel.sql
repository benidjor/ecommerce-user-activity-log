-- mart_funnel: 주별 단계(view→cart→purchase) distinct user + 단계 전환율.
-- nullif로 상위 단계 0일 때 division by zero 방지(전환율 null).
SELECT date_trunc('week', to_date(event_date)) AS week_start,
       count(DISTINCT CASE WHEN event_type = 'view'     THEN user_id END) AS users_view,
       count(DISTINCT CASE WHEN event_type = 'cart'     THEN user_id END) AS users_cart,
       count(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS users_purchase,
       count(DISTINCT CASE WHEN event_type = 'cart'     THEN user_id END)
         / nullif(count(DISTINCT CASE WHEN event_type = 'view' THEN user_id END), 0) AS view_to_cart,
       count(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END)
         / nullif(count(DISTINCT CASE WHEN event_type = 'cart' THEN user_id END), 0) AS cart_to_purchase
FROM activity
GROUP BY 1
ORDER BY 1
