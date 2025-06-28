-- 1. Tổng số hành vi theo loại
/*
SELECT  event_type,
		COUNT(*) as total_event
FROM fact_events
GROUP BY event_type;
*/

-- 2. Top 5 sản phẩm được xem nhiều nhất
/*
SELECT  p.product_id,
		p.brand,
		COUNT(*) as view
FROM dim_products p
INNER JOIN fact_events f
ON p.product_id = f.event_product_id
WHERE f.event_type = 'view'
GROUP BY p.product_id,p.brand
ORDER BY view DESC
LIMIT 5;
*/

-- 3. Người dùng có lần purchase nhiều nhất
/*
with user_event_purchase AS (
	SELECT 	event_user_id,
			COUNT(*) AS purchase_count
	FROM fact_events
	WHERE event_type = 'purchase'
	GROUP BY event_user_id
)
SELECT 	*,
		RANK() OVER (ORDER BY purchase_count) as user_rank
FROM user_event_purchase
LIMIT 10;
*/

-- 4. Thời gian trung bình hành động của mỗi user
WITH ranked_events AS (
	SELECT  event_user_id,
			event_time,
			LAG(event_time) OVER (PARTITION BY event_user_id ORDER BY event_time) AS prev_time
	FROM fact_events
)
SELECT  event_user_id,
		AVG(EXTRACT(EPOCH FROM (event_time - prev_time))) AS avg_time_between_events_seconds
FROM ranked_events
WHERE prev_time IS NOT NULL
GROUP BY event_user_id
ORDER BY avg_time_between_events_seconds;



