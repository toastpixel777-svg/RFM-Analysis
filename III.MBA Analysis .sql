-- 5. MBA ANALYSIS-- 
-- 5.1. Loss leader analysis 
WITH weekly_product_price AS (
  SELECT 
    product_id,
    DATE_TRUNC(DATE(event_time), WEEK(MONDAY)) AS week_start_date,
    AVG(price) AS weekly_avg_price,
    AVG(AVG(price)) OVER(PARTITION BY product_id) AS total_avg_price
  FROM `ecommerce-486217.raw_data.rfm_segment`
  WHERE event_type = 'purchase'
  GROUP BY 1, 2
),
loss_leader_candidates AS (
  -- loss leader is defined as products that have discount rates of 30% and above
  SELECT 
    product_id,
    week_start_date,
    weekly_avg_price,
    total_avg_price,
    SAFE_DIVIDE(total_avg_price - weekly_avg_price, total_avg_price) AS discount_depth
  FROM weekly_product_price
  WHERE total_avg_price > 0 
    AND SAFE_DIVIDE(total_avg_price - weekly_avg_price, total_avg_price) >= 0.3
),

-- 5.2. Cross-selling & Cherry-picker analysis
session_impact AS (
  SELECT 
    user_session,
    DATE_TRUNC(DATE(MIN(event_time)), WEEK(MONDAY)) AS week_start_date,
    COUNT(DISTINCT product_id) AS items_in_basket, 
    SUM(price) AS session_revenue, 
    ARRAY_AGG(DISTINCT product_id) AS product_list
  FROM `ecommerce-486217.raw_data.rfm_segment`
  WHERE event_type = 'purchase'
  GROUP BY 1
),

-- 5.3. Sessions where users purchased 'loss-leader'products
session_summary AS (
  SELECT 
    si.user_session,
    si.week_start_date,
    si.items_in_basket,
    si.session_revenue,
    r.user_segment,
   
    EXISTS(
      SELECT 1 
      FROM UNNEST(si.product_list) AS pid
      JOIN loss_leader_candidates ll ON ll.product_id = pid AND ll.week_start_date = si.week_start_date
    ) AS has_loss_leader
  FROM session_impact si
  JOIN (SELECT DISTINCT user_session, user_segment FROM `ecommerce-486217.raw_data.rfm_segment`) r 
    ON si.user_session = r.user_session
)

-- 5.4. Weekly Performance 
SELECT 
  FORMAT_DATE('%Y-%m', week_start_date) || '-W' || 
  CAST(FLOOR((EXTRACT(DAY FROM week_start_date) - 1) / 7) + 1 AS STRING) AS week_display,
  week_start_date,
  
  -- Basket Size Lift
  ROUND(AVG(CASE WHEN has_loss_leader = TRUE THEN items_in_basket END), 2) AS ll_session_items,
  ROUND(AVG(CASE WHEN has_loss_leader = FALSE THEN items_in_basket END), 2) AS normal_session_items,
  
  -- Revenue Lift 
  ROUND(AVG(CASE WHEN has_loss_leader = TRUE THEN session_revenue END), 2) AS ll_session_revenue,
  
  -- Cherry Picker
  ROUND(SAFE_DIVIDE(COUNTIF(has_loss_leader = TRUE AND items_in_basket = 1), COUNTIF(has_loss_leader = TRUE)) * 100, 2) AS cherry_pick_rate,
  
  -- Differences between segments 
  ROUND(SAFE_DIVIDE(COUNTIF(has_loss_leader = TRUE AND user_segment = 'Gold'), COUNTIF(has_loss_leader = TRUE)) * 100, 2) AS ll_gold_pct,
  ROUND(SAFE_DIVIDE(COUNTIF(has_loss_leader = TRUE AND user_segment = 'Silver'), COUNTIF(has_loss_leader = TRUE)) * 100, 2) AS ll_silver_pct,
  ROUND(SAFE_DIVIDE(COUNTIF(has_loss_leader = TRUE AND user_segment = 'Bronze'), COUNTIF(has_loss_leader = TRUE)) * 100, 2) AS ll_bronze_pct
FROM session_summary
GROUP BY 1, 2
ORDER BY 2;



--2. 