--6. USER ANALYSIS FOR THE SILVER SEGMENT
--6.1. User Session Analysis 
SELECT 
  user_id,
  user_session,
  user_segment,
  MIN(event_time) AS session_start,
  MAX(event_time) AS session_end,
  ROUND(TIMESTAMP_DIFF(MAX(event_time), MIN(event_time), SECOND) / 60.0, 2) AS session_duration_minutes,
  COUNT(*) AS event_count
FROM `ecommerce-486217.raw_data.rfm_segment`
WHERE 
  user_segment = 'Silver' 
GROUP BY 1, 2, 3
ORDER BY session_duration_minutes DESC;


--6.2. Weekly purchase analysis (Day of Week, Time, e.g.)
SELECT 
  EXTRACT(DAYOFWEEK FROM event_time) AS day_of_week,
  CASE EXTRACT(DAYOFWEEK FROM event_time)
    WHEN 1 THEN 'Sun' WHEN 2 THEN 'Mon' WHEN 3 THEN 'Tue' 
    WHEN 4 THEN 'Wed' WHEN 5 THEN 'Thu' WHEN 6 THEN 'Fri' WHEN 7 THEN 'Sat' 
  END AS day_name,
  
  COUNT(*) AS purchase_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS purchase_pct
FROM `ecommerce-486217.raw_data.rfm_segment`
WHERE 
  user_segment = 'Silver'               
  AND event_type = 'purchase'           
GROUP BY 1, 2
ORDER BY purchase_count DESC;           

SELECT 
  EXTRACT(HOUR FROM event_time) AS hour_of_day,
  
  COUNT(*) AS purchase_count,          
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS purchase_pct, 
  COUNT(DISTINCT user_id) AS unique_buyers,
  ROUND(SUM(price), 2) AS total_revenue
FROM `ecommerce-486217.raw_data.rfm_segment`
WHERE 
  user_segment = 'Silver'             
  AND event_type = 'purchase'           
  --Excluding weekends in the analysis 
  AND EXTRACT(DAYOFWEEK FROM event_time) BETWEEN 2 AND 6 
GROUP BY 1
ORDER BY purchase_count DESC;        


--6.3. View sessions before purchase
WITH product_journey AS (
  SELECT 
    user_id,
    product_id,
    COUNT(DISTINCT CASE WHEN event_type = 'view' THEN user_session END) AS view_sessions,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchase_count
  FROM `ecommerce-486217.raw_data.rfm_segment`
  WHERE user_segment = 'Silver'
  GROUP BY 1, 2
  HAVING purchase_count > 0 
)
SELECT 
  ROUND(AVG(view_sessions), 2) AS avg_view_sessions_before_purchase
FROM product_journey;


--6.4.ATC-> Purchase 
WITH atc_purchase_gap AS (
  SELECT 
    user_id,
    user_session,
    TIMESTAMP_DIFF(
      MIN(CASE WHEN event_type = 'purchase' THEN event_time END), 
      MIN(CASE WHEN event_type = 'cart' THEN event_time END), 
      MINUTE
    ) AS diff_minutes
  FROM `ecommerce-486217.raw_data.rfm_segment`
  WHERE user_segment = 'Silver'
  GROUP BY 1, 2
  
  HAVING MIN(CASE WHEN event_type = 'cart' THEN event_time END) IS NOT NULL 
     AND MIN(CASE WHEN event_type = 'purchase' THEN event_time END) IS NOT NULL
),
time_buckets AS (
  SELECT 
    CASE 
      -- Purchased within 24 hours 
      WHEN diff_minutes <= 60 THEN '0-1 Hour'
      WHEN diff_minutes <= 120 THEN '1-2 Hours'
      WHEN diff_minutes <= 180 THEN '2-3 Hours'
      WHEN diff_minutes <= 360 THEN '3-6 Hours'
      WHEN diff_minutes <= 720 THEN '6-12 Hours'
      WHEN diff_minutes <= 1440 THEN '12-24 Hours'
      -- Purchase action takes more than a day
      WHEN diff_minutes <= 2880 THEN '1-2 Days'
      WHEN diff_minutes <= 4320 THEN '2-3 Days'
      ELSE 'Over 3 Days'
    END AS gap_bucket,
    CASE 
      WHEN diff_minutes <= 1440 THEN 1 
      WHEN diff_minutes <= 4320 THEN 2
      ELSE 3
    END AS sort_group,
    diff_minutes
  FROM atc_purchase_gap
)
SELECT 
  gap_bucket,
  COUNT(*) AS purchase_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM time_buckets
GROUP BY 1, sort_group
ORDER BY sort_group, MIN(diff_minutes);





--6.5.Purchase Action of Silver Segment during weekdays
SELECT 
  EXTRACT(HOUR FROM event_time) AS hour_of_day,
  
  COUNTIF(event_type = 'view') AS view_count,
  ROUND(COUNTIF(event_type = 'view') * 100.0 / SUM(COUNTIF(event_type = 'view')) OVER(), 2) AS view_pct,
  
  COUNTIF(event_type = 'cart') AS atc_count,
  ROUND(COUNTIF(event_type = 'cart') * 100.0 / SUM(COUNTIF(event_type = 'cart')) OVER(), 2) AS atc_pct
FROM `ecommerce-486217.raw_data.rfm_segment`
WHERE 
  user_segment = 'Silver'               --
  AND event_type IN ('view', 'cart')   
  AND EXTRACT(DAYOFWEEK FROM event_time) BETWEEN 2 AND 6 
GROUP BY 1
ORDER BY atc_count DESC; 


--6.6.ATC-> Purchase
WITH atc_purchase_gap AS (
  SELECT 
    user_id,
    user_session,
    product_id,
    MIN(CASE WHEN event_type = 'cart' THEN event_time END) AS atc_time,
    MIN(CASE WHEN event_type = 'purchase' THEN event_time END) AS purchase_time
  FROM `ecommerce-486217.raw_data.rfm_segment`
  WHERE user_segment = 'Silver'
  GROUP BY 1, 2, 3
  HAVING atc_time IS NOT NULL AND purchase_time IS NOT NULL 
     AND purchase_time >= atc_time -- 시간 역전 데이터 방지
),
gap_buckets AS (
  SELECT 
    TIMESTAMP_DIFF(purchase_time, atc_time, HOUR) AS hours_diff
  FROM atc_purchase_gap
)
SELECT 
  CASE 
    WHEN hours_diff = 0 THEN 'Within 1 Hour'
    WHEN hours_diff BETWEEN 1 AND 3 THEN '1-3 Hours'
    WHEN hours_diff BETWEEN 4 AND 6 THEN '4-6 Hours'
    WHEN hours_diff BETWEEN 7 AND 12 THEN '7-12 Hours'
    ELSE 'Over 12 Hours'
  END AS time_bucket,
  
  COUNT(*) AS count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM gap_buckets
GROUP BY 1
ORDER BY 
  CASE time_bucket 
    WHEN 'Within 1 Hour' THEN 1 WHEN '1-3 Hours' THEN 2 
    WHEN '4-6 Hours' THEN 3 WHEN '7-12 Hours' THEN 4 ELSE 5 
  END;




