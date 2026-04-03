--1.Data Prep 
--1.1. Checking for 0 or negative price values
SELECT
  event_type,

  CASE 
    WHEN price < 0 THEN 'Negative'
    WHEN price = 0 THEN 'Zero'
    ELSE 'Positive'
  END AS price_status,
  COUNT(*) AS event_count,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER(PARTITION BY event_type) * 100, 2) AS type_ratio_pct,
  MIN(price) AS min_price,
  MAX(price) AS max_price
FROM
  `ecommerce-486217.raw_data.ecommerce_sales`
GROUP BY
  1, 2
ORDER BY
  event_type, price_status;

--1.2. Remove all 'user session' with 0 or negative price values
CREATE TABLE `ecommerce-486217.raw_data.clean_data`
PARTITION BY DATE(event_time)
AS
WITH invalid_sessions AS (
  SELECT DISTINCT user_session
  FROM `ecommerce-486217.raw_data.ecommerce_sales`
  WHERE price <= 0 AND user_session IS NOT NULL
)
SELECT *
FROM `ecommerce-486217.raw_data.ecommerce_sales`
WHERE user_session NOT IN (SELECT user_session FROM invalid_sessions)
  AND user_session IS NOT NULL;

SELECT * FROM 

--1.3.Checking for outliers
WITH session_counts AS (
  SELECT 
    'Original' AS stage,
    COUNT(DISTINCT user_session) AS session_cnt,
    COUNT(*) AS row_cnt
  FROM `ecommerce-486217.raw_data.ecommerce_sales`
  UNION ALL
  SELECT 
    'Cleaned' AS stage,
    COUNT(DISTINCT user_session) AS session_cnt,
    COUNT(*) AS row_cnt
  FROM `ecommerce-486217.raw_data.clean_data`
)
SELECT
  MAX(CASE WHEN stage = 'Original' THEN session_cnt END) AS total_sessions,
  MAX(CASE WHEN stage = 'Cleaned' THEN session_cnt END) AS cleaned_sessions,
  -- number of cases removed 
  MAX(CASE WHEN stage = 'Original' THEN session_cnt END) - 
  MAX(CASE WHEN stage = 'Cleaned' THEN session_cnt END) AS removed_sessions,
  -- proportion of cases removed
  ROUND((MAX(CASE WHEN stage = 'Original' THEN session_cnt END) - 
         MAX(CASE WHEN stage = 'Cleaned' THEN session_cnt END)) / 
         MAX(CASE WHEN stage = 'Original' THEN session_cnt END) * 100, 2) AS removal_pct
FROM session_counts;

--1.4.Check for price fluctuations for each product
SELECT
  product_id,
  MIN(price) AS min_price,
  MAX(price) AS max_price,
  COUNT(DISTINCT price) AS price_count,
  MAX(price) - MIN(price) AS price_gap, 
  ROUND((MAX(price) - MIN(price)) / NULLIF(MIN(price), 0) * 100, 2) AS fluctuation_pct
FROM
  `ecommerce-486217.raw_data.clean_data`
GROUP BY
  1
HAVING
  COUNT(DISTINCT price) > 1 
ORDER BY
  fluctuation_pct DESC
LIMIT 100;

SELECT
  product_id,
  MIN(price) AS min_price,
  MAX(price) AS max_price,
  COUNT(DISTINCT price) AS price_count,
  MAX(price) - MIN(price) AS price_gap, 
  ROUND(SAFE_DIVIDE((MAX(price) - MIN(price)), MIN(price)) * 100, 2) AS fluctuation_pct
FROM
  `ecommerce-486217.raw_data.clean_data`
GROUP BY
  product_id
HAVING
  price_count > 1
ORDER BY
  fluctuation_pct DESC
LIMIT 100;

-- 1.4.Daily Fluctuations for each product
SELECT
  DATE(event_time) AS event_date,
  price,
  COUNT(*) AS event_count,
  STRING_AGG(DISTINCT event_type, ', ') AS event_types
FROM
  `ecommerce-486217.raw_data.clean_data`
WHERE
  product_id = 5603947
GROUP BY
  1, 2
ORDER BY
  1 ASC;




--2.EDA
--2.1. Number of product_id, median price range, e.g. 
SELECT
  COUNT(DISTINCT product_id) AS unique_products,
  ROUND(AVG(price), 2) AS avg_price,
  APPROX_QUANTILES(price, 100)[OFFSET(50)] AS median_price,
  MIN(price) AS min_price,
  MAX(price) AS max_price
FROM `ecommerce-486217.raw_data.clean_data`;

--2.2.Checking for user_id with 0 user sessions
SELECT
  COUNT(DISTINCT user_id) AS total_users,
  COUNT(DISTINCT CASE WHEN user_session IS NULL THEN user_id END) AS users_with_no_session
FROM `ecommerce-486217.raw_data.clean_data`;
--all 1636278 users have user_sessions

-- 2.3. Create new table with only users with more than two purchase history
CREATE OR REPLACE TABLE `ecommerce-486217.raw_data.purchase_users_only`
PARTITION BY DATE(event_time)
AS
WITH buyers_count AS (
  SELECT 
    user_id,
    COUNTIF(event_type = 'purchase') AS purchase_cnt
  FROM `ecommerce-486217.raw_data.clean_data`
  GROUP BY 1
  HAVING purchase_cnt >= 2
)
SELECT s.*
FROM `ecommerce-486217.raw_data.clean_data` s
JOIN buyers_count b ON s.user_id = b.user_id;


--2.4.Checking for purchase quantity fluctuations
SELECT
  year,
  month,
  COUNT(*) AS sales_volume,
  ROUND(SUM(price), 2) AS total_revenue,
  ROUND(AVG(price), 2) AS avg_selling_price,
  APPROX_QUANTILES(price, 100)[OFFSET(50)] AS median_selling_price
FROM
  `ecommerce-486217.raw_data.purchase_users_only`
WHERE
  event_type = 'purchase'
GROUP BY
  year, month
ORDER BY
  year DESC, month DESC;


