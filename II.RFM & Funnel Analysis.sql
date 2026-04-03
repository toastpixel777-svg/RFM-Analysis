-- 3.RFM ANALYSIS --
-- 3.1.Checking for AOF, AOV, Recency 
SELECT 
  'Recency(Days)' AS metric, 
  COUNT(DISTINCT recency) AS unique_values, 
  MIN(recency) AS min_val, 
  MAX(recency) AS max_val 
FROM (
  SELECT 
    DATE_DIFF(
      (SELECT MAX(DATE(event_time)) FROM `ecommerce-486217.raw_data.purchase_users_only`), 
      MAX(DATE(event_time)), 
      DAY
    ) AS recency 
  FROM `ecommerce-486217.raw_data.purchase_users_only` 
  GROUP BY user_id
)

UNION ALL

SELECT 
  'Frequency(Counts)' AS metric, 
  COUNT(DISTINCT frequency) AS unique_values, 
  MIN(frequency) AS min_val, 
  MAX(frequency) AS max_val 
FROM (
  SELECT 
    COUNTIF(event_type = 'purchase') AS frequency 
  FROM `ecommerce-486217.raw_data.purchase_users_only` 
  GROUP BY user_id
)

UNION ALL

SELECT 
  'Monetary(Amount)' AS metric, 
  COUNT(DISTINCT monetary) AS unique_values, 
  MIN(monetary) AS min_val, 
  MAX(monetary) AS max_val 
FROM (
  SELECT 
    SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS monetary 
  FROM `ecommerce-486217.raw_data.purchase_users_only` 
  GROUP BY user_id
);

--3.2.Membership Segmentation
CREATE OR REPLACE TABLE `ecommerce-486217.raw_data.rfm_segment`
PARTITION BY DATE(event_time)
AS
WITH user_rfm AS (
  SELECT
    user_id,
    DATE_DIFF((SELECT MAX(DATE(event_time)) FROM `ecommerce-486217.raw_data.purchase_users_only`), MAX(DATE(event_time)), DAY) AS recency, 
    COUNTIF(event_type = 'purchase') AS frequency,
    ROUND(SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END), 2) AS monetary
  FROM `ecommerce-486217.raw_data.purchase_users_only`
  GROUP BY user_id
),
rfm_ranked AS (
  SELECT
    *,
    NTILE(5) OVER (ORDER BY recency DESC) AS r_score,
    NTILE(5) OVER (ORDER BY frequency ASC) AS f_score,
    NTILE(5) OVER (ORDER BY monetary ASC) AS m_score
  FROM user_rfm
),
weighted_calculation AS (
  SELECT
    *, 
    (r_score * 2 + f_score * 4 + m_score * 4) AS weighted_score
  FROM rfm_ranked
),
final_rank AS (
  SELECT
    *,
    PERCENT_RANK() OVER (ORDER BY weighted_score DESC) AS score_rank
  FROM weighted_calculation
),
user_segments AS (
  SELECT
    user_id, r_score, f_score, m_score, weighted_score,
    CASE
      WHEN score_rank <= 0.10 THEN 'Gold'       
      WHEN score_rank <= 0.40 THEN 'Silver'      
      ELSE 'Bronze'                            
    END AS user_segment
  FROM final_rank
)
SELECT
  s.*,
  u.r_score, u.f_score, u.m_score, u.weighted_score, u.user_segment
FROM `ecommerce-486217.raw_data.purchase_users_only` AS s
LEFT JOIN user_segments AS u ON s.user_id = u.user_id;


--3.3.Revenue Contribution, ARPU per segment
WITH segment_stats AS (
  SELECT
    user_segment,
    COUNT(DISTINCT user_id) AS user_count,
    COUNTIF(event_type = 'purchase') AS total_sales_volume,
    ROUND(SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END), 2) AS total_revenue
  FROM
    `ecommerce-486217.raw_data.rfm_segment`
  GROUP BY
    user_segment
),
total_metrics AS (
  SELECT
    SUM(user_count) AS total_users,
    SUM(total_revenue) AS grand_total_revenue
  FROM
    segment_stats
)

SELECT
  s.user_segment,
  s.user_count,
  ROUND(s.user_count / t.total_users * 100, 2) AS user_ratio_pct,
  s.total_sales_volume,
  s.total_revenue,
  -- Revenue Contributon (%)
  ROUND(s.total_revenue / t.grand_total_revenue * 100, 2) AS revenue_contribution_pct,
  -- ARPU
  ROUND(s.total_revenue / s.user_count, 2) AS arpu
FROM
  segment_stats s,
  total_metrics t
ORDER BY
  s.total_revenue DESC;


--3.4.Monthly User Activation Rates
WITH monthly_active AS (
  SELECT
    year,
    month,
    user_segment,
    COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS monthly_active_buyers
  FROM
    `ecommerce-486217.raw_data.rfm_segment`
  GROUP BY 1, 2, 3
),
cumulative_members AS (

  SELECT
    t1.year,
    t1.month,
    t1.user_segment,
    (
      SELECT COUNT(DISTINCT t2.user_id)
      FROM `ecommerce-486217.raw_data.rfm_segment` t2
      WHERE t2.user_segment = t1.user_segment
        AND (t2.year < t1.year OR (t2.year = t1.year AND t2.month <= t1.month))
    ) AS total_cumulative_members
  FROM
    (SELECT DISTINCT year, month, user_segment FROM `ecommerce-486217.raw_data.rfm_segment`) t1
)

SELECT
  c.year,
  c.month,
  c.user_segment,
  c.total_cumulative_members AS total_members, 
  COALESCE(a.monthly_active_buyers, 0) AS active_buyers, 
  -- User Activation (%)
  ROUND(SAFE_DIVIDE(COALESCE(a.monthly_active_buyers, 0), c.total_cumulative_members) * 100, 2) AS active_rate_pct
FROM
  cumulative_members c
LEFT JOIN
  monthly_active a ON c.year = a.year AND c.month = a.month AND c.user_segment = a.user_segment
ORDER BY
  c.year



--4.FUNNEL ANALYSIS--
--4.1.ATC, CVR for different membership segments
WITH session_steps AS (
  SELECT
    user_segment,
    user_session,
  
    MAX(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS saw_view,
    MAX(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS saw_cart,
    MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS saw_purchase
  FROM
    `ecommerce-486217.raw_data.rfm_segment`
  WHERE
    user_session IS NOT NULL 
  GROUP BY
    user_segment, user_session
),
segment_funnel AS (
  SELECT
    user_segment,
    COUNT(*) AS total_sessions,
    SUM(saw_view) AS view_sessions,
    SUM(saw_cart) AS cart_sessions,
    SUM(saw_purchase) AS purchase_sessions
  FROM
    session_steps
  GROUP BY
    user_segment
)

SELECT
  user_segment,
  view_sessions,
  cart_sessions,
  purchase_sessions,
  -- View -> ATC 
  ROUND(SAFE_DIVIDE(cart_sessions, view_sessions) * 100, 2) AS atc_rate,
  -- ATC -> Purchase 
  ROUND(SAFE_DIVIDE(purchase_sessions, cart_sessions) * 100, 2) AS purchase_rate,
  -- CVR per session
  ROUND(SAFE_DIVIDE(purchase_sessions, total_sessions) * 100, 2) AS cvr
FROM
  segment_funnel
ORDER BY
  CASE user_segment WHEN 'Gold' THEN 1 WHEN 'Silver' THEN 2 ELSE 3 END;

--4.2.Monthly Purchase Pattern
WITH session_steps AS (
  SELECT
    year,
    month,
    user_segment,
    user_session,
    MAX(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS saw_view,
    MAX(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS saw_cart,
    MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS saw_purchase
  FROM
    `ecommerce-486217.raw_data.rfm_segment`
  WHERE
    user_session IS NOT NULL
  GROUP BY
    1, 2, 3, 4
),
monthly_segment_funnel AS (
  SELECT
    year,
    month,
    user_segment,
    SUM(saw_view) AS view_sessions,
    SUM(saw_cart) AS cart_sessions,
    SUM(saw_purchase) AS purchase_sessions
  FROM
    session_steps
  GROUP BY
    1, 2, 3
)

SELECT
  year,
  month,
  user_segment,
  -- View -> ATC 
  ROUND(SAFE_DIVIDE(cart_sessions, view_sessions) * 100, 2) AS view_to_atc_pct,
  -- ATC -> Purchase 
  ROUND(SAFE_DIVIDE(purchase_sessions, cart_sessions) * 100, 2) AS atc_to_purchase_pct,
  -- CVR
  ROUND(SAFE_DIVIDE(purchase_sessions, view_sessions) * 100, 2) AS session_cvr_pct
FROM
  monthly_segment_funnel
ORDER BY
  year DESC, month DESC, 
  CASE user_segment WHEN 'Gold' THEN 1 WHEN 'Silver' THEN 2 ELSE 3 END;


--3. aof, aov, recency, frequency
WITH order_base AS (
  SELECT
    user_id,
    user_segment,
    user_session,
    DATE(event_time) AS order_date,
    SUM(price) AS order_amount 
  FROM
    `ecommerce-486217.raw_data.rfm_segment`
  WHERE
    event_type = 'purchase'
  GROUP BY
    1, 2, 3, 4
),
user_metrics AS (

  SELECT
    user_id,
    user_segment,
    SUM(order_amount) AS total_revenue,
    COUNT(user_session) AS total_orders, 
    COUNT(DISTINCT order_date) AS active_days,
    MAX(order_date) AS last_purchase_date,
    MIN(order_date) AS first_purchase_date
  FROM
    order_base
  GROUP BY
    1, 2
),
reference_date AS (
  --Recency 
  SELECT MAX(DATE(event_time)) AS max_date 
  FROM `ecommerce-486217.raw_data.rfm_segment`
)

SELECT
  m.user_segment,
  COUNT(m.user_id) AS user_cnt,
  -- AOV
  ROUND(SAFE_DIVIDE(SUM(m.total_revenue), SUM(m.total_orders)), 2) AS aov,
  -- AOF 
  ROUND(AVG(m.total_orders), 2) AS aof,
  -- Avg Recency
  ROUND(AVG(DATE_DIFF(r.max_date, m.last_purchase_date, DAY)), 1) AS avg_recency,
  -- Purchase Cycle (in Days)
  ROUND(AVG(
    CASE 
      WHEN m.active_days > 1 THEN SAFE_DIVIDE(DATE_DIFF(m.last_purchase_date, m.first_purchase_date, DAY), m.active_days - 1)
      ELSE NULL 
    END
  ), 1) AS avg_purchase_cycle_days
FROM
  user_metrics m,
  reference_date r
GROUP BY
  m.user_segment
ORDER BY
  CASE m.user_segment WHEN 'Gold' THEN 1 WHEN 'Silver' THEN 2 ELSE 3 END;

