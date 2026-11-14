-- category_performance
WITH views AS (                       -- HBase: product page views
    SELECT p.category_id, COUNT(*) AS product_views
    FROM hbase_page_views v JOIN mongo_products p ON v.product_id = p.product_id
    WHERE v.page_type = 'product_detail'
    GROUP BY p.category_id
),
carts AS (                            -- HBase: cart additions
    SELECT p.category_id, SUM(c.quantity) AS units_carted
    FROM hbase_session_cart c JOIN mongo_products p ON c.product_id = p.product_id
    GROUP BY p.category_id
),
sales AS (                            -- MongoDB: order line items
    SELECT category_id,
           SUM(quantity)                     AS units_sold,
           ROUND(SUM(line_total), 2)         AS revenue,
           ROUND(SUM(line_total * profit_margin), 2) AS est_profit,
           COUNT(DISTINCT transaction_id)    AS orders
    FROM mongo_transaction_items
    GROUP BY category_id
)
SELECT c.category_id, c.category_name,
       v.product_views, k.units_carted, s.units_sold, s.orders, s.revenue, s.est_profit,
       ROUND(s.est_profit / s.revenue, 4)          AS profit_margin,
       ROUND(k.units_carted / v.product_views, 4)  AS cart_units_per_view,
       ROUND(s.units_sold / k.units_carted, 4)     AS sold_per_carted_unit,
       RANK() OVER (ORDER BY s.revenue DESC)       AS revenue_rank,
       RANK() OVER (ORDER BY s.est_profit DESC)    AS profit_rank
FROM mongo_categories c
JOIN views v ON c.category_id = v.category_id
JOIN carts k ON c.category_id = k.category_id
JOIN sales s ON c.category_id = s.category_id
ORDER BY revenue_rank;

-- top_products_per_category
SELECT * FROM (
    SELECT p.category_name, i.product_id, p.product_name,
           SUM(i.quantity) AS units, ROUND(SUM(i.line_total), 2) AS revenue,
           ROW_NUMBER() OVER (PARTITION BY p.category_id ORDER BY SUM(i.line_total) DESC) AS rn
    FROM mongo_transaction_items i JOIN mongo_products p ON i.product_id = p.product_id
    GROUP BY p.category_id, p.category_name, i.product_id, p.product_name
) WHERE rn <= 3
ORDER BY category_name, rn;

-- weekly_revenue
SELECT week_start, orders, revenue, avg_order_value,
       ROUND(AVG(revenue) OVER (ORDER BY week_start ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2)
           AS revenue_4wk_avg
FROM (
    SELECT DATE(DATE_TRUNC('WEEK', ts)) AS week_start,
           COUNT(*) AS orders,
           ROUND(SUM(total), 2) AS revenue,
           ROUND(AVG(total), 2) AS avg_order_value
    FROM mongo_transactions
    WHERE status <> 'cancelled'
    GROUP BY DATE(DATE_TRUNC('WEEK', ts))
)
ORDER BY week_start;

-- channel_conversion
SELECT s.device_type, s.referrer,
       COUNT(*)                                            AS sessions,
       ROUND(AVG(CASE WHEN s.n_cart_items > 0 THEN 1 ELSE 0 END), 4) AS cart_rate,
       ROUND(AVG(CASE WHEN s.reached_checkout THEN 1 ELSE 0 END), 4) AS checkout_rate,
       ROUND(AVG(CASE WHEN t.transaction_id IS NOT NULL THEN 1 ELSE 0 END), 4) AS order_rate,
       ROUND(SUM(t.total) / COUNT(*), 2)                   AS revenue_per_session
FROM hbase_sessions s
LEFT JOIN mongo_transactions t ON s.session_id = t.session_id
GROUP BY s.device_type, s.referrer
ORDER BY order_rate DESC;

-- rfm_scores
WITH ref AS (SELECT MAX(ts) AS ref_ts FROM mongo_transactions),
base AS (
    SELECT u.user_id, u.country,
           DATEDIFF((SELECT ref_ts FROM ref), MAX(t.ts)) AS recency_days,
           COUNT(t.transaction_id)                       AS frequency,
           ROUND(SUM(t.total), 2)                        AS monetary
    FROM mongo_users u JOIN mongo_transactions t ON u.user_id = t.user_id
    GROUP BY u.user_id, u.country
),
scored AS (
    SELECT *,
           NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,  -- recent = 5
           NTILE(5) OVER (ORDER BY frequency)         AS f_score,
           NTILE(5) OVER (ORDER BY monetary)          AS m_score
    FROM base
)
SELECT *, r_score + f_score + m_score AS rfm_total,
       CASE WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
            WHEN f_score >= 4 AND m_score >= 3                   THEN 'Loyal'
            WHEN r_score <= 2 AND m_score >= 3                   THEN 'At risk (high value)'
            WHEN r_score <= 2                                    THEN 'Hibernating'
            ELSE 'Regular' END AS rfm_segment
FROM scored;

-- product_views_vs_sales
WITH v AS (SELECT product_id, COUNT(*) AS views
           FROM hbase_page_views WHERE page_type = 'product_detail' GROUP BY product_id),
     s AS (SELECT product_id, SUM(quantity) AS units, ROUND(SUM(line_total), 2) AS revenue
           FROM mongo_transaction_items GROUP BY product_id)
SELECT p.product_id, p.product_name, p.category_name, p.base_price, p.is_active, p.current_stock,
       COALESCE(v.views, 0) AS views, COALESCE(s.units, 0) AS units, COALESCE(s.revenue, 0) AS revenue,
       ROUND(COALESCE(s.units, 0) / NULLIF(v.views, 0), 4) AS units_per_view
FROM mongo_products p
LEFT JOIN v ON p.product_id = v.product_id
LEFT JOIN s ON p.product_id = s.product_id;

