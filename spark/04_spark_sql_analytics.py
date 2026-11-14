"""
Spark SQL analytics.

The clean Parquet tables are registered as SQL views named after the system
that would hold them in production, to show how one SQL layer can query data
that "lives" in MongoDB and HBase:

    mongo_products, mongo_categories, mongo_users,
    mongo_transactions, mongo_transaction_items       (document store)
    hbase_sessions, hbase_page_views, hbase_session_cart (wide-column store)

(integration/funnel_clv_analysis.py shows the same idea with data pulled
live from MongoDB and HBase instead of Parquet.)

Queries
  Q1 category_performance   CTEs + 3-way join + RANK(): views -> carts -> sales -> profit
  Q2 top_products_per_cat   ROW_NUMBER() top-3 per category
  Q3 weekly_revenue         time series with a 4-week moving average window
  Q4 channel_conversion     conversion by device x referrer (session -> order join)
  Q5 rfm_scores             NTILE() quintile scoring of every customer
  Q6 product_views_vs_sales most-viewed vs best-selling products
"""
import os

from common import config, get_spark, read_clean, save_csv

spark = get_spark("04-spark-sql")

VIEWS = {
    "mongo_products": "products", "mongo_categories": "categories", "mongo_users": "users",
    "mongo_transactions": "transactions", "mongo_transaction_items": "transaction_items",
    "hbase_sessions": "sessions", "hbase_page_views": "page_views", "hbase_session_cart": "session_cart",
}
for view, table in VIEWS.items():
    read_clean(spark, table).createOrReplaceTempView(view)

QUERIES = {}

QUERIES["category_performance"] = """
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
ORDER BY revenue_rank
"""

QUERIES["top_products_per_category"] = """
SELECT * FROM (
    SELECT p.category_name, i.product_id, p.product_name,
           SUM(i.quantity) AS units, ROUND(SUM(i.line_total), 2) AS revenue,
           ROW_NUMBER() OVER (PARTITION BY p.category_id ORDER BY SUM(i.line_total) DESC) AS rn
    FROM mongo_transaction_items i JOIN mongo_products p ON i.product_id = p.product_id
    GROUP BY p.category_id, p.category_name, i.product_id, p.product_name
) WHERE rn <= 3
ORDER BY category_name, rn
"""

QUERIES["weekly_revenue"] = """
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
ORDER BY week_start
"""

QUERIES["channel_conversion"] = """
SELECT s.device_type, s.referrer,
       COUNT(*)                                            AS sessions,
       ROUND(AVG(CASE WHEN s.n_cart_items > 0 THEN 1 ELSE 0 END), 4) AS cart_rate,
       ROUND(AVG(CASE WHEN s.reached_checkout THEN 1 ELSE 0 END), 4) AS checkout_rate,
       ROUND(AVG(CASE WHEN t.transaction_id IS NOT NULL THEN 1 ELSE 0 END), 4) AS order_rate,
       ROUND(SUM(t.total) / COUNT(*), 2)                   AS revenue_per_session
FROM hbase_sessions s
LEFT JOIN mongo_transactions t ON s.session_id = t.session_id
GROUP BY s.device_type, s.referrer
ORDER BY order_rate DESC
"""

QUERIES["rfm_scores"] = """
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
FROM scored
"""

QUERIES["product_views_vs_sales"] = """
WITH v AS (SELECT product_id, COUNT(*) AS views
           FROM hbase_page_views WHERE page_type = 'product_detail' GROUP BY product_id),
     s AS (SELECT product_id, SUM(quantity) AS units, ROUND(SUM(line_total), 2) AS revenue
           FROM mongo_transaction_items GROUP BY product_id)
SELECT p.product_id, p.product_name, p.category_name, p.base_price, p.is_active, p.current_stock,
       COALESCE(v.views, 0) AS views, COALESCE(s.units, 0) AS units, COALESCE(s.revenue, 0) AS revenue,
       ROUND(COALESCE(s.units, 0) / NULLIF(v.views, 0), 4) AS units_per_view
FROM mongo_products p
LEFT JOIN v ON p.product_id = v.product_id
LEFT JOIN s ON p.product_id = s.product_id
"""

os.makedirs(config.OUTPUT_DIR, exist_ok=True)
with open(os.path.join(config.OUTPUT_DIR, "spark_sql_queries.sql"), "w", encoding="utf-8") as f:
    for name, q in QUERIES.items():
        f.write(f"-- {name}\n{q.strip()};\n\n")

for name, q in QUERIES.items():
    df = spark.sql(q)
    print(f"\n=== {name} ===")
    df.show(8, truncate=40)
    save_csv(df, f"sql_{name}")

# Query plan of Q1 - shows broadcast joins chosen for the small tables
plan = spark.sql(QUERIES["category_performance"])._jdf.queryExecution().explainString(
    spark._jvm.org.apache.spark.sql.execution.ExplainMode.fromString("formatted"))
with open(os.path.join(config.OUTPUT_DIR, "spark_sql_category_performance_plan.txt"), "w", encoding="utf-8") as f:
    f.write(plan)

seg = spark.sql(f"SELECT rfm_segment, COUNT(*) users, ROUND(AVG(monetary),2) avg_spent "
                f"FROM ({QUERIES['rfm_scores']}) GROUP BY rfm_segment ORDER BY avg_spent DESC")
seg.show()
save_csv(seg, "sql_rfm_segment_summary")
print(f"Results in {config.OUTPUT_DIR}/sql_*.csv")
spark.stop()
