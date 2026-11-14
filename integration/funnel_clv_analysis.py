"""
Part 3 - Integrated analytics across MongoDB, HBase and Spark.

Business questions
------------------
A. FUNNEL: Where do shoppers drop out between viewing a product, adding it
   to the cart, reaching checkout and actually paying - and does it differ by
   device, traffic source or product category?
B. CLV:    Which customers are worth the most over the next 12 months, and
   does on-site engagement (session frequency / depth) predict that value?

Data sources
------------
  HBase    user_sessions       page-view paths + cart contents   (behaviour)
  MongoDB  transactions        orders with embedded line items   (money)
  MongoDB  users / categories  profiles, margins                 (reference)
  Spark                        joins, funnel + CLV computation, window functions

Reading strategy
----------------
* HBase: the table is pre-split by user_id range. Spark parallelises the
  list of key ranges and every partition opens its own Thrift connection and
  scans only its range (mapPartitions) - so the read scales with executors
  instead of funnelling through the driver.
* MongoDB: pymongo with a projection (only the fields needed) on the driver,
  converted to a Spark DataFrame. For production volumes the official
  MongoDB Spark Connector would push this down and read in parallel.
* --source files reads the cleaned Parquet instead, so the analysis can be
  reproduced without the databases running.

Usage:
    python integration/funnel_clv_analysis.py --source databases
    python integration/funnel_clv_analysis.py --source files
"""
import argparse
import functools
import json
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path[:0] = [ROOT, os.path.join(ROOT, "spark")]
import config  # noqa: E402
from common import get_spark, read_clean, save_csv  # noqa: E402

# HBase regions are split at these keys (see hbase/create_schema.hbase)
HBASE_SPLITS = ["", "user_002000", "user_004000", "user_006000", "user_008000", "user_999999"]

PV_TYPE = T.StructType([
    T.StructField("seq", T.IntegerType()), T.StructField("ts", T.StringType()),
    T.StructField("page_type", T.StringType()), T.StructField("product_id", T.StringType()),
    T.StructField("category_id", T.StringType()), T.StructField("view_duration", T.IntegerType())])
CART_TYPE = T.StructType([
    T.StructField("product_id", T.StringType()), T.StructField("quantity", T.IntegerType()),
    T.StructField("price", T.DoubleType())])
HBASE_ROW_SCHEMA = T.StructType([
    T.StructField("session_id", T.StringType()), T.StructField("user_id", T.StringType()),
    T.StructField("start_time", T.StringType()), T.StructField("duration_seconds", T.IntegerType()),
    T.StructField("device_type", T.StringType()), T.StructField("referrer", T.StringType()),
    T.StructField("conversion_status", T.StringType()),
    T.StructField("page_views", T.ArrayType(PV_TYPE)), T.StructField("cart", T.ArrayType(CART_TYPE))])


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------
def hbase_row_to_record(key, data):
    """Decode one user_sessions row (bytes) into a plain tuple."""
    d = {k.decode(): v.decode() for k, v in data.items()}
    pvs, cart = [], []
    for col in sorted(c for c in d if c.startswith("p:")):
        ts, ptype, pid, cid, dur = d[col].split("|")
        pvs.append((int(col[2:]), ts, ptype, pid or None, cid or None, int(dur or 0)))
    for col in (c for c in d if c.startswith("c:")):
        item = json.loads(d[col])
        cart.append((col[2:], int(item["quantity"]), float(item["price"])))
    return (d["s:session_id"], key.decode().split("#")[0], d["s:start_time"], int(d["s:duration"]),
            d["s:device"], d["s:referrer"], d["s:status"], pvs, cart)


def scan_hbase_range(key_range, host, port, table_name):
    """Runs inside a Spark worker: scan one row-key range of user_sessions.
    Host, port and table are passed in explicitly (not read from config.py)
    so the function is self-contained when Spark ships it to a worker
    process - important on Windows, where workers start as fresh processes."""
    import happybase
    start, stop = key_range
    conn = happybase.Connection(host, port=port, timeout=120000)
    try:
        table = conn.table(table_name)
        for key, data in table.scan(row_start=start.encode() or None, row_stop=stop.encode(),
                                    columns=[b"s", b"p", b"c"], batch_size=1000):
            yield hbase_row_to_record(key, data)
    finally:
        conn.close()


def sessions_from_hbase(spark, conn=None):
    ranges = list(zip(HBASE_SPLITS[:-1], HBASE_SPLITS[1:]))
    if conn is None:   # normal path: parallel scan, one range per partition
        scan = functools.partial(scan_hbase_range, host=config.HBASE_HOST, port=config.HBASE_PORT,
                                 table_name=config.HBASE_SESSIONS_TABLE)
        rdd = spark.sparkContext.parallelize(ranges, len(ranges)).flatMap(scan)
        raw = spark.createDataFrame(rdd, HBASE_ROW_SCHEMA)
    else:              # driver-side scan with a supplied connection (tests / small demos)
        table = conn.table(config.HBASE_SESSIONS_TABLE)
        rows = [hbase_row_to_record(k, v) for s, e in ranges
                for k, v in table.scan(row_start=s.encode() or None, row_stop=e.encode())]
        raw = spark.createDataFrame(rows, HBASE_ROW_SCHEMA)
    raw = raw.cache()

    sessions = raw.select(
        "session_id", "user_id", F.to_timestamp("start_time").alias("start_time"),
        "duration_seconds", "device_type", "referrer", "conversion_status",
        F.size("page_views").alias("n_pages"),
        F.size("cart").alias("n_cart_items"),
        F.exists("page_views", lambda p: p["page_type"].isin("checkout", "confirmation")).alias("reached_checkout"))
    page_views = (raw.select("session_id", "user_id", F.explode("page_views").alias("pv"))
                  .select("session_id", "user_id", "pv.seq", F.to_timestamp("pv.ts").alias("ts"),
                          "pv.page_type", "pv.product_id", "pv.category_id"))
    session_cart = (raw.select("session_id", "user_id", F.explode("cart").alias("c"))
                    .select("session_id", "user_id", "c.product_id", "c.quantity", "c.price"))
    return sessions, page_views, session_cart


def from_mongo(spark, db=None):
    """Transactions (+ items with margin) and users from MongoDB."""
    if db is None:
        from pymongo import MongoClient
        db = MongoClient(config.MONGO_URI)[config.MONGO_DB]

    margin = {s["subcategory_id"]: s["profit_margin"]
              for c in db.categories.find({}, {"subcategories": 1}) for s in c["subcategories"]}

    txn_rows, item_rows = [], []
    for t in db.transactions.find({}, {"session_id": 1, "user_id": 1, "timestamp": 1, "total": 1,
                                       "status": 1, "items": 1}):
        txn_rows.append((t["_id"], t.get("session_id"), t["user_id"], t["timestamp"], float(t["total"]), t["status"]))
        for it in t["items"]:
            item_rows.append((t["_id"], t.get("session_id"), it["product_id"], it.get("category_id"),
                              int(it["quantity"]), float(it["subtotal"]), margin.get(it.get("subcategory_id"))))
    transactions = spark.createDataFrame(txn_rows, "transaction_id string, session_id string, user_id string, "
                                                   "ts timestamp, total double, status string")
    items = spark.createDataFrame(item_rows, "transaction_id string, session_id string, product_id string, "
                                             "category_id string, quantity int, line_total double, "
                                             "profit_margin double")
    users = spark.createDataFrame(
        [(u["_id"], u["geo"]["country"], u["registration_date"]) for u in
         db.users.find({}, {"geo.country": 1, "registration_date": 1})],
        "user_id string, country string, registration_date timestamp")
    categories = spark.createDataFrame([(c["_id"], c["name"]) for c in db.categories.find({}, {"name": 1})],
                                       "category_id string, category_name string")
    return transactions, items, users, categories


def from_files(spark):
    txn = read_clean(spark, "transactions")
    items = (read_clean(spark, "transaction_items")
             .join(txn.select("transaction_id", "session_id"), "transaction_id"))
    return (read_clean(spark, "sessions"), read_clean(spark, "page_views"), read_clean(spark, "session_cart"),
            txn, items, read_clean(spark, "users"), read_clean(spark, "categories"))


# ---------------------------------------------------------------------------
# A. Funnel
# ---------------------------------------------------------------------------
STAGES = ["1_session", "2_viewed_product", "3_added_to_cart", "4_reached_checkout", "5_purchased"]


def funnel(sessions, page_views, transactions):
    viewed = (page_views.filter("page_type = 'product_detail'").select("session_id").distinct()
              .withColumn("viewed", F.lit(True)))
    paid = (transactions.filter(F.col("session_id").isNotNull()).select("session_id").distinct()
            .withColumn("paid", F.lit(True)))
    flags = (sessions.join(viewed, "session_id", "left").join(paid, "session_id", "left")
             .fillna(False, ["viewed", "paid"])
             # each stage requires all previous stages -> a strict funnel
             .withColumn("s2", F.col("viewed"))
             .withColumn("s3", F.col("s2") & (F.col("n_cart_items") > 0))
             .withColumn("s4", F.col("s3") & F.col("reached_checkout"))
             .withColumn("s5", F.col("s4") & F.col("paid"))
             .cache())

    def stage_counts(df, by=None):
        aggs = [F.count("*").alias(STAGES[0])] + [F.sum(F.col(f"s{i}").cast("int")).alias(STAGES[i - 1])
                                                  for i in range(2, 6)]
        g = df.groupBy(by).agg(*aggs) if by else df.agg(*aggs)
        return g.withColumn("view_to_purchase", F.round(F.col(STAGES[4]) / F.col(STAGES[1]), 4)) \
                .withColumn("checkout_to_purchase", F.round(F.col(STAGES[4]) / F.col(STAGES[3]), 4))

    overall = stage_counts(flags).first().asDict()
    rows, prev = [], None
    for s in STAGES:
        n = overall[s]
        rows.append((s, n, round(n / overall[STAGES[0]], 4), round(n / prev, 4) if prev else 1.0))
        prev = n
    overall_df = flags.sparkSession.createDataFrame(rows, "stage string, sessions long, pct_of_sessions double, "
                                                          "pct_of_previous double")
    # data-quality signal: sessions the web log calls "converted" but with no order
    ghost = flags.filter("conversion_status = 'converted' AND NOT paid").count()
    return overall_df, stage_counts(flags, "device_type"), stage_counts(flags, "referrer"), ghost


def category_funnel(page_views, session_cart, items, categories):
    """Per category: product views -> carted units -> units bought in the same session."""
    views = (page_views.filter("page_type = 'product_detail' AND product_id IS NOT NULL")
             .groupBy("category_id").agg(F.count("*").alias("product_views")))
    carted_sessions = session_cart.select("session_id", "product_id").distinct()
    pid_cat = page_views.select("product_id", "category_id").dropna().distinct()
    carts = (session_cart.join(pid_cat, "product_id")
             .groupBy("category_id").agg(F.sum("quantity").alias("units_carted")))
    bought = (items.filter(F.col("session_id").isNotNull())
              .join(carted_sessions, ["session_id", "product_id"])          # bought what they carted
              .groupBy("category_id").agg(F.sum("quantity").alias("units_bought"),
                                          F.round(F.sum("line_total"), 2).alias("revenue")))
    return (views.join(carts, "category_id").join(bought, "category_id").join(categories, "category_id")
            .withColumn("cart_per_view", F.round(F.col("units_carted") / F.col("product_views"), 4))
            .withColumn("buy_per_cart", F.round(F.col("units_bought") / F.col("units_carted"), 4))
            .withColumn("buy_per_view", F.round(F.col("units_bought") / F.col("product_views"), 4))
            .select("category_id", "category_name", "product_views", "units_carted", "units_bought",
                    "revenue", "cart_per_view", "buy_per_cart", "buy_per_view")
            .orderBy("buy_per_view"))


# ---------------------------------------------------------------------------
# B. Customer lifetime value
# ---------------------------------------------------------------------------
def clv(transactions, items, sessions, users):
    """
    12-month gross-profit CLV (a common simple model):
        CLV = avg_order_value x orders_per_year x gross_margin x retention
    orders_per_year = orders in the observed window scaled to 365 days.
    retention       = 1.0 if the customer bought in the last 30 days,
                      0.6 if 31-60 days ago, 0.3 otherwise (simple recency decay).
    """
    window = transactions.agg(F.min("ts").alias("a"), F.max("ts").alias("b")).first()
    window_days = max((window["b"] - window["a"]).days, 1)

    margin = (items.groupBy("transaction_id")
              .agg((F.sum(F.col("line_total") * F.col("profit_margin")) / F.sum("line_total")).alias("txn_margin")))
    orders = (transactions.join(margin, "transaction_id", "left")
              .groupBy("user_id")
              .agg(F.count("*").alias("orders"),
                   F.round(F.sum("total"), 2).alias("revenue"),
                   F.round(F.avg("txn_margin"), 4).alias("gross_margin"),
                   F.max("ts").alias("last_purchase"))
              .withColumn("recency_days", F.datediff(F.lit(window["b"]), "last_purchase")))
    engagement = (sessions.groupBy("user_id")
                  .agg(F.count("*").alias("sessions"),
                       F.round(F.avg("duration_seconds") / 60, 2).alias("avg_session_min"),
                       F.round(F.avg("n_pages"), 2).alias("avg_pages"),
                       F.round(F.avg((F.col("n_cart_items") > 0).cast("int")), 4).alias("cart_rate")))

    out = (users.select("user_id", "country").join(orders, "user_id", "left").join(engagement, "user_id", "left")
           .fillna(0, ["orders", "revenue", "sessions"])
           .withColumn("aov", F.when(F.col("orders") > 0, F.round(F.col("revenue") / F.col("orders"), 2)).otherwise(0))
           .withColumn("orders_per_year", F.round(F.col("orders") * 365 / window_days, 2))
           .withColumn("retention", F.when(F.col("recency_days") <= 30, 1.0)
                       .when(F.col("recency_days") <= 60, 0.6).otherwise(0.3))
           .withColumn("clv_12m", F.round(F.col("aov") * F.col("orders_per_year") *
                                          F.coalesce("gross_margin", F.lit(0)) * F.col("retention"), 2))
           .withColumn("clv_tier", F.ntile(4).over(Window.orderBy(F.desc("clv_12m")))))
    out = out.withColumn("clv_tier", F.concat(F.lit("Q"), "clv_tier")).cache()

    total = out.agg(F.sum("clv_12m")).first()[0]
    tiers = (out.groupBy("clv_tier")
             .agg(F.count("*").alias("users"),
                  F.round(F.avg("clv_12m"), 2).alias("avg_clv_12m"),
                  F.round(F.sum("clv_12m") / total, 4).alias("share_of_total_clv"),
                  F.round(F.avg("orders"), 2).alias("avg_orders_90d"),
                  F.round(F.avg("aov"), 2).alias("avg_aov"),
                  F.round(F.avg("sessions"), 2).alias("avg_sessions"),
                  F.round(F.avg("avg_session_min"), 2).alias("avg_session_min"),
                  F.round(F.avg("cart_rate"), 4).alias("avg_cart_rate"))
             .orderBy("clv_tier"))
    corr = {c: round(out.stat.corr(c, "clv_12m"), 4)
            for c in ("sessions", "avg_session_min", "avg_pages", "cart_rate", "orders", "aov")}
    return out, tiers, corr, window_days


# ---------------------------------------------------------------------------
def run(spark, source, mongo_db=None, hbase_conn=None):
    if source == "databases":
        print("Reading sessions from HBase and orders/users from MongoDB ...")
        sessions, page_views, session_cart = sessions_from_hbase(spark, conn=hbase_conn)
        transactions, items, users, categories = from_mongo(spark, mongo_db)
        # HBase may hold only a subset of sessions: the funnel is computed per
        # session (orders join to it), CLV engagement covers the loaded subset.
    else:
        print("Reading cleaned Parquet files ...")
        sessions, page_views, session_cart, transactions, items, users, categories = from_files(spark)

    overall, by_device, by_ref, ghost = funnel(sessions, page_views, transactions)
    print("\n=== A. Conversion funnel ===")
    overall.show(truncate=False)
    by_device.show(truncate=False)
    by_ref.show(truncate=False)
    print(f"Sessions labelled 'converted' with no matching order: {ghost:,}")
    cat = category_funnel(page_views, session_cart, items, categories)
    cat.show(5, truncate=False)

    users_clv, tiers, corr, window_days = clv(transactions, items, sessions, users)
    print(f"\n=== B. 12-month CLV (observed window {window_days} days) ===")
    tiers.show(truncate=False)
    print("Correlation with clv_12m:", corr)

    save_csv(overall, "funnel_overall")
    save_csv(by_device, "funnel_by_device")
    save_csv(by_ref, "funnel_by_referrer")
    save_csv(cat, "funnel_by_category")
    save_csv(tiers, "clv_tiers")
    save_csv(users_clv.orderBy(F.desc("clv_12m")).drop("last_purchase"), "clv_users")
    save_csv(spark.createDataFrame([(k, v) for k, v in corr.items()] + [("converted_without_order", float(ghost))],
                                   "metric string, value double"), "integration_stats")
    print(f"\nResults in {config.OUTPUT_DIR}/funnel_*.csv and clv_*.csv")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["databases", "files"], default="databases")
    spark = get_spark("integration-funnel-clv")
    run(spark, ap.parse_args().source)
    spark.stop()
