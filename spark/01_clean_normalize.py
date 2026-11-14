"""
Spark batch job 1 - clean and normalize the raw JSON into analysis-ready
Parquet tables (data/clean/).

Raw (nested JSON arrays)            Clean (flat, typed Parquet)
--------------------------------    -------------------------------------------
users.json                     ->   users
categories.json                ->   categories, subcategories
products.json                  ->   products   (+ derived subcategory_id)
sessions_*.json                ->   sessions, page_views (partitioned by date),
                                    session_cart
transactions.json              ->   transactions, transaction_items

Cleaning rules
  * ISO strings -> real timestamps (UTC session time zone)
  * trim / upper-case codes, standardise lower-case enums
  * drop rows missing a primary key, drop duplicate IDs
  * clamp impossible numbers (negative quantities/prices/durations -> null)
  * recompute money fields from line items and flag mismatches
  * flag geo inconsistency (generator pairs US state codes with random countries)
  * derive subcategory_id (missing from the generator, see shared.py)
A data-quality report is written to output/data_quality_report.json.
"""
import json
import os

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import (CATEGORIES_SCHEMA, PRODUCTS_SCHEMA, SESSIONS_SCHEMA,
                    TRANSACTIONS_SCHEMA, USERS_SCHEMA, config, get_spark,
                    read_raw, write_clean)

spark = get_spark("01-clean-normalize")
dq = {}  # data-quality metrics


def dedupe(df, key, order_col=None):
    """Keep one row per key (the latest by order_col if given)."""
    if order_col is None:
        return df.dropDuplicates([key])
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def non_negative(col):
    return F.when(col >= 0, col)


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------
raw_users = read_raw(spark, "users.json", USERS_SCHEMA)
users = (raw_users
         .filter(F.col("user_id").isNotNull())
         .select(
             F.trim("user_id").alias("user_id"),
             F.initcap(F.trim("geo_data.city")).alias("city"),
             F.upper(F.trim("geo_data.state")).alias("state"),
             F.upper(F.trim("geo_data.country")).alias("country"),
             F.to_timestamp("registration_date").alias("registration_date"),
             F.to_timestamp("last_active").alias("last_active"))
         .withColumn("registration_month", F.date_format("registration_date", "yyyy-MM"))
         # US state abbreviations are only meaningful when country == US
         .withColumn("geo_consistent", F.col("country") == "US"))
users = dedupe(users, "user_id", "last_active")
dq["users"] = {"raw": raw_users.count(), "clean": users.count(),
               "geo_inconsistent": users.filter(~F.col("geo_consistent")).count()}
write_clean(users, "users")

# ---------------------------------------------------------------------------
# Categories + subcategories
# ---------------------------------------------------------------------------
raw_cats = read_raw(spark, "categories.json", CATEGORIES_SCHEMA)
categories = raw_cats.select("category_id", F.col("name").alias("category_name"),
                             F.size("subcategories").alias("n_subcategories"))
subcategories = (raw_cats
                 .select("category_id", F.posexplode("subcategories").alias("sub_idx", "s"))
                 .select("category_id", "sub_idx", "s.subcategory_id",
                         F.col("s.name").alias("subcategory_name"), "s.profit_margin"))
write_clean(categories, "categories")
write_clean(subcategories, "subcategories")

# ---------------------------------------------------------------------------
# Products (+ deterministic subcategory, identical to MongoDB loader)
# ---------------------------------------------------------------------------
raw_products = read_raw(spark, "products.json", PRODUCTS_SCHEMA)
products = (raw_products
            .filter(F.col("product_id").isNotNull())
            .join(F.broadcast(categories), "category_id", "left")
            .withColumn("sub_idx", (F.crc32("product_id") % F.col("n_subcategories")).cast("int"))
            .join(F.broadcast(subcategories.select("category_id", "sub_idx", "subcategory_id", "profit_margin")),
                  ["category_id", "sub_idx"], "left")
            .select(
                "product_id", F.trim("name").alias("product_name"), "category_id", "category_name",
                "subcategory_id", "profit_margin",
                non_negative(F.col("base_price")).alias("base_price"),
                non_negative(F.col("current_stock")).alias("current_stock"),
                "is_active",
                F.to_timestamp("creation_date").alias("creation_date"),
                (F.size("price_history") - 1).alias("n_price_changes"),
                F.array_min("price_history.price").alias("min_price"),
                F.array_max("price_history.price").alias("max_price")))
products = dedupe(products, "product_id")
dq["products"] = {"raw": raw_products.count(), "clean": products.count(),
                  "inactive": products.filter(~F.col("is_active")).count(),
                  "out_of_stock": products.filter(F.col("current_stock") == 0).count()}
write_clean(products, "products")

# ---------------------------------------------------------------------------
# Sessions -> sessions / page_views / session_cart
# ---------------------------------------------------------------------------
raw_sessions = read_raw(spark, "sessions_*.json", SESSIONS_SCHEMA).repartition(16)
raw_sessions = raw_sessions.filter(F.col("session_id").isNotNull() & F.col("user_id").isNotNull())
raw_sessions = raw_sessions.dropDuplicates(["session_id"]).cache()

sessions = (raw_sessions.select(
    "session_id", "user_id",
    F.to_timestamp("start_time").alias("start_time"),
    F.to_timestamp("end_time").alias("end_time"),
    non_negative(F.col("duration_seconds")).alias("duration_seconds"),
    F.lower("device_profile.type").alias("device_type"),
    F.col("device_profile.os").alias("os"),
    F.col("device_profile.browser").alias("browser"),
    F.lower("referrer").alias("referrer"),
    F.lower("conversion_status").alias("conversion_status"),
    F.upper("geo_data.country").alias("country"),
    F.size("page_views").alias("n_pages"),
    F.size("viewed_products").alias("n_products_viewed"),
    F.size("cart_contents").alias("n_cart_items"),
    F.aggregate(F.map_values("cart_contents"), F.lit(0.0),
                lambda acc, v: acc + v["quantity"] * v["price"]).alias("cart_value"),
    F.exists("page_views", lambda p: p["page_type"].isin("checkout", "confirmation")).alias("reached_checkout"))
    .withColumn("session_date", F.to_date("start_time")))
write_clean(sessions, "sessions")

page_views = (raw_sessions
              .select("session_id", "user_id", F.posexplode("page_views").alias("seq", "pv"))
              .select("session_id", "user_id", "seq",
                      F.to_timestamp("pv.timestamp").alias("ts"),
                      F.lower("pv.page_type").alias("page_type"),
                      "pv.product_id", "pv.category_id",
                      non_negative(F.col("pv.view_duration")).alias("view_duration"))
              .withColumn("event_date", F.to_date("ts")))
# Partitioning by day lets date-range queries skip whole directories
# (partition pruning) - the same idea as HBase's time-ordered row keys.
write_clean(page_views, "page_views", partition_by="event_date")

session_cart = (raw_sessions
                .select("session_id", "user_id", F.explode("cart_contents").alias("product_id", "item"))
                .select("session_id", "user_id", "product_id", "item.quantity", "item.price"))
write_clean(session_cart, "session_cart")

dq["sessions"] = {"clean": sessions.count(),
                  "page_views": page_views.count(),
                  "by_status": {r["conversion_status"]: r["count"]
                                for r in sessions.groupBy("conversion_status").count().collect()}}

# ---------------------------------------------------------------------------
# Transactions -> transactions / transaction_items
# ---------------------------------------------------------------------------
raw_txns = read_raw(spark, "transactions.json", TRANSACTIONS_SCHEMA)
txns = (raw_txns
        .filter(F.col("transaction_id").isNotNull() & F.col("user_id").isNotNull())
        .dropDuplicates(["transaction_id"])
        .withColumn("ts", F.to_timestamp("timestamp"))
        .withColumn("items_subtotal", F.round(F.aggregate("items", F.lit(0.0),
                                                          lambda a, i: a + i["subtotal"]), 2))
        .withColumn("discount", F.coalesce("discount", F.lit(0.0))))

transactions = txns.select(
    "transaction_id",
    F.when(F.col("session_id") != "", F.col("session_id")).alias("session_id"),
    "user_id", "ts",
    F.to_date("ts").alias("txn_date"),
    F.date_format("ts", "yyyy-MM").alias("txn_month"),
    F.size("items").alias("n_lines"),
    F.aggregate("items", F.lit(0), lambda a, i: a + i["quantity"]).alias("n_units"),
    "subtotal", "discount", "total",
    F.lower("payment_method").alias("payment_method"),
    F.lower("status").alias("status"),
    # integrity checks: header totals must agree with the line items
    (F.abs(F.col("subtotal") - F.col("items_subtotal")) > 0.01).alias("subtotal_mismatch"),
    (F.abs(F.col("total") - (F.col("subtotal") - F.col("discount"))) > 0.01).alias("total_mismatch"),
    F.col("session_id").isNotNull().alias("is_session_linked"))
write_clean(transactions, "transactions")

transaction_items = (txns
                     .select("transaction_id", "user_id", "ts", F.explode("items").alias("it"))
                     .select("transaction_id", "user_id", "ts", F.to_date("ts").alias("txn_date"),
                             "it.product_id",
                             non_negative(F.col("it.quantity")).alias("quantity"),
                             non_negative(F.col("it.unit_price")).alias("unit_price"),
                             F.col("it.subtotal").alias("line_total"))
                     .join(F.broadcast(products.select("product_id", "category_id", "subcategory_id",
                                                       "profit_margin")), "product_id", "left"))
write_clean(transaction_items, "transaction_items")

# Cross-entity checks
max_session_ts = sessions.agg(F.max("start_time")).first()[0]
converted_without_txn = (sessions.filter("conversion_status = 'converted'")
                         .join(transactions.select("session_id"), "session_id", "left_anti").count())
dq["transactions"] = {
    "raw": raw_txns.count(),
    "clean": transactions.count(),
    "session_linked": transactions.filter("is_session_linked").count(),
    "subtotal_mismatch": transactions.filter("subtotal_mismatch").count(),
    "total_mismatch": transactions.filter("total_mismatch").count(),
    "items": transaction_items.count(),
    "items_unknown_product": transaction_items.filter(F.col("category_id").isNull()).count(),
    "timestamp_after_last_session_start": transactions.filter(F.col("ts") > F.lit(max_session_ts)).count(),
}
dq["cross_checks"] = {"converted_sessions_without_transaction": converted_without_txn}

os.makedirs(config.OUTPUT_DIR, exist_ok=True)
with open(os.path.join(config.OUTPUT_DIR, "data_quality_report.json"), "w", encoding="utf-8") as f:
    json.dump(dq, f, indent=2)
print(json.dumps(dq, indent=2))
print(f"Clean Parquet tables written to {config.CLEAN_DIR}")
spark.stop()
