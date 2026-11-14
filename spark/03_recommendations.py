"""
Spark batch job 3 - product recommendation indicators (market-basket style).

  A. "Customers who bought X also bought Y"   (pairs inside one transaction)
  B. "Products frequently viewed together"    (pairs inside one session)

For every pair (X, Y):
  pair_count      baskets containing both
  support         pair_count / total baskets
  confidence      P(Y | X) = pair_count / baskets containing X
  lift            confidence / P(Y)  -> >1 means "more often together than chance"

Pairs are generated with a self-join on the basket id using X < Y, so each
unordered pair is produced once; both directions are then derived for the
confidence/lift of X->Y and Y->X.

Outputs: output/also_bought_pairs.csv, output/viewed_together_pairs.csv,
         output/recommendations_sample.csv
"""
from pyspark.sql import functions as F

from common import get_spark, read_clean, save_csv

spark = get_spark("03-recommendations")
products = read_clean(spark, "products").select("product_id", "product_name", "category_id")


def pair_stats(baskets, basket_col, min_pair_count):
    """baskets: DataFrame(basket_col, product_id) with one row per distinct item."""
    baskets = baskets.dropDuplicates([basket_col, "product_id"]).cache()
    n_baskets = baskets.select(basket_col).distinct().count()
    item_counts = baskets.groupBy("product_id").agg(F.count("*").alias("item_count"))

    a, b = baskets.alias("a"), baskets.alias("b")
    pairs = (a.join(b, (F.col(f"a.{basket_col}") == F.col(f"b.{basket_col}")) &
                    (F.col("a.product_id") < F.col("b.product_id")))
             .groupBy(F.col("a.product_id").alias("x"), F.col("b.product_id").alias("y"))
             .agg(F.count("*").alias("pair_count"))
             .filter(F.col("pair_count") >= min_pair_count))

    # expand to both directions so confidence(X->Y) and (Y->X) are both available
    directed = pairs.unionByName(pairs.select(F.col("y").alias("x"), F.col("x").alias("y"), "pair_count"))
    ic = F.broadcast(item_counts)
    return (directed
            .join(ic.withColumnRenamed("product_id", "x").withColumnRenamed("item_count", "x_count"), "x")
            .join(ic.withColumnRenamed("product_id", "y").withColumnRenamed("item_count", "y_count"), "y")
            .withColumn("support", F.round(F.col("pair_count") / n_baskets, 6))
            .withColumn("confidence", F.round(F.col("pair_count") / F.col("x_count"), 4))
            .withColumn("lift", F.round(F.col("confidence") / (F.col("y_count") / n_baskets), 2))
            .select("x", "y", "pair_count", "x_count", "y_count", "support", "confidence", "lift")), n_baskets


# A. Bought together -------------------------------------------------------
bought = read_clean(spark, "transaction_items").select("transaction_id", "product_id")
also_bought, n_txn = pair_stats(bought, "transaction_id", min_pair_count=2)
also_bought = also_bought.orderBy(F.desc("pair_count"), F.desc("lift")).cache()
print(f"Transactions: {n_txn:,}   directed pairs with >=2 co-purchases: {also_bought.count():,}")
also_bought.show(10)

# B. Viewed together -------------------------------------------------------
viewed = (read_clean(spark, "page_views")
          .filter((F.col("page_type") == "product_detail") & F.col("product_id").isNotNull())
          .select("session_id", "product_id"))
viewed_together, n_sess = pair_stats(viewed, "session_id", min_pair_count=3)
viewed_together = viewed_together.orderBy(F.desc("pair_count"), F.desc("lift")).cache()
print(f"Sessions with product views: {n_sess:,}   directed pairs viewed together >=3 times: "
      f"{viewed_together.count():,}")
viewed_together.show(10)

# Combined recommendation list for the 20 best-selling products -------------
top_sellers = (read_clean(spark, "transaction_items").groupBy("product_id")
               .agg(F.sum("quantity").alias("units")).orderBy(F.desc("units")).limit(20)
               .withColumnRenamed("product_id", "x"))
recs = (also_bought.select("x", "y", F.col("lift").alias("buy_lift"), F.col("pair_count").alias("co_purchases"))
        .join(viewed_together.select("x", "y", F.col("pair_count").alias("co_views")), ["x", "y"], "full")
        .fillna(0, ["co_purchases", "co_views", "buy_lift"])
        # simple blended score: co-purchases weigh 3x more than co-views
        .withColumn("score", 3 * F.col("co_purchases") + F.col("co_views"))
        .join(F.broadcast(top_sellers), "x")
        .join(F.broadcast(products.withColumnRenamed("product_id", "y")
                          .withColumnRenamed("product_name", "recommended_name")), "y")
        .select("x", "y", "recommended_name", "co_purchases", "co_views", "buy_lift", "score"))
from pyspark.sql.window import Window  # noqa: E402
recs = (recs.withColumn("rank", F.row_number().over(Window.partitionBy("x").orderBy(F.desc("score"))))
        .filter("rank <= 5").orderBy("x", "rank"))
recs.show(10, truncate=False)

print("saved", save_csv(also_bought.limit(500), "also_bought_pairs"))
print("saved", save_csv(viewed_together.limit(500), "viewed_together_pairs"))
print("saved", save_csv(recs, "recommendations_sample"))
spark.stop()
