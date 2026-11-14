"""
Spark batch job 2 - cohort analysis of purchasing patterns.

Cohort      = month the user registered (users.registration_month)
Period      = calendar month of the purchase (transactions.txn_month)
Age         = months between registration and purchase

For every (cohort, period) cell we compute
  cohort_size            users who registered that month
  active_buyers          distinct users of that cohort who bought in the period
  activity_rate          active_buyers / cohort_size
  revenue                sum of order totals
  revenue_per_user       revenue / cohort_size   (fair comparison across cohort sizes)
  avg_order_value

Outputs: output/cohort_matrix.csv (long format, one row per cell)
         output/cohort_summary.csv (one row per cohort)
"""
from pyspark.sql import functions as F

from common import get_spark, read_clean, save_csv

spark = get_spark("02-cohort-analysis")

users = read_clean(spark, "users").select("user_id", "registration_date", "registration_month")
txns = (read_clean(spark, "transactions")
        .filter(F.col("status").isin("completed", "shipped", "delivered", "processing"))
        .select("transaction_id", "user_id", "ts", "txn_month", "total"))

cohort_size = users.groupBy("registration_month").agg(F.count("*").alias("cohort_size"))

# users table is small (10k rows) -> broadcast join avoids shuffling transactions
cohort_txn = (txns.join(F.broadcast(users), "user_id")
              .withColumn("months_since_registration",
                          F.floor(F.months_between("ts", "registration_date")).cast("int")))

matrix = (cohort_txn
          .groupBy("registration_month", "txn_month")
          .agg(F.countDistinct("user_id").alias("active_buyers"),
               F.count("*").alias("orders"),
               F.round(F.sum("total"), 2).alias("revenue"),
               F.round(F.avg("total"), 2).alias("avg_order_value"),
               F.round(F.avg("months_since_registration"), 1).alias("avg_months_since_registration"))
          .join(cohort_size, "registration_month")
          .withColumn("activity_rate", F.round(F.col("active_buyers") / F.col("cohort_size"), 4))
          .withColumn("revenue_per_user", F.round(F.col("revenue") / F.col("cohort_size"), 2))
          .orderBy("registration_month", "txn_month"))

summary = (cohort_txn.groupBy("registration_month")
           .agg(F.countDistinct("user_id").alias("buyers"),
                F.count("*").alias("orders"),
                F.round(F.sum("total"), 2).alias("revenue"))
           .join(cohort_size, "registration_month")
           .withColumn("buyer_rate", F.round(F.col("buyers") / F.col("cohort_size"), 4))
           .withColumn("orders_per_user", F.round(F.col("orders") / F.col("cohort_size"), 2))
           .withColumn("revenue_per_user", F.round(F.col("revenue") / F.col("cohort_size"), 2))
           .orderBy("registration_month"))

print("Cohort x month (revenue per cohort member):")
(matrix.groupBy("registration_month").pivot("txn_month").agg(F.first("revenue_per_user"))
 .orderBy("registration_month").show(truncate=False))
summary.show(truncate=False)

print("saved", save_csv(matrix, "cohort_matrix"))
print("saved", save_csv(summary, "cohort_summary"))
spark.stop()
