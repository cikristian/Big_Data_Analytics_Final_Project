"""
Shared Spark helpers: session builder, explicit raw-JSON schemas, I/O.

Explicit schemas (instead of schema inference) matter at this size:
inference makes Spark read every multi-hundred-MB file an extra time, and it
would type cart_contents as a struct with one field per product ID (5,000
columns). Declaring it as a MapType keeps it compact.
"""
import csv
import glob
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import types as T

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import config  # noqa: E402

IS_WINDOWS = os.name == "nt"


def _prepare_environment():
    """
    Environment fixes that must happen BEFORE the JVM starts.

    * PYSPARK_PYTHON: make Spark's Python workers use the same interpreter
      (venv) as the driver. On Windows the default "python3" is often the
      Microsoft Store stub, which makes every task fail with
      "Python worker failed to connect back".
    * PYTHONPATH: lets worker processes import config.py / shared.py.
    * Windows only: Spark writes local files through Hadoop's Windows native
      code, which needs winutils.exe + hadoop.dll. setup_windows.ps1 puts them
      in tools\\hadoop\\bin; we point HADOOP_HOME there and add it to PATH so
      the JVM can load hadoop.dll.
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    extra = [ROOT, os.path.join(ROOT, "spark"), os.path.join(ROOT, "integration")]
    current = os.environ.get("PYTHONPATH", "")
    os.environ["PYTHONPATH"] = os.pathsep.join(extra + ([current] if current else []))

    if IS_WINDOWS:
        bundled = os.path.join(ROOT, "tools", "hadoop")
        if "HADOOP_HOME" not in os.environ and os.path.exists(os.path.join(bundled, "bin", "winutils.exe")):
            os.environ["HADOOP_HOME"] = bundled
        home = os.environ.get("HADOOP_HOME")
        if not home or not os.path.exists(os.path.join(home, "bin", "winutils.exe")):
            print("WARNING: winutils.exe not found. Writing Parquet will fail on Windows.\n"
                  "         Run setup_windows.ps1 (or set HADOOP_HOME) - see README 'Running on Windows'.")
        else:
            os.environ["PATH"] = os.path.join(home, "bin") + os.pathsep + os.environ.get("PATH", "")


def spark_path(path):
    """Hadoop understands 'C:/dir/file' but not every API copes with
    backslashes, so normalise separators."""
    return path.replace("\\", "/")


def get_spark(app_name):
    _prepare_environment()
    builder = (SparkSession.builder
               .appName(app_name)
               .master(config.SPARK_MASTER)
               .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY))
    if IS_WINDOWS:
        # Avoid binding to a VPN / Hyper-V adapter address, a common cause of
        # "connection refused" between the driver and the Python workers.
        builder = (builder.config("spark.driver.host", "localhost")
                          .config("spark.driver.bindAddress", "127.0.0.1"))
    spark = (builder
             # Small dataset on one machine: 200 default shuffle partitions would
             # create hundreds of tiny tasks. AQE coalesces them further.
             # Small dataset on one machine: 200 default shuffle partitions would
             # create hundreds of tiny tasks. AQE coalesces them further.
             .config("spark.sql.shuffle.partitions", config.SPARK_SHUFFLE_PARTITIONS)
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Raw schemas (mirror dataset_generator.py output)
# ---------------------------------------------------------------------------
GEO = T.StructType([
    T.StructField("city", T.StringType()),
    T.StructField("state", T.StringType()),
    T.StructField("country", T.StringType()),
])

USERS_SCHEMA = T.StructType([
    T.StructField("user_id", T.StringType()),
    T.StructField("geo_data", GEO),
    T.StructField("registration_date", T.StringType()),
    T.StructField("last_active", T.StringType()),
])

CATEGORIES_SCHEMA = T.StructType([
    T.StructField("category_id", T.StringType()),
    T.StructField("name", T.StringType()),
    T.StructField("subcategories", T.ArrayType(T.StructType([
        T.StructField("subcategory_id", T.StringType()),
        T.StructField("name", T.StringType()),
        T.StructField("profit_margin", T.DoubleType()),
    ]))),
])

PRODUCTS_SCHEMA = T.StructType([
    T.StructField("product_id", T.StringType()),
    T.StructField("name", T.StringType()),
    T.StructField("category_id", T.StringType()),
    T.StructField("base_price", T.DoubleType()),
    T.StructField("current_stock", T.IntegerType()),
    T.StructField("is_active", T.BooleanType()),
    T.StructField("price_history", T.ArrayType(T.StructType([
        T.StructField("price", T.DoubleType()),
        T.StructField("date", T.StringType()),
    ]))),
    T.StructField("creation_date", T.StringType()),
])

SESSIONS_SCHEMA = T.StructType([
    T.StructField("session_id", T.StringType()),
    T.StructField("user_id", T.StringType()),
    T.StructField("start_time", T.StringType()),
    T.StructField("end_time", T.StringType()),
    T.StructField("duration_seconds", T.IntegerType()),
    T.StructField("geo_data", T.StructType(GEO.fields + [T.StructField("ip_address", T.StringType())])),
    T.StructField("device_profile", T.StructType([
        T.StructField("type", T.StringType()),
        T.StructField("os", T.StringType()),
        T.StructField("browser", T.StringType()),
    ])),
    T.StructField("viewed_products", T.ArrayType(T.StringType())),
    T.StructField("page_views", T.ArrayType(T.StructType([
        T.StructField("timestamp", T.StringType()),
        T.StructField("page_type", T.StringType()),
        T.StructField("product_id", T.StringType()),
        T.StructField("category_id", T.StringType()),
        T.StructField("view_duration", T.IntegerType()),
    ]))),
    # keys are product IDs -> a map, not a struct
    T.StructField("cart_contents", T.MapType(T.StringType(), T.StructType([
        T.StructField("quantity", T.IntegerType()),
        T.StructField("price", T.DoubleType()),
    ]))),
    T.StructField("conversion_status", T.StringType()),
    T.StructField("referrer", T.StringType()),
])

TRANSACTIONS_SCHEMA = T.StructType([
    T.StructField("transaction_id", T.StringType()),
    T.StructField("session_id", T.StringType()),
    T.StructField("user_id", T.StringType()),
    T.StructField("timestamp", T.StringType()),
    T.StructField("items", T.ArrayType(T.StructType([
        T.StructField("product_id", T.StringType()),
        T.StructField("quantity", T.IntegerType()),
        T.StructField("unit_price", T.DoubleType()),
        T.StructField("subtotal", T.DoubleType()),
    ]))),
    T.StructField("subtotal", T.DoubleType()),
    T.StructField("discount", T.DoubleType()),
    T.StructField("total", T.DoubleType()),
    T.StructField("payment_method", T.StringType()),
    T.StructField("status", T.StringType()),
])


def read_raw(spark, pattern, schema):
    """Generator files are single JSON arrays -> multiLine mode is required.
    Globs are expanded here (Spark warns noisily when given a wildcard path)."""
    paths = sorted(glob.glob(os.path.join(config.RAW_DIR, pattern)))
    if not paths:
        raise FileNotFoundError(f"No files match {pattern} in {config.RAW_DIR} - run the generator first")
    return (spark.read.schema(schema)
            .option("multiLine", True)
            .option("mode", "PERMISSIVE")
            .json([spark_path(p) for p in paths]))


def read_clean(spark, name):
    return spark.read.parquet(spark_path(os.path.join(config.CLEAN_DIR, name)))


def write_clean(df, name, partition_by=None):
    w = df.write.mode("overwrite")
    if partition_by:
        w = w.partitionBy(partition_by)
    w.parquet(spark_path(os.path.join(config.CLEAN_DIR, name)))


def save_csv(df, name):
    """Analysis results are small: collect to the driver and write one tidy CSV
    (easier to chart and to read in the report than Spark's part files)."""
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    path = os.path.join(config.OUTPUT_DIR, f"{name}.csv")
    rows = df.collect()
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(df.columns)
        w.writerows([["" if v is None else v for v in r] for r in rows])
    return path
