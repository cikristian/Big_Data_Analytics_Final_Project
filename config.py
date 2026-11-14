"""
Central configuration shared by every script in the project.

All values can be overridden with environment variables, so the same code
runs on a laptop, in Docker, or on a cluster without edits.
"""
import os

ROOT = os.path.dirname(os.path.abspath(__file__))

# --- Data locations -------------------------------------------------------
RAW_DIR = os.getenv("RAW_DIR", os.path.join(ROOT, "data", "raw"))        # generator output (JSON arrays)
CLEAN_DIR = os.getenv("CLEAN_DIR", os.path.join(ROOT, "data", "clean"))  # Spark-cleaned Parquet
OUTPUT_DIR = os.getenv("OUTPUT_DIR", os.path.join(ROOT, "output"))      # analysis results (CSV/JSON)
CHART_DIR = os.getenv("CHART_DIR", os.path.join(ROOT, "output", "charts"))

# --- MongoDB --------------------------------------------------------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "ecommerce")

# --- HBase (Thrift server, used by happybase) ------------------------------
HBASE_HOST = os.getenv("HBASE_HOST", "localhost")
HBASE_PORT = int(os.getenv("HBASE_PORT", "9090"))
HBASE_SESSIONS_TABLE = "user_sessions"
HBASE_METRICS_TABLE = "product_metrics"

# How many session files to load into HBase (each file = 100k sessions at
# full scale). "Significant representative subset" per the brief.
HBASE_SESSION_FILES = int(os.getenv("HBASE_SESSION_FILES", "1"))

# --- Spark ----------------------------------------------------------------
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "4g")
SPARK_SHUFFLE_PARTITIONS = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
