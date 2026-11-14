"""
Small helpers shared by the MongoDB loader, HBase loader and Spark jobs.

Keeping them in one place guarantees that every system derives the same
values (e.g. the same subcategory for a product, the same HBase row key).
"""
import datetime
import json
import os
import zlib

JAVA_LONG_MAX = 9223372036854775807  # Long.MAX_VALUE, used for reverse timestamps


def load_json_array(path):
    """The generator writes each file as one JSON array."""
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def session_files(raw_dir):
    """sessions_0.json, sessions_1.json, ... in numeric order."""
    files = [f for f in os.listdir(raw_dir) if f.startswith("sessions_") and f.endswith(".json")]
    return [os.path.join(raw_dir, f) for f in sorted(files, key=lambda f: int(f[9:-5]))]


def parse_ts(value):
    """ISO-8601 string -> naive datetime (None stays None)."""
    return datetime.datetime.fromisoformat(value) if value else None


def assign_subcategory_index(product_id, n_subcategories):
    """
    The generator gives every product a category_id but NO subcategory_id,
    although the brief's sample data shows one. We derive it deterministically
    with CRC32 so MongoDB (zlib.crc32) and Spark (F.crc32) agree exactly.
    """
    return zlib.crc32(product_id.encode("utf-8")) % n_subcategories


def reverse_ts(dt):
    """
    Reverse timestamp for HBase row keys: newer events get SMALLER values,
    so a prefix scan on user_id returns the most recent sessions first.
    Zero-padded to 19 digits so lexicographic order == numeric order.
    """
    # Generator timestamps are naive; treat them as UTC so keys don't depend
    # on the time zone of the machine running the loader.
    millis = int(dt.replace(tzinfo=datetime.timezone.utc).timestamp() * 1000)
    return f"{JAVA_LONG_MAX - millis:019d}"


def session_row_key(user_id, start_time, session_id):
    """user_sessions row key: <user_id>#<reverse_ts>#<session_id>"""
    return f"{user_id}#{reverse_ts(start_time)}#{session_id}"


def product_metric_row_key(product_id, day):
    """product_metrics row key: <product_id>#<yyyyMMdd>"""
    return f"{product_id}#{day.strftime('%Y%m%d')}"
