"""
Load session data and daily product metrics into HBase via the Thrift API.

  user_sessions    <- sessions_N.json (first HBASE_SESSION_FILES files)
  product_metrics  <- daily counters computed from the same sessions plus
                      transactions.json

Row-key and column-family design: see hbase/create_schema.hbase and
report/TECHNICAL_REPORT.md section 3.2.

Usage:
    python hbase/load_hbase.py               # loads 1 session file (100k sessions at full scale)
    python hbase/load_hbase.py --files 2
"""
import argparse
import collections
import json
import os
import sys
import time

import happybase

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: E402
from shared import (load_json_array, parse_ts, product_metric_row_key,  # noqa: E402
                    session_files, session_row_key)

BATCH_SIZE = 2000  # mutations buffered client-side before one round trip

SCHEMA = {
    config.HBASE_SESSIONS_TABLE: {"s": dict(max_versions=1, bloom_filter_type="ROW"),
                                  "p": dict(max_versions=1),
                                  "c": dict(max_versions=1)},
    config.HBASE_METRICS_TABLE: {"m": dict(max_versions=1)},
}


def b(v):
    """HBase stores bytes only."""
    return str(v).encode("utf-8")


def ensure_tables(conn, reset=True):
    existing = {t.decode() for t in conn.tables()}
    for name, families in SCHEMA.items():
        if name in existing and reset:
            conn.delete_table(name, disable=True)
            existing.discard(name)
        if name not in existing:
            conn.create_table(name, families)
            print(f"created table {name}")


def session_to_cells(sess):
    """One session -> {column: value}. Page views and cart items become
    individual columns, which is exactly what a wide, sparse table is for."""
    geo, dev = sess["geo_data"], sess["device_profile"]
    cells = {
        b"s:session_id": b(sess["session_id"]),
        b"s:start_time": b(sess["start_time"]),
        b"s:end_time": b(sess["end_time"]),
        b"s:duration": b(sess["duration_seconds"]),
        b"s:status": b(sess["conversion_status"]),
        b"s:referrer": b(sess["referrer"]),
        b"s:device": b(dev["type"]),
        b"s:os": b(dev["os"]),
        b"s:browser": b(dev["browser"]),
        b"s:city": b(geo.get("city")),
        b"s:country": b(geo.get("country")),
        b"s:ip": b(geo.get("ip_address")),
        b"s:n_pages": b(len(sess["page_views"])),
        b"s:n_products": b(len(sess["viewed_products"])),
    }
    # p:000 = "timestamp|page_type|product_id|category_id|view_duration"
    for i, pv in enumerate(sess["page_views"]):
        cells[f"p:{i:03d}".encode()] = b("|".join(
            "" if pv[k] is None else str(pv[k]) for k in ("timestamp", "page_type", "product_id",
                                       "category_id", "view_duration")))
    # c:<product_id> = {"quantity": .., "price": ..}
    for pid, item in sess["cart_contents"].items():
        cells[f"c:{pid}".encode()] = b(json.dumps(item))
    return cells


def load_sessions(conn, files):
    table = conn.table(config.HBASE_SESSIONS_TABLE)
    metrics = collections.defaultdict(lambda: collections.Counter())
    n = 0
    for path in files:
        sessions = load_json_array(path)
        with table.batch(batch_size=BATCH_SIZE) as batch:
            for sess in sessions:
                start = parse_ts(sess["start_time"])
                key = session_row_key(sess["user_id"], start, sess["session_id"])
                batch.put(key.encode(), session_to_cells(sess))
                n += 1
                # accumulate daily product counters while we are here
                for pv in sess["page_views"]:
                    if pv["page_type"] == "product_detail" and pv["product_id"]:
                        day = parse_ts(pv["timestamp"]).date()
                        metrics[(pv["product_id"], day)]["views"] += 1
                for pid in sess["cart_contents"]:
                    metrics[(pid, start.date())]["cart_adds"] += 1
        print(f"  {os.path.basename(path)}: {n:,} sessions loaded")
    return n, metrics


def add_purchase_metrics(metrics):
    for t in load_json_array(os.path.join(config.RAW_DIR, "transactions.json")):
        day = parse_ts(t["timestamp"]).date()
        for it in t["items"]:
            m = metrics[(it["product_id"], day)]
            m["purchases"] += 1
            m["units_sold"] += it["quantity"]
            m["revenue_cents"] += round(it["subtotal"] * 100)


def load_metrics(conn, metrics):
    table = conn.table(config.HBASE_METRICS_TABLE)
    with table.batch(batch_size=BATCH_SIZE) as batch:
        for (pid, day), c in metrics.items():
            batch.put(product_metric_row_key(pid, day).encode(), {
                b"m:views": b(c["views"]),
                b"m:cart_adds": b(c["cart_adds"]),
                b"m:purchases": b(c["purchases"]),
                b"m:units_sold": b(c["units_sold"]),
                b"m:revenue": b(f"{c['revenue_cents'] / 100:.2f}"),
            })
    return len(metrics)


def main(conn=None, n_files=None):
    t0 = time.time()
    conn = conn or happybase.Connection(config.HBASE_HOST, port=config.HBASE_PORT, timeout=60000)
    ensure_tables(conn)
    files = session_files(config.RAW_DIR)[: n_files or config.HBASE_SESSION_FILES]
    print(f"Loading {len(files)} session file(s) into {config.HBASE_SESSIONS_TABLE}")
    n, metrics = load_sessions(conn, files)
    add_purchase_metrics(metrics)
    m = load_metrics(conn, metrics)
    print(f"Loaded {n:,} sessions and {m:,} product-day metric rows in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--files", type=int, default=None, help="Number of session files to load")
    main(n_files=ap.parse_args().files)
