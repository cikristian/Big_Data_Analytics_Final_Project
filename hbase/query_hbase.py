"""
Example HBase queries (Part 1) - all are row-key RANGE SCANS, never full
table scans, which is the point of the row-key design.

  recent_sessions(user, n)                 newest n sessions of a user
  sessions_between(user, start, end)       a user's sessions in a time window
  session_journey(row_key)                 full page-view path of one session
  product_metrics(product, start, end)     daily time series for one product

Usage:
    python hbase/query_hbase.py --user user_000042
    python hbase/query_hbase.py --user user_000042 --start 2026-09-01 --end 2026-09-30
    python hbase/query_hbase.py --product prod_00123 --start 2026-10-01 --end 2026-10-31
"""
import argparse
import datetime
import json
import os
import sys

import happybase

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: E402
from shared import product_metric_row_key, reverse_ts  # noqa: E402


def _decode(data):
    return {k.decode(): v.decode() for k, v in data.items()}


def recent_sessions(table, user_id, n=10):
    """Prefix scan. Because the timestamp is reversed, the first rows ARE the
    newest sessions, so `limit` stops the scan early. Only family `s` is
    fetched - page views are not transferred."""
    rows = table.scan(row_prefix=f"{user_id}#".encode(), columns=[b"s"], limit=n)
    return [(k.decode(), _decode(v)) for k, v in rows]


def sessions_between(table, user_id, start, end):
    """Time-range scan for one user. Newer = smaller key, so the scan starts
    at `end` and stops after `start`. '$' sorts right after '#', which makes
    the stop row include sessions that start exactly at `start`."""
    row_start = f"{user_id}#{reverse_ts(end)}".encode()
    row_stop = f"{user_id}#{reverse_ts(start)}$".encode()
    rows = table.scan(row_start=row_start, row_stop=row_stop, columns=[b"s"])
    return [(k.decode(), _decode(v)) for k, v in rows]


def session_journey(table, row_key):
    """Single-row GET of the page-view family, decoded into an ordered path."""
    row = _decode(table.row(row_key.encode(), columns=[b"s", b"p", b"c"]))
    pages = []
    for col in sorted(c for c in row if c.startswith("p:")):
        ts, ptype, pid, cid, dur = row[col].split("|")
        pages.append({"timestamp": ts, "page_type": ptype, "product_id": pid or None,
                      "category_id": cid or None, "view_duration": int(dur or 0)})
    cart = {c[2:]: json.loads(v) for c, v in row.items() if c.startswith("c:")}
    return {"summary": {k[2:]: v for k, v in row.items() if k.startswith("s:")},
            "page_views": pages, "cart": cart}


def product_metrics(table, product_id, start_day, end_day):
    """Daily counters for one product between two dates (inclusive), plus
    the view-to-purchase conversion for the period."""
    rows = table.scan(
        row_start=product_metric_row_key(product_id, start_day).encode(),
        row_stop=product_metric_row_key(product_id, end_day + datetime.timedelta(days=1)).encode(),
    )
    series = []
    for k, v in rows:
        d = _decode(v)
        series.append({"day": k.decode().split("#")[1],
                       **{c[2:]: float(x) for c, x in d.items()}})
    views = sum(r["views"] for r in series)
    purchases = sum(r["purchases"] for r in series)
    return {"days": series, "total_views": views, "total_purchases": purchases,
            "view_to_purchase_rate": round(purchases / views, 4) if views else None}


def _date(s):
    return datetime.datetime.strptime(s, "%Y-%m-%d")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--user", default="user_000042")
    ap.add_argument("--product", default=None)
    ap.add_argument("--start", type=_date, default=None)
    ap.add_argument("--end", type=_date, default=None)
    ap.add_argument("-n", type=int, default=5)
    a = ap.parse_args()

    conn = happybase.Connection(config.HBASE_HOST, port=config.HBASE_PORT)
    sessions = conn.table(config.HBASE_SESSIONS_TABLE)

    print(f"\n--- {a.n} most recent sessions for {a.user} ---")
    recent = recent_sessions(sessions, a.user, a.n)
    for key, s in recent:
        print(f"  {s['s:start_time']}  {s['s:session_id']}  {s['s:device']:<7} "
              f"{s['s:referrer']:<13} pages={s['s:n_pages']:<3} {s['s:status']}")

    if a.start and a.end:
        rng = sessions_between(sessions, a.user, a.start, a.end + datetime.timedelta(days=1))
        print(f"\n--- {a.user}: {len(rng)} sessions between {a.start.date()} and {a.end.date()} ---")
        for key, s in rng:
            print(f"  {s['s:start_time']}  {s['s:status']}")

    if recent:
        j = session_journey(sessions, recent[0][0])
        print(f"\n--- journey of newest session {j['summary']['session_id']} ---")
        for p in j["page_views"]:
            print(f"  {p['timestamp'][11:19]}  {p['page_type']:<17} {p['product_id'] or '':<11} {p['view_duration']}s")
        print(f"  cart: {j['cart']}")

    if a.product:
        end = a.end or datetime.datetime.now()
        start = a.start or end - datetime.timedelta(days=30)
        m = product_metrics(conn.table(config.HBASE_METRICS_TABLE), a.product, start, end)
        print(f"\n--- {a.product} daily metrics {start.date()}..{end.date()} ---")
        for r in m["days"]:
            print(f"  {r['day']}  views={r['views']:.0f} cart_adds={r['cart_adds']:.0f} "
                  f"purchases={r['purchases']:.0f} revenue=${r['revenue']:,.2f}")
        print(f"  view->purchase rate: {m['view_to_purchase_rate']}")


if __name__ == "__main__":
    main()
