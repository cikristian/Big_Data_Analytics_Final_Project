"""
MongoDB aggregation pipelines (Part 1 analytics).

  1. revenue_by_category_month  - revenue, units and ESTIMATED PROFIT per
                                  category per month (uses $unwind, $lookup,
                                  $filter on an embedded array)
  2. customer_segments          - RFM-style segmentation built from the
                                  embedded users.purchase_summary ($switch)
  3. product_popularity         - top products by units and by revenue in ONE
                                  pass using $facet

Each pipeline is a plain function returning the pipeline list, so it can be
printed, explained (db.command("explain")) or reused from the integration
layer. Results are written to output/mongo_*.json.

Usage:
    python mongodb/aggregations.py
"""
import datetime
import json
import os
import sys

from pymongo import MongoClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: E402


# ---------------------------------------------------------------------------
# 1. Revenue + estimated profit by category and month
# ---------------------------------------------------------------------------
def revenue_by_category_month_pipeline(start=None, end=None):
    match = {"status": {"$in": ["completed", "shipped", "delivered", "processing"]}}
    if start or end:
        match["timestamp"] = {k: v for k, v in (("$gte", start), ("$lt", end)) if v}
    return [
        {"$match": match},                       # uses the timestamp index when a range is given
        {"$unwind": "$items"},                   # one doc per line item
        {"$group": {                             # first roll up to subcategory+month
            "_id": {
                "category_id": "$items.category_id",
                "subcategory_id": "$items.subcategory_id",
                "month": {"$dateToString": {"format": "%Y-%m", "date": "$timestamp"}},
            },
            "revenue": {"$sum": "$items.subtotal"},
            "units": {"$sum": "$items.quantity"},
        }},
        {"$lookup": {                            # join the small categories collection
            "from": "categories",
            "localField": "_id.category_id",
            "foreignField": "_id",
            "as": "cat",
        }},
        {"$unwind": "$cat"},
        {"$addFields": {                         # pick this subcategory's margin out of the embedded array
            "profit_margin": {"$arrayElemAt": [
                {"$map": {
                    "input": {"$filter": {
                        "input": "$cat.subcategories",
                        "as": "s",
                        "cond": {"$eq": ["$$s.subcategory_id", "$_id.subcategory_id"]},
                    }},
                    "as": "s",
                    "in": "$$s.profit_margin",
                }}, 0]},
        }},
        {"$group": {                             # then roll up to category+month
            "_id": {"category_id": "$_id.category_id", "month": "$_id.month"},
            "category_name": {"$first": "$cat.name"},
            "revenue": {"$sum": "$revenue"},
            "units": {"$sum": "$units"},
            "est_profit": {"$sum": {"$multiply": ["$revenue", "$profit_margin"]}},
        }},
        {"$project": {
            "_id": 0,
            "category_id": "$_id.category_id",
            "category_name": 1,
            "month": "$_id.month",
            "revenue": {"$round": ["$revenue", 2]},
            "units": 1,
            "est_profit": {"$round": ["$est_profit", 2]},
        }},
        {"$sort": {"month": 1, "revenue": -1}},
    ]


# ---------------------------------------------------------------------------
# 2. Customer segmentation (Recency / Frequency / Monetary)
# ---------------------------------------------------------------------------
def customer_segments_pipeline(reference_date):
    day_ms = 1000 * 60 * 60 * 24
    return [
        {"$project": {
            "orders": {"$ifNull": ["$purchase_summary.total_orders", 0]},
            "spent": {"$ifNull": ["$purchase_summary.total_spent", 0]},
            "recency_days": {"$cond": [
                {"$ifNull": ["$purchase_summary.last_purchase", False]},
                {"$divide": [{"$subtract": [reference_date, "$purchase_summary.last_purchase"]}, day_ms]},
                None,
            ]},
        }},
        {"$addFields": {"segment": {"$switch": {
            "branches": [
                {"case": {"$eq": ["$orders", 0]}, "then": "No purchase"},
                {"case": {"$and": [{"$gte": ["$orders", 8]}, {"$lte": ["$recency_days", 14]}]},
                 "then": "Champions"},
                {"case": {"$gte": ["$orders", 6]}, "then": "Loyal"},
                {"case": {"$gt": ["$recency_days", 30]}, "then": "At risk"},
                {"case": {"$lte": ["$orders", 2]}, "then": "Occasional"},
            ],
            "default": "Regular",
        }}}},
        {"$group": {
            "_id": "$segment",
            "users": {"$sum": 1},
            "avg_orders": {"$avg": "$orders"},
            "avg_spent": {"$avg": "$spent"},
            "total_revenue": {"$sum": "$spent"},
            "avg_recency_days": {"$avg": "$recency_days"},
        }},
        {"$project": {
            "_id": 0,
            "segment": "$_id",
            "users": 1,
            "avg_orders": {"$round": ["$avg_orders", 2]},
            "avg_spent": {"$round": ["$avg_spent", 2]},
            "total_revenue": {"$round": ["$total_revenue", 2]},
            "avg_recency_days": {"$round": ["$avg_recency_days", 1]},
        }},
        {"$sort": {"total_revenue": -1}},
    ]


# ---------------------------------------------------------------------------
# 3. Product popularity: top by units AND by revenue in a single pass
# ---------------------------------------------------------------------------
def product_popularity_pipeline(top_n=10):
    def top_branch(sort_field):
        return [
            {"$sort": {sort_field: -1}},
            {"$limit": top_n},
            {"$lookup": {"from": "products", "localField": "_id",
                         "foreignField": "_id", "as": "p"}},
            {"$unwind": "$p"},
            {"$project": {"_id": 0, "product_id": "$_id", "name": "$p.name",
                          "category": "$p.category.name", "units": 1,
                          "revenue": {"$round": ["$revenue", 2]}, "orders": 1}},
        ]

    return [
        {"$unwind": "$items"},
        {"$group": {
            "_id": "$items.product_id",
            "units": {"$sum": "$items.quantity"},
            "revenue": {"$sum": "$items.subtotal"},
            "orders": {"$sum": 1},
        }},
        {"$facet": {                     # two rankings from the same grouped data
            "top_by_units": top_branch("units"),
            "top_by_revenue": top_branch("revenue"),
        }},
    ]


def _json_default(o):
    if isinstance(o, (datetime.datetime, datetime.date)):
        return o.isoformat()
    raise TypeError(type(o))


def run_all(db):
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)

    # Use the latest order date as "today" so results don't depend on when
    # the script is run.
    latest = next(db.transactions.find({}, {"timestamp": 1}).sort("timestamp", -1).limit(1))["timestamp"]

    results = {
        "mongo_revenue_by_category_month": list(db.transactions.aggregate(
            revenue_by_category_month_pipeline(), allowDiskUse=True)),
        "mongo_customer_segments": list(db.users.aggregate(
            customer_segments_pipeline(latest))),
        "mongo_product_popularity": list(db.transactions.aggregate(
            product_popularity_pipeline(), allowDiskUse=True))[0],
    }

    for name, data in results.items():
        with open(os.path.join(config.OUTPUT_DIR, f"{name}.json"), "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=_json_default)

    # --- Console summary ---------------------------------------------------
    print("\n=== 1. Revenue by category (all months, top 5) ===")
    totals = {}
    for r in results["mongo_revenue_by_category_month"]:
        t = totals.setdefault(r["category_name"], [0, 0])
        t[0] += r["revenue"]
        t[1] += r["est_profit"]
    for name, (rev, prof) in sorted(totals.items(), key=lambda x: -x[1][0])[:5]:
        print(f"  {name:<35} revenue ${rev:>12,.2f}   est. profit ${prof:>11,.2f}")

    print("\n=== 2. Customer segments ===")
    for s in results["mongo_customer_segments"]:
        print(f"  {s['segment']:<12} users {s['users']:>5}  avg orders {s['avg_orders']:>5}  "
              f"avg spent ${s['avg_spent']:>9,.2f}  revenue ${s['total_revenue']:>13,.2f}")

    print("\n=== 3. Top 5 products by units ===")
    for p in results["mongo_product_popularity"]["top_by_units"][:5]:
        print(f"  {p['product_id']}  {p['name'][:35]:<35} units {p['units']:>4}  revenue ${p['revenue']:>10,.2f}")
    print(f"\nResults written to {config.OUTPUT_DIR}/mongo_*.json")
    return results


if __name__ == "__main__":
    run_all(MongoClient(config.MONGO_URI)[config.MONGO_DB])
