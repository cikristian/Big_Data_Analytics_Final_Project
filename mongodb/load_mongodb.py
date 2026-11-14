"""
Load the generated JSON files into MongoDB using the document model
described in report/TECHNICAL_REPORT.md (section 3.1).

Collections
-----------
categories    small reference collection, subcategories embedded
products      catalog; category + subcategory info EMBEDDED (denormalized),
              price_history kept as an embedded array
users         profile + an embedded `purchase_summary` sub-document that is
              (re)computed from transactions at the end of the load
transactions  one document per order, line items EMBEDDED; each item carries
              a snapshot of category_id/subcategory_id for revenue reporting

Usage:
    python mongodb/load_mongodb.py                 # load everything
    python mongodb/load_mongodb.py --txn-limit 20000
"""
import argparse
import os
import sys
import time

from pymongo import ASCENDING, DESCENDING, MongoClient, UpdateOne

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: E402
from shared import assign_subcategory_index, load_json_array, parse_ts  # noqa: E402

BATCH = 5000


def insert_batched(coll, docs):
    """insert_many in batches; ordered=False keeps going past duplicate keys."""
    total = 0
    for i in range(0, len(docs), BATCH):
        coll.insert_many(docs[i:i + BATCH], ordered=False)
        total += len(docs[i:i + BATCH])
    return total


def build_categories(raw_categories):
    return [{
        "_id": c["category_id"],
        "name": c["name"],
        "subcategories": c["subcategories"],
    } for c in raw_categories]


def build_products(raw_products, cat_by_id):
    docs = []
    for p in raw_products:
        cat = cat_by_id[p["category_id"]]
        subs = cat["subcategories"]
        sub = subs[assign_subcategory_index(p["product_id"], len(subs))]
        docs.append({
            "_id": p["product_id"],
            "name": p["name"],
            # Embedded copy of the hierarchy: the catalog is read far more
            # often than categories are renamed, so we avoid a $lookup on
            # every product page / report.
            "category": {"id": cat["category_id"], "name": cat["name"]},
            "subcategory": {"id": sub["subcategory_id"], "name": sub["name"],
                            "profit_margin": sub["profit_margin"]},
            "base_price": p["base_price"],
            "current_stock": p["current_stock"],
            "is_active": p["is_active"],
            "price_history": [{"price": h["price"], "date": parse_ts(h["date"])}
                              for h in p["price_history"]],
            "creation_date": parse_ts(p["creation_date"]),
        })
    return docs


def build_users(raw_users):
    return [{
        "_id": u["user_id"],
        "geo": u["geo_data"],
        "registration_date": parse_ts(u["registration_date"]),
        "last_active": parse_ts(u["last_active"]),
        "purchase_summary": None,  # filled by refresh_purchase_summaries()
    } for u in raw_users]


def build_transactions(raw_txns, product_docs):
    prod = {p["_id"]: p for p in product_docs}
    docs = []
    for t in raw_txns:
        items = []
        for it in t["items"]:
            p = prod.get(it["product_id"], {})
            items.append({
                **it,
                # Snapshot at order time: revenue-by-category reports must not
                # change if a product is later re-categorised.
                "category_id": p.get("category", {}).get("id"),
                "subcategory_id": p.get("subcategory", {}).get("id"),
            })
        docs.append({
            "_id": t["transaction_id"],
            "session_id": t["session_id"],
            "user_id": t["user_id"],
            "timestamp": parse_ts(t["timestamp"]),
            "items": items,
            "item_count": sum(i["quantity"] for i in items),
            "subtotal": t["subtotal"],
            "discount": t["discount"],
            "total": t["total"],
            "payment_method": t["payment_method"],
            "status": t["status"],
        })
    return docs


def refresh_purchase_summaries(db):
    """
    Compute each user's purchase summary with an aggregation and embed it in
    the user document. Profile pages then need ONE read instead of scanning
    the user's whole order history.
    """
    pipeline = [
        {"$group": {
            "_id": "$user_id",
            "total_orders": {"$sum": 1},
            "total_spent": {"$sum": "$total"},
            "first_purchase": {"$min": "$timestamp"},
            "last_purchase": {"$max": "$timestamp"},
        }},
    ]
    ops = []
    for row in db.transactions.aggregate(pipeline, allowDiskUse=True):
        ops.append(UpdateOne({"_id": row["_id"]}, {"$set": {"purchase_summary": {
            "total_orders": row["total_orders"],
            "total_spent": round(row["total_spent"], 2),
            "avg_order_value": round(row["total_spent"] / row["total_orders"], 2),
            "first_purchase": row["first_purchase"],
            "last_purchase": row["last_purchase"],
        }}}))
    for i in range(0, len(ops), BATCH):
        db.users.bulk_write(ops[i:i + BATCH], ordered=False)
    return len(ops)


def create_indexes(db):
    """Indexes that match the query patterns in aggregations.py."""
    db.products.create_index([("category.id", ASCENDING)])
    db.products.create_index([("is_active", ASCENDING), ("current_stock", ASCENDING)])
    db.users.create_index([("geo.country", ASCENDING)])
    db.users.create_index([("purchase_summary.total_spent", DESCENDING)])
    db.transactions.create_index([("user_id", ASCENDING), ("timestamp", DESCENDING)])
    db.transactions.create_index([("timestamp", ASCENDING)])
    db.transactions.create_index([("items.product_id", ASCENDING)])  # multikey index
    db.transactions.create_index([("session_id", ASCENDING)], sparse=True)


def main(db=None, txn_limit=None):
    if db is None:
        db = MongoClient(config.MONGO_URI)[config.MONGO_DB]
    raw = config.RAW_DIR
    t0 = time.time()

    # Fresh load each run so the script is idempotent.
    for name in ("categories", "products", "users", "transactions"):
        db[name].drop()

    raw_categories = load_json_array(os.path.join(raw, "categories.json"))
    cat_by_id = {c["category_id"]: c for c in raw_categories}
    print("categories  :", insert_batched(db.categories, build_categories(raw_categories)))

    product_docs = build_products(load_json_array(os.path.join(raw, "products.json")), cat_by_id)
    print("products    :", insert_batched(db.products, product_docs))

    print("users       :", insert_batched(db.users, build_users(load_json_array(os.path.join(raw, "users.json")))))

    raw_txns = load_json_array(os.path.join(raw, "transactions.json"))
    if txn_limit:
        raw_txns = raw_txns[:txn_limit]
    print("transactions:", insert_batched(db.transactions, build_transactions(raw_txns, product_docs)))

    print("user purchase summaries:", refresh_purchase_summaries(db))
    create_indexes(db)
    print(f"Done in {time.time() - t0:.1f}s")
    return db


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--txn-limit", type=int, default=None,
                    help="Load only the first N transactions (default: all)")
    main(txn_limit=ap.parse_args().txn_limit)
