#!/usr/bin/env bash
# End-to-end pipeline. Run from the repo root.
#
#   ./run_all.sh                 # full run (needs `docker compose up -d` first)
#   SKIP_DB=1 ./run_all.sh       # Spark + charts only, no MongoDB/HBase needed
#   SCALE=0.25 ./run_all.sh      # bigger dataset (default 0.1 = 200k sessions)
set -euo pipefail
cd "$(dirname "$0")"
SCALE="${SCALE:-0.1}"

step() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

if [ ! -f data/raw/transactions.json ]; then
  step "1/7 Generating dataset (scale $SCALE)"
  python data_gen/dataset_generator.py --scale "$SCALE" --out data/raw
else
  step "1/7 Dataset already in data/raw (delete it to regenerate)"
fi

if [ -z "${SKIP_DB:-}" ]; then
  step "2/7 Loading MongoDB + running aggregation pipelines"
  python mongodb/load_mongodb.py
  python mongodb/aggregations.py

  step "3/7 Loading HBase + sample queries"
  python hbase/load_hbase.py
  python hbase/query_hbase.py --user user_000042 --product prod_00123
else
  step "2-3/7 Skipping MongoDB/HBase (SKIP_DB set)"
fi

step "4/7 Spark: clean + normalize"
(cd spark && python 01_clean_normalize.py)

step "5/7 Spark: cohort analysis, recommendations, Spark SQL"
(cd spark && python 02_cohort_analysis.py && python 03_recommendations.py && python 04_spark_sql_analytics.py)

step "6/7 Integrated funnel + CLV analysis"
if [ -z "${SKIP_DB:-}" ]; then
  python integration/funnel_clv_analysis.py --source databases
else
  python integration/funnel_clv_analysis.py --source files
fi

step "7/7 Charts"
python visualization/architecture_diagram.py
python visualization/make_charts.py

step "Done - results in output/, charts in output/charts/"
