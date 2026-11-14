"""Draws output/charts/00_architecture.png - the system architecture figure for the report."""
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: E402

TEXT, TEXT2, SURFACE, LINE = "#0b0b0b", "#52514e", "#fcfcfb", "#8a8984"
FILL = {"src": "#eeede9", "mongo": "#d6ecdf", "hbase": "#fbe0d3", "spark": "#cde2fb", "out": "#eeede9"}

fig, ax = plt.subplots(figsize=(11, 5.6))
fig.patch.set_facecolor(SURFACE)
ax.set_xlim(0, 110)
ax.set_ylim(0, 56)
ax.axis("off")


def box(x, y, w, h, title, lines, kind):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.4,rounding_size=1.2",
                                fc=FILL[kind], ec=LINE, lw=1))
    ax.text(x + w / 2, y + h - 2.6, title, ha="center", va="top", fontsize=10.5, fontweight="bold", color=TEXT)
    ax.text(x + w / 2, y + h - 6.6, "\n".join(lines), ha="center", va="top", fontsize=8.2,
            color=TEXT2, linespacing=1.45)


def arrow(x1, y1, x2, y2, label=None, dy=1.2):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=12, color=LINE, lw=1.2))
    if label:
        ax.text((x1 + x2) / 2, (y1 + y2) / 2 + dy, label, ha="center", fontsize=7.5, color=TEXT2)


box(1, 18, 19, 20, "Generator", ["dataset_generator.py", "users, products,", "categories, sessions,",
                                 "transactions", "(JSON arrays)"], "src")
box(29, 33, 24, 21, "MongoDB", ["document model", "products (+hierarchy)", "users (+purchase summary)",
                                "transactions (+items)", "aggregation pipelines"], "mongo")
box(29, 2, 24, 21, "HBase", ["wide-column model", "user_sessions", "user#rev_ts#session", "product_metrics",
                             "product#yyyyMMdd"], "hbase")
box(62, 10, 25, 36, "Apache Spark", ["1 clean + normalize", "  -> Parquet (partitioned)", "2 cohort analysis",
                                     "3 also-bought / viewed-together", "4 Spark SQL analytics",
                                     "5 funnel + CLV (integration)", "", "parallel HBase range scans",
                                     "MongoDB reads via pymongo"], "spark")
box(93, 16, 16, 24, "Insights", ["CSV / JSON results", "7 static charts", "technical report"], "out")

arrow(20, 32, 29, 43, "catalog, users,\norders", dy=2)
arrow(20, 24, 29, 13, "sessions,\nmetrics", dy=-4)
arrow(20, 28, 62, 28, "raw JSON (batch)", dy=1)
arrow(53, 43, 62, 38, "orders, users")
arrow(53, 13, 62, 18, "session paths", dy=-3.5)
arrow(87, 28, 93, 28)
ax.set_title("System architecture: each store holds the data whose access pattern it serves best",
             loc="left", fontsize=12, fontweight="bold", color=TEXT)
os.makedirs(config.CHART_DIR, exist_ok=True)
fig.tight_layout()
fig.savefig(os.path.join(config.CHART_DIR, "00_architecture.png"), dpi=200)
print("saved 00_architecture.png")
