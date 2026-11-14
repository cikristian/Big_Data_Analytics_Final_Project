"""
Part 4 - static charts for the report (PNG, 200 dpi) from the CSV/JSON
results in output/. Run after the Spark, MongoDB and integration jobs.

  01_weekly_revenue.png            sales over time (+ 4-week moving average)
  02_category_revenue_profit.png   revenue vs estimated profit by category
  03_cohort_heatmap.png            revenue per cohort member, by purchase month
  04_conversion_funnel.png         session -> view -> cart -> checkout -> purchase
  05_views_vs_sales.png            most-viewed vs best-selling products
  06_customer_segments.png         RFM segments: size and average spend
  07_clv_concentration.png         share of 12-month CLV by customer quartile
"""
import json
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker as mtick  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config  # noqa: E402

OUT, CH = config.OUTPUT_DIR, config.CHART_DIR
os.makedirs(CH, exist_ok=True)

# Validated colour-blind-safe categorical order + one-hue blue ramp
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
TEXT, TEXT2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
SEQ = LinearSegmentedColormap.from_list("blue_seq", ["#cde2fb", "#86b6ef", "#3987e5", "#1c5cab", "#0d366b"])

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "font.family": "DejaVu Sans", "font.size": 10, "text.color": TEXT,
    "axes.edgecolor": GRID, "axes.labelcolor": TEXT2, "axes.titlesize": 13,
    "axes.titleweight": "bold", "axes.titlelocation": "left", "axes.titlepad": 24,
    "xtick.color": TEXT2, "ytick.color": TEXT2, "axes.grid": True, "grid.color": GRID,
    "grid.linewidth": 0.8, "axes.spines.top": False, "axes.spines.right": False,
    "legend.frameon": False,
})
money = mtick.FuncFormatter(lambda v, _: f"${v / 1e6:.1f}M" if abs(v) >= 1e6 else f"${v / 1e3:.0f}k")


def csv(name):
    return pd.read_csv(os.path.join(OUT, f"{name}.csv"))


def subtitle(ax, text):
    ax.text(0, 1.01, text, transform=ax.transAxes, color=TEXT2, fontsize=9, va="bottom")


def save(fig, name):
    fig.tight_layout()
    fig.savefig(os.path.join(CH, name), dpi=200)
    plt.close(fig)
    print("saved", name)


# 1. Weekly revenue ---------------------------------------------------------
w = csv("sql_weekly_revenue")
w["week_start"] = pd.to_datetime(w["week_start"])
full = w[w["orders"] >= 0.8 * w["orders"].median()].copy()  # drop the partial first/last weeks
full["revenue_4wk_avg"] = full["revenue"].rolling(4, min_periods=1).mean()
fig, ax = plt.subplots(figsize=(9, 4.2))
ax.plot(full["week_start"], full["revenue"], color=BLUE, lw=2, marker="o", ms=5, label="Weekly revenue")
ax.plot(full["week_start"], full["revenue_4wk_avg"], color=ORANGE, lw=2, ls="--", label="4-week moving average")
ax.yaxis.set_major_formatter(money)
ax.set_ylim(0, full["revenue"].max() * 1.15)
ax.set_title("Weekly revenue is flat at about $4.2M")
subtitle(ax, "All orders, complete weeks only (partial first and last weeks removed)")
ax.legend(loc="lower right")
ax.grid(axis="x", visible=False)
save(fig, "01_weekly_revenue.png")

# 2. Revenue vs estimated profit by category --------------------------------
c = csv("sql_category_performance").sort_values("revenue")
fig, ax = plt.subplots(figsize=(9, 7.5))
y = range(len(c))
ax.barh([i + 0.2 for i in y], c["revenue"], height=0.38, color=BLUE, label="Revenue")
ax.barh([i - 0.2 for i in y], c["est_profit"], height=0.38, color=ORANGE, label="Estimated gross profit")
ax.set_yticks(list(y), c["category_name"], fontsize=8.5)
ax.xaxis.set_major_formatter(money)
ax.grid(axis="y", visible=False)
top = c.iloc[-1]
ax.set_title("High revenue doesn't mean high profit")
subtitle(ax, f"{top['category_name']} is #1 by revenue but #{int(top['profit_rank'])} by profit "
             f"({top['profit_margin']:.0%} margin)")
ax.legend(loc="lower right")
save(fig, "02_category_revenue_profit.png")

# 3. Cohort heatmap ----------------------------------------------------------
m = csv("cohort_matrix")
# first and last purchase months are partial (data window starts/ends mid-month);
# keep only complete months so cells are comparable
counts = m.groupby("txn_month")["orders"].sum()
complete = counts[counts >= 0.5 * counts.max()].index
partial = sorted(set(counts.index) - set(complete))
piv = m[m["txn_month"].isin(complete)].pivot(index="registration_month", columns="txn_month",
                                              values="revenue_per_user")
piv.columns = [pd.Timestamp(x + "-01").strftime("%b %Y") for x in piv.columns]
piv.index = [pd.Timestamp(x + "-01").strftime("%b %Y") for x in piv.index]
fig, ax = plt.subplots(figsize=(8.5, 4.8))
im = ax.imshow(piv.values, cmap=SEQ, aspect="auto")
ax.set_xticks(range(len(piv.columns)), piv.columns)
ax.set_yticks(range(len(piv.index)), piv.index)
ax.set_xlabel("Purchase month")
ax.set_ylabel("Registration cohort")
ax.grid(False)
for i in range(piv.shape[0]):
    for j in range(piv.shape[1]):
        v = piv.values[i, j]
        ax.text(j, i, f"${v:,.0f}", ha="center", va="center", fontsize=8.5,
                color="white" if v > (piv.values.min() + piv.values.max()) / 2 else TEXT)
cb = fig.colorbar(im, ax=ax, fraction=0.035, pad=0.02,
                  format=mtick.FuncFormatter(lambda v, _: f"${v:,.0f}"))
cb.outline.set_visible(False)
ax.set_title("Spend per user doesn't depend on when they joined")
subtitle(ax, "Revenue per cohort member by purchase month"
             + (f" (partial months {', '.join(partial)} excluded)" if partial else ""))
save(fig, "03_cohort_heatmap.png")

# 4. Funnel ------------------------------------------------------------------
f = csv("funnel_overall")
labels = ["Session", "Viewed a product", "Added to cart", "Reached checkout", "Purchased"]
fig, ax = plt.subplots(figsize=(9, 4.3))
ax.barh(labels[::-1], f["sessions"][::-1], color=BLUE, height=0.6)
for i, (_, r) in enumerate(f.iloc[::-1].iterrows()):
    step = "" if r["stage"].startswith("1") else f"   ({r['pct_of_previous']:.0%} of previous step)"
    ax.text(r["sessions"] + f["sessions"].max() * 0.01, i, f"{r['sessions']:,}  ·  {r['pct_of_sessions']:.1%}{step}",
            va="center", fontsize=9, color=TEXT)
ax.set_xlim(0, f["sessions"].max() * 1.55)
ax.xaxis.set_major_formatter(mtick.FuncFormatter(lambda v, _: f"{v / 1e3:.0f}k"))
ax.grid(axis="y", visible=False)
ax.set_title("Most shoppers are lost between cart and payment")
subtitle(ax, "Each stage requires the previous one; purchase = session linked to an order in MongoDB")
save(fig, "04_conversion_funnel.png")

# 5. Views vs sales ----------------------------------------------------------
p = csv("sql_product_views_vs_sales")
p = p[p["views"] > 0]
r = p["views"].corr(p["units"])
top_viewed = set(p.nlargest(10, "views")["product_id"])
top_sold = set(p.nlargest(10, "units")["product_id"])
fig, ax = plt.subplots(figsize=(8.5, 5))
soldout = p["current_stock"] == 0
ax.scatter(p.loc[~soldout, "views"], p.loc[~soldout, "units"], s=10, color="#b7d3f6", edgecolors="none",
           label="In-stock product")
ax.scatter(p.loc[soldout, "views"], p.loc[soldout, "units"], s=14, color=AQUA, edgecolors="none",
           label=f"Sold out ({soldout.sum()})")
tv = p[p["product_id"].isin(top_viewed)]
ts = p[p["product_id"].isin(top_sold)]
ax.scatter(tv["views"], tv["units"], s=40, color=BLUE, edgecolors=SURFACE, linewidths=1.5, label="Top 10 most viewed")
ax.scatter(ts["views"], ts["units"], s=40, color=ORANGE, marker="D", edgecolors=SURFACE, linewidths=1.5,
           label="Top 10 best sellers")
ax.set_xlabel("Product page views")
ax.set_ylabel("Units sold")
ax.set_title("Most-viewed products are not the best sellers")
subtitle(ax, f"r = {r:.2f}; {len(top_viewed & top_sold)} products in both top-10 lists; "
             f"sold-out products get {1 - p.loc[soldout, 'views'].mean() / p.loc[~soldout, 'views'].mean():.0%} "
             f"fewer views")
ax.legend(loc="upper left")
save(fig, "05_views_vs_sales.png")

# 6. Customer segments (MongoDB aggregation) -----------------------------------
seg_path = os.path.join(OUT, "mongo_customer_segments.json")
if os.path.exists(seg_path):
    s = pd.DataFrame(json.load(open(seg_path, encoding="utf-8")))
    title_note = "MongoDB aggregation on users.purchase_summary"
else:  # fall back to the Spark SQL RFM result if MongoDB wasn't run
    s = csv("sql_rfm_segment_summary").rename(columns={"rfm_segment": "segment", "avg_spent": "avg_spent"})
    title_note = "Spark SQL NTILE scoring"
s = s.sort_values("avg_spent")
fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 4.2), sharey=True)
a1.barh(s["segment"], s["users"], color=BLUE, height=0.6)
a1.set_title("Customers per segment", fontsize=11)
a2.barh(s["segment"], s["avg_spent"], color=BLUE, height=0.6)
a2.set_title("Average spend in 90 days", fontsize=11)
a2.xaxis.set_major_formatter(money)
for ax_, col, fmt in ((a1, "users", "{:,.0f}"), (a2, "avg_spent", "${:,.0f}")):
    ax_.grid(axis="y", visible=False)
    for i, v in enumerate(s[col]):
        ax_.text(v, i, " " + fmt.format(v), va="center", fontsize=8.5)
    ax_.set_xlim(0, s[col].max() * 1.25)
fig.suptitle(f"Customer segments (recency / frequency / spend) - {title_note}",
             x=0.01, ha="left", fontweight="bold", fontsize=12)
save(fig, "06_customer_segments.png")

# 7. CLV concentration --------------------------------------------------------
t = csv("clv_tiers")
names = {"Q1": "Top 25%", "Q2": "25-50%", "Q3": "50-75%", "Q4": "Bottom 25%"}
fig, ax = plt.subplots(figsize=(8, 3.8))
ax.bar([names[q] for q in t["clv_tier"]], t["share_of_total_clv"], color=BLUE, width=0.55)
for i, (_, row) in enumerate(t.iterrows()):
    ax.text(i, row["share_of_total_clv"] + 0.01,
            f"{row['share_of_total_clv']:.0%}\navg ${row['avg_clv_12m']:,.0f}", ha="center", fontsize=9)
ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0, decimals=0))
ax.set_ylim(0, t["share_of_total_clv"].max() * 1.3)
ax.grid(axis="x", visible=False)
ax.set_xlabel("Customers ranked by predicted 12-month CLV")
ax.set_title(f"The top quarter of customers hold {t['share_of_total_clv'].iloc[0]:.0%} of future value")
subtitle(ax, "Share of total predicted 12-month gross-profit CLV")
save(fig, "07_clv_concentration.png")
