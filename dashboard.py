"""
Well Production Forecasting & Performance Analytics — Streamlit Dashboard
Wells-Dataset: Bakken / Williston Basin, North Dakota (DoomDust7/Wells-Dataset)

Pages:
  1. 🏠 Overview         — KPI cards, top operators, basin distribution
  2. 📈 Production Trends — time-series by basin/operator, quarterly heatmap
  3. 🔥 Flaring & ESG    — flaring intensity, ethane vs dry gas, scatter analysis
  4. 🔮 Forecasting       — Arps decline curves, historical vs forecast, model quality
  5. 💰 Well Economics   — breakeven, IRR, NPV, EUR, water cut, IP benchmarks
"""

import os
import glob
import json
from urllib.parse import unquote
import warnings
warnings.filterwarnings("ignore")

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ── Page Config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Well Production Analytics",
    page_icon="🛢️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Theme / CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  /* Sidebar */
  [data-testid="stSidebar"] { background: #0d1b2a; }
  [data-testid="stSidebar"] * { color: #e8eaf0 !important; }

  /* KPI Cards */
  .kpi-row { display: flex; gap: 16px; margin-bottom: 20px; }
  .kpi-card {
    flex: 1;
    background: linear-gradient(135deg, #0d1b2a 0%, #1a3354 100%);
    border: 1px solid #2d5080;
    border-radius: 12px;
    padding: 18px 22px;
    text-align: center;
  }
  .kpi-val  { font-size: 2rem; font-weight: 700; color: #f5a623; line-height: 1; }
  .kpi-lbl  { font-size: 0.78rem; color: #9ab; margin-top: 6px; text-transform: uppercase; letter-spacing: .06em; }
  .kpi-sub  { font-size: 0.72rem; color: #7a9; margin-top: 4px; }

  /* Section headers */
  .sec-hdr {
    font-size: 1.1rem; font-weight: 600; color: #e8eaf0;
    border-left: 4px solid #f5a623;
    padding-left: 10px; margin: 18px 0 10px;
  }

  /* Flaring badge */
  .badge-low    { background:#1a4a2e; color:#5be; border-radius:6px; padding:2px 8px; }
  .badge-medium { background:#4a3a10; color:#f5a623; border-radius:6px; padding:2px 8px; }
  .badge-high   { background:#4a1a1a; color:#e74c3c; border-radius:6px; padding:2px 8px; }

  /* Hide default streamlit padding */
  .block-container { padding-top: 1.5rem; }
</style>
""", unsafe_allow_html=True)

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE    = os.path.dirname(os.path.abspath(__file__))
GOLD    = lambda t: os.path.join(BASE, "data", "delta", "gold",   t)
SILVER  = lambda t: os.path.join(BASE, "data", "delta", "silver", t)

PLOTLY_TEMPLATE = "plotly_dark"
PALETTE_BLUE    = px.colors.sequential.Blues_r
PALETTE_MULTI   = px.colors.qualitative.Bold

# ── Delta Table Reader (no Spark required) ─────────────────────────────────────
def _active_files_from_log(path: str) -> "list[str]":
    """Parse Delta log JSON commits to get the current snapshot's active files."""
    log_dir = os.path.join(path, "_delta_log")
    if not os.path.exists(log_dir):
        return []

    active, removed = set(), set()
    for jf in sorted(glob.glob(os.path.join(log_dir, "*.json"))):
        try:
            with open(jf) as f:
                for raw in f:
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        entry = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if "add" in entry and entry["add"]:
                        active.add(unquote(entry["add"]["path"]))
                    elif "remove" in entry and entry["remove"]:
                        removed.add(unquote(entry["remove"]["path"]))
        except OSError:
            continue

    return [os.path.join(path, p) for p in (active - removed) if p.endswith(".parquet")]


SAMPLE_DIR = os.path.join(BASE, "data", "sample")


@st.cache_data(show_spinner=False)
def _load(path: str) -> pd.DataFrame:
    """Read a Delta table into a Pandas DataFrame.
    Falls back to data/sample/<table>.csv for cloud / no-Spark deployments."""
    # 0. CSV sample fallback (fast, works on Streamlit Cloud with no Delta/Spark)
    table_name = os.path.basename(path)
    csv_path = os.path.join(SAMPLE_DIR, f"{table_name}.csv")
    if os.path.exists(csv_path):
        return pd.read_csv(csv_path)

    # 1. Try delta-rs (most correct)
    try:
        from deltalake import DeltaTable
        return DeltaTable(path).to_pandas()
    except Exception:
        pass

    # 2. Parse Delta log manually
    files = _active_files_from_log(path)
    if files:
        dfs = []
        for f in files:
            if os.path.exists(f):
                dfs.append(pd.read_parquet(f))
        if dfs:
            return pd.concat(dfs, ignore_index=True)

    # 3. Last resort: all non-log parquet files
    all_pq = glob.glob(os.path.join(path, "**", "*.parquet"), recursive=True)
    all_pq = [f for f in all_pq if "_delta_log" not in f]
    return pd.concat([pd.read_parquet(f) for f in all_pq], ignore_index=True) if all_pq else pd.DataFrame()


@st.cache_data(show_spinner=False)
def load_all() -> dict:
    """Load all gold + silver tables needed for the dashboard."""
    return {
        "op_perf":     _load(GOLD("gold_operator_performance")),
        "basin":       _load(GOLD("gold_basin_production_trends")),
        "flaring":     _load(GOLD("gold_flaring_intensity")),
        "ethane":      _load(GOLD("gold_ethane_dry_gas")),
        "well_sum":    _load(GOLD("gold_well_summary")),
        "forecast":    _load(GOLD("gold_production_forecast")),
        "silver_prod": _load(SILVER("silver_production")),
        # New real-data tables
        "economics":    _load(GOLD("gold_well_economics")),
        "ip_bench":     _load(GOLD("gold_ip_benchmarks")),
        "flaring_ts":   _load(GOLD("gold_flaring_timeseries")),
        "three_stream": _load(GOLD("gold_three_stream_production")),
    }


# ── Helpers ────────────────────────────────────────────────────────────────────
def fmt_num(v, suffix="", dp=1):
    if pd.isna(v):
        return "—"
    if abs(v) >= 1e9:
        return f"{v/1e9:.{dp}f}B{suffix}"
    if abs(v) >= 1e6:
        return f"{v/1e6:.{dp}f}M{suffix}"
    if abs(v) >= 1e3:
        return f"{v/1e3:.{dp}f}K{suffix}"
    return f"{v:.{dp}f}{suffix}"


def kpi_card(val, label, sub=""):
    return f"""
    <div class="kpi-card">
      <div class="kpi-val">{val}</div>
      <div class="kpi-lbl">{label}</div>
      {"<div class='kpi-sub'>"+sub+"</div>" if sub else ""}
    </div>"""


def section(title: str):
    st.markdown(f'<div class="sec-hdr">{title}</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — Overview
# ══════════════════════════════════════════════════════════════════════════════
def page_overview(D: dict):
    op   = D["op_perf"].copy()
    well = D["well_sum"].copy()
    bas  = D["basin"].copy()

    # ── KPI Row ───────────────────────────────────────────────────────────────
    total_oil   = op["total_oil_bbl"].sum()
    total_gas   = op["total_gas_mcf"].sum()
    n_wells     = int(well["api_number"].nunique())
    n_operators = int(op["operator"].nunique())
    median_eur  = well["eur"].median() if "eur" in well.columns else None

    year_min = well["first_production_month"].min()[:4] if "first_production_month" in well.columns and len(well) else "—"
    year_max = well["last_production_month"].max()[:4]  if "last_production_month"  in well.columns and len(well) else "—"
    date_range = f"{year_min} – {year_max}"

    cards = "".join([
        kpi_card(fmt_num(n_wells),      "Total Wells",       "unique API numbers"),
        kpi_card(str(n_operators),      "Operators",         "active producers"),
        kpi_card(fmt_num(total_oil, " BBL"), "Cumulative Oil", date_range),
        kpi_card(fmt_num(total_gas, " MCF"), "Cumulative Gas", date_range),
        kpi_card(fmt_num(median_eur) if median_eur else "—", "Median EUR", "estimated ultimate recovery"),
    ])
    st.markdown(f'<div class="kpi-row">{cards}</div>', unsafe_allow_html=True)

    # ── Row 1: Top Operators + Basin Pie ──────────────────────────────────────
    section("Top Operators by Cumulative Oil Production")
    col1, col2 = st.columns([3, 2])

    with col1:
        top10 = op.nlargest(10, "total_oil_bbl").sort_values("total_oil_bbl")
        fig = px.bar(
            top10, x="total_oil_bbl", y="operator", orientation="h",
            color="total_oil_bbl", color_continuous_scale="Blues",
            labels={"total_oil_bbl": "Oil Production (BBL)", "operator": ""},
            template=PLOTLY_TEMPLATE,
        )
        fig.update_layout(
            showlegend=False, coloraxis_showscale=False,
            height=370, margin=dict(l=0, r=20, t=10, b=10),
        )
        fig.update_traces(
            text=[fmt_num(v, " BBL") for v in top10["total_oil_bbl"]],
            textposition="outside",
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        basin_agg = (
            well.groupby("shale_play")["api_number"]
            .count().reset_index(name="wells")
        )
        fig2 = px.pie(
            basin_agg, names="shale_play", values="wells",
            color_discrete_sequence=PALETTE_MULTI,
            hole=0.45, template=PLOTLY_TEMPLATE,
        )
        fig2.update_layout(
            height=370, margin=dict(l=0, r=0, t=10, b=10),
            legend=dict(orientation="h", yanchor="bottom", y=-0.2),
        )
        fig2.update_traces(textinfo="percent+label", textfont_size=11)
        st.plotly_chart(fig2, use_container_width=True)

    # ── Row 2: Operator Oil vs Gas Bubble ────────────────────────────────────
    section("Oil vs Gas Production Mix — Operator Comparison")
    op_bubble = op.dropna(subset=["total_oil_bbl", "total_gas_mcf"])
    hover_extra = {}
    if "ticker" in op_bubble.columns:
        hover_extra["ticker"] = True
    if "public_private" in op_bubble.columns:
        hover_extra["public_private"] = True
    hover_extra["well_count"] = True
    hover_extra["flaring_intensity_ratio"] = ":.3f"
    fig3 = px.scatter(
        op_bubble,
        x="total_oil_bbl", y="total_gas_mcf",
        size="well_count", color="flaring_intensity_ratio",
        color_continuous_scale="RdYlGn_r",
        hover_name="operator",
        hover_data=hover_extra,
        labels={
            "total_oil_bbl": "Total Oil (BBL)",
            "total_gas_mcf": "Total Gas (MCF)",
            "flaring_intensity_ratio": "Flaring Ratio",
            "public_private": "Company Type",
        },
        template=PLOTLY_TEMPLATE,
    )
    fig3.update_layout(height=400, margin=dict(l=0, r=0, t=10, b=10))
    fig3.update_coloraxes(colorbar_title="Flaring<br>Intensity")
    st.plotly_chart(fig3, use_container_width=True)

    # ── Row 3: Well Summary Table ─────────────────────────────────────────────
    section("Well Portfolio Summary")
    display_cols = [
        "well_name", "operator", "formation", "shale_play", "basin",
        "lateral_length_ft", "completion_date",
        "first_production_month", "last_production_month",
        "cumulative_oil_bbl", "cumulative_gas_mcf",
        "eur", "peak_oil_production", "active_months",
    ]
    available = [c for c in display_cols if c in well.columns]
    disp = well[available].copy()
    for col in ["cumulative_oil_bbl", "cumulative_gas_mcf", "peak_oil_production", "eur"]:
        if col in disp.columns:
            disp[col] = disp[col].map(lambda v: fmt_num(v))
    if "lateral_length_ft" in disp.columns:
        disp["lateral_length_ft"] = disp["lateral_length_ft"].map(
            lambda v: f"{v:,.0f} ft" if pd.notna(v) else "—")
    disp.columns = [c.replace("_", " ").title() for c in disp.columns]
    st.dataframe(disp, use_container_width=True, height=300)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — Production Trends
# ══════════════════════════════════════════════════════════════════════════════
def page_trends(D: dict):
    bas   = D["basin"].copy()
    sprod = D["silver_prod"].copy()

    bas["production_month"] = pd.to_datetime(bas["production_month"])
    sprod["production_month"] = pd.to_datetime(sprod["production_month"])

    # Sidebar filters
    all_basins = sorted(bas["shale_play"].dropna().unique())
    sel_basins = st.sidebar.multiselect("Basins", all_basins, default=all_basins, key="trend_basins")
    commodity  = st.sidebar.radio("Commodity", ["Oil (O)", "Gas (G)", "Both"], key="trend_comm")
    comm_map   = {"Oil (O)": ["O"], "Gas (G)": ["G"], "Both": ["O", "G"]}
    sel_comm   = comm_map[commodity]

    bas_f = bas[bas["shale_play"].isin(sel_basins) & bas["oil_and_gas_group"].isin(sel_comm)]

    # ── Basin Production Time-Series ──────────────────────────────────────────
    section("Monthly Production by Basin")
    fig = px.line(
        bas_f.sort_values("production_month"),
        x="production_month", y="total_production",
        color="shale_play", line_dash="oil_and_gas_group",
        labels={
            "production_month": "Month", "total_production": "Production",
            "shale_play": "Basin", "oil_and_gas_group": "Type",
        },
        color_discrete_sequence=PALETTE_MULTI,
        template=PLOTLY_TEMPLATE,
    )
    fig.update_layout(height=380, margin=dict(l=0, r=0, t=10, b=10),
                      legend=dict(orientation="h", y=-0.25))
    st.plotly_chart(fig, use_container_width=True)

    # ── MoM Growth ────────────────────────────────────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        section("Month-over-Month Growth Rate (%)")
        mom = bas_f.dropna(subset=["mom_growth_pct"]).sort_values("production_month")
        fig2 = px.bar(
            mom, x="production_month", y="mom_growth_pct", color="shale_play",
            barmode="group",
            labels={"mom_growth_pct": "MoM Growth (%)", "production_month": "Month"},
            color_discrete_sequence=PALETTE_MULTI,
            template=PLOTLY_TEMPLATE,
        )
        fig2.add_hline(y=0, line_color="white", line_width=0.8, opacity=0.4)
        fig2.update_layout(height=320, margin=dict(l=0, r=0, t=10, b=10),
                           legend=dict(orientation="h", y=-0.3))
        st.plotly_chart(fig2, use_container_width=True)

    with col2:
        section("Year-over-Year Growth Rate (%)")
        yoy = bas_f.dropna(subset=["yoy_growth_pct"]).sort_values("production_month")
        fig3 = px.bar(
            yoy, x="production_month", y="yoy_growth_pct", color="shale_play",
            barmode="group",
            labels={"yoy_growth_pct": "YoY Growth (%)", "production_month": "Month"},
            color_discrete_sequence=PALETTE_MULTI,
            template=PLOTLY_TEMPLATE,
        )
        fig3.add_hline(y=0, line_color="white", line_width=0.8, opacity=0.4)
        fig3.update_layout(height=320, margin=dict(l=0, r=0, t=10, b=10),
                           legend=dict(orientation="h", y=-0.3))
        st.plotly_chart(fig3, use_container_width=True)

    # ── Quarterly Heatmap ──────────────────────────────────────────────────────
    section("Quarterly Production Heatmap (All Basins · Oil)")
    sprod_oil = sprod[sprod["oil_and_gas_group"] == "O"].copy()
    sprod_oil["production_year"]    = sprod_oil["production_month"].dt.year.astype(str)
    sprod_oil["production_quarter"] = sprod_oil["production_month"].dt.to_period("Q").astype(str)
    heat = (
        sprod_oil.groupby(["production_year", "production_quarter"])["production"]
        .sum().reset_index()
        .pivot(index="production_year", columns="production_quarter", values="production")
        .fillna(0)
    )
    fig4 = px.imshow(
        heat, color_continuous_scale="Blues",
        labels=dict(color="Oil (BBL)", x="Quarter", y="Year"),
        template=PLOTLY_TEMPLATE, aspect="auto",
        text_auto=".2s",
    )
    fig4.update_layout(height=280, margin=dict(l=0, r=0, t=10, b=10))
    st.plotly_chart(fig4, use_container_width=True)

    # ── Operator Drill-Down ────────────────────────────────────────────────────
    section("Operator Monthly Production Drill-Down")
    ops = sorted(sprod["operator"].dropna().unique())
    sel_op = st.selectbox("Select Operator", ops, key="trend_op")

    op_data = (
        sprod[sprod["operator"] == sel_op]
        .groupby(["production_month", "oil_and_gas_group"])["production"]
        .sum().reset_index()
        .sort_values("production_month")
    )
    fig5 = px.area(
        op_data, x="production_month", y="production",
        color="oil_and_gas_group",
        color_discrete_map={"O": "#3498db", "G": "#e67e22"},
        labels={"production_month": "Month", "production": "Production",
                "oil_and_gas_group": "Type"},
        template=PLOTLY_TEMPLATE,
    )
    fig5.update_layout(height=320, margin=dict(l=0, r=0, t=10, b=10),
                       legend=dict(orientation="h", y=-0.25))
    st.plotly_chart(fig5, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — Flaring & ESG
# ══════════════════════════════════════════════════════════════════════════════
def page_esg(D: dict):
    flaring    = D["flaring"].copy()
    ethane     = D["ethane"].copy()
    flaring_ts = D.get("flaring_ts", pd.DataFrame())

    has_intensity = ("flaring_intensity_ratio" in flaring.columns
                     and flaring["flaring_intensity_ratio"].notna().any())

    # Classify by intensity ratio when available; fall back to volume percentiles
    if has_intensity:
        flaring["volume_category"] = flaring["flaring_intensity_ratio"].apply(
            lambda v: "High" if v > 0.15 else ("Low" if v < 0.05 else "Medium")
            if pd.notna(v) else "Unknown"
        )
    else:
        p33 = flaring["total_flared_gas_mcf"].quantile(0.33)
        p67 = flaring["total_flared_gas_mcf"].quantile(0.67)
        flaring["volume_category"] = flaring["total_flared_gas_mcf"].apply(
            lambda v: "High" if v >= p67 else ("Low" if v <= p33 else "Medium")
        )

    # ── KPI Cards ─────────────────────────────────────────────────────────────
    total_flared    = flaring["total_flared_gas_mcf"].sum()
    n_operators     = len(flaring)
    top_flarer      = flaring.loc[flaring["total_flared_gas_mcf"].idxmax(), "operator"]
    top_flared_vol  = flaring["total_flared_gas_mcf"].max()
    median_mcf      = flaring["total_flared_gas_mcf"].median()
    avg_intensity   = flaring["flaring_intensity_ratio"].mean() if has_intensity else None

    cards = "".join([
        kpi_card(fmt_num(total_flared, " MCF"),   "Total Flared Gas",       f"across {n_operators} operators"),
        kpi_card(fmt_num(top_flared_vol, " MCF"), "Highest Single Operator", top_flarer.title()[:30]),
        kpi_card(fmt_num(median_mcf, " MCF"),     "Median Operator Flaring", "50th percentile"),
        kpi_card(f"{avg_intensity*100:.1f}%" if avg_intensity else "—",
                 "Avg Flaring Intensity",         "flared / gross gas produced"),
    ])
    st.markdown(f'<div class="kpi-row">{cards}</div>', unsafe_allow_html=True)

    # ── Monthly Flaring Trend (real per-well data) ────────────────────────────
    if not flaring_ts.empty and "production_month" in flaring_ts.columns:
        section("Monthly Flaring Trend by Top Operators")
        flaring_ts["production_month"] = pd.to_datetime(flaring_ts["production_month"])
        flr_monthly = (
            flaring_ts.groupby(["production_month", "operator"])["flared_gas_mcf"]
            .sum().reset_index()
        )
        top_flarers = (flr_monthly.groupby("operator")["flared_gas_mcf"]
                       .sum().nlargest(8).index.tolist())
        flr_top = flr_monthly[flr_monthly["operator"].isin(top_flarers)]
        fig_ts = px.line(
            flr_top.sort_values("production_month"),
            x="production_month", y="flared_gas_mcf",
            color="operator",
            labels={"production_month": "Month", "flared_gas_mcf": "Flared Gas (MCF)",
                    "operator": "Operator"},
            color_discrete_sequence=PALETTE_MULTI,
            template=PLOTLY_TEMPLATE,
        )
        fig_ts.update_layout(height=340, margin=dict(l=0, r=0, t=10, b=10),
                             legend=dict(orientation="h", y=-0.3))
        st.plotly_chart(fig_ts, use_container_width=True)

    # ── Flaring Volume Ranking ────────────────────────────────────────────────
    section("Operator Flaring Volume Ranking (Real Data)")
    col1, col2 = st.columns([3, 2])

    color_map = {"Low": "#27ae60", "Medium": "#f39c12", "High": "#e74c3c"}

    with col1:
        fl_sorted = flaring.sort_values("total_flared_gas_mcf", ascending=True)
        fig = px.bar(
            fl_sorted, x="total_flared_gas_mcf", y="operator", orientation="h",
            color="volume_category", color_discrete_map=color_map,
            labels={"total_flared_gas_mcf": "Total Flared Gas (MCF)",
                    "operator": "", "volume_category": "Volume Tier"},
            template=PLOTLY_TEMPLATE,
        )
        fig.add_shape(
            type="line", x0=p33, x1=p33, y0=0, y1=1, xref="x", yref="paper",
            line=dict(dash="dash", color="orange", width=1.5),
        )
        fig.add_annotation(x=p33, y=1.04, xref="x", yref="paper",
                           text="Low/Med split", showarrow=False,
                           font=dict(color="#aaa", size=10))
        fig.add_shape(
            type="line", x0=p67, x1=p67, y0=0, y1=1, xref="x", yref="paper",
            line=dict(dash="dash", color="red", width=1.5),
        )
        fig.add_annotation(x=p67, y=1.04, xref="x", yref="paper",
                           text="Med/High split", showarrow=False,
                           font=dict(color="#aaa", size=10))
        fig.update_layout(height=480, margin=dict(l=0, r=30, t=30, b=10))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        section("Volume Tier Distribution")
        cat_counts = flaring["volume_category"].value_counts().reset_index()
        cat_counts.columns = ["category", "count"]
        fig2 = px.pie(
            cat_counts, names="category", values="count",
            color="category", color_discrete_map=color_map,
            hole=0.5, template=PLOTLY_TEMPLATE,
        )
        fig2.update_layout(height=240, margin=dict(l=0, r=0, t=10, b=10),
                           showlegend=True,
                           legend=dict(orientation="h", y=-0.15))
        fig2.update_traces(textinfo="percent+label")
        st.plotly_chart(fig2, use_container_width=True)

        section("Flaring by Volume Tier")
        summary = (
            flaring.groupby("volume_category")
            .agg(
                operators=("operator", "count"),
                total_flared=("total_flared_gas_mcf", "sum"),
                avg_flared=("total_flared_gas_mcf", "mean"),
            )
            .reset_index()
        )
        summary["total_flared"] = summary["total_flared"].map(fmt_num)
        summary["avg_flared"]   = summary["avg_flared"].map(fmt_num)
        summary.columns = ["Tier", "# Operators", "Total Flared", "Avg per Operator"]
        st.dataframe(summary, use_container_width=True, hide_index=True)

    # ── Top 10 Flaring Operators ───────────────────────────────────────────────
    section("Top 10 Flaring Operators — Absolute Volume (MCF)")
    top10 = flaring.nlargest(10, "total_flared_gas_mcf").copy()
    top10["operator_label"] = top10["operator"].str.title()
    fig3 = px.bar(
        top10, x="operator_label", y="total_flared_gas_mcf",
        color="volume_category", color_discrete_map=color_map,
        text=top10["total_flared_gas_mcf"].map(lambda v: fmt_num(v)),
        labels={"operator_label": "Operator",
                "total_flared_gas_mcf": "Total Flared Gas (MCF)",
                "volume_category": "Tier"},
        template=PLOTLY_TEMPLATE,
    )
    fig3.update_traces(textposition="outside")
    fig3.update_layout(height=380, margin=dict(l=0, r=0, t=10, b=80),
                       xaxis_tickangle=-30, uniformtext_minsize=8)
    st.plotly_chart(fig3, use_container_width=True)

    # ── Ethane vs Dry Gas ─────────────────────────────────────────────────────
    section("Ethane (Liquid-Rich) vs Dry Gas Production by Operator")
    col3, col4 = st.columns([3, 2])

    with col3:
        eth_agg = (
            ethane.groupby("operator")[["liquid_rich_production", "dry_gas_production"]]
            .sum().reset_index()
            .assign(total=lambda d: d["liquid_rich_production"] + d["dry_gas_production"])
            .sort_values("total", ascending=False)
            .head(14)
        )
        fig4 = go.Figure()
        fig4.add_bar(
            x=eth_agg["operator"], y=eth_agg["liquid_rich_production"] / 1e6,
            name="Liquid-Rich (Oil)", marker_color="#3498db",
        )
        fig4.add_bar(
            x=eth_agg["operator"], y=eth_agg["dry_gas_production"] / 1e6,
            name="Dry Gas", marker_color="#e67e22",
        )
        fig4.update_layout(
            barmode="stack", template=PLOTLY_TEMPLATE,
            height=360, margin=dict(l=0, r=0, t=10, b=10),
            legend=dict(orientation="h", y=-0.25),
            yaxis_title="Production (M units)",
        )
        st.plotly_chart(fig4, use_container_width=True)

    with col4:
        section("Liquid-Rich vs Dry Gas Split")
        eth_yr = (
            ethane.groupby("production_year")[["liquid_rich_production", "dry_gas_production"]]
            .sum().reset_index()
        )
        eth_yr["liquid_rich_%"] = (
            eth_yr["liquid_rich_production"] /
            (eth_yr["liquid_rich_production"] + eth_yr["dry_gas_production"])
        ) * 100
        fig5 = px.bar(
            eth_yr, x="production_year", y="liquid_rich_%",
            labels={"production_year": "Year", "liquid_rich_%": "Liquid-Rich Share (%)"},
            color_discrete_sequence=["#3498db"],
            template=PLOTLY_TEMPLATE,
        )
        fig5.add_hline(y=50, line_dash="dash", line_color="orange",
                       annotation_text="50% split", line_width=1.5)
        fig5.update_layout(height=360, margin=dict(l=0, r=0, t=10, b=10))
        st.plotly_chart(fig5, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — Forecasting
# ══════════════════════════════════════════════════════════════════════════════
def page_forecast(D: dict):
    fcst  = D["forecast"].copy()
    sprod = D["silver_prod"].copy()

    fcst["forecast_month"]   = pd.to_datetime(fcst["forecast_month"])
    sprod["production_month"] = pd.to_datetime(sprod["production_month"])

    # ── KPI Cards ─────────────────────────────────────────────────────────────
    by_entity = fcst.drop_duplicates(["entity_type", "entity_id"])
    avg_r2    = by_entity["r2_score"].mean()
    pct_hyp   = (fcst["model_type"] == "hyperbolic").mean() * 100
    n_well_fc = int((by_entity["entity_type"] == "well").sum())
    n_op_fc   = int((by_entity["entity_type"] == "operator").sum())
    n_bas_fc  = int((by_entity["entity_type"] == "basin").sum())
    horizon   = fcst["forecast_month"].max().strftime("%b %Y")

    cards = "".join([
        kpi_card(f"{avg_r2:.4f}",  "Average R² Score",    "goodness-of-fit on historical data"),
        kpi_card(f"{pct_hyp:.0f}%","Hyperbolic Model",    "winning model type (most realistic)"),
        kpi_card(str(n_well_fc),   "Wells Forecasted",    f"24-month horizon → {horizon}"),
        kpi_card(f"{n_op_fc} / {n_bas_fc}", "Operators / Basins", "forecast entities"),
    ])
    st.markdown(f'<div class="kpi-row">{cards}</div>', unsafe_allow_html=True)

    # ── Selector ──────────────────────────────────────────────────────────────
    section("Production Forecast — Historical vs Projected")
    c1, c2, c3 = st.columns(3)
    entity_type = c1.selectbox("Entity Type", ["basin", "operator", "well"],
                                format_func=lambda x: x.title(), key="fc_type")
    entities    = sorted(fcst[fcst["entity_type"] == entity_type]["entity_id"].unique())
    entity_id   = c2.selectbox("Select Entity", entities, key="fc_id")
    show_band   = c3.checkbox("Show ±15% confidence band", value=True, key="fc_band")

    # Historical aggregation
    if entity_type == "well":
        hist = (
            sprod[sprod["api_number"] == entity_id]
            .groupby(["production_month", "oil_and_gas_group"])["production"]
            .sum().unstack(fill_value=0).reset_index()
        )
    elif entity_type == "operator":
        hist = (
            sprod[sprod["operator"] == entity_id]
            .groupby(["production_month", "oil_and_gas_group"])["production"]
            .sum().unstack(fill_value=0).reset_index()
        )
    else:  # basin
        hist = (
            sprod[sprod["shale_play"] == entity_id]
            .groupby(["production_month", "oil_and_gas_group"])["production"]
            .sum().unstack(fill_value=0).reset_index()
        )
    hist.columns.name = None
    for col in ["O", "G"]:
        if col not in hist.columns:
            hist[col] = 0.0

    # Forecast subset
    fc_sub = (
        fcst[(fcst["entity_type"] == entity_type) & (fcst["entity_id"] == entity_id)]
        .sort_values("forecast_month")
    )

    if fc_sub.empty:
        st.warning("No forecast data found for this selection.")
    else:
        fig = go.Figure()

        # Historical oil
        fig.add_trace(go.Scatter(
            x=hist["production_month"], y=hist["O"],
            name="Historical Oil", mode="lines",
            line=dict(color="#3498db", width=2),
        ))
        # Historical gas
        fig.add_trace(go.Scatter(
            x=hist["production_month"], y=hist["G"],
            name="Historical Gas", mode="lines",
            line=dict(color="#e67e22", width=2),
        ))

        # Forecast oil
        fig.add_trace(go.Scatter(
            x=fc_sub["forecast_month"], y=fc_sub["forecast_oil_bbl"],
            name="Forecast Oil", mode="lines",
            line=dict(color="#3498db", width=2, dash="dash"),
        ))
        # Forecast gas
        fig.add_trace(go.Scatter(
            x=fc_sub["forecast_month"], y=fc_sub["forecast_gas_mcf"],
            name="Forecast Gas", mode="lines",
            line=dict(color="#e67e22", width=2, dash="dash"),
        ))

        # Confidence bands
        if show_band:
            fig.add_trace(go.Scatter(
                x=pd.concat([fc_sub["forecast_month"], fc_sub["forecast_month"].iloc[::-1]]),
                y=pd.concat([fc_sub["forecast_oil_bbl"] * 1.15,
                             fc_sub["forecast_oil_bbl"].iloc[::-1] * 0.85]),
                fill="toself", fillcolor="rgba(52,152,219,0.12)",
                line=dict(width=0), showlegend=True, name="Oil ±15%",
            ))
            fig.add_trace(go.Scatter(
                x=pd.concat([fc_sub["forecast_month"], fc_sub["forecast_month"].iloc[::-1]]),
                y=pd.concat([fc_sub["forecast_gas_mcf"] * 1.15,
                             fc_sub["forecast_gas_mcf"].iloc[::-1] * 0.85]),
                fill="toself", fillcolor="rgba(230,126,34,0.10)",
                line=dict(width=0), showlegend=True, name="Gas ±15%",
            ))

        # Vertical divider at forecast start — use add_shape (avoids Plotly 6 timestamp bug)
        div_date = fc_sub["forecast_month"].min()
        fig.add_shape(
            type="line",
            x0=div_date, x1=div_date, y0=0, y1=1,
            xref="x", yref="paper",
            line=dict(dash="dot", color="rgba(180,180,180,0.55)", width=1.5),
        )
        fig.add_annotation(
            x=div_date, y=1.04, xref="x", yref="paper",
            text="▶ Forecast Start", showarrow=False,
            font=dict(color="#aaa", size=11),
        )

        fig.update_layout(
            template=PLOTLY_TEMPLATE, height=430,
            margin=dict(l=0, r=0, t=20, b=10),
            legend=dict(orientation="h", y=-0.2),
            xaxis_title="Month", yaxis_title="Production",
            title=dict(
                text=f"<b>{entity_id}</b> — Arps {fc_sub['model_type'].iloc[0].title()} Decline  |  R² = {fc_sub['r2_score'].iloc[0]:.4f}",
                font=dict(size=14),
            ),
        )
        st.plotly_chart(fig, use_container_width=True)

        # Model parameters card
        p = fc_sub.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Model",    p["model_type"].title())
        c2.metric("R² Score", f"{p['r2_score']:.4f}")
        c3.metric("Qᵢ (Initial Rate)", fmt_num(p["qi"]))
        c4.metric("Dᵢ (Decline Rate)", f"{p['di']:.4f}/month")

    # ── R² Distribution ───────────────────────────────────────────────────────
    section("Model Fit Quality Across All Entities")
    col1, col2 = st.columns(2)

    with col1:
        well_r2 = by_entity[by_entity["entity_type"] == "well"]["r2_score"].dropna()
        fig_r2 = go.Figure()
        fig_r2.add_trace(go.Histogram(
            x=well_r2, nbinsx=30,
            marker_color="#3498db", opacity=0.8, name="R² score",
        ))
        fig_r2.add_vline(
            x=float(well_r2.mean()), line_dash="dash", line_color="#f5a623",
            annotation_text=f"Mean = {well_r2.mean():.4f}",
            annotation_position="top",
        )
        fig_r2.update_layout(
            template=PLOTLY_TEMPLATE, height=300,
            xaxis_title="R² Score", yaxis_title="# Wells",
            title="R² Distribution — Well-Level Fits",
            margin=dict(l=0, r=0, t=40, b=10),
        )
        st.plotly_chart(fig_r2, use_container_width=True)

    with col2:
        model_counts = (
            by_entity.groupby(["entity_type", "model_type"])
            .size().reset_index(name="count")
        )
        fig_mc = px.bar(
            model_counts, x="entity_type", y="count", color="model_type",
            barmode="group",
            labels={"entity_type": "Entity Type", "count": "# Entities",
                    "model_type": "Model"},
            color_discrete_sequence=PALETTE_MULTI,
            template=PLOTLY_TEMPLATE,
        )
        fig_mc.update_layout(
            height=300, title="Best-Fit Model by Entity Type",
            margin=dict(l=0, r=0, t=40, b=10),
            legend=dict(orientation="h", y=-0.3),
        )
        st.plotly_chart(fig_mc, use_container_width=True)

    # ── 24-Month Forecast Summary Table ───────────────────────────────────────
    section("24-Month Cumulative Forecast Summary")
    fc_summary = (
        fcst.groupby(["entity_type", "entity_id", "model_type", "r2_score"])
        .agg(
            forecast_oil_bbl=("forecast_oil_bbl", "sum"),
            forecast_gas_mcf=("forecast_gas_mcf", "sum"),
        )
        .reset_index()
        .sort_values(["entity_type", "forecast_oil_bbl"], ascending=[True, False])
    )
    fc_summary["forecast_oil_bbl"] = fc_summary["forecast_oil_bbl"].map(fmt_num)
    fc_summary["forecast_gas_mcf"] = fc_summary["forecast_gas_mcf"].map(fmt_num)
    fc_summary["r2_score"]         = fc_summary["r2_score"].map(lambda v: f"{v:.4f}")
    fc_summary.columns = ["Type", "Entity", "Model", "R²", "Forecast Oil", "Forecast Gas"]
    st.dataframe(fc_summary, use_container_width=True, height=350)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 5 — Well Economics
# ══════════════════════════════════════════════════════════════════════════════
def page_economics(D: dict):
    econ  = D.get("economics",  pd.DataFrame())
    ip    = D.get("ip_bench",   pd.DataFrame())
    three = D.get("three_stream", pd.DataFrame())

    if econ.empty:
        st.info("Well economics data not yet available. Run the pipeline first: `python run_pipeline.py --stage gold`")
        return

    econ = econ.copy()

    # ── Sidebar filters ───────────────────────────────────────────────────────
    formations = sorted(econ["formation"].dropna().unique()) if "formation" in econ.columns else []
    sel_formations = st.sidebar.multiselect("Formation", formations, default=formations, key="econ_form")
    if sel_formations:
        econ = econ[econ["formation"].isin(sel_formations)]

    econ_cats = sorted(econ["economics_category"].dropna().unique()) if "economics_category" in econ.columns else []
    sel_cats = st.sidebar.multiselect("Economics Category", econ_cats, default=econ_cats, key="econ_cat")
    if sel_cats:
        econ = econ[econ["economics_category"].isin(sel_cats)]

    # ── KPI Cards ─────────────────────────────────────────────────────────────
    avg_be      = econ["breakeven_oil_price"].mean() if "breakeven_oil_price" in econ.columns else None
    median_irr  = econ["irr"].median() if "irr" in econ.columns else None
    total_npv   = econ["npv"].sum() if "npv" in econ.columns else None
    pct_econ    = (econ["economics_category"] == "Economic").mean() * 100 if "economics_category" in econ.columns else None

    cards = "".join([
        kpi_card(f"${avg_be:.1f}" if avg_be else "—",        "Avg Breakeven Price",   "$/BBL oil"),
        kpi_card(f"{median_irr:.1f}%" if median_irr else "—", "Median IRR",            "internal rate of return"),
        kpi_card(fmt_num(total_npv, " $M") if total_npv else "—", "Total Portfolio NPV", "net present value"),
        kpi_card(f"{pct_econ:.0f}%" if pct_econ else "—",    "% Economic Wells",      "at current WTI price"),
    ])
    st.markdown(f'<div class="kpi-row">{cards}</div>', unsafe_allow_html=True)

    # ── Breakeven Price Distribution ──────────────────────────────────────────
    section("Breakeven Oil Price Distribution by Formation")
    if "breakeven_oil_price" in econ.columns and econ["breakeven_oil_price"].notna().any():
        avg_wti = econ["avg_wti_price"].mean() if "avg_wti_price" in econ.columns else 65.0
        color_col = "formation" if "formation" in econ.columns else None
        fig1 = px.histogram(
            econ.dropna(subset=["breakeven_oil_price"]),
            x="breakeven_oil_price",
            color=color_col,
            nbins=40,
            color_discrete_sequence=PALETTE_MULTI,
            labels={"breakeven_oil_price": "Breakeven Oil Price ($/BBL)"},
            template=PLOTLY_TEMPLATE,
        )
        fig1.add_vline(x=avg_wti, line_dash="dash", line_color="#f5a623",
                       annotation_text=f"Avg WTI ${avg_wti:.0f}",
                       annotation_position="top right")
        fig1.update_layout(height=340, margin=dict(l=0, r=0, t=20, b=10),
                           legend=dict(orientation="h", y=-0.25))
        st.plotly_chart(fig1, use_container_width=True)

    # ── Risk-Return Scatter ───────────────────────────────────────────────────
    col1, col2 = st.columns(2)
    with col1:
        section("Risk-Return: Breakeven vs IRR")
        if all(c in econ.columns for c in ["breakeven_oil_price", "irr"]):
            scatter_df = econ.dropna(subset=["breakeven_oil_price", "irr"])
            size_col = "eur" if ("eur" in scatter_df.columns and scatter_df["eur"].notna().any()) else None
            if size_col:
                scatter_df = scatter_df.dropna(subset=[size_col])
            color_col = "economics_category" if "economics_category" in scatter_df.columns else None
            cat_colors = {"Economic": "#27ae60", "Marginal": "#f39c12",
                          "Uneconomic": "#e74c3c", "Unknown": "#7f8c8d"}
            fig2 = px.scatter(
                scatter_df,
                x="breakeven_oil_price", y="irr",
                size=size_col,
                color=color_col,
                color_discrete_map=cat_colors if color_col else None,
                hover_name="well_name" if "well_name" in scatter_df.columns else None,
                hover_data={"operator": True, "npv": True} if "operator" in scatter_df.columns else {},
                labels={"breakeven_oil_price": "Breakeven ($/BBL)", "irr": "IRR (%)",
                        "economics_category": "Category"},
                template=PLOTLY_TEMPLATE,
            )
            fig2.update_layout(height=340, margin=dict(l=0, r=0, t=20, b=10))
            st.plotly_chart(fig2, use_container_width=True)

    with col2:
        section("EUR vs Lateral Length (Drilling Efficiency)")
        if all(c in econ.columns for c in ["lateral_length_ft", "eur"]):
            eur_df = econ.dropna(subset=["lateral_length_ft", "eur"])
            color_col = "formation" if "formation" in eur_df.columns else None
            fig3 = px.scatter(
                eur_df,
                x="lateral_length_ft", y="eur",
                color=color_col,
                color_discrete_sequence=PALETTE_MULTI,
                trendline="ols",
                labels={"lateral_length_ft": "Lateral Length (ft)", "eur": "EUR",
                        "formation": "Formation"},
                template=PLOTLY_TEMPLATE,
            )
            fig3.update_layout(height=340, margin=dict(l=0, r=0, t=20, b=10))
            st.plotly_chart(fig3, use_container_width=True)

    # ── IP Benchmarks ─────────────────────────────────────────────────────────
    if not ip.empty and "ip30" in ip.columns:
        section("IP30 Performance by Formation (P50 Benchmark)")
        ip_filt = ip.dropna(subset=["ip30"])
        if "formation" in ip_filt.columns:
            fig4 = px.box(
                ip_filt, x="formation", y="ip30",
                color="formation",
                color_discrete_sequence=PALETTE_MULTI,
                labels={"formation": "Formation", "ip30": "IP30 (BOE)"},
                template=PLOTLY_TEMPLATE,
                points="outliers",
            )
            fig4.update_layout(height=340, margin=dict(l=0, r=0, t=20, b=10),
                               showlegend=False)
            st.plotly_chart(fig4, use_container_width=True)

        col3, col4 = st.columns(2)
        with col3:
            section("IP30 vs EUR Correlation")
            ip_eur = ip.dropna(subset=["ip30"]).copy()
            if "eur" not in ip_eur.columns and not econ.empty and "eur" in econ.columns:
                ip_eur = ip_eur.merge(
                    econ[["api_number", "eur"]].dropna(), on="api_number", how="left")
            if "eur" in ip_eur.columns and ip_eur["eur"].notna().any():
                ip_eur2 = ip_eur.dropna(subset=["ip30", "eur"])
                color_col2 = "formation" if "formation" in ip_eur2.columns else None
                fig5 = px.scatter(
                    ip_eur2, x="ip30", y="eur", color=color_col2,
                    color_discrete_sequence=PALETTE_MULTI,
                    labels={"ip30": "IP30 (BOE)", "eur": "EUR"},
                    template=PLOTLY_TEMPLATE,
                )
                fig5.update_layout(height=300, margin=dict(l=0, r=0, t=20, b=10))
                st.plotly_chart(fig5, use_container_width=True)

        with col4:
            section("Performance Tier Distribution")
            if "performance_tier" in ip.columns:
                tier_counts = ip["performance_tier"].value_counts().reset_index()
                tier_counts.columns = ["tier", "count"]
                tier_colors = {"Top": "#27ae60", "Mid": "#f39c12", "Bottom": "#e74c3c"}
                fig6 = px.pie(
                    tier_counts, names="tier", values="count",
                    color="tier", color_discrete_map=tier_colors,
                    hole=0.45, template=PLOTLY_TEMPLATE,
                )
                fig6.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=10))
                fig6.update_traces(textinfo="percent+label")
                st.plotly_chart(fig6, use_container_width=True)

    # ── Water Cut Ranking ─────────────────────────────────────────────────────
    if not econ.empty and "water_cut_pct" in econ.columns:
        wat_df = econ.dropna(subset=["water_cut_pct"]).nlargest(20, "water_cut_pct")
        if not wat_df.empty:
            section("Water Cut by Operator (Top 20 Wells)")
            color_col3 = "formation" if "formation" in wat_df.columns else None
            fig7 = px.bar(
                wat_df.sort_values("water_cut_pct"),
                x="water_cut_pct", y="well_name" if "well_name" in wat_df.columns else "api_number",
                orientation="h",
                color=color_col3,
                color_discrete_sequence=PALETTE_MULTI,
                labels={"water_cut_pct": "Water Cut (%)",
                        "well_name": "Well", "api_number": "API"},
                template=PLOTLY_TEMPLATE,
            )
            fig7.update_layout(height=420, margin=dict(l=0, r=0, t=10, b=10))
            st.plotly_chart(fig7, use_container_width=True)

    # ── Economics Summary Table ────────────────────────────────────────────────
    section("Well Economics Summary")
    tbl_cols = ["well_name", "operator", "formation", "economics_category",
                "breakeven_oil_price", "irr", "npv", "eur",
                "cumulative_oil_bbl", "cumulative_revenue_usd", "water_cut_pct"]
    avail = [c for c in tbl_cols if c in econ.columns]
    tbl = econ[avail].copy()
    for c in ["cumulative_oil_bbl", "cumulative_revenue_usd", "npv", "eur"]:
        if c in tbl.columns:
            tbl[c] = tbl[c].map(fmt_num)
    for c in ["irr", "breakeven_oil_price"]:
        if c in tbl.columns:
            tbl[c] = tbl[c].map(lambda v: f"{v:.1f}" if pd.notna(v) else "—")
    if "water_cut_pct" in tbl.columns:
        tbl["water_cut_pct"] = tbl["water_cut_pct"].map(
            lambda v: f"{v:.1f}%" if pd.notna(v) else "—")
    tbl.columns = [c.replace("_", " ").title() for c in tbl.columns]
    st.dataframe(tbl, use_container_width=True, height=350)


# ══════════════════════════════════════════════════════════════════════════════
# Main App
# ══════════════════════════════════════════════════════════════════════════════
def main():
    # ── Sidebar ────────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("""
        <div style="text-align:center; padding: 12px 0 20px;">
          <div style="font-size:2.2rem">🛢️</div>
          <div style="font-size:1.1rem; font-weight:700; color:#f5a623;">
            Well Production<br>Analytics
          </div>
          <div style="font-size:0.72rem; color:#7a9; margin-top:6px;">
            Shell UnextGen Hackathon<br>Sept – Oct 2023
          </div>
        </div>
        """, unsafe_allow_html=True)

        page = st.radio(
            "Navigation",
            ["🏠 Overview", "📈 Production Trends", "🔥 Flaring & ESG",
             "🔮 Forecasting", "💰 Well Economics"],
            label_visibility="collapsed",
        )

        st.markdown("---")
        st.markdown("""
        <div style="font-size:0.72rem; color:#7a9; padding: 8px 0;">
          <b style="color:#f5a623;">Stack</b><br>
          PySpark 3.5 · Delta Lake 3.1<br>
          Medallion Architecture<br>
          Arps DCA · Streamlit<br><br>
          <b style="color:#f5a623;">Data</b><br>
          Wells-Dataset · Bakken<br>
          Williston Basin, ND<br>
          14 Delta tables
        </div>
        """, unsafe_allow_html=True)

    # ── Load Data ──────────────────────────────────────────────────────────────
    with st.spinner("Loading data from Delta Lake..."):
        D = load_all()

    # ── Page Title ─────────────────────────────────────────────────────────────
    titles = {
        "🏠 Overview":          ("🏠 Overview", "Portfolio-level KPIs across all operators, wells, and basins · Bakken/Williston Basin"),
        "📈 Production Trends": ("📈 Production Trends", "Time-series analysis by basin and operator · MoM/YoY growth rates"),
        "🔥 Flaring & ESG":     ("🔥 Flaring & ESG", "Operator flaring intensity rankings · Monthly trends · Ethane vs dry gas commodity mix"),
        "🔮 Forecasting":       ("🔮 Forecasting", "Arps decline curve analysis · 24-month production projections on real well data"),
        "💰 Well Economics":    ("💰 Well Economics", "Breakeven pricing · IRR/NPV · EUR vs lateral length · IP benchmarks · Water cut"),
    }
    title, subtitle = titles[page]
    st.markdown(f"## {title}")
    st.caption(subtitle)
    st.markdown("---")

    # ── Route to Page ──────────────────────────────────────────────────────────
    if page == "🏠 Overview":
        page_overview(D)
    elif page == "📈 Production Trends":
        page_trends(D)
    elif page == "🔥 Flaring & ESG":
        page_esg(D)
    elif page == "🔮 Forecasting":
        page_forecast(D)
    elif page == "💰 Well Economics":
        page_economics(D)


if __name__ == "__main__":
    main()
