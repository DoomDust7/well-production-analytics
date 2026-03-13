"""
Well Production Forecasting & Performance Analytics — Streamlit Dashboard
Shell UnextGen Hackathon | Sept–Oct 2023

Pages:
  1. 🏠 Overview         — KPI cards, top operators, basin distribution
  2. 📈 Production Trends — time-series by basin/operator, quarterly heatmap
  3. 🔥 Flaring & ESG    — flaring intensity, ethane vs dry gas, scatter analysis
  4. 🔮 Forecasting       — Arps decline curves, historical vs forecast, model quality
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


@st.cache_data(show_spinner=False)
def _load(path: str) -> pd.DataFrame:
    """Read a Delta table into a Pandas DataFrame."""
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

    cards = "".join([
        kpi_card(fmt_num(n_wells),      "Total Wells",       "unique API numbers"),
        kpi_card(str(n_operators),      "Operators",         "active producers"),
        kpi_card(fmt_num(total_oil, " BBL"), "Cumulative Oil",  "2018 – 2023"),
        kpi_card(fmt_num(total_gas, " MCF"), "Cumulative Gas",  "2018 – 2023"),
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
    fig3 = px.scatter(
        op_bubble,
        x="total_oil_bbl", y="total_gas_mcf",
        size="well_count", color="flaring_intensity_ratio",
        color_continuous_scale="RdYlGn_r",
        hover_name="operator",
        hover_data={"well_count": True, "flaring_intensity_ratio": ":.3f"},
        labels={
            "total_oil_bbl": "Total Oil (BBL)",
            "total_gas_mcf": "Total Gas (MCF)",
            "flaring_intensity_ratio": "Flaring Ratio",
        },
        template=PLOTLY_TEMPLATE,
    )
    fig3.update_layout(height=400, margin=dict(l=0, r=0, t=10, b=10))
    fig3.update_coloraxes(colorbar_title="Flaring<br>Intensity")
    st.plotly_chart(fig3, use_container_width=True)

    # ── Row 3: Well Summary Table ─────────────────────────────────────────────
    section("Well Portfolio Summary")
    display_cols = [
        "well_name", "operator", "shale_play", "basin",
        "first_production_month", "last_production_month",
        "cumulative_oil_bbl", "cumulative_gas_mcf",
        "peak_oil_production", "active_months",
    ]
    available = [c for c in display_cols if c in well.columns]
    disp = well[available].copy()
    disp["cumulative_oil_bbl"]  = disp["cumulative_oil_bbl"].map(lambda v: fmt_num(v))
    disp["cumulative_gas_mcf"]  = disp["cumulative_gas_mcf"].map(lambda v: fmt_num(v))
    disp["peak_oil_production"] = disp["peak_oil_production"].map(lambda v: fmt_num(v))
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
    flaring = D["flaring"].copy()
    ethane  = D["ethane"].copy()

    # ── KPI Cards ─────────────────────────────────────────────────────────────
    total_flared  = flaring["total_flared_gas_mcf"].sum()
    high_count    = int((flaring["flaring_category"] == "High").sum())
    avg_intensity = flaring["flaring_intensity_ratio"].dropna().mean()
    low_count     = int((flaring["flaring_category"] == "Low").sum())

    cards = "".join([
        kpi_card(fmt_num(total_flared, " MCF"), "Total Flared Gas", "across all operators"),
        kpi_card(f"{avg_intensity:.1%}",        "Avg Flaring Intensity", "flaring / gas production"),
        kpi_card(str(high_count),               "High-Intensity Operators", "flaring > 15% of gas"),
        kpi_card(str(low_count),                "Low-Intensity Operators", "flaring < 5% of gas"),
    ])
    st.markdown(f'<div class="kpi-row">{cards}</div>', unsafe_allow_html=True)

    # ── Flaring Intensity Ranking ─────────────────────────────────────────────
    section("Operator Flaring Intensity Ranking")
    col1, col2 = st.columns([3, 2])

    with col1:
        color_map = {"Low": "#27ae60", "Medium": "#f39c12", "High": "#e74c3c"}
        fl_sorted = flaring.dropna(subset=["flaring_intensity_ratio"]).sort_values(
            "flaring_intensity_ratio", ascending=True
        )
        fig = px.bar(
            fl_sorted, x="flaring_intensity_ratio", y="operator", orientation="h",
            color="flaring_category", color_discrete_map=color_map,
            labels={"flaring_intensity_ratio": "Flaring Intensity (%)",
                    "operator": "", "flaring_category": "Category"},
            template=PLOTLY_TEMPLATE,
        )
        fig.add_vline(x=0.05,  line_dash="dash", line_color="orange", line_width=1.5,
                      annotation_text="Medium (5%)",  annotation_position="top")
        fig.add_vline(x=0.15, line_dash="dash", line_color="red",    line_width=1.5,
                      annotation_text="High (15%)", annotation_position="top")
        fig.update_layout(height=440, margin=dict(l=0, r=20, t=30, b=10))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        section("Category Distribution")
        cat_counts = flaring["flaring_category"].value_counts().reset_index()
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

        section("Flaring Summary")
        summary = (
            flaring.groupby("flaring_category")
            .agg(
                operators=("operator", "count"),
                avg_intensity=("flaring_intensity_ratio", "mean"),
                total_flared=("total_flared_gas_mcf", "sum"),
            )
            .reset_index()
        )
        summary["avg_intensity"] = summary["avg_intensity"].map(lambda v: f"{v:.1%}")
        summary["total_flared"]  = summary["total_flared"].map(lambda v: fmt_num(v))
        summary.columns = ["Category", "# Operators", "Avg Intensity", "Total Flared (MCF)"]
        st.dataframe(summary, use_container_width=True, hide_index=True)

    # ── Flaring vs Production Scatter ─────────────────────────────────────────
    section("Flaring Volume vs Gas Production — Outlier Analysis")
    fl_scatter = flaring.dropna(subset=["total_flared_gas_mcf", "total_gas_production_mcf"])
    fig3 = px.scatter(
        fl_scatter,
        x="total_gas_production_mcf", y="total_flared_gas_mcf",
        size="total_flared_gas_mcf", color="flaring_category",
        color_discrete_map=color_map,
        hover_name="operator",
        hover_data={"flaring_intensity_ratio": ":.3f"},
        labels={
            "total_gas_production_mcf": "Total Gas Production (MCF)",
            "total_flared_gas_mcf": "Total Flared Gas (MCF)",
            "flaring_category": "Category",
        },
        template=PLOTLY_TEMPLATE,
    )
    # Diagonal reference line (1:1)
    max_v = max(fl_scatter["total_gas_production_mcf"].max(),
                fl_scatter["total_flared_gas_mcf"].max())
    fig3.add_trace(go.Scatter(
        x=[0, max_v], y=[0, max_v * 0.15],
        mode="lines", line=dict(dash="dot", color="orange", width=1.5),
        name="15% threshold", showlegend=True,
    ))
    fig3.update_layout(height=380, margin=dict(l=0, r=0, t=10, b=10))
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
            ["🏠 Overview", "📈 Production Trends", "🔥 Flaring & ESG", "🔮 Forecasting"],
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
          300 wells · 72 months<br>
          8 Delta tables<br>
          2,904 forecast rows
        </div>
        """, unsafe_allow_html=True)

    # ── Load Data ──────────────────────────────────────────────────────────────
    with st.spinner("Loading data from Delta Lake..."):
        D = load_all()

    # ── Page Title ─────────────────────────────────────────────────────────────
    titles = {
        "🏠 Overview":          ("🏠 Overview", "Portfolio-level KPIs across all operators, wells, and basins"),
        "📈 Production Trends": ("📈 Production Trends", "Time-series analysis by basin and operator · MoM/YoY growth rates"),
        "🔥 Flaring & ESG":     ("🔥 Flaring & ESG", "Operator flaring intensity rankings · Ethane vs dry gas commodity mix"),
        "🔮 Forecasting":       ("🔮 Forecasting", "Arps decline curve analysis · 24-month production projections"),
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


if __name__ == "__main__":
    main()
