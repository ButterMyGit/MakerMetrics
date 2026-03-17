"""Streamlit analytics dashboard for TheSlabGuy sales data."""

import os
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from streamlit_autorefresh import st_autorefresh

load_dotenv()

import firebase_admin
from firebase_admin import credentials, firestore

from watcher import WATCH_DIR, process_existing_csvs

# ---------------------------------------------------------------------------
# Page config — must be first Streamlit call
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="TheSlabGuy — Sales Dashboard",
    page_icon="🃏",
    layout="wide",
    initial_sidebar_state="expanded",
)

st_autorefresh(interval=20_000, key="dashboard_live_refresh")

# ---------------------------------------------------------------------------
# Global style
# ---------------------------------------------------------------------------
st.markdown(
    """
    <style>
    [data-testid="stAppViewContainer"] {
        background:
            radial-gradient(circle at top left, rgba(46,117,182,0.20), transparent 26%),
            linear-gradient(180deg, #0d1321 0%, #121a2b 100%);
        color: #edf3ff;
    }
    [data-testid="stHeader"] { background: rgba(0, 0, 0, 0); }
    [data-testid="stSidebar"] { background-color: #11192b; }
    [data-testid="stSidebar"] * { color: #edf3ff !important; }
    .block-container { padding-top: 1.35rem; }
    .kpi-card {
        background: rgba(255, 255, 255, 0.06);
        border-left: 4px solid #2E75B6;
        border-radius: 10px;
        padding: 14px 18px;
        margin-bottom: 4px;
        box-shadow: 0 10px 28px rgba(0, 0, 0, 0.18);
    }
    .kpi-label {
        font-size: 12px;
        color: #9fb2d1;
        text-transform: uppercase;
        letter-spacing: .05em;
    }
    .kpi-value {
        font-size: 28px;
        font-weight: 700;
        color: #f8fbff;
        margin-top: 2px;
    }
    .section-header {
        font-size: 18px;
        font-weight: 700;
        color: #f3f7ff;
        border-bottom: 2px solid #2E75B6;
        padding-bottom: 4px;
        margin: 24px 0 12px 0;
    }
    .chart-caption {
        color: #bed0ec;
        font-size: 0.95rem;
        margin: 0 0 0.35rem 0;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

_CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    margin=dict(l=18, r=18, t=24, b=18),
    font=dict(color="#edf3ff"),
    legend=dict(font=dict(color="#edf3ff")),
)

PRIMARY = "#2E75B6"
SECONDARY = "#e6a817"
CATEGORICAL_COLORS = [
    "#2E75B6",
    "#E6A817",
    "#2FB7A1",
    "#E86F51",
    "#7A5AF8",
    "#5BC0EB",
    "#9BC53D",
    "#F08A5D",
    "#D95D8B",
    "#6C8EAD",
]

# ---------------------------------------------------------------------------
# Firebase helpers
# ---------------------------------------------------------------------------
@st.cache_resource
def get_db():
    cred_path = os.getenv("FIREBASE_CREDENTIALS", "secrets/firebase.json")
    cred_file = Path(cred_path)
    if cred_file.is_dir():
        raise RuntimeError(
            f"FIREBASE_CREDENTIALS points to a directory, expected a JSON file: {cred_path}. "
            "In Docker, mount ./secrets to /run/secrets and place firebase.json inside ./secrets/."
        )
    if not cred_file.exists():
        raise FileNotFoundError(
            f"Firebase credentials file not found at {cred_path}. "
            "Place your service account key at secrets/firebase.json."
        )
    if not firebase_admin._apps:
        cred = credentials.Certificate(cred_path)
        firebase_admin.initialize_app(cred)
    return firestore.client()


@st.cache_data(ttl=15)
def load_data() -> tuple[pd.DataFrame, str]:
    db   = get_db()
    docs = db.collection("sales").stream()
    rows = [d.to_dict() for d in docs]
    load_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if not rows:
        return pd.DataFrame(), load_time

    df = pd.DataFrame(rows)

    # Drop internal Firestore timestamp fields (not safe for display)
    for col in ["_created_at", "_updated_at"]:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    # Dates
    for col in ["Sale Date", "Date Paid", "Date Shipped"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Numerics
    numeric_cols = [
        "Price", "Item Total", "Discount Amount", "Shipping", "Shipping Discount",
        "Sales Tax", "Order Value", "Order Total", "Card Processing Fees", "Order Net",
        "Adjusted Order Total", "Adjusted Card Processing Fees",
        "Adjusted Net Order Amount", "VAT Paid by Buyer", "InPerson Discount",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Quantity
    if "Quantity" in df.columns:
        df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0).astype(int)

    # Helper columns
    if "Sale Date" in df.columns:
        df["Year-Month"] = df["Sale Date"].dt.to_period("M").astype(str)
        df["Week"]       = df["Sale Date"].dt.to_period("W").astype(str)

    return df, load_time


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def section(title: str):
    st.markdown(f'<div class="section-header">{title}</div>', unsafe_allow_html=True)


def kpi_card(label: str, value: str):
    st.markdown(
        f'<div class="kpi-card"><div class="kpi-label">{label}</div>'
        f'<div class="kpi-value">{value}</div></div>',
        unsafe_allow_html=True,
    )


def fmt_currency(val: float) -> str:
    return f"${val:,.2f}"


def empty_chart_note():
    st.info("No data for the selected filters.")


def chart_caption(text: str):
    st.markdown(f'<div class="chart-caption">{text}</div>', unsafe_allow_html=True)


def apply_chart_theme(fig: go.Figure, height: int, *, pie_like: bool = False) -> go.Figure:
    margin = dict(l=22, r=22, t=18, b=22) if pie_like else _CHART_LAYOUT["margin"]
    fig.update_layout(
        paper_bgcolor=_CHART_LAYOUT["paper_bgcolor"],
        plot_bgcolor=_CHART_LAYOUT["plot_bgcolor"],
        margin=margin,
        font=_CHART_LAYOUT["font"],
        legend=_CHART_LAYOUT["legend"],
        height=height,
    )
    fig.update_xaxes(
        color="#edf3ff",
        gridcolor="rgba(237,243,255,0.10)",
        zeroline=False,
        linecolor="rgba(237,243,255,0.18)",
    )
    fig.update_yaxes(
        color="#edf3ff",
        gridcolor="rgba(237,243,255,0.10)",
        zeroline=False,
        linecolor="rgba(237,243,255,0.18)",
    )
    return fig


def style_donut(fig: go.Figure, height: int) -> go.Figure:
    fig.update_traces(
        textinfo="none",
        marker=dict(line=dict(color="#121a2b", width=2)),
    )
    fig = apply_chart_theme(fig, height, pie_like=True)
    fig.update_layout(
        showlegend=True,
        legend=dict(
            font=dict(color="#edf3ff"),
            bgcolor="rgba(0,0,0,0)",
            orientation="v",
            x=1.0,
            y=1.0,
            xanchor="left",
            yanchor="top",
            title_text="",
        ),
    )
    return fig


def get_order_level_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "Order ID" not in df.columns:
        return df.copy()
    sort_cols = [col for col in ["Sale Date", "Transaction ID"] if col in df.columns]
    ordered = df.sort_values(sort_cols) if sort_cols else df.copy()
    return ordered.drop_duplicates(subset=["Order ID"], keep="first").copy()


@st.cache_data(ttl=900)
def build_order_forecast(df: pd.DataFrame, months_ahead: int) -> tuple[pd.Series, pd.Series, pd.DataFrame, str]:
    order_df = get_order_level_df(df)
    if order_df.empty or "Sale Date" not in order_df.columns or "Order ID" not in order_df.columns:
        return pd.Series(dtype=float), pd.Series(dtype=float), pd.DataFrame(), "No model"

    working = order_df.dropna(subset=["Sale Date"]).copy()
    if working.empty:
        return pd.Series(dtype=float), pd.Series(dtype=float), pd.DataFrame(), "No model"

    working["Day"] = working["Sale Date"].dt.normalize()
    daily = working.groupby("Day")["Order ID"].nunique().sort_index().astype(float)
    full_index = pd.date_range(daily.index.min(), daily.index.max(), freq="D")
    daily = daily.reindex(full_index, fill_value=0.0)

    horizon_days = max(35, months_ahead * 35)
    forecast_index = pd.date_range(daily.index.max() + pd.Timedelta(days=1), periods=horizon_days, freq="D")
    model_name = "Recent average"

    if len(daily) >= 56 and daily.nunique() > 1:
        try:
            fit = ExponentialSmoothing(
                daily,
                trend="add",
                seasonal="add",
                seasonal_periods=7,
                initialization_method="estimated",
            ).fit(optimized=True)
            forecast = fit.forecast(horizon_days)
            model_name = "Holt-Winters exponential smoothing"
        except Exception:
            forecast = pd.Series(daily.tail(28).mean(), index=forecast_index)
    elif len(daily) >= 14 and daily.nunique() > 1:
        try:
            fit = ExponentialSmoothing(
                daily,
                trend="add",
                initialization_method="estimated",
            ).fit(optimized=True)
            forecast = fit.forecast(horizon_days)
            model_name = "Trend exponential smoothing"
        except Exception:
            forecast = pd.Series(daily.tail(28).mean(), index=forecast_index)
    else:
        forecast = pd.Series(daily.tail(min(14, len(daily))).mean(), index=forecast_index)

    forecast = pd.Series(forecast, index=forecast_index).clip(lower=0)
    monthly = forecast.groupby(forecast.index.to_period("M")).sum().head(months_ahead)
    monthly_df = monthly.reset_index()
    monthly_df.columns = ["Month", "Projected Orders"]
    monthly_df["Month"] = monthly_df["Month"].astype(str)
    monthly_df["Projected Orders"] = monthly_df["Projected Orders"].round(1)
    return daily, forecast, monthly_df, model_name


@st.cache_data(ttl=900)
def build_buyer_clusters(df: pd.DataFrame, requested_clusters: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty or "Buyer User ID" not in df.columns:
        return pd.DataFrame(), pd.DataFrame()

    order_df = get_order_level_df(df)
    order_df = order_df[order_df["Buyer User ID"].notna()].copy()
    if order_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    order_df["Has Coupon"] = 0
    if "Coupon Code" in order_df.columns:
        order_df["Has Coupon"] = order_df["Coupon Code"].fillna("").str.strip().ne("").astype(int)

    if "Ship State" in order_df.columns:
        order_df["Cluster State"] = order_df["Ship State"].fillna("Unknown")
    else:
        order_df["Cluster State"] = "Unknown"

    top_states = order_df["Cluster State"].value_counts().head(6).index
    order_df["Cluster State"] = order_df["Cluster State"].where(order_df["Cluster State"].isin(top_states), "Other")

    buyer_orders = order_df.groupby("Buyer User ID").agg(
        Name=("Full Name", "first") if "Full Name" in order_df.columns else ("Buyer User ID", "first"),
        State=("Cluster State", lambda x: x.mode().iloc[0] if not x.mode().empty else "Unknown"),
        Orders=("Order ID", "nunique"),
        Total_Spent=("Order Total", "sum") if "Order Total" in order_df.columns else ("Order Net", "sum"),
        Coupon_Rate=("Has Coupon", "mean"),
    ).reset_index()

    if "Quantity" in df.columns:
        buyer_units = df[df["Buyer User ID"].notna()].groupby("Buyer User ID")["Quantity"].sum().reset_index(name="Units")
        buyer_orders = buyer_orders.merge(buyer_units, on="Buyer User ID", how="left")
    else:
        buyer_orders["Units"] = 0

    buyer_orders["Units"] = buyer_orders["Units"].fillna(0)
    buyer_orders["Avg Order Value"] = buyer_orders["Total_Spent"].div(buyer_orders["Orders"].replace(0, 1))

    if len(buyer_orders) < 3:
        return pd.DataFrame(), pd.DataFrame()

    cluster_count = min(requested_clusters, len(buyer_orders))
    numeric_features = buyer_orders[["Total_Spent", "Orders", "Avg Order Value", "Coupon_Rate", "Units"]].fillna(0)
    state_features = pd.get_dummies(buyer_orders["State"], prefix="State")
    feature_matrix = pd.concat([numeric_features, state_features], axis=1)
    scaled = StandardScaler().fit_transform(feature_matrix)

    model = KMeans(n_clusters=cluster_count, n_init=20, random_state=42)
    raw_labels = model.fit_predict(scaled)
    buyer_orders["_segment_raw"] = raw_labels

    segment_order = (
        buyer_orders.groupby("_segment_raw")["Total_Spent"]
        .mean()
        .sort_values(ascending=False)
        .index.tolist()
    )
    segment_map = {raw: f"Segment {idx + 1}" for idx, raw in enumerate(segment_order)}
    buyer_orders["Segment"] = buyer_orders["_segment_raw"].map(segment_map)

    summary = (
        buyer_orders.groupby("Segment")
        .agg(
            Buyers=("Buyer User ID", "count"),
            Avg_Orders=("Orders", "mean"),
            Avg_Spend=("Total_Spent", "mean"),
            Avg_AOV=("Avg Order Value", "mean"),
            Avg_Coupon_Rate=("Coupon_Rate", "mean"),
            Primary_State=("State", lambda x: x.mode().iloc[0] if not x.mode().empty else "Unknown"),
        )
        .reset_index()
        .sort_values("Segment")
    )
    return buyer_orders.drop(columns=["_segment_raw"]), summary


def save_uploaded_csvs(uploaded_files) -> tuple[list[Path], Path]:
    staging_dir = Path(tempfile.mkdtemp(prefix="etsy_upload_"))
    saved_paths = []
    for uploaded_file in uploaded_files:
        safe_name = Path(uploaded_file.name).name
        target = staging_dir / safe_name
        target.write_bytes(uploaded_file.getbuffer())
        saved_paths.append(target)
    return saved_paths, staging_dir


def process_sidebar_uploads(uploaded_files) -> dict[str, int]:
    WATCH_DIR.mkdir(parents=True, exist_ok=True)
    saved_paths, staging_dir = save_uploaded_csvs(uploaded_files)
    try:
        summary = process_existing_csvs(get_db(), saved_paths)
    finally:
        for leftover in staging_dir.glob("*"):
            try:
                leftover.unlink()
            except OSError:
                pass
        try:
            staging_dir.rmdir()
        except OSError:
            pass
    return summary


# ---------------------------------------------------------------------------
# Main app
# ---------------------------------------------------------------------------
def main():
    # ---- Load data ---------------------------------------------------------
    df_all, load_time = load_data()

    # ---- Sidebar -----------------------------------------------------------
    with st.sidebar:
        st.title("🃏 TheSlabGuy")
        st.caption("Sales Dashboard")
        st.caption("Auto-refreshes every 20 seconds.")
        st.divider()

        # Date preset
        preset = st.selectbox(
            "Date range",
            ["All time", "Last 30 days", "Last 90 days", "This year", "Last year", "Custom"],
        )
        today = date.today()
        if preset == "Last 30 days":
            start_d, end_d = today - timedelta(days=30), today
        elif preset == "Last 90 days":
            start_d, end_d = today - timedelta(days=90), today
        elif preset == "This year":
            start_d, end_d = date(today.year, 1, 1), today
        elif preset == "Last year":
            start_d, end_d = date(today.year - 1, 1, 1), date(today.year - 1, 12, 31)
        elif preset == "Custom":
            start_d = st.date_input("From", value=today - timedelta(days=90))
            end_d   = st.date_input("To",   value=today)
        else:
            start_d, end_d = None, None

        order_types = ["All"]
        if "Order Type" in df_all.columns:
            order_types.extend(sorted(df_all["Order Type"].dropna().unique().tolist()))
        sel_order_type = st.selectbox("Order type", order_types)

        styles = ["All"]
        if "Style" in df_all.columns:
            styles.extend(sorted(df_all["Style"].dropna().unique().tolist()))
        sel_style = st.selectbox("Style / Variation", styles)

        st.divider()
        st.caption("Upload Etsy CSVs")
        uploads = st.file_uploader(
            "Drag and drop Etsy exports here",
            type=["csv"],
            accept_multiple_files=True,
            label_visibility="collapsed",
        )
        if uploads and st.button("Process uploaded CSVs", use_container_width=True):
            with st.spinner("Processing uploaded CSVs..."):
                summary = process_sidebar_uploads(uploads)
            st.cache_data.clear()
            st.session_state["upload_result"] = summary
            st.rerun()

        if "upload_result" in st.session_state:
            summary = st.session_state["upload_result"]
            st.success(
                "Processed uploads: "
                f"{summary['pairs']} paired batch(es), "
                f"{summary['solo_items']} solo item file(s), "
                f"{summary['combined']} combined file(s), "
                f"{summary['skipped_orders']} unmatched orders file(s)."
            )

        st.divider()
        if st.button("🔄 Force refresh"):
            st.cache_data.clear()
            st.rerun()
        st.caption(f"Last loaded: {load_time}")

    # ---- Apply filters -----------------------------------------------------
    df = df_all.copy()

    if df.empty:
        st.warning("No sales data found. Drop a CSV into data/watch/ to get started.")
        return

    if start_d and "Sale Date" in df.columns:
        df = df[df["Sale Date"].dt.date >= start_d]
    if end_d and "Sale Date" in df.columns:
        df = df[df["Sale Date"].dt.date <= end_d]
    if sel_order_type != "All" and "Order Type" in df.columns:
        df = df[df["Order Type"] == sel_order_type]
    if sel_style != "All" and "Style" in df.columns:
        df = df[df["Style"] == sel_style]

    order_df = get_order_level_df(df)

    # ---- KPIs --------------------------------------------------------------
    section("Overview")
    k1, k2, k3, k4, k5, k6 = st.columns(6)

    total_orders  = order_df["Order ID"].nunique()                         if "Order ID"      in order_df.columns else 0
    units_sold    = int(df["Quantity"].sum())                              if "Quantity"      in df.columns else 0
    net_revenue   = order_df["Order Net"].sum()                            if "Order Net"     in order_df.columns else 0
    avg_order_val = (
        order_df["Order Total"].mean()
        if "Order Total" in order_df.columns and total_orders > 0
        else 0
    )
    repeat_buyers = 0
    if "Buyer User ID" in order_df.columns and "Order ID" in order_df.columns:
        buyer_orders = order_df.groupby("Buyer User ID")["Order ID"].nunique()
        repeat_buyers = int((buyer_orders > 1).sum())
    unique_products = df["Card Name"].nunique() if "Card Name" in df.columns else 0

    with k1: kpi_card("Total Orders",     f"{total_orders:,}")
    with k2: kpi_card("Units Sold",       f"{units_sold:,}")
    with k3: kpi_card("Net Revenue",      fmt_currency(net_revenue))
    with k4: kpi_card("Avg Order Value",  fmt_currency(avg_order_val))
    with k5: kpi_card("Repeat Buyers",    f"{repeat_buyers:,}")
    with k6: kpi_card("Unique Products",  f"{unique_products:,}")

    st.divider()

    # ===== SECTIONS 1 & 2 (side by side) ====================================
    col_left, col_right = st.columns(2)

    # ---- Section 1: Most Popular Products ----------------------------------
    with col_left:
        section("Most Popular Products")
        if "Card Name" not in df.columns or df.empty:
            empty_chart_note()
        else:
            top_n   = st.slider("Top N", 5, 20, 10, key="top_n")
            rank_by = st.radio("Rank by", ["Units", "Orders", "Revenue"], horizontal=True, key="rank_by")

            prod = (
                df.groupby("Card Name")
                .agg(
                    Units=("Quantity", "sum"),
                    Orders=("Order ID", "nunique"),
                    Revenue=("Order Net", "sum"),
                )
                .reset_index()
                .sort_values(rank_by, ascending=False)
                .head(top_n)
                .sort_values(rank_by, ascending=True)   # ascending for horizontal bar
            )

            if prod.empty:
                empty_chart_note()
            else:
                fig = px.bar(
                    prod,
                    x=rank_by,
                    y="Card Name",
                    orientation="h",
                    hover_data={"Units": True, "Orders": True, "Revenue": ":.2f"},
                    color_discrete_sequence=[PRIMARY],
                )
                fig = apply_chart_theme(fig, max(320, top_n * 34))
                st.plotly_chart(fig, use_container_width=True)

    # ---- Section 2: Sales Over Time ----------------------------------------
    with col_right:
        section("Sales Over Time")
        if df.empty or "Sale Date" not in df.columns:
            empty_chart_note()
        else:
            gran     = st.radio("Granularity", ["Monthly", "Weekly"], horizontal=True, key="gran")
            time_met = st.radio("Metric", ["Orders", "Units", "Revenue"], horizontal=True, key="time_met")
            period_col = "Year-Month" if gran == "Monthly" else "Week"

            if period_col not in df.columns:
                empty_chart_note()
            else:
                agg = (
                    df.groupby(period_col)
                    .agg(
                        Orders=("Order ID", "nunique"),
                        Units=("Quantity", "sum"),
                        Revenue=("Order Net", "sum"),
                    )
                    .reset_index()
                    .sort_values(period_col)
                )
                agg["Cumulative"] = agg[time_met].cumsum()

                fig = go.Figure()
                fig.add_bar(
                    x=agg[period_col], y=agg[time_met],
                    name=time_met, marker_color=PRIMARY,
                )
                fig.add_scatter(
                    x=agg[period_col], y=agg["Cumulative"],
                    name=f"Cumulative {time_met}",
                    mode="lines+markers",
                    line=dict(color=SECONDARY, width=2),
                    yaxis="y2",
                )
                fig = apply_chart_theme(fig, 410)
                fig.update_layout(
                    yaxis=dict(title=time_met),
                    yaxis2=dict(
                        title=f"Cumulative {time_met}",
                        overlaying="y",
                        side="right",
                        color="#edf3ff",
                    ),
                    legend=dict(orientation="h", y=1.08),
                    xaxis=dict(tickangle=-45),
                )
                st.plotly_chart(fig, use_container_width=True)

    # ===== SECTION 3: Style + Payment ========================================
    section("Style Breakdown & Payment Methods")
    c3l, c3r = st.columns(2)

    with c3l:
        if "Style" not in df.columns or "Quantity" not in df.columns:
            empty_chart_note()
        else:
            style_agg = df.groupby("Style")["Quantity"].sum().reset_index()
            style_agg.columns = ["Style", "Units"]
            style_agg = style_agg.sort_values("Units", ascending=False)
            if style_agg.empty:
                empty_chart_note()
            else:
                chart_caption("Units by Style")
                fig = px.pie(
                    style_agg,
                    names="Style",
                    values="Units",
                    hole=0.45,
                    color_discrete_sequence=CATEGORICAL_COLORS,
                )
                fig.update_traces(
                    hovertemplate="%{label}<br>Units: %{value}<br>%{percent}<extra></extra>",
                )
                fig = style_donut(fig, 430)
                st.plotly_chart(fig, use_container_width=True)

    with c3r:
        has_ot = "Order Type"    in df.columns
        has_pm = "Payment Method" in df.columns or "Payment Type" in df.columns
        pm_col = "Payment Method" if "Payment Method" in df.columns else "Payment Type"

        if not has_ot or not has_pm:
            empty_chart_note()
        else:
            payment_agg = (
                df.groupby(["Order Type", pm_col])["Order ID"]
                .nunique()
                .reset_index()
                .sort_values("Order ID", ascending=False)
            )
            payment_agg.columns = ["Order Type", "Payment", "Orders"]
            payment_agg["Label"] = payment_agg["Order Type"] + " / " + payment_agg["Payment"]
            if payment_agg.empty:
                empty_chart_note()
            else:
                chart_caption("Orders by Type and Payment")
                fig = px.pie(
                    payment_agg,
                    names="Label",
                    values="Orders",
                    hole=0.45,
                    color_discrete_sequence=CATEGORICAL_COLORS,
                )
                fig.update_traces(
                    hovertemplate="%{label}<br>Orders: %{value}<br>%{percent}<extra></extra>",
                )
                fig = style_donut(fig, 430)
                st.plotly_chart(fig, use_container_width=True)

    # ===== SECTION 4: US Map =================================================
    section("US Sales by State")
    has_state   = "Ship State"   in df.columns
    has_country = "Ship Country" in df.columns

    if not has_state:
        empty_chart_note()
    else:
        us_df = df.copy()
        if has_country:
            us_df = us_df[us_df["Ship Country"].str.contains("United States", na=False)]

        map_met = st.radio("Map metric", ["Orders", "Units", "Revenue"], horizontal=True, key="map_met")
        state_agg = (
            us_df.groupby("Ship State")
            .agg(Orders=("Order ID", "nunique"),
                 Units=("Quantity", "sum"),
                 Revenue=("Order Net", "sum"))
            .reset_index()
        )

        if state_agg.empty:
            empty_chart_note()
        else:
            fig = px.choropleth(
                state_agg,
                locations="Ship State",
                locationmode="USA-states",
                color=map_met,
                scope="usa",
                color_continuous_scale=["#dce9f5", PRIMARY],
                hover_data={"Orders": True, "Units": True, "Revenue": ":.2f"},
            )
            fig = apply_chart_theme(fig, 430)
            fig.update_layout(
                coloraxis_colorbar=dict(
                    tickfont=dict(color="#edf3ff"),
                    title=dict(font=dict(color="#edf3ff")),
                )
            )
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(
                state_agg.sort_values(map_met, ascending=False).reset_index(drop=True),
                use_container_width=True,
            )

    # ===== SECTION 5: Order Projection ======================================
    section("Order Projection")
    if order_df.empty or "Sale Date" not in order_df.columns or "Order ID" not in order_df.columns:
        empty_chart_note()
    else:
        forecast_months = st.slider("Forecast horizon (months)", 1, 6, 3, key="forecast_months")
        actual_daily, forecast_daily, forecast_monthly, model_name = build_order_forecast(df, forecast_months)

        if actual_daily.empty or forecast_monthly.empty:
            empty_chart_note()
        else:
            fp_left, fp_right = st.columns([2, 1])

            with fp_left:
                chart_caption(f"Daily orders forecast using {model_name}")
                actual_window = actual_daily.tail(90)
                fig = go.Figure()
                fig.add_scatter(
                    x=actual_window.index,
                    y=actual_window.values,
                    mode="lines",
                    name="Actual Orders",
                    line=dict(color=PRIMARY, width=2),
                )
                fig.add_scatter(
                    x=forecast_daily.index,
                    y=forecast_daily.values,
                    mode="lines",
                    name="Forecast Orders",
                    line=dict(color=SECONDARY, width=2, dash="dash"),
                )
                fig = apply_chart_theme(fig, 360)
                fig.update_layout(yaxis_title="Orders", xaxis_title="Date")
                st.plotly_chart(fig, use_container_width=True)

            with fp_right:
                next_month_orders = forecast_monthly["Projected Orders"].iloc[0]
                total_horizon_orders = forecast_monthly["Projected Orders"].sum()
                st.metric("Next month forecast", f"{next_month_orders:.1f}")
                st.metric(f"Next {forecast_months} months", f"{total_horizon_orders:.1f}")
                st.dataframe(forecast_monthly, use_container_width=True)

    # ===== SECTION 6: Repeat Buyers ==========================================
    section("Buyer Analysis")
    if "Buyer User ID" not in df.columns:
        empty_chart_note()
    else:
        buyer_agg = (
            df.groupby("Buyer User ID")
            .agg(
                Name=("Full Name", "first") if "Full Name" in df.columns else ("Buyer User ID", "first"),
                Orders=("Order ID", "nunique"),
                Units=("Quantity", "sum"),
                Total_Spent=("Order Total", "sum") if "Order Total" in df.columns else ("Order Net", "sum"),
                Last_Purchase=("Sale Date", "max") if "Sale Date" in df.columns else ("Buyer User ID", "first"),
            )
            .reset_index()
        )

        if "Card Name" in df.columns:
            top_items = (
                df.groupby("Buyer User ID")["Card Name"]
                .apply(lambda x: ", ".join(x.dropna().unique()[:3]))
                .reset_index()
            )
            buyer_agg = buyer_agg.merge(top_items, on="Buyer User ID", how="left")
        else:
            buyer_agg["Card Name"] = ""

        buyer_agg["Tier"] = buyer_agg["Orders"].apply(
            lambda n: "⭐⭐⭐ VIP" if n >= 3 else ("⭐⭐ Returning" if n == 2 else "⭐ New")
        )
        buyer_agg["Total Spent"]    = buyer_agg["Total_Spent"].apply(fmt_currency)
        buyer_agg["Last Purchase"]  = pd.to_datetime(buyer_agg["Last_Purchase"], errors="coerce").dt.strftime("%Y-%m-%d")

        cb5l, cb5r = st.columns([2, 1])

        with cb5l:
            show_repeat = st.checkbox("Show repeat buyers only", value=True)
            display_df  = buyer_agg[buyer_agg["Orders"] > 1] if show_repeat else buyer_agg
            display_cols = [c for c in ["Buyer User ID", "Name", "Orders", "Units",
                                         "Total Spent", "Last Purchase", "Card Name", "Tier"]
                            if c in display_df.columns]
            st.dataframe(
                display_df[display_cols].sort_values("Orders", ascending=False).reset_index(drop=True),
                use_container_width=True,
            )

        with cb5r:
            chart_caption("Buyer Tier Share")
            tier_counts = buyer_agg["Tier"].value_counts().reset_index()
            tier_counts.columns = ["Tier", "Buyers"]
            fig = px.pie(tier_counts, names="Tier", values="Buyers", hole=0.4,
                         color_discrete_sequence=CATEGORICAL_COLORS)
            fig.update_traces(
                hovertemplate="%{label}<br>Buyers: %{value}<br>%{percent}<extra></extra>",
            )
            fig = style_donut(fig, 320)
            st.plotly_chart(fig, use_container_width=True)

    # ===== SECTION 6: Coupon Effectiveness ===================================
    section("Coupon Effectiveness")
    if "Coupon Code" not in df.columns:
        empty_chart_note()
    else:
        coupon_df    = df[df["Coupon Code"].notna() & (df["Coupon Code"] != "")].copy()
        no_coupon_df = df[df["Coupon Code"].isna()  | (df["Coupon Code"] == "")].copy()

        c6l, c6r = st.columns([2, 1])

        with c6l:
            if coupon_df.empty:
                st.info("No coupon usage in selected period.")
            else:
                coupon_tbl = (
                    coupon_df.groupby("Coupon Code")
                    .agg(
                        Uses=("Order ID", "nunique"),
                        Total_Discounted=("Discount Amount", "sum") if "Discount Amount" in coupon_df.columns else ("Order ID", "nunique"),
                        Avg_Net_Order=("Order Net",    "mean")    if "Order Net"     in coupon_df.columns else ("Order ID", "nunique"),
                    )
                    .reset_index()
                )
                coupon_tbl.columns = ["Coupon Code", "Uses", "Total Discounted", "Avg Net Order"]
                coupon_tbl["Total Discounted"] = coupon_tbl["Total Discounted"].apply(fmt_currency)
                coupon_tbl["Avg Net Order"]    = coupon_tbl["Avg Net Order"].apply(fmt_currency)
                st.dataframe(coupon_tbl, use_container_width=True)

        with c6r:
            coupon_order_count    = coupon_df["Order ID"].nunique()    if "Order ID" in coupon_df.columns    else 0
            no_coupon_order_count = no_coupon_df["Order ID"].nunique() if "Order ID" in no_coupon_df.columns else 0
            pie_data = pd.DataFrame({
                "Type":   ["With coupon", "Without coupon"],
                "Orders": [coupon_order_count, no_coupon_order_count],
            })
            chart_caption("Order Share by Coupon Usage")
            fig = px.pie(pie_data, names="Type", values="Orders", hole=0.4,
                         color_discrete_sequence=CATEGORICAL_COLORS)
            fig.update_traces(
                hovertemplate="%{label}<br>Orders: %{value}<br>%{percent}<extra></extra>",
            )
            fig = style_donut(fig, 280)
            st.plotly_chart(fig, use_container_width=True)

            if "Order Net" in df.columns:
                avg_with    = coupon_df["Order Net"].mean()    if not coupon_df.empty    else 0
                avg_without = no_coupon_df["Order Net"].mean() if not no_coupon_df.empty else 0
                st.metric("Avg net w/ coupon",    fmt_currency(avg_with))
                st.metric("Avg net w/o coupon",   fmt_currency(avg_without))

    # ===== SECTION 8: Customer Segments ======================================
    section("Customer Segments")
    if "Buyer User ID" not in df.columns:
        empty_chart_note()
    else:
        eligible_buyers = df[df["Buyer User ID"].notna()]["Buyer User ID"].nunique()
        if eligible_buyers < 3:
            st.info("Need at least 3 buyers with IDs to build customer clusters.")
        else:
            max_clusters = min(6, eligible_buyers)
            cluster_count = st.slider("Number of segments", 3, max_clusters, min(4, max_clusters), key="cluster_count")
            cluster_df, cluster_summary = build_buyer_clusters(df, cluster_count)

            if cluster_df.empty:
                empty_chart_note()
            else:
                seg_left, seg_right = st.columns([2, 1])

                with seg_left:
                    chart_caption("Buyer segments based on spend, orders, coupon usage, units, and primary state")
                    fig = px.scatter(
                        cluster_df,
                        x="Total_Spent",
                        y="Orders",
                        color="Segment",
                        size="Units",
                        hover_data={
                            "Name": True,
                            "State": True,
                            "Avg Order Value": ":.2f",
                            "Coupon_Rate": ":.0%",
                            "Total_Spent": ":.2f",
                            "Units": True,
                        },
                        color_discrete_sequence=CATEGORICAL_COLORS,
                    )
                    fig = apply_chart_theme(fig, 360)
                    fig.update_traces(marker=dict(line=dict(width=1, color="#0d1321")))
                    fig.update_layout(xaxis_title="Total Spend", yaxis_title="Orders")
                    st.plotly_chart(fig, use_container_width=True)

                with seg_right:
                    summary_display = cluster_summary.copy()
                    summary_display["Avg_Spend"] = summary_display["Avg_Spend"].apply(fmt_currency)
                    summary_display["Avg_AOV"] = summary_display["Avg_AOV"].apply(fmt_currency)
                    summary_display["Avg_Coupon_Rate"] = summary_display["Avg_Coupon_Rate"].map(lambda val: f"{val:.0%}")
                    summary_display["Avg_Orders"] = summary_display["Avg_Orders"].map(lambda val: f"{val:.1f}")
                    st.dataframe(summary_display, use_container_width=True)

    # ===== SECTION 9: Fulfillment Speed ======================================
    section("Fulfillment Speed")
    has_paid    = "Date Paid"    in df.columns
    has_shipped = "Date Shipped" in df.columns

    if not has_paid or not has_shipped:
        empty_chart_note()
    else:
        ship_df = df[["Date Paid", "Date Shipped"]].dropna()
        ship_df = ship_df.copy()
        ship_df["Days to Ship"] = (
            ship_df["Date Shipped"] - ship_df["Date Paid"]
        ).dt.total_seconds() / 86400
        ship_df = ship_df[ship_df["Days to Ship"] >= 0]

        if ship_df.empty:
            empty_chart_note()
        else:
            avg_days    = ship_df["Days to Ship"].mean()
            median_days = ship_df["Days to Ship"].median()
            same_day    = int((ship_df["Days to Ship"] == 0).sum())

            c7m1, c7m2, c7m3 = st.columns(3)
            c7m1.metric("Avg days to ship",    f"{avg_days:.1f}")
            c7m2.metric("Median days to ship", f"{median_days:.1f}")
            c7m3.metric("Same-day shipments",  f"{same_day:,}")

            fig = px.histogram(
                ship_df, x="Days to Ship",
                nbins=15,
                color_discrete_sequence=[PRIMARY],
            )
            fig = apply_chart_theme(fig, 320)
            fig.update_layout(xaxis_title="Days to Ship", yaxis_title="Count")
            st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    main()

