"""Streamlit analytics dashboard for Etsy sales data."""

import json
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

SALES_COLLECTION = os.getenv("SALES_COLLECTION", "sales")
LISTINGS_COLLECTION = os.getenv("LISTINGS_COLLECTION", "listings")

APP_SETTINGS_DIR = Path("settings")
APP_SETTINGS_FILE = APP_SETTINGS_DIR / "dashboard_settings.json"
DEFAULT_STORE_NAME = "Etsy Sales Dashboard"
SECTION_OPTIONS = {
    "Overview KPIs": "overview",
    "Most Popular Products": "products",
    "Sales Over Time": "sales_over_time",
    "Style Breakdown & Payment Methods": "style_payment",
    "US Sales by State": "us_map",
    "Sales Projection": "projection",
    "Buyer Analysis": "buyer_analysis",
    "Coupon Effectiveness": "coupon",
    "Customer Segments": "segments",
    "Fulfillment Speed": "fulfillment",
    "Order History": "order_history",
}
ALL_SECTION_KEYS = list(SECTION_OPTIONS.values())


def _default_app_settings() -> dict:
    return {
        "store_name": DEFAULT_STORE_NAME,
        "logo_path": "",
        "selected_sections": ALL_SECTION_KEYS,
        "onboarding_complete": False,
    }


def _coerce_selected_sections(raw_sections) -> list[str]:
    if not isinstance(raw_sections, list):
        return ALL_SECTION_KEYS
    selected = [section for section in raw_sections if section in SECTION_OPTIONS.values()]
    return selected or ALL_SECTION_KEYS


def load_app_settings() -> dict:
    defaults = _default_app_settings()
    if not APP_SETTINGS_FILE.is_file():
        return defaults

    try:
        loaded = json.loads(APP_SETTINGS_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return defaults

    settings = defaults.copy()
    if isinstance(loaded, dict):
        settings.update(loaded)

    store_name = str(settings.get("store_name", DEFAULT_STORE_NAME)).strip()
    settings["store_name"] = store_name or DEFAULT_STORE_NAME
    settings["logo_path"] = str(settings.get("logo_path", "")).strip()
    settings["selected_sections"] = _coerce_selected_sections(settings.get("selected_sections"))
    settings["onboarding_complete"] = bool(settings.get("onboarding_complete", False))
    return settings


def save_app_settings(settings: dict):
    APP_SETTINGS_DIR.mkdir(parents=True, exist_ok=True)
    APP_SETTINGS_FILE.write_text(json.dumps(settings, indent=2), encoding="utf-8")


def logo_path_is_valid(path_str: str) -> bool:
    return bool(path_str) and Path(path_str).is_file()


def save_logo_bytes(logo_bytes: bytes, filename: str | None) -> str:
    APP_SETTINGS_DIR.mkdir(parents=True, exist_ok=True)
    suffix = Path(filename or "logo.png").suffix.lower()
    if suffix not in {".png", ".jpg", ".jpeg", ".webp"}:
        suffix = ".png"

    target_path = APP_SETTINGS_DIR / f"store_logo{suffix}"
    for old_path in APP_SETTINGS_DIR.glob("store_logo.*"):
        if old_path != target_path and old_path.is_file():
            old_path.unlink()

    target_path.write_bytes(logo_bytes)
    return str(target_path)


def section_labels_from_keys(section_keys: list[str]) -> list[str]:
    return [label for label, key in SECTION_OPTIONS.items() if key in section_keys]


def section_keys_from_labels(section_labels: list[str]) -> list[str]:
    selected = [SECTION_OPTIONS[label] for label in section_labels if label in SECTION_OPTIONS]
    return selected or ALL_SECTION_KEYS


def clear_onboarding_session_state():
    for key in [
        "onboarding_step",
        "onboarding_section_labels",
        "onboarding_logo_upload",
        "onboarding_logo_bytes",
        "onboarding_logo_name",
    ]:
        st.session_state.pop(key, None)


BOOT_SETTINGS = load_app_settings()
BOOT_STORE_NAME = BOOT_SETTINGS.get("store_name", DEFAULT_STORE_NAME)
BOOT_ICON = BOOT_SETTINGS.get("logo_path", "") if logo_path_is_valid(BOOT_SETTINGS.get("logo_path", "")) else "📊"

import firebase_admin
from firebase_admin import credentials, firestore

from watcher import WATCH_DIR, process_existing_csvs

# ---------------------------------------------------------------------------
# Page config — must be first Streamlit call
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title=f"{BOOT_STORE_NAME} — Sales Dashboard",
    page_icon=BOOT_ICON,
    layout="wide",
    initial_sidebar_state="expanded",
)

THEME_GLOW_COLOR = "rgba(46,117,182,0.14)"
THEME_GRID_COLOR = "rgba(120,120,120,0.20)"
THEME_AXIS_LINE = "rgba(120,120,120,0.32)"
THEME_MARKER_BORDER = "rgba(120,120,120,0.38)"
UI_BORDER_COLOR = "#7daecc"

# ---------------------------------------------------------------------------
# Global style
# ---------------------------------------------------------------------------
st.markdown(
    f"""
    <style>
    [data-testid="stAppViewContainer"] {{
        background:
            radial-gradient(circle at top left, {THEME_GLOW_COLOR}, transparent 26%),
            var(--background-color);
        color: var(--text-color);
    }}
    [data-testid="stHeader"] {{ background: rgba(0, 0, 0, 0); }}
    [data-testid="stSidebar"] {{ background-color: var(--secondary-background-color); }}
    [data-testid="stSidebar"] * {{ color: var(--text-color) !important; }}
    [data-testid="stSidebar"] div[data-baseweb="input"] {{
        background-color: var(--secondary-background-color) !important;
        border: 1px solid {UI_BORDER_COLOR} !important;
        border-radius: 8px;
    }}
    [data-testid="stSidebar"] div[data-baseweb="input"]:focus-within {{
        border-color: #2E75B6 !important;
        box-shadow: 0 0 0 1px rgba(46, 117, 182, 0.35) !important;
    }}
    [data-testid="stSidebar"] div[data-baseweb="input"] input {{
        color: var(--text-color) !important;
        caret-color: var(--text-color) !important;
    }}
    [data-testid="stTextInput"] div[data-baseweb="input"] {{
        background-color: var(--secondary-background-color) !important;
        border: 1px solid {UI_BORDER_COLOR} !important;
        border-radius: 8px;
    }}
    [data-testid="stTextInput"] div[data-baseweb="input"] input {{
        color: var(--text-color) !important;
        caret-color: var(--text-color) !important;
    }}
    [data-testid="stTextInput"] div[data-baseweb="input"]:focus-within {{
        box-shadow: 0 0 0 1px rgba(46, 117, 182, 0.35) !important;
        border-color: #2E75B6 !important;
    }}
    [data-testid="stSidebar"] div[data-baseweb="select"] > div,
    [data-testid="stMultiSelect"] div[data-baseweb="select"] > div {{
        border: 1px solid {UI_BORDER_COLOR} !important;
        border-radius: 10px !important;
        background: rgba(46, 117, 182, 0.08) !important;
        min-height: 3rem !important;
        padding: 0.2rem 0.35rem !important;
    }}
    [data-testid="stSidebar"] div[data-baseweb="select"] [role="combobox"],
    [data-testid="stMultiSelect"] div[data-baseweb="select"] [role="combobox"] {{
        gap: 0.35rem;
        align-items: flex-start;
    }}
    [data-testid="stSidebar"] div[data-baseweb="select"] > div > div:first-child,
    [data-testid="stMultiSelect"] div[data-baseweb="select"] > div > div:first-child {{
        max-height: 8.6rem;
        overflow-y: auto;
        padding-right: 0.2rem;
        scrollbar-width: thin;
    }}
    [data-testid="stSidebar"] div[data-baseweb="select"]:focus-within > div,
    [data-testid="stMultiSelect"] div[data-baseweb="select"]:focus-within > div {{
        border-color: #2E75B6 !important;
        box-shadow: 0 0 0 1px rgba(46, 117, 182, 0.35) !important;
    }}
    [data-testid="stMultiSelect"] [data-baseweb="tag"] {{
        border: 1px solid {UI_BORDER_COLOR} !important;
        border-radius: 10px !important;
        background-color: rgba(46, 117, 182, 0.24) !important;
        padding: 0.22rem 0.34rem !important;
        max-width: 100% !important;
        min-height: 1.7rem !important;
        align-items: center !important;
        overflow: hidden !important;
    }}
    [data-testid="stMultiSelect"] [data-baseweb="tag"] > * {{
        min-width: 0 !important;
    }}
    [data-testid="stMultiSelect"] [data-baseweb="tag"] span {{
        color: var(--text-color) !important;
        font-weight: 500;
        font-size: 0.88rem !important;
        line-height: 1.15 !important;
        white-space: nowrap !important;
        overflow: hidden !important;
        text-overflow: ellipsis !important;
        display: block !important;
        max-width: 100% !important;
    }}
    [data-testid="stMultiSelect"] [data-baseweb="tag"] svg {{
        fill: var(--text-color) !important;
        margin-top: 0 !important;
        flex-shrink: 0 !important;
    }}
    [data-testid="stSidebar"] [role="option"][aria-selected="true"],
    [data-testid="stMultiSelect"] [role="option"][aria-selected="true"] {{
        background-color: rgba(46, 117, 182, 0.18) !important;
        color: var(--text-color) !important;
    }}
    [data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] {{
        border: 1px dashed {UI_BORDER_COLOR} !important;
        border-radius: 12px !important;
        padding: 0.35rem !important;
        background: rgba(46, 117, 182, 0.06) !important;
    }}
    [data-testid="stSidebar"] [data-testid="stFileUploaderDropzoneInstructions"],
    [data-testid="stSidebar"] [data-testid="stFileUploader"] small,
    [data-testid="stSidebar"] [data-testid="stFileUploader"] p {{
        white-space: normal !important;
        overflow-wrap: anywhere !important;
        line-height: 1.25 !important;
    }}
    [data-testid="stSidebar"] div.stButton > button,
    [data-testid="stSidebar"] [data-testid="stFileUploader"] button {{
        justify-content: flex-start;
        text-align: left;
        white-space: nowrap !important;
        text-overflow: ellipsis;
        overflow: hidden;
        font-size: clamp(0.72rem, 1.8vw, 0.9rem) !important;
        padding-left: 0.7rem !important;
        padding-right: 0.7rem !important;
    }}
    .block-container {{ padding-top: 1.35rem; }}
    .kpi-card {{
        background: var(--secondary-background-color);
        border: 1px solid rgba(46, 117, 182, 0.42);
        border-left: 4px solid #2E75B6;
        border-radius: 10px;
        padding: 14px 18px;
        margin-bottom: 4px;
        box-shadow: 0 10px 28px rgba(0, 0, 0, 0.18);
    }}
    .kpi-label {{
        font-size: 12px;
        color: inherit;
        opacity: 0.75;
        text-transform: uppercase;
        letter-spacing: .05em;
    }}
    .kpi-value {{
        font-size: 28px;
        font-weight: 700;
        color: inherit;
        margin-top: 2px;
    }}
    .section-header {{
        font-size: 18px;
        font-weight: 700;
        color: inherit;
        border-bottom: 2px solid #2E75B6;
        padding-bottom: 4px;
        margin: 24px 0 12px 0;
    }}
    .chart-caption {{
        color: inherit;
        opacity: 0.85;
        font-size: 0.95rem;
        margin: 0 0 0.35rem 0;
    }}
    </style>
    """,
    unsafe_allow_html=True,
)

_CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    margin=dict(l=18, r=18, t=24, b=18),
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
def resolve_credentials_path(raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_file():
        return path

    candidates = []
    if path.is_dir():
        candidates.append(path / "firebase.json")
        candidates.extend(sorted(path.glob("*.json")))

    if path.parent.exists():
        candidates.append(path.parent / "firebase.json")
        candidates.extend(sorted(path.parent.glob("*.json")))

    for candidate in candidates:
        if candidate.is_file():
            return candidate

    if path.is_dir():
        raise FileNotFoundError(
            f"No Firebase JSON credential file found in directory: {path}. "
            "Put firebase.json in that directory or set FIREBASE_CREDENTIALS to a JSON file path."
        )

    raise FileNotFoundError(
        f"Firebase credentials file not found at {raw_path}. "
        "Place your service account key at secrets/firebase.json or set FIREBASE_CREDENTIALS accordingly."
    )


@st.cache_resource
def get_db():
    cred_path = os.getenv("FIREBASE_CREDENTIALS", "secrets/firebase.json")
    resolved_cred_path = resolve_credentials_path(cred_path)
    if not firebase_admin._apps:
        cred = credentials.Certificate(str(resolved_cred_path))
        firebase_admin.initialize_app(cred)
    return firestore.client()


@st.cache_data(ttl=15)
def load_data() -> tuple[pd.DataFrame, str]:
    db   = get_db()
    docs = db.collection(SALES_COLLECTION).stream()
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


@st.cache_data(ttl=60)
def load_listings_data() -> pd.DataFrame:
    db = get_db()
    docs = db.collection(LISTINGS_COLLECTION).stream()
    rows = [d.to_dict() for d in docs]

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    for col in ["_created_at", "_updated_at"]:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    if "Listing Title" in df.columns and "Card Name" not in df.columns:
        parts = df["Listing Title"].astype(str).str.split("|", n=1, expand=True)
        df["Card Name"] = parts[0].str.strip() if 0 in parts else None

    if "Card Name" in df.columns:
        df["Card Name"] = (
            df["Card Name"]
            .fillna("")
            .astype(str)
            .str.strip()
            .replace("", "Unknown Item")
        )

    if "Available Quantity" in df.columns:
        df["Available Quantity"] = pd.to_numeric(df["Available Quantity"], errors="coerce").fillna(0).astype(int)

    if "Listing Price" in df.columns:
        df["Listing Price"] = pd.to_numeric(df["Listing Price"], errors="coerce").fillna(0.0)

    return df


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


def dataframe_with_one_index(df: pd.DataFrame, **kwargs):
    display_df = df.copy()
    display_df.index = pd.RangeIndex(start=1, stop=len(display_df) + 1, step=1)
    st.dataframe(display_df, **kwargs)


def render_credits():
    year = date.today().year
    st.divider()
    st.caption(
        f"© {year} • MIT License • Made by [ButterMyGit](https://github.com/ButterMyGit) • "
        "Initially built for [TheSlabGuy](https://theslabguy.etsy.com)"
    )


def apply_chart_theme(fig: go.Figure, height: int, *, pie_like: bool = False) -> go.Figure:
    margin = dict(l=22, r=22, t=18, b=22) if pie_like else _CHART_LAYOUT["margin"]
    fig.update_layout(
        paper_bgcolor=_CHART_LAYOUT["paper_bgcolor"],
        plot_bgcolor=_CHART_LAYOUT["plot_bgcolor"],
        margin=margin,
        height=height,
    )
    fig.update_xaxes(
        gridcolor=THEME_GRID_COLOR,
        zeroline=False,
        linecolor=THEME_AXIS_LINE,
    )
    fig.update_yaxes(
        gridcolor=THEME_GRID_COLOR,
        zeroline=False,
        linecolor=THEME_AXIS_LINE,
    )
    return fig


def style_donut(fig: go.Figure, height: int) -> go.Figure:
    fig.update_traces(
        textinfo="none",
        marker=dict(line=dict(color=THEME_MARKER_BORDER, width=2)),
    )
    fig = apply_chart_theme(fig, height, pie_like=True)
    fig.update_layout(
        showlegend=True,
        legend=dict(
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


@st.cache_data(ttl=120)
def build_order_history(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "Order ID" not in df.columns:
        return pd.DataFrame()

    working = df.copy()
    if "Card Name" not in working.columns and "Item Name" in working.columns:
        working["Card Name"] = (
            working["Item Name"]
            .fillna("")
            .astype(str)
            .str.split("|", n=1)
            .str[0]
            .str.strip()
        )

    def _join_unique(series: pd.Series, max_items: int = 4) -> str:
        values = series.fillna("").astype(str).str.strip()
        values = values[values != ""]
        unique_values = list(dict.fromkeys(values.tolist()))
        if not unique_values:
            return ""
        if len(unique_values) <= max_items:
            return ", ".join(unique_values)
        return ", ".join(unique_values[:max_items]) + f" +{len(unique_values) - max_items} more"

    agg_spec = {}
    if "Card Name" in working.columns:
        agg_spec["Items"] = ("Card Name", _join_unique)
    if "Style" in working.columns:
        agg_spec["Styles"] = ("Style", _join_unique)
    if "Quantity" in working.columns:
        agg_spec["Units"] = ("Quantity", "sum")

    if agg_spec:
        order_line_details = working.groupby("Order ID").agg(**agg_spec).reset_index()
    else:
        order_line_details = working[["Order ID"]].drop_duplicates().copy()

    if "Units" in order_line_details.columns:
        order_line_details["Units"] = (
            pd.to_numeric(order_line_details["Units"], errors="coerce")
            .fillna(0)
            .astype(int)
        )

    order_base = get_order_level_df(working)
    base_cols = [
        "Order ID",
        "Transaction ID",
        "Sale Date",
        "Date Paid",
        "Date Shipped",
        "Full Name",
        "Buyer Username",
        "Order Type",
        "Payment Method",
        "Payment Type",
        "Ship State",
        "Ship Country",
        "Coupon Code",
        "Order Total",
        "Card Processing Fees",
        "Order Net",
        "Shipping",
        "Sales Tax",
        "Discount Amount",
    ]
    order_base = order_base[[col for col in base_cols if col in order_base.columns]].copy()

    if "Payment Method" not in order_base.columns and "Payment Type" in order_base.columns:
        order_base.rename(columns={"Payment Type": "Payment Method"}, inplace=True)

    if "Full Name" not in order_base.columns and "Buyer Username" in order_base.columns:
        order_base["Full Name"] = order_base["Buyer Username"]

    history = order_base.merge(order_line_details, on="Order ID", how="left")
    history["Buyer"] = history.get("Full Name", pd.Series(index=history.index)).fillna(
        history.get("Buyer Username", pd.Series(index=history.index))
    )

    if "Items" in history.columns:
        history["Items"] = history["Items"].fillna("")
    if "Styles" in history.columns:
        history["Styles"] = history["Styles"].fillna("")
    if "Units" not in history.columns:
        history["Units"] = 0

    return history


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
def build_item_forecast(
    sales_df: pd.DataFrame,
    months_ahead: int,
) -> tuple[pd.DataFrame, list[str]]:
    required_cols = {"Sale Date", "Quantity"}
    if sales_df.empty or not required_cols.issubset(sales_df.columns):
        return pd.DataFrame(), []

    working = sales_df.dropna(subset=["Sale Date"]).copy()
    if working.empty:
        return pd.DataFrame(), []

    if "Card Name" not in working.columns:
        if "Item Name" in working.columns:
            working["Card Name"] = (
                working["Item Name"]
                .fillna("")
                .astype(str)
                .str.split("|", n=1)
                .str[0]
            )
        else:
            working["Card Name"] = "Unknown Item"

    working["Card Name"] = (
        working["Card Name"]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace("", "Unknown Item")
    )
    working["Quantity"] = pd.to_numeric(working["Quantity"], errors="coerce").fillna(0.0)
    working["Day"] = working["Sale Date"].dt.normalize()

    daily_by_item = (
        working.groupby(["Card Name", "Day"])["Quantity"]
        .sum()
        .reset_index()
    )
    full_index = pd.date_range(daily_by_item["Day"].min(), daily_by_item["Day"].max(), freq="D")
    forecast_start = full_index.max() + pd.Timedelta(days=1)
    month_periods = pd.period_range(start=forecast_start.to_period("M"), periods=months_ahead, freq="M")
    month_labels = [str(period) for period in month_periods]

    product_universe = sorted(set(daily_by_item["Card Name"]))
    if not product_universe:
        return pd.DataFrame(), []

    horizon_days = max(35, months_ahead * 35)
    forecast_index = pd.date_range(forecast_start, periods=horizon_days, freq="D")

    rows = []
    for card in product_universe:
        item_hist = daily_by_item[daily_by_item["Card Name"] == card].set_index("Day")["Quantity"]
        series = item_hist.reindex(full_index, fill_value=0.0).astype(float)

        if series.sum() <= 0:
            pred = pd.Series(0.0, index=forecast_index)
        elif len(series) >= 56 and series.nunique() > 1:
            try:
                fit = ExponentialSmoothing(
                    series,
                    trend="add",
                    seasonal="add",
                    seasonal_periods=7,
                    initialization_method="estimated",
                ).fit(optimized=True)
                pred = fit.forecast(horizon_days)
            except Exception:
                pred = pd.Series(series.tail(28).mean(), index=forecast_index)
        elif len(series) >= 14 and series.nunique() > 1:
            try:
                fit = ExponentialSmoothing(
                    series,
                    trend="add",
                    initialization_method="estimated",
                ).fit(optimized=True)
                pred = fit.forecast(horizon_days)
            except Exception:
                pred = pd.Series(series.tail(28).mean(), index=forecast_index)
        else:
            pred = pd.Series(series.tail(min(14, len(series))).mean(), index=forecast_index)

        pred = pd.Series(pred, index=forecast_index).clip(lower=0)
        monthly = pred.groupby(pred.index.to_period("M")).sum()

        row = {
            "Card Name": card,
            "Historical Units": int(float(series.sum())),
        }
        forecast_total = 0
        for period in month_periods:
            value = float(monthly.get(period, 0.0))
            units = max(int(value), 0)
            row[str(period)] = units
            forecast_total += units
        row["Forecast Units"] = forecast_total
        rows.append(row)

    result = pd.DataFrame(rows).sort_values(
        ["Forecast Units", "Historical Units"],
        ascending=[False, False],
    ).reset_index(drop=True)
    return result, month_labels


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
    settings = load_app_settings()
    store_name = settings.get("store_name", DEFAULT_STORE_NAME)
    logo_path = settings.get("logo_path", "")
    has_logo = logo_path_is_valid(logo_path)
    selected_sections = _coerce_selected_sections(settings.get("selected_sections"))
    selected_section_set = set(selected_sections)
    settings_open = st.session_state.get("settings_menu_open", False)

    if not settings_open:
        st.session_state["settings_section_labels"] = section_labels_from_keys(selected_sections)

    if not settings.get("onboarding_complete", False):
        st.markdown("## Welcome")
        st.caption("Set up your dashboard in two quick steps.")

        st.session_state.setdefault("onboarding_step", 1)
        st.session_state.setdefault("onboarding_section_labels", section_labels_from_keys(selected_sections))

        _, center, _ = st.columns([1, 2, 1])
        with center:
            st.progress(st.session_state["onboarding_step"] / 2)

            if st.session_state["onboarding_step"] == 1:
                st.markdown("### Step 1 of 2")
                st.markdown("### Brand profile")

                logo_upload = st.file_uploader(
                    "Store logo (optional)",
                    type=["png", "jpg", "jpeg", "webp"],
                    key="onboarding_logo_upload",
                )
                if logo_upload is not None:
                    st.session_state["onboarding_logo_bytes"] = logo_upload.getvalue()
                    st.session_state["onboarding_logo_name"] = logo_upload.name
                    st.image(logo_upload, width=220)
                    st.caption("Uploaded at original quality. Browser tab icons are automatically downscaled by the browser.")
                elif has_logo:
                    st.image(logo_path, width=220)
                    st.caption("Using your current saved logo. Browser tab icons are automatically downscaled by the browser.")

                _, step1_col_r = st.columns([1, 1])
                with step1_col_r:
                    if st.button("Next", use_container_width=True, key="onboarding_next"):
                        st.session_state["onboarding_step"] = 2
                        st.rerun()
            else:
                st.markdown("### Step 2 of 2")
                st.markdown("### Dashboard sections")
                st.caption("You can change these options any time later from the sidebar settings menu.")

                st.multiselect(
                    "Sections to display",
                    options=list(SECTION_OPTIONS.keys()),
                    key="onboarding_section_labels",
                    placeholder="Choose sections to display",
                )

                step2_col_l, step2_col_r = st.columns([1, 1])
                with step2_col_l:
                    if st.button("Back", use_container_width=True, key="onboarding_back"):
                        st.session_state["onboarding_step"] = 1
                        st.rerun()
                with step2_col_r:
                    if st.button("Finish Setup", use_container_width=True, key="onboarding_finish"):
                        chosen_labels = st.session_state.get("onboarding_section_labels", list(SECTION_OPTIONS.keys()))
                        updated_logo_path = logo_path

                        if "onboarding_logo_bytes" in st.session_state:
                            updated_logo_path = save_logo_bytes(
                                st.session_state["onboarding_logo_bytes"],
                                st.session_state.get("onboarding_logo_name"),
                            )

                        save_app_settings(
                            {
                                "store_name": store_name,
                                "logo_path": updated_logo_path,
                                "selected_sections": section_keys_from_labels(chosen_labels),
                                "onboarding_complete": True,
                            }
                        )
                        st.session_state["settings_section_labels"] = chosen_labels
                        st.session_state["settings_menu_open"] = False
                        clear_onboarding_session_state()
                        st.cache_data.clear()
                        st.rerun()
        return

    if not settings_open:
        st_autorefresh(interval=20_000, key="dashboard_autorefresh")

    # ---- Load data ---------------------------------------------------------
    df_all, load_time = load_data()

    # ---- Sidebar -----------------------------------------------------------
    with st.sidebar:
        if has_logo:
            st.image(logo_path, width=110)
        st.title(store_name)
        st.caption("Sales Dashboard")
        st.caption("Auto-refreshes every 20 seconds.")

        toggle_label = "Close settings" if settings_open else "Settings"
        if st.button(toggle_label, use_container_width=True, key="settings_toggle"):
            settings_open = not settings_open
            st.session_state["settings_menu_open"] = settings_open
            if settings_open:
                st.session_state["settings_section_labels"] = section_labels_from_keys(selected_sections)
            st.rerun()

        if settings_open:
            st.caption("Update your logo and visible dashboard sections.")
            if has_logo:
                st.image(logo_path, width=220)
                st.caption("Stored at original quality. Browser tabs use small favicon sizes, which can look softer.")

            settings_logo_upload = st.file_uploader(
                "Replace logo (optional)",
                type=["png", "jpg", "jpeg", "webp"],
                key="settings_logo_upload",
            )
            if st.button(
                "Remove logo",
                use_container_width=True,
                key="settings_remove_logo",
                disabled=not has_logo,
            ):
                for old_logo in APP_SETTINGS_DIR.glob("store_logo.*"):
                    try:
                        old_logo.unlink()
                    except OSError:
                        pass

                save_app_settings(
                    {
                        "store_name": store_name,
                        "logo_path": "",
                        "selected_sections": section_keys_from_labels(
                            st.session_state.get("settings_section_labels", list(SECTION_OPTIONS.keys()))
                        ),
                        "onboarding_complete": True,
                    }
                )
                st.session_state["settings_menu_open"] = False
                st.cache_data.clear()
                st.rerun()

            st.multiselect(
                "Dashboard sections",
                options=list(SECTION_OPTIONS.keys()),
                key="settings_section_labels",
                placeholder="Choose sections to display",
            )
            st.caption("These settings can be changed later at any time.")

            if st.button("Save settings", use_container_width=True, key="settings_save"):
                updated_logo_path = logo_path

                if settings_logo_upload is not None:
                    updated_logo_path = save_logo_bytes(settings_logo_upload.getvalue(), settings_logo_upload.name)

                save_app_settings(
                    {
                        "store_name": store_name,
                        "logo_path": updated_logo_path,
                        "selected_sections": section_keys_from_labels(
                            st.session_state.get("settings_section_labels", list(SECTION_OPTIONS.keys()))
                        ),
                        "onboarding_complete": True,
                    }
                )
                st.session_state["settings_menu_open"] = False
                st.cache_data.clear()
                st.rerun()

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
            end_d = st.date_input("To", value=today)
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
                f"{summary.get('listings', 0)} listing file(s), "
                f"{summary['skipped_orders']} unmatched orders file(s)."
            )

        st.divider()
        if st.button("Force refresh"):
            st.cache_data.clear()
            st.rerun()
        st.caption(f"Last loaded: {load_time}")

    # ---- Apply filters -----------------------------------------------------
    df = df_all.copy()

    if df.empty:
        st.warning("No sales data found. Drop a CSV into data/watch/ to get started.")
        render_credits()
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

    st.caption("Performance snapshot for your selected filters.")

    if "overview" in selected_section_set:
        # ---- KPIs ----------------------------------------------------------
        section("Overview")
        k1, k2, k3, k4, k5, k6 = st.columns(6)

        total_orders = order_df["Order ID"].nunique() if "Order ID" in order_df.columns else 0
        units_sold = int(df["Quantity"].sum()) if "Quantity" in df.columns else 0
        net_revenue = order_df["Order Net"].sum() if "Order Net" in order_df.columns else 0
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

        with k1:
            kpi_card("Total Orders", f"{total_orders:,}")
        with k2:
            kpi_card("Units Sold", f"{units_sold:,}")
        with k3:
            kpi_card("Net Revenue", fmt_currency(net_revenue))
        with k4:
            kpi_card("Avg Order Value", fmt_currency(avg_order_val))
        with k5:
            kpi_card("Repeat Buyers", f"{repeat_buyers:,}")
        with k6:
            kpi_card("Unique Products", f"{unique_products:,}")

        st.divider()

    # ===== SECTIONS 1 & 2 (side by side) ====================================
    show_products = "products" in selected_section_set
    show_sales_over_time = "sales_over_time" in selected_section_set

    if show_products or show_sales_over_time:
        if show_products and show_sales_over_time:
            col_left, col_right = st.columns(2)
        elif show_products:
            col_left, col_right = st.container(), None
        else:
            col_left, col_right = None, st.container()

        if show_products and col_left is not None:
            with col_left:
                section("Most Popular Products")
                if "Card Name" not in df.columns or df.empty:
                    empty_chart_note()
                else:
                    top_n = st.slider("Top N", 5, 20, 10, key="top_n")
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
                        .sort_values(rank_by, ascending=True)
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

        if show_sales_over_time and col_right is not None:
            with col_right:
                section("Sales Over Time")
                if df.empty or "Sale Date" not in df.columns:
                    empty_chart_note()
                else:
                    gran = st.radio("Granularity", ["Monthly", "Weekly"], horizontal=True, key="gran")
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
                            x=agg[period_col],
                            y=agg[time_met],
                            name=time_met,
                            marker_color=PRIMARY,
                        )
                        fig.add_scatter(
                            x=agg[period_col],
                            y=agg["Cumulative"],
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
                            ),
                            legend=dict(orientation="h", y=1.08),
                            xaxis=dict(tickangle=-45),
                        )
                        st.plotly_chart(fig, use_container_width=True)

    # ===== SECTION 3: Style + Payment ========================================
    if "style_payment" in selected_section_set:
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
            has_ot = "Order Type" in df.columns
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
    if "us_map" in selected_section_set:
        section("US Sales by State")
        has_state = "Ship State" in df.columns
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
                fig.update_traces(marker_line_color=THEME_MARKER_BORDER, marker_line_width=0.9)
                fig.update_geos(
                    showcoastlines=True,
                    coastlinecolor=UI_BORDER_COLOR,
                    coastlinewidth=0.8,
                    showframe=True,
                    framecolor=UI_BORDER_COLOR,
                    framewidth=1.2,
                    bgcolor="rgba(0,0,0,0)",
                    projection_type="albers usa",
                )
                fig.update_layout(
                    coloraxis_colorbar=dict(
                        outlinewidth=1,
                        outlinecolor=UI_BORDER_COLOR,
                    )
                )
                st.plotly_chart(fig, use_container_width=True)
                dataframe_with_one_index(
                    state_agg.sort_values(map_met, ascending=False).reset_index(drop=True),
                    use_container_width=True,
                )

    # ===== SECTION 5: Order Projection ======================================
    if "projection" in selected_section_set:
        section("Sales Projection")
        forecast_months = st.slider("Forecast horizon (months)", 1, 6, 3, key="forecast_months")

        forecast_df = df.copy()
        forecast_order_df = get_order_level_df(forecast_df)

        if "Sale Date" in forecast_df.columns:
            sale_dates = pd.to_datetime(forecast_df["Sale Date"], errors="coerce").dropna()
            if not sale_dates.empty:
                start_default = sale_dates.dt.date.min()
                today_date = date.today()
                max_picker_date = today_date if today_date >= start_default else start_default

                if "projection_history_start" not in st.session_state:
                    st.session_state["projection_history_start"] = start_default
                if st.session_state["projection_history_start"] < start_default:
                    st.session_state["projection_history_start"] = start_default

                if st.session_state.get("projection_history_end_seed") != max_picker_date:
                    st.session_state["projection_history_end"] = max_picker_date
                    st.session_state["projection_history_end_seed"] = max_picker_date
                elif "projection_history_end" not in st.session_state:
                    st.session_state["projection_history_end"] = max_picker_date

                if st.session_state["projection_history_end"] < start_default:
                    st.session_state["projection_history_end"] = start_default

                hist_col_l, hist_col_r = st.columns(2)
                with hist_col_l:
                    hist_start = st.date_input(
                        "Historical start date",
                        min_value=start_default,
                        max_value=max_picker_date,
                        key="projection_history_start",
                    )
                with hist_col_r:
                    hist_end = st.date_input(
                        "Historical end date",
                        min_value=start_default,
                        max_value=max_picker_date,
                        key="projection_history_end",
                    )

                if hist_start > hist_end:
                    hist_start, hist_end = hist_end, hist_start
                    st.session_state["projection_history_start"] = hist_start
                    st.session_state["projection_history_end"] = hist_end

                forecast_df = forecast_df[
                    (forecast_df["Sale Date"].dt.date >= hist_start)
                    & (forecast_df["Sale Date"].dt.date <= hist_end)
                ]
                forecast_order_df = get_order_level_df(forecast_df)
                st.caption(f"Historical range used for forecasting: {hist_start} to {hist_end}")

        st.markdown("#### Order Forecast")
        if forecast_order_df.empty or "Sale Date" not in forecast_order_df.columns or "Order ID" not in forecast_order_df.columns:
            empty_chart_note()
        else:
            actual_daily, forecast_daily, forecast_monthly, model_name = build_order_forecast(forecast_df, forecast_months)

            if actual_daily.empty or forecast_monthly.empty:
                empty_chart_note()
            else:
                fp_left, fp_right = st.columns([2, 1])

                with fp_left:
                    chart_caption(f"Daily orders forecast using {model_name}")
                    fig = go.Figure()
                    fig.add_scatter(
                        x=actual_daily.index,
                        y=actual_daily.values,
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
                    dataframe_with_one_index(forecast_monthly, use_container_width=True)

        st.markdown("#### Item Unit Forecast")
        item_forecast_df, month_cols = build_item_forecast(forecast_df, forecast_months)

        if month_cols:
            forecast_start = pd.Period(month_cols[0], freq="M").start_time.date()
            forecast_end = pd.Period(month_cols[-1], freq="M").end_time.date()
            st.caption(
                f"Forecast window: {forecast_start} to {forecast_end} ({forecast_months} month"
                f"{'s' if forecast_months != 1 else ''})."
            )

        if item_forecast_df.empty:
            st.info("Not enough data to build per-item forecasts yet.")
        else:
            it_left, it_right = st.columns([2, 1])

            with it_left:
                chart_caption("Projected units by item across the selected forecast horizon")
                plot_df = item_forecast_df.head(12).sort_values("Forecast Units", ascending=True)
                fig = px.bar(
                    plot_df,
                    x="Forecast Units",
                    y="Card Name",
                    orientation="h",
                    hover_data={
                        "Historical Units": True,
                    },
                    color_discrete_sequence=[SECONDARY],
                )
                fig = apply_chart_theme(fig, max(340, len(plot_df) * 32))
                fig.update_layout(xaxis_title="Projected Units", yaxis_title="Item")
                st.plotly_chart(fig, use_container_width=True)

            with it_right:
                st.metric("Forecasted items", f"{len(item_forecast_df):,}")
                if month_cols:
                    month_one_units = item_forecast_df[month_cols[0]].sum()
                    st.metric(f"{month_cols[0]} units", f"{int(month_one_units):,}")

            display_cols = [
                "Card Name",
                "Historical Units",
                *month_cols,
                "Forecast Units",
            ]
            dataframe_with_one_index(item_forecast_df[display_cols], use_container_width=True, height=340)

    # ===== SECTION 6: Repeat Buyers ==========================================
    if "buyer_analysis" in selected_section_set:
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
            buyer_agg["Total Spent"] = buyer_agg["Total_Spent"].apply(fmt_currency)
            buyer_agg["Last Purchase"] = pd.to_datetime(buyer_agg["Last_Purchase"], errors="coerce").dt.strftime("%Y-%m-%d")

            cb5l, cb5r = st.columns([2, 1])

            with cb5l:
                show_repeat = st.checkbox("Show repeat buyers only", value=True)
                display_df = buyer_agg[buyer_agg["Orders"] > 1] if show_repeat else buyer_agg
                display_cols = [c for c in ["Buyer User ID", "Name", "Orders", "Units",
                                             "Total Spent", "Last Purchase", "Card Name", "Tier"]
                                if c in display_df.columns]
                dataframe_with_one_index(
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
    if "coupon" in selected_section_set:
        section("Coupon Effectiveness")
        if "Coupon Code" not in df.columns:
            empty_chart_note()
        else:
            coupon_df = df[df["Coupon Code"].notna() & (df["Coupon Code"] != "")].copy()
            no_coupon_df = df[df["Coupon Code"].isna() | (df["Coupon Code"] == "")].copy()

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
                            Avg_Net_Order=("Order Net", "mean") if "Order Net" in coupon_df.columns else ("Order ID", "nunique"),
                        )
                        .reset_index()
                    )
                    coupon_tbl.columns = ["Coupon Code", "Uses", "Total Discounted", "Avg Net Order"]
                    coupon_tbl["Total Discounted"] = coupon_tbl["Total Discounted"].apply(fmt_currency)
                    coupon_tbl["Avg Net Order"] = coupon_tbl["Avg Net Order"].apply(fmt_currency)
                    dataframe_with_one_index(coupon_tbl, use_container_width=True)

            with c6r:
                coupon_order_count = coupon_df["Order ID"].nunique() if "Order ID" in coupon_df.columns else 0
                no_coupon_order_count = no_coupon_df["Order ID"].nunique() if "Order ID" in no_coupon_df.columns else 0
                pie_data = pd.DataFrame({
                    "Type": ["With coupon", "Without coupon"],
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
                    avg_with = coupon_df["Order Net"].mean() if not coupon_df.empty else 0
                    avg_without = no_coupon_df["Order Net"].mean() if not no_coupon_df.empty else 0
                    st.metric("Avg net w/ coupon", fmt_currency(avg_with))
                    st.metric("Avg net w/o coupon", fmt_currency(avg_without))

    # ===== SECTION 8: Customer Segments ======================================
    if "segments" in selected_section_set:
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
                        fig.update_traces(marker=dict(line=dict(width=1, color=THEME_MARKER_BORDER)))
                        fig.update_layout(xaxis_title="Total Spend", yaxis_title="Orders")
                        st.plotly_chart(fig, use_container_width=True)

                    with seg_right:
                        summary_display = cluster_summary.copy()
                        summary_display["Avg_Spend"] = summary_display["Avg_Spend"].apply(fmt_currency)
                        summary_display["Avg_AOV"] = summary_display["Avg_AOV"].apply(fmt_currency)
                        summary_display["Avg_Coupon_Rate"] = summary_display["Avg_Coupon_Rate"].map(lambda val: f"{val:.0%}")
                        summary_display["Avg_Orders"] = summary_display["Avg_Orders"].map(lambda val: f"{val:.1f}")
                        dataframe_with_one_index(summary_display, use_container_width=True)

    # ===== SECTION 9: Fulfillment Speed ======================================
    if "fulfillment" in selected_section_set:
        section("Fulfillment Speed")
        has_paid = "Date Paid" in df.columns
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
                avg_days = ship_df["Days to Ship"].mean()
                median_days = ship_df["Days to Ship"].median()
                same_day = int((ship_df["Days to Ship"] == 0).sum())

                c7m1, c7m2, c7m3 = st.columns(3)
                c7m1.metric("Avg days to ship", f"{avg_days:.1f}")
                c7m2.metric("Median days to ship", f"{median_days:.1f}")
                c7m3.metric("Same-day shipments", f"{same_day:,}")

                fig = px.histogram(
                    ship_df,
                    x="Days to Ship",
                    nbins=15,
                    color_discrete_sequence=[PRIMARY],
                )
                fig = apply_chart_theme(fig, 320)
                fig.update_layout(xaxis_title="Days to Ship", yaxis_title="Count")
                st.plotly_chart(fig, use_container_width=True)

    # ===== SECTION 10: Order History =========================================
    if "order_history" in selected_section_set:
        section("Order History")
        history_df = build_order_history(df)

        if history_df.empty:
            empty_chart_note()
        else:
            h1, h2, h3, h4 = st.columns([2, 1, 1, 1])

            search_text = h1.text_input(
                "Search order, buyer, or item",
                value="",
                key="history_search",
            )
            coupon_filter = h2.selectbox(
                "Coupon",
                ["All", "With coupon", "Without coupon"],
                key="history_coupon_filter",
            )
            sort_choice = h3.selectbox(
                "Sort",
                [
                    "Most recent to oldest",
                    "Oldest to most recent",
                    "Highest net to lowest net",
                    "Lowest net to highest net",
                ],
                index=0,
                key="history_sort_choice",
            )
            max_rows = h4.selectbox(
                "Rows",
                [25, 50, 100, 250],
                index=1,
                key="history_row_count",
            )

            filtered_history = history_df.copy()

            if search_text.strip():
                needle = search_text.strip().lower()
                search_cols = [
                    col
                    for col in ["Transaction ID", "Order ID", "Buyer", "Items", "Styles"]
                    if col in filtered_history.columns
                ]
                if search_cols:
                    haystack = (
                        filtered_history[search_cols]
                        .fillna("")
                        .astype(str)
                        .agg(" | ".join, axis=1)
                        .str.lower()
                    )
                    filtered_history = filtered_history[haystack.str.contains(needle, na=False)]

            if coupon_filter != "All" and "Coupon Code" in filtered_history.columns:
                has_coupon = filtered_history["Coupon Code"].fillna("").astype(str).str.strip().ne("")
                if coupon_filter == "With coupon":
                    filtered_history = filtered_history[has_coupon]
                else:
                    filtered_history = filtered_history[~has_coupon]

            if sort_choice == "Most recent to oldest":
                if "Sale Date" in filtered_history.columns:
                    filtered_history = filtered_history.sort_values("Sale Date", ascending=False, na_position="last")
                else:
                    filtered_history = filtered_history.sort_values("Order ID", ascending=False)
            elif sort_choice == "Oldest to most recent":
                if "Sale Date" in filtered_history.columns:
                    filtered_history = filtered_history.sort_values("Sale Date", ascending=True, na_position="last")
                else:
                    filtered_history = filtered_history.sort_values("Order ID", ascending=True)
            elif sort_choice == "Highest net to lowest net" and "Order Net" in filtered_history.columns:
                filtered_history = filtered_history.sort_values("Order Net", ascending=False, na_position="last")
            elif sort_choice == "Lowest net to highest net" and "Order Net" in filtered_history.columns:
                filtered_history = filtered_history.sort_values("Order Net", ascending=True, na_position="last")

            display = filtered_history.head(max_rows).copy()
            st.caption(f"Showing {len(display):,} of {len(filtered_history):,} orders")

            if "Order Net" in display.columns and "Shipping" in display.columns:
                display["Earnings"] = (
                    pd.to_numeric(display["Order Net"], errors="coerce").fillna(0)
                    - pd.to_numeric(display["Shipping"], errors="coerce").fillna(0)
                )
            elif {
                "Order Total",
                "Shipping",
                "Card Processing Fees",
            }.issubset(display.columns):
                display["Earnings"] = (
                    pd.to_numeric(display["Order Total"], errors="coerce").fillna(0)
                    - pd.to_numeric(display["Shipping"], errors="coerce").fillna(0)
                    - pd.to_numeric(display["Card Processing Fees"], errors="coerce").fillna(0)
                )
            elif {"Order Total", "Shipping"}.issubset(display.columns):
                display["Earnings"] = (
                    pd.to_numeric(display["Order Total"], errors="coerce").fillna(0)
                    - pd.to_numeric(display["Shipping"], errors="coerce").fillna(0)
                )

            for dt_col in ["Sale Date", "Date Paid", "Date Shipped"]:
                if dt_col in display.columns:
                    display[dt_col] = pd.to_datetime(display[dt_col], errors="coerce").dt.strftime("%Y-%m-%d")

            for money_col in [
                "Order Total",
                "Card Processing Fees",
                "Order Net",
                "Earnings",
                "Shipping",
                "Sales Tax",
                "Discount Amount",
            ]:
                if money_col in display.columns:
                    display[money_col] = pd.to_numeric(display[money_col], errors="coerce").fillna(0).map(fmt_currency)

            st.caption("Order Total = sticker price - discount + shipping")
            st.caption("Order Net = sticker price - discount + shipping - card processing fee")
            st.caption("Earnings = Order Net - shipping (shipping reimburses postage)")

            display_columns = [
                "Sale Date",
                "Transaction ID",
                "Buyer",
                "Items",
                "Units",
                "Order Total",
                "Order Net",
                "Earnings",
                "Coupon Code",
                "Order Type",
                "Payment Method",
                "Order ID",
                "Ship State",
                "Date Shipped",
            ]
            display_columns = [col for col in display_columns if col in display.columns]
            dataframe_with_one_index(display[display_columns], use_container_width=True, height=360)

    render_credits()


if __name__ == "__main__":
    main()

