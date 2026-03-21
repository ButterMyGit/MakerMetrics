"""
watcher.py — Etsy CSV ingestion watcher

Monitors WATCH_DIR for incoming Etsy CSV exports, merges/cleans/enriches them,
and upserts records into Firestore using Transaction ID as the document ID.

Supported formats
-----------------
  items    — EtsySoldOrderItems export  (has Transaction ID + Item Name, no Full Name)
  orders   — EtsySoldOrders export      (has Full Name, no Transaction ID)
  combined — Finance / combined export  (has Transaction ID + Full Name)
    listings — Etsy listings export       (has TITLE + PRICE + QUANTITY, no Transaction ID)

Run with:  python watcher.py
"""

import logging
import os
import re
import shutil
import threading
import time
import hashlib
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1 import SERVER_TIMESTAMP

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
load_dotenv()
WATCH_DIR = Path(os.getenv("WATCH_DIR", "data/watch"))
PAIR_WAIT = int(os.getenv("PAIR_WAIT_SECONDS", "30"))
CRED_PATH = os.getenv("FIREBASE_CREDENTIALS", "secrets/firebase.json")
SALES_COLLECTION = os.getenv("SALES_COLLECTION", "sales")
LISTINGS_COLLECTION = os.getenv("LISTINGS_COLLECTION", "listings")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Shared pairing state  { minute_key: {"items": [(Path, Timer)], "orders": [Path]} }
_pair_state: dict[str, dict] = {}
_pair_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Firebase init
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


def init_firebase() -> firestore.Client:
    resolved_cred_path = resolve_credentials_path(CRED_PATH)
    if not firebase_admin._apps:
        cred = credentials.Certificate(str(resolved_cred_path))
        firebase_admin.initialize_app(cred)
    return firestore.client()


# ---------------------------------------------------------------------------
# Format detection
# ---------------------------------------------------------------------------
def detect_format(df: pd.DataFrame) -> str:
    cols = {str(col).strip() for col in df.columns}
    upper_cols = {col.upper() for col in cols}

    has_listing_title = "TITLE" in upper_cols
    has_listing_price = "PRICE" in upper_cols
    has_listing_qty = "QUANTITY" in upper_cols
    has_tid = "TRANSACTION ID" in upper_cols
    has_full = "FULL NAME" in upper_cols
    has_item = "ITEM NAME" in upper_cols

    if has_listing_title and has_listing_price and has_listing_qty and not has_tid:
        return "listings"
    if has_tid and has_full:
        return "combined"
    if has_tid and has_item:
        return "items"
    if has_full:
        return "orders"
    if has_tid:
        return "items"
    return "orders"


# ---------------------------------------------------------------------------
# Column deduplication helper
# ---------------------------------------------------------------------------
def prefer(df: pd.DataFrame, keep: str, drop: str) -> pd.DataFrame:
    """Fill nulls in `keep` from `drop`, then drop `drop`.
    If only `drop` exists, rename it to `keep`.
    """
    if keep in df.columns and drop in df.columns:
        df[keep] = df[keep].fillna(df[drop])
        df.drop(columns=[drop], inplace=True)
    elif drop in df.columns:
        df.rename(columns={drop: keep}, inplace=True)
    return df


# ---------------------------------------------------------------------------
# Merge: items (Format A) + orders (Format B)
# ---------------------------------------------------------------------------
_ORDERS_DROP_COLS = [
    "Sale Date", "Coupon Code", "Coupon Details", "Discount Amount",
    "Shipping Discount", "Shipping", "Sales Tax", "Order Type", "Payment Type",
    "InPerson Discount", "InPerson Location", "SKU", "Currency", "Date Shipped",
    "Ship City", "Ship State", "Ship Zipcode", "Ship Country", "Street 1", "Street 2",
]

def merge_items_orders(items: pd.DataFrame, orders: pd.DataFrame) -> pd.DataFrame:
    if "Order ID" not in items.columns or "Order ID" not in orders.columns:
        log.warning(
            "Cannot merge items+orders reliably — missing Order ID column(s). "
            "items_has_order_id=%s orders_has_order_id=%s",
            "Order ID" in items.columns,
            "Order ID" in orders.columns,
        )
        return items

    # Normalize keys before joining so IDs like "12345.0" and "12345" match.
    items["Order ID"] = items["Order ID"].map(_clean_id)
    orders["Order ID"] = orders["Order ID"].map(_clean_id)

    # Orders export should be one row per order; enforce this to avoid fan-out joins.
    orders = orders.dropna(subset=["Order ID"]).drop_duplicates(subset=["Order ID"], keep="first")

    items = items.rename(columns={
        "Buyer":           "Buyer Username",
        "Order Shipping":  "Shipping",
        "Order Sales Tax": "Sales Tax",
        "Ship Address1":   "Street 1",
        "Ship Address2":   "Street 2",
    })
    orders = orders.rename(columns={"Buyer": "_orders_display"})
    drop = [c for c in _ORDERS_DROP_COLS if c in orders.columns]
    orders = orders.drop(columns=drop)

    merged = items.merge(orders, on="Order ID", how="left", suffixes=("", "_orders"), indicator=True)

    # Prefer order-level metrics from the SoldOrders file when available.
    order_priority_cols = [
        "Order Value",
        "Order Total",
        "Card Processing Fees",
        "Order Net",
        "Adjusted Order Total",
        "Adjusted Card Processing Fees",
        "Adjusted Net Order Amount",
        "Date Paid",
    ]
    for col in order_priority_cols:
        orders_col = f"{col}_orders"
        if orders_col in merged.columns:
            if col in merged.columns:
                merged[col] = merged[orders_col].combine_first(merged[col])
            else:
                merged[col] = merged[orders_col]
            merged.drop(columns=[orders_col], inplace=True)

    if "Full Name" in merged.columns and "_orders_display" in merged.columns:
        merged["Full Name"] = merged["Full Name"].fillna(merged["_orders_display"])
    if "_orders_display" in merged.columns:
        merged.drop(columns=["_orders_display"], inplace=True)

    # Drop any remaining orders-suffixed duplicates after explicit coalescing.
    remaining_orders_cols = [c for c in merged.columns if c.endswith("_orders")]
    if remaining_orders_cols:
        merged.drop(columns=remaining_orders_cols, inplace=True)

    unmatched = int((merged["_merge"] == "left_only").sum())
    if unmatched:
        log.warning(
            "Merged items+orders with %d unmatched item rows out of %d. "
            "Check Order ID formatting in source files.",
            unmatched,
            len(merged),
        )
    merged.drop(columns=["_merge"], inplace=True)

    return merged


# ---------------------------------------------------------------------------
# Dedup: combined format (Format C)
# ---------------------------------------------------------------------------
def dedup_combined(df: pd.DataFrame) -> pd.DataFrame:
    df = prefer(df, "Buyer Username",    "Buyer_x")
    df = prefer(df, "Full Name",         "Buyer_y")
    df = prefer(df, "Coupon Code",       "Order Coupon Code")
    df = prefer(df, "Coupon Details",    "Order Coupon Details")
    df = prefer(df, "Discount Amount",   "Order Discount Amount")
    df = prefer(df, "Shipping",          "Order Shipping Amount")
    df = prefer(df, "Shipping",          "Order Shipping")
    df = prefer(df, "Shipping Discount", "Order Shipping Discount")
    df = prefer(df, "Sales Tax",         "Order Sales Tax Amount")
    df = prefer(df, "Sales Tax",         "Order Sales Tax")
    df = prefer(df, "Order Type",        "Item Order Type")
    df = prefer(df, "Payment Type",      "Item Payment Type")
    df = prefer(df, "InPerson Discount", "Item InPerson Discount")
    df = prefer(df, "InPerson Discount", "Order InPerson Discount")
    df = prefer(df, "InPerson Location", "Item InPerson Location")
    df = prefer(df, "InPerson Location", "Order InPerson Location")
    df = prefer(df, "SKU",               "Item SKU")
    df = prefer(df, "SKU",               "Order SKU")
    df = prefer(df, "Currency",          "Item Currency")
    df = prefer(df, "Date Shipped",      "Item Date Shipped")
    df = prefer(df, "Street 1",          "Ship Address1")
    df = prefer(df, "Street 2",          "Ship Address2")
    df = prefer(df, "Ship City",         "Item Ship City")
    df = prefer(df, "Ship State",        "Item Ship State")
    df = prefer(df, "Ship Zipcode",      "Item Ship Zipcode")
    df = prefer(df, "Ship Country",      "Item Ship Country")
    return df


# ---------------------------------------------------------------------------
# Cleaning pipeline
# ---------------------------------------------------------------------------
_CURRENCY_COLS = [
    "Price", "Item Total", "Discount Amount", "Shipping", "Shipping Discount",
    "Sales Tax", "Order Value", "Order Total", "Card Processing Fees", "Order Net",
    "Adjusted Order Total", "Adjusted Card Processing Fees",
    "Adjusted Net Order Amount", "VAT Paid by Buyer", "InPerson Discount",
]
_DATE_COLS   = ["Sale Date", "Date Paid", "Date Shipped"]
_ID_COLS     = ["Transaction ID", "Order ID"]
_DATE_FMTS   = ["%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"]


def _parse_currency(val):
    if pd.isna(val):
        return None
    s = str(val).strip().replace("$", "").replace(",", "").strip()
    if re.fullmatch(r"[-\s]*", s) or s == "":
        return 0.0
    try:
        return float(s)
    except ValueError:
        return None


def _parse_date(val):
    if pd.isna(val):
        return None
    s = str(val).strip()
    for fmt in _DATE_FMTS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _clean_id(val):
    if pd.isna(val):
        return None

    s = str(val).strip()
    if not s:
        return None

    # Etsy exports sometimes include # prefixes or numeric IDs rendered as xxxxx.0
    s = s.lstrip("#").strip()
    if re.fullmatch(r"\d+\.0+", s):
        s = s.split(".", 1)[0]

    return s if s else None


def clean(df: pd.DataFrame) -> pd.DataFrame:
    # Currency
    for col in _CURRENCY_COLS:
        if col in df.columns:
            df[col] = df[col].map(_parse_currency)

    # Dates
    for col in _DATE_COLS:
        if col in df.columns:
            df[col] = df[col].map(_parse_date)

    # IDs
    for col in _ID_COLS:
        if col in df.columns:
            df[col] = df[col].map(_clean_id)

    # Strings — empty / whitespace → None
    str_cols = df.select_dtypes(include="object").columns
    for col in str_cols:
        df[col] = df[col].apply(
            lambda v: None if (pd.isna(v) or str(v).strip() == "") else str(v).strip()
        )

    # Drop fully empty rows
    df.dropna(how="all", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


# ---------------------------------------------------------------------------
# Enrichment
# ---------------------------------------------------------------------------
def enrich(df: pd.DataFrame) -> pd.DataFrame:
    if "Item Name" in df.columns:
        parts = df["Item Name"].str.split("|", n=1, expand=True)
        df["Card Name"]    = parts[0].str.strip() if 0 in parts else None
        df["Product Type"] = parts[1].str.strip() if 1 in parts else None

    if "Variations" in df.columns:
        df["Style"] = (
            df["Variations"]
            .astype(str)
            .str.replace(r"^(Style:|Custom Property:)\s*", "", regex=True)
            .str.strip()
        )
        df["Style"] = df["Style"].apply(lambda v: None if v in ("nan", "") else v)

    return df


# ---------------------------------------------------------------------------
# Firestore upsert
# ---------------------------------------------------------------------------
BATCH_SIZE = 490


def upsert_to_firestore(db: firestore.Client, df: pd.DataFrame, source_file: str):
    sales_ref = db.collection(SALES_COLLECTION)
    batch = db.batch()
    count = 0
    skipped = 0
    committed = 0

    for _, row in df.iterrows():
        tid = row.get("Transaction ID")
        if not tid:
            log.warning("Skipping row — missing Transaction ID (source: %s)", source_file)
            skipped += 1
            continue

        doc_ref = sales_ref.document(str(tid))
        record  = {k: v for k, v in row.items() if pd.notna(v) and v is not None}
        record["_updated_at"] = SERVER_TIMESTAMP

        snap = doc_ref.get()
        if not snap.exists:
            record["_created_at"] = SERVER_TIMESTAMP

        batch.set(doc_ref, record, merge=True)
        count += 1

        if count % BATCH_SIZE == 0:
            batch.commit()
            committed += count
            log.info("  Committed batch of %d records", BATCH_SIZE)
            batch = db.batch()
            count = 0

    if count:
        batch.commit()
        committed += count

    log.info("Upserted %d records, skipped %d (source: %s)", committed, skipped, source_file)


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug[:180]


def _listing_doc_id(row: dict) -> str | None:
    listing_id = row.get("Listing ID")
    if listing_id:
        cleaned_listing_id = _clean_id(listing_id)
        if cleaned_listing_id:
            listing_slug = _slugify(str(cleaned_listing_id).strip())
            if listing_slug:
                return f"listing-id-{listing_slug}"

    sku = row.get("SKU")
    if sku:
        sku_slug = _slugify(str(sku).strip())
        if sku_slug:
            return sku_slug

    title = row.get("Listing Title")
    style_1 = row.get("Variation 1 Values")
    style_2 = row.get("Variation 2 Values")
    base = " | ".join([str(v).strip() for v in [title, style_1, style_2] if v])

    if not base:
        return None

    slug = _slugify(base)
    if slug:
        return slug

    digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]
    return f"listing-{digest}"


def clean_listings(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "LISTING_ID": "Listing ID",
        "LISTING ID": "Listing ID",
        "TITLE": "Listing Title",
        "DESCRIPTION": "Listing Description",
        "PRICE": "Listing Price",
        "CURRENCY_CODE": "Currency",
        "QUANTITY": "Available Quantity",
        "TAGS": "Tags",
        "MATERIALS": "Materials",
        "VARIATION 1 VALUES": "Variation 1 Values",
        "VARIATION 2 VALUES": "Variation 2 Values",
    }

    for source, target in rename_map.items():
        if source in df.columns and target not in df.columns:
            df.rename(columns={source: target}, inplace=True)

    str_cols = df.select_dtypes(include="object").columns
    for col in str_cols:
        df[col] = df[col].apply(
            lambda v: None if (pd.isna(v) or str(v).strip() == "") else str(v).strip()
        )

    if "Listing Price" in df.columns:
        df["Listing Price"] = df["Listing Price"].map(_parse_currency)

    if "Available Quantity" in df.columns:
        qty = pd.to_numeric(df["Available Quantity"], errors="coerce").fillna(0)
        df["Available Quantity"] = qty.astype(int)

    if "Listing ID" in df.columns:
        df["Listing ID"] = df["Listing ID"].map(_clean_id)

    if "Listing Title" in df.columns:
        parts = df["Listing Title"].str.split("|", n=1, expand=True)
        df["Card Name"] = parts[0].str.strip() if 0 in parts else None
        df["Product Type"] = parts[1].str.strip() if 1 in parts else None

    style_source = None
    for candidate in ["Variation 1 Values", "Variations", "Style"]:
        if candidate in df.columns:
            style_source = candidate
            break

    if style_source:
        df["Style"] = (
            df[style_source]
            .astype(str)
            .str.replace(r"^(Style:|Custom Property:)\s*", "", regex=True)
            .str.strip()
        )
        df["Style"] = df["Style"].apply(lambda v: None if v in ("nan", "") else v)

    df.dropna(how="all", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def upsert_listings_to_firestore(db: firestore.Client, df: pd.DataFrame, source_file: str):
    listings_ref = db.collection(LISTINGS_COLLECTION)
    batch = db.batch()
    count = 0
    skipped = 0
    committed = 0

    for _, row in df.iterrows():
        record = {k: v for k, v in row.items() if pd.notna(v) and v is not None}
        doc_id = _listing_doc_id(record)

        if not doc_id:
            skipped += 1
            continue

        doc_ref = listings_ref.document(doc_id)
        record["_updated_at"] = SERVER_TIMESTAMP
        record["_source_file"] = source_file

        snap = doc_ref.get()
        if not snap.exists:
            record["_created_at"] = SERVER_TIMESTAMP

        batch.set(doc_ref, record, merge=True)
        count += 1

        if count % BATCH_SIZE == 0:
            batch.commit()
            committed += count
            log.info("  Committed batch of %d listing records", BATCH_SIZE)
            batch = db.batch()
            count = 0

    if count:
        batch.commit()
        committed += count

    log.info("Upserted %d listings, skipped %d (source: %s)", committed, skipped, source_file)


# ---------------------------------------------------------------------------
# Archive
# ---------------------------------------------------------------------------
def archive(path: Path):
    dest_dir = WATCH_DIR / "processed"
    dest_dir.mkdir(parents=True, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = dest_dir / f"{ts}_{path.name}"
    shutil.move(str(path), dest)
    log.info("Archived %s → %s", path.name, dest)


# ---------------------------------------------------------------------------
# Core processing
# ---------------------------------------------------------------------------
def process_files(db: firestore.Client, items_path: Path, orders_path: Path | None = None):
    log.info("Processing: items=%s  orders=%s", items_path.name, orders_path.name if orders_path else "None")

    items_df = pd.read_csv(items_path, dtype=str)
    items_df.columns = items_df.columns.str.strip()

    if orders_path:
        orders_df = pd.read_csv(orders_path, dtype=str)
        orders_df.columns = orders_df.columns.str.strip()
        df = merge_items_orders(items_df, orders_df)
        log.info("Merged items+orders: %d rows", len(df))
    else:
        fmt = detect_format(items_df)
        if fmt == "combined":
            df = dedup_combined(items_df)
            log.info("Deduped combined format: %d rows", len(df))
        else:
            df = items_df
            log.info("Solo items format: %d rows", len(df))

    df = clean(df)
    df = enrich(df)
    upsert_to_firestore(db, df, items_path.name)

    archive(items_path)
    if orders_path:
        archive(orders_path)


def process_listings_file(db: firestore.Client, path: Path):
    log.info("Processing listings: %s", path.name)

    listings_df = pd.read_csv(path, dtype=str)
    listings_df.columns = listings_df.columns.str.strip()
    listings_df = clean_listings(listings_df)
    upsert_listings_to_firestore(db, listings_df, path.name)
    archive(path)


def process_single_csv(db: firestore.Client, path: Path):
    """Entry point for a single CSV arriving without a pair."""
    df = pd.read_csv(path, dtype=str)
    df.columns = df.columns.str.strip()
    fmt = detect_format(df)

    if fmt == "orders":
        log.warning(
            "Skipping %s — it is a SoldOrders file with no Transaction ID. "
            "Drop the matching SoldOrderItems file alongside it to process.",
            path.name,
        )
        return

    if fmt == "listings":
        process_listings_file(db, path)
        return

    process_files(db, path)


def process_existing_csvs(db: firestore.Client, paths: list[Path] | None = None) -> dict[str, int]:
    """Process a batch of existing CSVs, pairing items/orders FIFO by filename order."""
    existing = sorted(list(paths) if paths is not None else WATCH_DIR.glob("*.csv"), key=lambda p: p.name)
    summary = {
        "input_files": len(existing),
        "pairs": 0,
        "solo_items": 0,
        "combined": 0,
        "listings": 0,
        "skipped_orders": 0,
    }

    if not existing:
        return summary

    log.info("Found %d existing CSV(s) — processing batch", len(existing))
    items_q = []
    orders_q = []
    listings_q = []
    other_q = []

    for path in existing:
        try:
            hdr = pd.read_csv(path, dtype=str, nrows=0)
            hdr.columns = hdr.columns.str.strip()
            fmt = detect_format(hdr)
        except Exception as exc:
            log.warning("Could not read %s: %s", path.name, exc)
            continue

        if fmt == "items":
            items_q.append(path)
        elif fmt == "orders":
            orders_q.append(path)
        elif fmt == "listings":
            listings_q.append(path)
        else:
            other_q.append(path)

    for item_path in items_q:
        if orders_q:
            order_path = orders_q.pop(0)
            log.info("Batch pair: %s + %s", item_path.name, order_path.name)
            process_files(db, item_path, order_path)
            summary["pairs"] += 1
        else:
            process_single_csv(db, item_path)
            summary["solo_items"] += 1

    for order_path in orders_q:
        log.warning(
            "Batch: SoldOrders file %s has no matching SoldOrderItems file — cannot ingest (no Transaction ID).",
            order_path.name,
        )
        summary["skipped_orders"] += 1

    for path in listings_q:
        process_listings_file(db, path)
        summary["listings"] += 1

    for path in other_q:
        process_single_csv(db, path)
        summary["combined"] += 1

    return summary


# ---------------------------------------------------------------------------
# Pairing logic
# ---------------------------------------------------------------------------
def _minute_key() -> str:
    return datetime.now().strftime("%Y%m%d%H%M")


def _adjacent_keys(key: str) -> list[str]:
    """Return the key itself plus adjacent minute keys (±1 min)."""
    dt  = datetime.strptime(key, "%Y%m%d%H%M")
    return [
        (dt.replace(minute=max(0, dt.minute - 1))).strftime("%Y%m%d%H%M"),
        key,
        (dt.replace(minute=min(59, dt.minute + 1))).strftime("%Y%m%d%H%M"),
    ]


def handle_new_file(db: firestore.Client, path: Path):
    if not path.suffix.lower() == ".csv":
        return
    if not path.exists():
        return

    time.sleep(1)  # ensure write is complete

    df  = pd.read_csv(path, dtype=str)
    df.columns = df.columns.str.strip()
    fmt = detect_format(df)

    if fmt == "combined":
        process_files(db, path)
        return

    if fmt == "listings":
        process_listings_file(db, path)
        return

    if fmt == "orders":
        # Check whether any items file is already waiting for a partner
        with _pair_lock:
            waiting_items = None
            for k in _adjacent_keys(_minute_key()):
                bucket = _pair_state.get(k, {})
                items_list = bucket.get("items", [])
                if items_list:
                    item_path, item_timer = items_list.pop(0)
                    if item_timer:
                        item_timer.cancel()
                    if not items_list:
                        bucket.pop("items", None)
                    if not bucket:
                        _pair_state.pop(k, None)
                    waiting_items = item_path
                    break

        if waiting_items:
            log.info(
                "Matched %s with waiting items file %s — processing pair",
                path.name, waiting_items.name,
            )
            process_files(db, waiting_items, path)
        else:
            log.warning(
                "Received SoldOrders file %s — waiting to see if a SoldOrderItems "
                "file arrives. It cannot be ingested alone (no Transaction ID).",
                path.name,
            )
            with _pair_lock:
                key = _minute_key()
                _pair_state.setdefault(key, {}).setdefault("orders", []).append(path)
        return

    # fmt == "items"
    key = _minute_key()
    with _pair_lock:
        # Look for a waiting orders file in adjacent minute buckets
        partner = None
        for k in _adjacent_keys(key):
            bucket = _pair_state.get(k, {})
            orders_list = bucket.get("orders", [])
            if orders_list:
                partner = orders_list.pop(0)
                if not orders_list:
                    bucket.pop("orders", None)
                if not bucket:
                    _pair_state.pop(k, None)
                break

        if partner:
            process_files(db, path, partner)
            return

        # No orders file yet — register this items file and start a solo countdown

    def _solo_fallback():
        with _pair_lock:
            for k in list(_pair_state.keys()):
                bucket = _pair_state.get(k, {})
                items_list = bucket.get("items", [])
                entry = next(((p, t) for p, t in items_list if p == path), None)
                if entry:
                    items_list.remove(entry)
                    if not items_list:
                        bucket.pop("items", None)
                    if not bucket:
                        _pair_state.pop(k, None)
                    break
        log.info("Pair timeout — processing %s solo", path.name)
        process_single_csv(db, path)

    timer = threading.Timer(PAIR_WAIT, _solo_fallback)
    with _pair_lock:
        _pair_state.setdefault(key, {}).setdefault("items", []).append((path, timer))
    timer.start()
    log.info(
        "Waiting up to %ds for a matching SoldOrders file (items: %s)",
        PAIR_WAIT, path.name,
    )


# ---------------------------------------------------------------------------
# Watchdog handler
# ---------------------------------------------------------------------------
class EtsyCSVHandler(FileSystemEventHandler):
    def __init__(self, db: firestore.Client):
        self.db = db

    def _handle(self, path_str: str):
        path = Path(path_str)
        # Only top-level files in WATCH_DIR, not subdirectories
        if path.parent.resolve() != WATCH_DIR.resolve():
            return
        if path.suffix.lower() != ".csv":
            return
        handle_new_file(self.db, path)

    def on_created(self, event):
        if not event.is_directory:
            self._handle(event.src_path)

    def on_moved(self, event):
        if not event.is_directory:
            self._handle(event.dest_path)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    db = init_firebase()
    log.info("Firebase connected. Watching %s", WATCH_DIR.resolve())

    WATCH_DIR.mkdir(parents=True, exist_ok=True)

    process_existing_csvs(db)

    handler  = EtsyCSVHandler(db)
    observer = Observer()
    observer.schedule(handler, str(WATCH_DIR), recursive=False)
    observer.start()
    log.info("Watcher started. Drop Etsy CSV exports into %s", WATCH_DIR)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutting down watcher…")
        observer.stop()
    observer.join()


if __name__ == "__main__":
    main()

