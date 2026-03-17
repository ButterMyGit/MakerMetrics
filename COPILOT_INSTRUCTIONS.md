# GitHub Copilot Instructions — TheSlabGuy Sales Dashboard (Local)

You are building a local sales analytics dashboard for an Etsy shop called
TheSlabGuy that sells custom Pokémon card display slabs. The stack is:

- **Python 3.12+**
- **Streamlit** — dashboard UI
- **Plotly** — all charts
- **pandas** — data processing
- **firebase-admin** — Firestore as the database
- **unsure still** — file system monitoring for CSV ingestion

This project will eventually be containerized with Docker. It will have a service that watches a local folder for the json and/or csv exports from Etsy, ingests them into Firestore, and a Streamlit dashboard that reads from Firestore and displays analytics.

---

## Project Structure

Create the following layout:

```
slabguy-dashboard/
├── app.py                  # Streamlit dashboard
├── watcher.py              # CSV ingestion watcher
├── requirements.txt
├── .env                    # secrets — never commit
├── .env.example            # template
├── .gitignore
├── secrets/
│   └── firebase.json       # Firebase service account key — never commit
└── data/
    └── watch/
        └── processed/      # ingested CSVs are moved here automatically
```

---

## Step 1 — Project Setup

Create `.gitignore`:
```
.env
secrets/
venv/
data/watch/*.csv
data/watch/processed/
__pycache__/
*.pyc
.DS_Store
```
plus any IDE-specific ignores (e.g. `.vscode/`) or anything MacOS creates.

Create `.env.example`:
```
FIREBASE_CREDENTIALS=secrets/firebase.json
WATCH_DIR=data/watch
PAIR_WAIT_SECONDS=30
```

Copy `.env.example` to `.env` — this is what the app reads at runtime.

---

## Step 2 — Firebase Credentials

I will place the Firebase service account JSON file (downloaded from Firebase Console
→ Project Settings → Service Accounts → Generate new private key) at:
```
secrets/firebase.json
```

This file is in `.gitignore` and must never be committed.

---

## Step 3 — watcher.py

Build `watcher.py` with the following behaviour:

### Input format detection

The watcher must accept these CSV formats without any manual configuration:

**Format A — Raw `EtsySoldOrderItems` export**
Detected by: has `Transaction ID` column, has `Item Name`, no `Full Name`
Contains: per-item data, transaction IDs, listing IDs, variations, item prices

**Format B — Raw `EtsySoldOrders` export**
Detected by: has `Full Name`, no `Transaction ID`
Contains: order-level data, buyer name, shipping address, order totals

Detection logic:
```python
def detect_format(df):
    cols = set(df.columns.str.strip())
    has_tid  = "Transaction ID" in cols
    has_full = "Full Name" in cols
    has_item = "Item Name" in cols
    if has_tid and has_full:  return "combined"
    if has_tid and has_item:  return "items"
    if has_full:              return "orders"
    if has_tid:               return "items"
    return "orders"
```

### Pairing logic (Format A + B)

When a SoldOrderItems file is dropped, wait up to `PAIR_WAIT_SECONDS` (default 30)
for a matching SoldOrders file to appear. If both arrive within that window,
join them on `Order ID`. If only the items file arrives, process it solo
(it has enough data via Transaction ID). A solo SoldOrders file cannot be
ingested — log a warning explaining it has no Transaction ID.

Use `threading.Timer` for the delayed solo fallback. Use a shared dict
keyed by `datetime.now().strftime("%Y%m%d%H%M")` for pairing. Match keys
that differ by at most 1 (adjacent minutes).

### Merge logic (joining Format A + B)

Join on `Order ID` with a left merge (items is the left table).

Before merging, apply these renames to the items DataFrame:
```python
items = items.rename(columns={
    "Buyer":           "Buyer Username",   # items Buyer = Etsy username
    "Order Shipping":  "Shipping",
    "Order Sales Tax": "Sales Tax",
    "Ship Address1":   "Street 1",
    "Ship Address2":   "Street 2",
})
```

Drop these columns from the orders DataFrame before merging (items version
is canonical or the two are identical):
```
Sale Date, Coupon Code, Coupon Details, Discount Amount, Shipping Discount,
Shipping, Sales Tax, Order Type, Payment Type, InPerson Discount,
InPerson Location, SKU, Currency, Date Shipped, Ship City, Ship State,
Ship Zipcode, Ship Country, Street 1, Street 2
```

Before dropping, rename `Buyer` in orders to `_orders_display`. After the
merge, fill any null `Full Name` values from `_orders_display`, then drop it.

### Deduplication logic (Format C — combined CSV)

Apply these column-merge rules in order using a `prefer(keep, drop)` helper
that fills nulls in `keep` from `drop`, then drops `drop`:

```
Buyer Username    ← Buyer_x
Full Name         ← Buyer_y
Coupon Code       ← Order Coupon Code
Coupon Details    ← Order Coupon Details
Discount Amount   ← Order Discount Amount
Shipping          ← Order Shipping Amount  (then again ← Order Shipping)
Shipping Discount ← Order Shipping Discount
Sales Tax         ← Order Sales Tax Amount (then again ← Order Sales Tax)
Order Type        ← Item Order Type
Payment Type      ← Item Payment Type
InPerson Discount ← Item InPerson Discount, then ← Order InPerson Discount
InPerson Location ← Item InPerson Location, then ← Order InPerson Location
SKU               ← Item SKU, then ← Order SKU
Currency          ← Item Currency
Date Shipped      ← Item Date Shipped
Street 1          ← Ship Address1
Street 2          ← Ship Address2
Ship City         ← Item Ship City
Ship State        ← Item Ship State
Ship Zipcode      ← Item Ship Zipcode
Ship Country      ← Item Ship Country
```

### Cleaning pipeline (applied to all formats after merge/dedup)

**Currency columns** — strip `$`, `,`, whitespace; replace `-` with `0`; parse as float:
```
Price, Item Total, Discount Amount, Shipping, Shipping Discount,
Sales Tax, Order Value, Order Total, Card Processing Fees, Order Net,
Adjusted Order Total, Adjusted Card Processing Fees,
Adjusted Net Order Amount, VAT Paid by Buyer, InPerson Discount
```

**Date columns** — parse with formats `%m/%d/%Y`, `%m/%d/%y`, `%Y-%m-%d`
(try each in order, return None if all fail):
```
Sale Date, Date Paid, Date Shipped
```

**ID columns** — cast to string, strip, split on `.` and take first part
(removes float suffixes like `3633762006.0`):
```
Transaction ID, Order ID
```

**String columns** — replace empty strings and whitespace-only values with None.

**Drop fully empty rows.**

### Enrichment (derived columns, applied after cleaning)

Split `Item Name` on `|` into:
- `Card Name` — everything before the pipe, stripped
- `Product Type` — everything after the pipe, stripped (e.g. "Custom Pokémon Artwork Display Slab")

Clean `Variations` into `Style` by stripping the `Style:` or `Custom Property:` prefix:
```python
df["Style"] = df["Variations"].str.replace(r"^(Style:|Custom Property:)", "", regex=True).str.strip()
```

### Firestore upsert

Use `Transaction ID` (as string) as the Firestore document ID in a `sales` collection.
Use `batch.set(doc_ref, record, merge=True)` for upsert semantics.
Commit in batches of 490 (Firestore limit is 500).
Add `_updated_at: SERVER_TIMESTAMP` on every write.
Add `_created_at: SERVER_TIMESTAMP` only on new documents (check `.exists` first).
Skip rows with missing/null Transaction ID and log a warning.

### Archiving

After successful processing, move the CSV to `data/watch/processed/` with a
timestamp prefix: `YYYYMMDD_HHMMSS_originalname.csv`.

### Watchdog setup

Use `watchdog.observers.Observer` with a `FileSystemEventHandler` that handles
`on_created` and `on_moved` events (some OS/tools move files into folders rather
than creating them). Filter to `.csv` files in `WATCH_DIR` only (not subdirectories).
Sleep 1 second after detecting a file before reading (ensures write is complete).

On startup, process any `.csv` files already sitting in the watch folder.

Load config from `.env` using `python-dotenv`:
```python
from dotenv import load_dotenv
load_dotenv()
WATCH_DIR  = Path(os.getenv("WATCH_DIR", "data/watch"))
PAIR_WAIT  = int(os.getenv("PAIR_WAIT_SECONDS", "30"))
cred_path  = os.getenv("FIREBASE_CREDENTIALS", "secrets/firebase.json")
```

---

## Step 4 — app.py

Build `app.py` as a Streamlit dashboard with the following sections.

### Config

```python
st.set_page_config(
    page_title="TheSlabGuy — Sales Dashboard",
    page_icon="🃏",
    layout="wide",
    initial_sidebar_state="expanded",
)
```

Load `.env` with `python-dotenv` before initialising Firebase.

### Firebase / data loading

Cache the Firebase client with `@st.cache_resource`.
Cache data with `@st.cache_data(ttl=300)` (5-minute refresh).

In the data loader, fetch all documents from the `sales` Firestore collection,
convert to a DataFrame, then:
- Parse date columns with `pd.to_datetime(..., errors="coerce")`
- Parse numeric columns with `pd.to_numeric(..., errors="coerce").fillna(0)`
- Parse `Quantity` as int
- Add `Year-Month` helper column: `df["Sale Date"].dt.to_period("M").astype(str)`
- Add `Week` helper column: `df["Sale Date"].dt.to_period("W").astype(str)`

### Sidebar filters

- **Quick date preset** selectbox: All time, Last 30 days, Last 90 days, This year, Last year, Custom
- Custom preset shows two `st.date_input` widgets
- **Order type** multiselect (default: all)
- **Style / Variation** multiselect (default: all)
- Force refresh button that calls `st.cache_data.clear()` then `st.rerun()`
- Show last load time

Apply all filters to produce a working `df` from `df_all`.

### KPI row (6 columns)

Display these metrics in custom HTML cards with a blue left border:
- Total Orders (`Order ID` nunique)
- Units Sold (`Quantity` sum)
- Net Revenue (`Order Net` sum, formatted `$X,XXX.XX`)
- Avg Order Value (mean of first `Order Total` per `Order ID`, formatted `$X.XX`)
- Repeat Buyers (count of `Buyer User ID`s with more than 1 unique `Order ID`)
- Unique Products (`Card Name` nunique)

### Section 1 — Most Popular Products (left column)

Horizontal bar chart using Plotly Express.
Group by `Card Name`, aggregate: `Units` (Quantity sum), `Orders` (Order ID nunique), `Revenue` (Order Net sum).
`st.slider` for top N (5–20, default 10).
`st.radio` to rank by Units / Orders / Revenue.
Sort ascending for horizontal bar so highest is at top.
Include `Units`, `Orders`, `Revenue` in hover data regardless of rank axis.

### Section 2 — Sales Over Time (right column)

Combo chart: Plotly `go.Figure` with bars for the selected metric and a
secondary-axis line for the cumulative total.
`st.radio` for granularity: Monthly (`Year-Month`) / Weekly (`Week`).
`st.radio` for metric: Orders / Units / Revenue.
Bar colour: `#2E75B6`. Cumulative line colour: `#e6a817`.

### Section 3 — Style Breakdown + Payment Sunburst (two columns)

Left: Donut pie of `Style` by Units.
Right: Sunburst of `Order Type → Payment Method` by Orders.

### Section 4 — US Geographic Map

Filter to `Ship Country` containing "United States".
Group by `Ship State`, aggregate Orders / Units / Revenue.
`st.radio` for map metric.
Plotly choropleth with `locationmode="USA-states"`, `scope="usa"`,
color scale `["#dce9f5", "#2E75B6"]`.
Show the state summary as a `st.dataframe` below the map.

### Section 5 — Repeat Buyers

Group by `Buyer User ID`, aggregate:
- `Name` (Full Name first value)
- `Orders` (Order ID nunique)
- `Units` (Quantity sum)
- `Total Spent` (Order Total sum, formatted `$X.XX`)
- `Last Purchase` (Sale Date max, formatted `YYYY-MM-DD`)
- `Items` (up to 3 unique Card Names joined by `, `)

Add `Tier` column:
- 3+ orders → `⭐⭐⭐ VIP`
- 2 orders  → `⭐⭐ Returning`
- 1 order   → `⭐ New`

Checkbox to toggle "show repeat buyers only" (default: checked).
Tier donut chart in the right column.

### Section 6 — Coupon Effectiveness

Filter rows where `Coupon Code` is not null/empty.
Table: Coupon Code | Uses | Total Discounted | Avg Net Order.
Right column: pie of coupon vs no-coupon orders, plus
`st.metric` for avg net with vs. without coupon.

### Section 7 — Fulfillment Speed

Compute `Days to Ship = Date Shipped - Date Paid` (in days, drop negatives).
Histogram of distribution (Plotly, 15 bins, blue).
Metrics: average days, median days, same-day shipment count.

### Styling notes

- Use `unsafe_allow_html=True` for metric cards and section headers
- All charts: `paper_bgcolor="rgba(0,0,0,0)"`, `plot_bgcolor="rgba(0,0,0,0)"`
- All charts: `margin=dict(l=0, r=0, t=10, b=0)`
- Section headers: `font-size:18px`, bold, dark colour, blue bottom border
- Sidebar background: `#1a1a2e` (via CSS injection)
- Primary colour: `#2E75B6`

---

## Step 5 — Running Locally

Open two terminals (both with venv activated):

**Terminal 1 — watcher:**
```bash
cd slabguy-dashboard
source venv/bin/activate
python watcher.py
```

**Terminal 2 — dashboard:**
```bash
cd slabguy-dashboard
source venv/bin/activate
streamlit run app.py
```

Dashboard opens at `http://localhost:8501`.

To ingest data: copy any Etsy CSV export into `data/watch/`. The watcher
picks it up within 1 second, processes it, and moves it to `data/watch/processed/`.
The dashboard refreshes automatically within 5 minutes, or hit "Force refresh"
in the sidebar.

---

## Accepted CSV formats (reference)

### EtsySoldOrderItems columns (raw export)
```
Sale Date, Item Name, Buyer, Quantity, Price, Coupon Code, Coupon Details,
Discount Amount, Shipping Discount, Order Shipping, Order Sales Tax, Item Total,
Currency, Transaction ID, Listing ID, Date Paid, Date Shipped, Ship Name,
Ship Address1, Ship Address2, Ship City, Ship State, Ship Zipcode, Ship Country,
Order ID, Variations, Order Type, Listings Type, Payment Type, InPerson Discount,
InPerson Location, VAT Paid by Buyer, SKU
```

### EtsySoldOrders columns (raw export)
```
Sale Date, Order ID, Buyer User ID, Full Name, First Name, Last Name,
Number of Items, Payment Method, Date Shipped, Street 1, Street 2, Ship City,
Ship State, Ship Zipcode, Ship Country, Currency, Order Value, Coupon Code,
Coupon Details, Discount Amount, Shipping Discount, Shipping, Sales Tax,
Order Total, Status, Card Processing Fees, Order Net, Adjusted Order Total,
Adjusted Card Processing Fees, Adjusted Net Order Amount, Buyer, Order Type,
Payment Type, InPerson Discount, InPerson Location, SKU
```

### Combined / finance sheet export columns
```
Sale Date, Order ID, Transaction ID, Listing ID, Full Name, First Name,
Last Name, Buyer Username, Buyer User ID, Item Name, Variations, SKU,
Quantity, Price, Item Total, Coupon Code, Coupon Details, Discount Amount,
Shipping, Sales Tax, Order Value, Order Total, Card Processing Fees, Order Net,
Adjusted Order Total, Adjusted Card Processing Fees, Adjusted Net Order Amount,
VAT Paid by Buyer, Currency, Payment Method, Payment Type, Order Type,
Date Paid, Date Shipped, Number of Items, Street 1, Street 2, Ship City,
Ship State, Ship Zipcode, Ship Country, Status, InPerson Discount, InPerson Location
```

Currency columns in this format have `$ ` prefixes and ` $ -   ` for zero
values — the cleaning pipeline handles this.

---

## Key implementation rules for Copilot

- Do not use `st.form` — use standard `st.button`, `st.selectbox`, etc.
- Do not use `WidthType.PERCENTAGE` anywhere — not applicable here but noted
- Never hardcode Firebase credentials — always read from `.env` / environment
- `Transaction ID` is the Firestore document ID — always cast to plain string,
  strip `.0` float suffixes before using as a doc ID
- Re-dropping an already-ingested CSV must be safe — the `merge=True` upsert
  ensures no double-counting
- All Plotly charts must have transparent backgrounds
- The `prefer(keep, drop)` dedup helper must: if both columns exist, fillna
  keep from drop then drop the drop column; if only drop exists, rename it to keep
