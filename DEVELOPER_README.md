# Developer README

This covers architecture, data flow, and extension points for the Etsy Stats Dashboard. For setup and day-to-day use, see [README.md](README.md).

---

## Stack

| Layer | Library |
|---|---|
| UI | Streamlit |
| Charts | Plotly |
| Data | pandas |
| Database | Firebase Firestore (via firebase-admin) |
| File watching | watchdog |
| Forecasting | statsmodels |
| Clustering | scikit-learn |
| Runtime | Python 3.12+, Docker / Docker Compose |

---

## Project layout

```
etsy-data-dashboard/
├── app.py                     # Streamlit UI — filters, metrics, all sections
├── watcher.py                 # CSV ingestion — detection, pairing, cleaning, upsert
├── requirements.txt
├── Dockerfile                 # Shared image used by both services
├── docker-compose.yml         # dashboard + watcher as separate services
├── .env.example
├── secrets/
│   └── firebase.json          # Service account key — gitignored
└── data/
    └── watch/
        └── processed/         # Ingested files are moved here
```

---

## Architecture

Two processes run independently and communicate only through Firestore.

```
CSV file dropped
      ↓
  watcher.py
  detect format → clean → enrich → upsert to Firestore
                                          ↓
                                     app.py
                                     reads Firestore → renders dashboard
```

### watcher.py

Monitors `WATCH_DIR` for `.csv` files using `watchdog`. Handles both `on_created` and `on_moved` events (some tools move files in rather than create them). Sleeps 1 second after detection to ensure the write is complete before reading.

On startup, processes any CSVs already sitting in the watch folder.

### app.py

Reads the `sales` and `listings` Firestore collections on load, caches with `@st.cache_data(ttl=300)`, and computes all aggregates in memory. Sidebar uploads call the same cleaning/upsert logic as the watcher directly, without going through the file system.

---

## CSV format detection

`detect_format(df)` inspects column headers to determine which pipeline to run:

| Format | Key signals | Notes |
|---|---|---|
| `items` | `Transaction ID` + `Item Name`, no `Full Name` | Raw SoldOrderItems export |
| `orders` | `Full Name`, no `Transaction ID` | Raw SoldOrders export |
| `combined` | Both `Transaction ID` and `Full Name` | Finance sheet or pre-merged export |
| `listings` | `TITLE` + `PRICE` + `QUANTITY`, no `Transaction ID` | Etsy listings export |

---

## Pairing (items + orders files)

Etsy's two raw export files contain complementary data — items has transaction-level detail, orders has buyer and shipping info. When both are dropped together they're joined on `Order ID`.

Pairing state is keyed by `YYYYMMDDHHMM` (current minute). Files within the same or adjacent minute are matched. If a partner doesn't arrive within `PAIR_WAIT_SECONDS`, the items file is processed solo (it has enough data via Transaction ID). A solo orders file is never ingested — it has no Transaction ID to use as a document ID — and logs a warning.

---

## Firestore collections

### `sales`

- **Document ID:** `Transaction ID` (string, `.0` float suffix stripped)
- **Source:** items exports, combined exports, merged item+order pipelines
- **Write mode:** `merge=True` — safe to re-ingest; always sets `_updated_at`, sets `_created_at` only on first write

### `listings`

- **Document ID:** SKU slug if available, otherwise a slug/hash derived from title + variations
- **Source:** Etsy listings CSV export (`TITLE`, `PRICE`, `QUANTITY` format)
- **Write mode:** `merge=True`, same timestamp pattern as sales
- **Used for:** including active listings with zero recent sales in item-level forecasts

---

## Cleaning pipeline

Applied to all formats after format detection and any merging/deduplication.

**Currency columns** (`Price`, `Item Total`, `Discount Amount`, `Shipping`, `Sales Tax`, `Order Net`, etc.)
Strip `$`, `,`, whitespace. Replace `-` with `0`. Parse as float.

**Date columns** (`Sale Date`, `Date Paid`, `Date Shipped`)
Try formats `%m/%d/%Y`, `%m/%d/%y`, `%Y-%m-%d` in order. Return `None` on failure.

**ID columns** (`Transaction ID`, `Order ID`)
Cast to string, strip, split on `.` and take the first part (removes pandas float suffixes like `3633762006.0`).

**Strings**
Replace empty strings and whitespace-only values with `None`. Drop fully empty rows.

---

## Enrichment (derived fields)

Applied after cleaning, before upsert.

**`Card Name`** — everything before the `|` in `Item Name`, stripped.
**`Product Type`** — everything after the `|`, stripped (e.g. `Custom Pokémon Artwork Display Slab`).
**`Style`** — `Variations` with the `Style:` or `Custom Property:` prefix removed.

Listings rows get the same `Card Name` / `Product Type` split from their title field, plus `Style` from variations when available.

---

## Column deduplication (combined format)

When the input is a combined/finance-sheet export, redundant paired columns are collapsed using a `prefer(keep, drop)` helper: if both exist, nulls in `keep` are filled from `drop` and `drop` is removed; if only `drop` exists it's renamed to `keep`.

Priority rules (items-file version wins in all cases):

| Final column | Dropped column(s) |
|---|---|
| `Buyer Username` | `Buyer_x` |
| `Full Name` | `Buyer_y` |
| `Coupon Code` | `Order Coupon Code` |
| `Coupon Details` | `Order Coupon Details` |
| `Discount Amount` | `Order Discount Amount` |
| `Shipping` | `Order Shipping Amount`, `Order Shipping` |
| `Shipping Discount` | `Order Shipping Discount` |
| `Sales Tax` | `Order Sales Tax Amount`, `Order Sales Tax` |
| `Order Type` | `Item Order Type` |
| `Payment Type` | `Item Payment Type` |
| `InPerson Discount` | `Item InPerson Discount`, `Order InPerson Discount` |
| `InPerson Location` | `Item InPerson Location`, `Order InPerson Location` |
| `SKU` | `Item SKU`, `Order SKU` |
| `Currency` | `Item Currency` |
| `Date Shipped` | `Item Date Shipped` (full 4-digit year version) |
| `Street 1` / `Street 2` | `Ship Address1` / `Ship Address2` |
| `Ship City/State/Zipcode/Country` | `Item Ship *` variants |

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `FIREBASE_CREDENTIALS` | `secrets/firebase.json` | Path to service account JSON |
| `WATCH_DIR` | `data/watch` | Folder monitored for CSV drops |
| `PAIR_WAIT_SECONDS` | `30` | Seconds to wait for a partner file before processing solo |
| `SALES_COLLECTION` | `sales` | Firestore collection name for sales data |
| `LISTINGS_COLLECTION` | `listings` | Firestore collection name for listings data |

---

## Docker

Both services use a shared `Dockerfile`. `docker-compose.yml` defines them as separate containers so the watcher and dashboard can be restarted independently.

Volume mounts:
- `./data/watch` → `/app/data/watch`
- `./secrets` → `/run/secrets` (read-only)

```bash
docker compose up --build        # start both
docker compose up dashboard      # dashboard only
docker compose up watcher        # watcher only
docker compose logs -f watcher   # tail watcher logs
```

---

## Validation

```bash
python3 -m py_compile watcher.py app.py
```

---

## Extension points

**New CSV format** — add a branch in `detect_format()` and a corresponding processing path. The cleaning and enrichment steps are shared, so you only need to handle the format-specific merge/rename logic.

**New dashboard section** — add it in `app.py` after the filter application block. All filtered data is available as `df`.

**New Firestore collection** — add an env var for the collection name, a loader function with `@st.cache_data`, and call it alongside the existing `sales`/`listings` loaders.

**New derived fields** — add them in the `enrich()` function in `watcher.py`. They'll be stored in Firestore and available in the dashboard on next refresh.

---

## Known constraints

- A `SoldOrders`-only file cannot be ingested alone — it has no Transaction ID. It must be paired with a `SoldOrderItems` file.
- Forecast quality degrades with sparse history. A few months of consistent data produces meaningfully better projections than a handful of scattered orders.
- If multiple environments (e.g. local dev and a deployed instance) point to the same Firebase project, they share data — changes from one are visible in the other.