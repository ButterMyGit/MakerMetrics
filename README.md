# Etsy Stats Dashboard

A personal sales analytics dashboard built for Etsy shops. Drop in your Etsy CSV exports and get a live, filterable view of your orders, top products, buyers, geographic sales, forecasts, and more — all running locally or in Docker on your own machine.

On first launch, a two-step onboarding flow lets you upload an optional logo and choose which dashboard sections to show. For now, store name is fixed in-app. Logo and visible sections can be changed later from the sidebar settings menu. These settings are persisted in `settings/dashboard_settings.json`.

If you want to understand the code or extend it, see [DEVELOPER_README.md](DEVELOPER_README.md).

---

## What's in the dashboard

**Six KPI cards at the top** update with your active filters:
Total Orders, Units Sold, Net Revenue, Avg Order Value, Repeat Buyers, Unique Products.

**Analytics sections:**

- **Most Popular Products** — ranked by units, orders, or revenue, with a top-N slider
- **Sales Over Time** — monthly or weekly bars with a cumulative overlay
- **Style Breakdown & Payment Methods** — donut and sunburst charts
- **US Sales by State** — choropleth map, switchable between orders, units, and revenue
- **Sales Projection** — forecasted order volume by month, plus per-item unit forecasts that include active listings with no recent sales
- **Buyer Analysis** — repeat buyer table with VIP/Returning/New tiers
- **Coupon Effectiveness** — usage counts, total discounts, and avg net with vs. without coupons
- **Customer Segments** — behavior-based clustering
- **Fulfillment Speed** — days from payment to shipment, histogram and averages
- **Order History** — searchable, sortable full order table at the bottom

The dashboard auto-refreshes every 20 seconds. You can also force a refresh from the sidebar.

---

## Before you start: Firebase setup

This app uses **Firebase Firestore** as its database — it's free for personal use and means your data survives even if your local machine has issues. Setup takes about 5 minutes.

### 1. Create a Firebase project

1. Go to [console.firebase.google.com](https://console.firebase.google.com) and sign in with your Google account.
2. Click **Add project**, give it a name (e.g. `etsy-sales-dashboard`), and click through the setup. You can disable Google Analytics since you won't need it.

### 2. Enable Firestore

1. In the left sidebar, click **Build → Firestore Database**.
2. Click **Create database**.
3. Choose **Start in production mode** and click Next.
4. Pick any region — `us-central1` is a safe default if you're in the US. Click **Enable**.

### 3. Get your credentials file

This is the key that lets the app talk to your database.

1. Click the **gear icon** next to "Project Overview" in the top-left → **Project settings**.
2. Click the **Service accounts** tab.
3. Click **Generate new private key** → **Generate key**.
4. A `.json` file will download automatically. Rename it `firebase.json`.
5. Place it at `secrets/firebase.json` inside this project folder.

> **Important:** Keep this file private. It's already in `.gitignore` so it won't be committed if you use Git, but don't share it or upload it anywhere.

That's it for Firebase. You won't need to touch it again.

---

## Running the app

### Option A — Docker (recommended)

Requires [Docker](https://www.docker.com/products/docker-desktop/) to be installed. In your terminal, head over to this project folder and run:

```bash
docker compose pull
docker compose up
```

Then open [http://localhost:8501](http://localhost:8501).

That's it! Both services (dashboard and watcher) start together. The compose file is configured to pull prebuilt Hub images: `buttermygit/etsy-sales-dashboard:dashboard` and `buttermygit/etsy-sales-dashboard:watcher`, so local image builds are not required for normal use.

To stop services, press `Ctrl+C` or run `docker compose down`. If you ever want to get rid of this project, just delete the folder and the Docker containers will be removed. Your Firebase database will still exist in Google's cloud if you want to come back to it later. Providing the same `.json` credentials file will reconnect it to the same database.

### Option B — Local Python (no Docker)

Requires Python 3.12+.

```bash
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Then open two terminals (both with the venv activated):

**Terminal 1 — watcher:**
```bash
python watcher.py
```

**Terminal 2 — dashboard:**
```bash
streamlit run app.py
```

Dashboard opens at [http://localhost:8501](http://localhost:8501).

---

## Loading your Etsy data

You have two options — use whichever fits your workflow.

### Option 1: Upload from the sidebar

1. Open the dashboard.
2. In the sidebar, drag and drop one or more Etsy CSV files.
3. Click **Process uploaded CSVs**.

### Option 2: Drop files into the watch folder

Copy any Etsy CSV export into `data/watch/`. The watcher picks it up, processes it, and moves the file to `data/watch/processed/` when done. They are safe to remove once processed as Firebase will have been updated.

### Which CSV files does Etsy give you?

Etsy has three separate exports, both found under **Shop Manager → Settings → Options → Download Data**:

| File | What it contains |
|---|---|
| **Sold Order Items** | One row per item sold — transaction IDs, listing details, variations, item prices (most important due to transaction ID) |
| **Sold Orders** | One row per order — buyer name, shipping address, order totals |
| **Currently For Sale** | One row per active listing — listing details, prices, inventory |

You can drop any of these at any time, in any order. If you drop them at the same time, they're automatically joined.

> **Dropping the same file twice is safe.** The app deduplicates by Transaction ID so nothing gets double-counted.

---

## Optional configuration

Copy `.env.example` to `.env` if you want to change any defaults:

```
FIREBASE_CREDENTIALS=secrets/firebase.json
WATCH_DIR=data/watch
PAIR_WAIT_SECONDS=30
SALES_COLLECTION=sales
LISTINGS_COLLECTION=listings
```

`PAIR_WAIT_SECONDS` controls how long the watcher waits for a matching orders file before processing an items file on its own.

---

## Troubleshooting

**"Credentials failed" or Firebase errors**
Confirm `secrets/firebase.json` exists, is the correct file, and wasn't corrupted during download. Re-download from Firebase if unsure.

**No data appears after uploading**
Check that the file is an Etsy CSV (not a manually edited spreadsheet). If using the watch folder, check that the watcher process is actually running.

**Order counts seem low**
If you dropped only a `SoldOrders` file, that file alone can't be imported — it has no Transaction ID. Drop the matching `SoldOrderItems` file alongside it.

**Docker can't find the credentials**
Make sure the `secrets/` folder exists at the project root and contains `firebase.json`. Docker mounts the entire folder.

---

## Credits

Built by [ButterMyGit](https://github.com/ButterMyGit). Initially built for [TheSlabGuy](https://theslabguy.etsy.com).

Licensed under the [MIT License](LICENSE).