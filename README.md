# MakerMetrics

Analytics for Etsy sellers — without API access. Import the CSV exports Etsy already gives you and get a fast, mobile-friendly dashboard with actionable insights, honest forecasting, and an AI analyst that answers questions about your real data.

Built with **Next.js + Supabase**, deployable to **Vercel** in minutes. Multi-user out of the box: every account sees only its own data (enforced by Postgres row-level security).

> The previous Streamlit + Firestore + Docker version lives in [`legacy/`](legacy/).

## Features

- **Dashboard** — net revenue, orders, units, AOV, repeat-buyer rate, and Etsy fees, each compared against the previous period; monthly performance; day-of-week patterns; recent orders.
- **Orders** — full order history with instant search across buyer, item, SKU, coupon, order ID, and location, plus a detail view with line items and a fee breakdown.
- **Products** — every product ranked by units/revenue with a momentum label (rising / falling / new / dormant based on the last 90 days vs the prior 90), per-product monthly drilldowns, and style / product-type breakdowns.
- **Customers** — repeat rate, average lifetime value, median reorder gap, new-vs-returning buyers by month, top buyers with VIP tiers, geography, coupon effectiveness, and fulfillment speed.
- **Forecast** — monthly revenue and order projections using seasonal decomposition with a damped robust trend. Accuracy is measured with a walk-forward backtest and reported next to the chart (including how it compares to a naive "same month last year" guess). Per-product next-month estimates allocate the shop forecast by sales mix instead of fitting noisy per-item models.
- **AI Analyst** — bring your own API key (Anthropic, OpenAI, or Google). The model answers questions by calling the same analytics functions the dashboard uses, so numbers are grounded in your data, not hallucinated. Keys stay in your browser.
- **CSV import** — drag-and-drop `EtsySoldOrderItems*.csv` and `EtsySoldOrders*.csv`. Files are parsed in the browser, merged on Order ID, deduplicated by Transaction ID, and upserted — re-importing the same file is always safe.

## Getting started

### 1. Create a Supabase project

1. Create a project at [supabase.com](https://supabase.com) (free tier is plenty).
2. In the SQL editor, run the migration in [`supabase/migrations/0001_init.sql`](supabase/migrations/0001_init.sql) — or use the CLI: `supabase db push`.
3. Grab your project URL and anon/publishable key from **Project Settings → API**.

### 2. Run locally

```bash
cp .env.example .env.local   # fill in your Supabase URL + key
npm install
npm run dev
```

### 3. Deploy to Vercel

Push to GitHub, import the repo at [vercel.com/new](https://vercel.com/new), and set the two environment variables from `.env.example`. That's it — no servers, no Docker.

### 4. Import your data

In Etsy: **Shop Manager → Settings → Options → Download Data**. Download for each year:

| Export | Required? | What it adds |
|---|---|---|
| Sold Order Items | Yes | One row per item sold — products, variations, quantities |
| Sold Orders | Recommended | Exact order totals, processing fees, net amounts |

Then drag both files onto the **Import** page.

## Notes on the data

- Etsy CSVs have no street-level analytics value, so MakerMetrics deliberately does **not** store street addresses — only city/state/zip/country.
- "Net revenue" = Etsy's order net (after transaction + processing fees, before shipping label costs).
- Item names in the form `Card Name | Product Type` are split automatically; `Style:` variations are parsed into a Style field (matching the legacy behavior).

## Tech stack

| Layer | Choice |
|---|---|
| Framework | Next.js (App Router, TypeScript) |
| Database + auth | Supabase (Postgres with RLS, email/password auth) |
| UI | Tailwind CSS + shadcn/ui, Recharts |
| AI | Vercel AI SDK with BYOK providers (Anthropic / OpenAI / Google) |
| CSV parsing | PapaParse (client-side) |

## Development

```bash
npm run dev     # dev server
npm run lint    # eslint
npm run build   # production build + typecheck
```

Analytics and forecasting logic is framework-free TypeScript in `src/lib/analytics/`, shared by the UI and the AI assistant's tools.

## License

MIT — see [LICENSE](LICENSE).
