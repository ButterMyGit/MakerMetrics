# MakerMetrics

[![Website](https://img.shields.io/badge/Website-makermetrics.pro-111827?style=for-the-badge)](https://makermetrics.pro)
[![Built with Next.js](https://img.shields.io/badge/Next.js-000000?style=for-the-badge&logo=nextdotjs&logoColor=white)](https://nextjs.org)
[![Supabase](https://img.shields.io/badge/Supabase-3ECF8E?style=for-the-badge&logo=supabase&logoColor=white)](https://supabase.com)
[![Vercel](https://img.shields.io/badge/Vercel-000000?style=for-the-badge&logo=vercel&logoColor=white)](https://vercel.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue?style=for-the-badge)](LICENSE)

Analytics for Etsy sellers, without API access needed! Import the CSV exports Etsy already gives you and get a fast, mobile-friendly dashboard with insights, forecasting, and an ability to connect your own AI API key.

Try out MakerMetrics at [makermetrics.pro](https://makermetrics.pro).

Built with Next.js, Supabase, and Vercel.

## Features

- **Dashboard** — net revenue, orders, units, AOV, repeat-buyer rate, and Etsy fees, each compared against the previous period; monthly performance; day-of-week patterns; recent orders.

<img width="1920" height="969" alt="2026-06-16_18-21-03" src="https://github.com/user-attachments/assets/bb2405b3-d539-4832-b629-74afdc71188f" />

- **Orders** — full order history with instant search across buyer, item, SKU, coupon, order ID, and location, plus a detail view with line items and a fee breakdown.

<img width="1920" height="969" alt="2026-06-16_19-07-51" src="https://github.com/user-attachments/assets/f63e07dd-db2f-4887-9458-73f0262cb3de" />

- **Products** — every product ranked by units/revenue with a momentum label (rising / falling / new / dormant based on the last 90 days vs the prior 90), per-product monthly drilldowns, and style / product-type breakdowns.

<img width="1920" height="968" alt="2026-06-16_19-09-19" src="https://github.com/user-attachments/assets/70c3012a-1d28-4998-892f-ed2e2a4793d8" />

- **Customers** — repeat rate, average lifetime value, median reorder gap, new-vs-returning buyers by month, top buyers with VIP tiers, geography, coupon effectiveness, and fulfillment speed.

<img width="1920" height="968" alt="2026-06-16_19-09-58" src="https://github.com/user-attachments/assets/9661f71e-8c1f-4534-8ac0-0e6bcb93b1b8" />

- **Forecast** — monthly revenue and order projections using seasonal decomposition with a damped robust trend. Accuracy is measured with a walk-forward backtest and reported next to the chart (including how it compares to a naive "same month last year" guess). Per-product next-month estimates allocate the shop forecast by sales mix instead of fitting noisy per-item models.

<img width="1920" height="968" alt="2026-06-16_19-12-15" src="https://github.com/user-attachments/assets/7bb94eac-2840-4de7-902f-1f52a391b4c2" />

- **AI Analyst** — bring your own API key (Anthropic, OpenAI, or Google). The model answers questions by calling the same analytics functions the dashboard uses.

<img width="1920" height="969" alt="2026-06-16_19-12-49" src="https://github.com/user-attachments/assets/e2f6b390-acc4-4b47-b7b6-fb905ff2a0a7" />

- **CSV import** — drag-and-drop `EtsySoldOrderItems*.csv` and `EtsySoldOrders*.csv`. Files are parsed in the browser, merged on Order ID, deduplicated by Transaction ID, and upserted — re-importing the same file is always safe. Your data and account are easily deletable at any time!

<img width="1920" height="967" alt="image" src="https://github.com/user-attachments/assets/80d5cf37-4cf6-49a3-b511-09e8a7bd6fc2" />


## If you want to self-host the project

### 1. Create a Supabase project

1. Create a project at [supabase.com](https://supabase.com) (free tier is plenty).
2. Apply the project database schema in the Supabase SQL editor or with the Supabase CLI.
3. Grab your project URL and anon/publishable key from **Project Settings → API**.

### 2. Run locally

```bash
npm install
npm run dev
```

Create `.env.local` with:

```bash
NEXT_PUBLIC_SUPABASE_URL=your-supabase-url
NEXT_PUBLIC_SUPABASE_ANON_KEY=your-supabase-anon-key
SUPABASE_SERVICE_ROLE_KEY=optional-service-role-key-for-account-deletion
AI_SETTINGS_ENCRYPTION_KEY=32-plus-character-secret-for-ai-key-encryption
NEXT_PUBLIC_DEMO_EMAIL=demo-account-email
DEMO_PASSWORD=demo-account-password
```

### 3. Deploy to Vercel

Push to GitHub, import the repo at [vercel.com/new](https://vercel.com/new), and set the same environment variables in Vercel.

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
- Item names in the form `Product Name | Product Type` are split automatically; `Style:` variations are parsed into a Style field.

## Tech stack

| Layer | Choice |
|---|---|
| Framework | Next.js (App Router, TypeScript) |
| Database + auth | Supabase |
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
