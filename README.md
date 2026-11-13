# MakerMetrics

[![Website](https://img.shields.io/badge/Website-makermetrics.pro-111827?style=for-the-badge)](https://makermetrics.pro)
[![Built with Next.js](https://img.shields.io/badge/Next.js-000000?style=for-the-badge&logo=nextdotjs&logoColor=white)](https://nextjs.org)
[![Supabase](https://img.shields.io/badge/Supabase-3ECF8E?style=for-the-badge&logo=supabase&logoColor=white)](https://supabase.com)
[![Vercel](https://img.shields.io/badge/Vercel-000000?style=for-the-badge&logo=vercel&logoColor=white)](https://vercel.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue?style=for-the-badge)](LICENSE)

See what your Etsy shop earns after fees and what needs your attention. Import the CSV exports Etsy already gives you; no API access needed.

Try out MakerMetrics at [makermetrics.pro](https://makermetrics.pro).

Built with Next.js, Supabase, and Vercel.

## Features

- **Dashboard**: revenue, net after Etsy fees, orders, average order value and repeat buyers for the selected period, each compared with the period before it. Defaults to the last 30 days. A "Needs attention" list calls out real changes (a product slowing down, revenue pacing behind, repeat buyers due to reorder, late shipping) and only when there are enough orders behind them. The month in progress is drawn as partial, with where it is expected to finish.
- **Orders**: a sortable, filterable table with CSV export. Each order opens beside the list as a top-to-bottom breakdown: item subtotal, discount, shipping, tax collected by Etsy, processing fee, estimated Etsy fees, net. Tabs for **Fulfillment** (shipping speed and on-time rate against your processing time) and **Coupons** (every code, what it cost, and how busy the shop was while it ran).
- **Products**: one row per Etsy listing with a six-month sparkline, item sales, and the raw 90-day change (for example 7 → 1). Rising/falling labels appear only above a minimum volume. Click a product for its variants, buyers and monthly history.
- **Customers**: repeat buyers, revenue per buyer, typical time to reorder, new vs returning by month, top buyers with their full order history, and where orders ship.
- **Outlook**: a forecast for the month in progress and the next three. Several simple models are back-tested on your own history and the most accurate one is used; if "same as last month" wins, that is what you get. Seasonal patterns are only used with two full years of data. Revenue is derived from forecast orders, ranges come from how wrong the back-test was, and the measured accuracy is shown above the chart.
- **AI Analyst**: bring your own API key (Anthropic, OpenAI or Google). The model list is loaded from the provider with your key, and the analyst answers by calling the same calculations as the pages. Buyer names are replaced with labels such as "Buyer 12" unless you choose to share them.
- **CSV import**: drop `EtsySoldOrderItems*.csv`, `EtsySoldOrders*.csv` and optionally `EtsyDirectCheckoutPayments*.csv`. Files are parsed in the browser, merged on Order ID and matched by Transaction ID, so re-importing is always safe.
- **Everywhere**: date range presets and custom ranges (kept in the URL), an "i" on each metric explaining its formula, privacy mode for screenshots, and search with Ctrl/Cmd+K.

## If you want to self-host the project

### 1. Create a Supabase project

1. Create a project at [supabase.com](https://supabase.com) (free tier is plenty).
2. Apply the database schema: run each file in [`supabase/migrations/`](supabase/migrations) in order, either pasted into the Supabase SQL editor or with `supabase db push`. The files are idempotent, so re-running them is safe.
3. Grab your project URL and anon/publishable key from **Project Settings → API**.
4. Optional: to offer a read-only demo account, create that user in Supabase Auth and flag it (SQL editor):

   ```sql
   update auth.users
      set raw_app_meta_data = coalesce(raw_app_meta_data, '{}'::jsonb) || '{"is_demo": true}'
    where email = 'demo@example.com';
   ```

   Row level security then blocks every write from that account. Set `NEXT_PUBLIC_DEMO_EMAIL` and `DEMO_PASSWORD` so the login page can offer "Try the demo".

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

| Variable | Required? | Used for |
|---|---|---|
| `NEXT_PUBLIC_SUPABASE_URL`, `NEXT_PUBLIC_SUPABASE_ANON_KEY` | Yes | Database and sign-in |
| `SUPABASE_SERVICE_ROLE_KEY` | For account deletion | Lets **Settings → Delete account** remove the auth user. Server-side only. |
| `AI_SETTINGS_ENCRYPTION_KEY` | For the AI Analyst | Encrypts saved AI provider keys (AES-256-GCM). 32+ characters. |
| `NEXT_PUBLIC_DEMO_EMAIL`, `DEMO_PASSWORD` | Optional | The read-only demo account on the login page |
| `NEXT_PUBLIC_SITE_URL` | Optional | Public URL of the site (e.g. `https://makermetrics.pro`). Used for the Etsy callback URL; defaults to the request's origin. |

### 3. Deploy to Vercel

Push to GitHub, import the repo at [vercel.com/new](https://vercel.com/new), and set the same environment variables in Vercel.

### 4. Import your data

In Etsy: **Shop Manager → Settings → Options → Download Data**. Download for each year:

| Export | Required? | What it adds |
|---|---|---|
| Sold Order Items | Yes | One row per item sold — products, variations, quantities |
| Sold Orders | Recommended | Exact order totals, processing fees, net amounts |
| Etsy Payments Sales (`EtsyDirectCheckoutPayments*.csv`) | Optional | Refund amounts, attached to the orders they belong to |

Then drag the files onto the **Import** page. Orders and payments files only add detail to orders that are already in an order items file, so a refund is never counted as a sale.

## Accounts, privacy and settings

- **Demo account**: the login page can sign visitors into a read-only account. Imports, settings changes and deletion are blocked for it in the database, not only in the UI.
- **Chat history**: AI Analyst conversations are kept in your browser (localStorage), per account, and never stored on the server.
- **Device cache**: your sales rows are cached in the browser (IndexedDB) so the app opens instantly; the cache is cleared when you sign out or delete your account.
- **Accent color**: Settings → Appearance changes the chart color and follows your account across devices.
- **Deleting data**: "Delete all data" removes every imported row; Settings → Delete account removes the account and everything stored for it.

## Connecting Etsy directly (optional)

Settings → Etsy connection pulls orders, payment fees, the payment-account ledger and listings from Etsy's Open API v3, so you don't have to upload CSVs. It is read-only (`transactions_r`, `listings_r`, `shops_r`).

1. In [Etsy → Your Apps](https://www.etsy.com/developers/your-apps), create an app for your own shop and add the callback URL shown in Settings (`https://<your site>/api/etsy/callback`).
2. Paste the app's keystring and shared secret into Settings and approve access on Etsy.
3. Press **Sync now**. Later syncs only fetch what changed.

Things to know:

- Etsy's "Seller App" keys work for **your own shop only** and may not be used commercially. Offering this to other sellers as a paid feature needs Etsy's Personal and then Commercial access.
- Keys and tokens are stored encrypted (AES-256-GCM) and never sent to the browser.
- Coupon codes are not in the API; they still come from the Sold Order Items CSV. Importing a CSV and syncing can be mixed freely, since rows are matched by transaction ID.
- Etsy's API has no traffic data (visits, views, search terms, ads).

## Notes on the data

- MakerMetrics deliberately does **not** store street addresses, only city/state/zip/country.
- **Revenue** = item prices − your discounts + shipping you charged. Sales tax that Etsy collects and remits is excluded.
- **Net after Etsy fees** = revenue − payment processing fees (actual, from the Sold Orders export) − refunds − an estimated 6.5% transaction fee − an estimated $0.20 listing fee per unit. Etsy Ads, Offsite Ads fees, shipping labels and your own costs are not in the order exports and are not included.
- Products are identified by Etsy listing ID, so a retitled listing stays one product. The display name is the listing title up to the first `|`.
- Variations are read as name/value pairs (`Size: 8x10, Color: Black`), whatever the option is called.
- Traffic data (visits, views, favorites, search terms, conversion) is not available outside Etsy's own Shop Stats and is not part of MakerMetrics.

## Tech stack

| Layer | Choice |
|---|---|
| Framework | Next.js (App Router, TypeScript) |
| Database + auth | Supabase |
| UI | Tailwind CSS + shadcn/ui, Recharts (loaded on demand) |
| AI | Vercel AI SDK with BYOK providers (Anthropic / OpenAI / Google) |
| CSV parsing | PapaParse (client-side) |

## Development

```bash
npm run dev     # dev server
npm test        # unit tests (Vitest) for parsing, analytics and forecasting
npm run lint    # eslint
npm run build   # production build + typecheck
```

Analytics and forecasting logic is framework-free TypeScript in `src/lib/analytics/`, shared by the UI and the AI assistant's tools.

## License

MIT — see [LICENSE](LICENSE).
