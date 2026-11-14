import { tool } from "ai";
import { z } from "zod";
import { addDays } from "@/lib/dates";
import {
  breakdownBy,
  buyerStats,
  compareKpis,
  computeKpis,
  couponAnalysis,
  customerKpis,
  dayOfWeekBreakdown,
  filterOrders,
  fulfillmentStats,
  geoBreakdown,
  monthlySeries,
  productDetail,
  productStats,
  variationBreakdown,
  type Order,
} from "@/lib/analytics/core";
import { MODEL_LABELS, itemForecasts, shopForecast } from "@/lib/analytics/forecast";
import { buildInsights } from "@/lib/analytics/insights";

const isoDate = z.string().regex(/^\d{4}-\d{2}-\d{2}$/);

const dateRangeInput = {
  start: isoDate.nullish().describe("Start date (YYYY-MM-DD), inclusive. Omit for all history."),
  end: isoDate.nullish().describe("End date (YYYY-MM-DD), inclusive. Omit for up to the latest data."),
};

function r2(n: number | null | undefined): number | null {
  return n == null || !Number.isFinite(n) ? null : Math.round(n * 100) / 100;
}

export interface AnalystContext {
  /** every order of the shop, oldest first */
  orders: Order[];
  /** last date the data covers */
  through: string | null;
  today: string;
  processingDays?: number | null;
  /** when false, buyers are referred to as "Buyer 12" instead of by name */
  shareBuyerNames: boolean;
}

/**
 * Tool set for the AI analyst. Every tool runs the same deterministic
 * analytics as the app's pages over the signed-in user's orders, so the
 * model reports numbers instead of inventing them.
 */
export function buildAnalystTools(ctx: AnalystContext) {
  const { orders, through } = ctx;
  const inRange = (start?: string | null, end?: string | null) => filterOrders(orders, start ?? null, end ?? null);

  // Stable pseudonyms, numbered by first order, unless the user opted in to names.
  const aliases = new Map<string, string>();
  for (const o of orders) {
    if (o.buyerKey && !aliases.has(o.buyerKey)) aliases.set(o.buyerKey, `Buyer ${aliases.size + 1}`);
  }
  const buyerLabel = (key: string | null, name: string) =>
    ctx.shareBuyerNames ? name : key ? (aliases.get(key) ?? "Buyer") : "Buyer";

  const findProducts = (query: string) => {
    const q = query.toLowerCase();
    return productStats(orders, through).filter((p) =>
      `${p.name} ${p.title ?? ""}`.toLowerCase().includes(q)
    );
  };

  const orderSummary = (o: Order) => ({
    orderId: o.orderId,
    date: o.saleDate,
    buyer: buyerLabel(o.buyerKey, o.buyerName),
    items: o.rows.map((r) => ({
      name: r.cardName ?? r.itemName,
      options: r.style,
      qty: r.quantity,
      price: r.price,
    })),
    units: o.units,
    revenue: o.money.revenue,
    discount: o.money.discount,
    refund: o.money.refund,
    netAfterFees: o.money.netAfterFees,
    coupons: o.coupons,
    shipsTo: ctx.shareBuyerNames ? o.location : (o.shipState ?? o.shipCountry),
    shipped: o.dateShipped,
  });

  return {
    get_overview: tool({
      description:
        "Headline numbers for a date range: revenue, net after Etsy fees, orders, units, average order value, repeat-buyer rate, fees, discounts, refunds. When both dates are given it also returns the same numbers for the previous period of equal length and the changes. Start here for 'how am I doing' questions.",
      inputSchema: z.object(dateRangeInput),
      execute: async ({ start, end }) => {
        const k = computeKpis(inRange(start, end), orders);
        const effectiveEnd = end ?? through ?? ctx.today;
        const comparison = start ? compareKpis(orders, start, effectiveEnd) : null;
        return {
          rangeApplied: { start: start ?? null, end: start ? effectiveEnd : (end ?? null) },
          ...k,
          avgOrderValue: r2(k.avgOrderValue),
          previousPeriod: comparison
            ? {
                range: comparison.previousRange,
                revenue: comparison.previous.revenue,
                netAfterFees: comparison.previous.netAfterFees,
                orders: comparison.previous.orders,
                units: comparison.previous.units,
                avgOrderValue: r2(comparison.previous.avgOrderValue),
                repeatBuyerRate: comparison.previous.repeatBuyerRate,
                changes: comparison.deltas,
              }
            : null,
        };
      },
    }),

    get_monthly_performance: tool({
      description:
        "Month-by-month orders, units, revenue and net after fees. A month flagged `partial` is still in progress (day X of Y) and must not be compared with full months as if it were complete.",
      inputSchema: z.object(dateRangeInput),
      execute: async ({ start, end }) => monthlySeries(inRange(start, end), end ? null : through),
    }),

    get_products: tool({
      description:
        "Per-product stats: units, orders, item sales, average price, first/last sold, units in the last 90 days vs the 90 days before, and a trend label (rising / falling / new / dormant, or null when volume is too low to call). Use `search` to filter by product name, e.g. every product whose title contains a word.",
      inputSchema: z.object({
        ...dateRangeInput,
        search: z.string().nullish().describe("Only products whose name or listing title contains this text"),
        sort: z.enum(["units", "itemRevenue", "recentChange", "lastSold"]).default("units"),
        limit: z.number().int().min(1).max(500).default(20),
      }),
      execute: async ({ start, end, search, sort, limit }) => {
        let list = productStats(inRange(start, end), through, orders);
        if (search) {
          const q = search.toLowerCase();
          list = list.filter((p) => `${p.name} ${p.title ?? ""}`.toLowerCase().includes(q));
        }
        if (sort === "itemRevenue") list.sort((a, b) => b.itemRevenue - a.itemRevenue);
        if (sort === "recentChange")
          list.sort((a, b) => a.recentUnits - a.priorUnits - (b.recentUnits - b.priorUnits));
        if (sort === "lastSold") list.sort((a, b) => (b.lastSold ?? "").localeCompare(a.lastSold ?? ""));
        return {
          matched: list.length,
          products: list.slice(0, limit).map((p) => ({
            name: p.name,
            listingId: p.listingId,
            units: p.units,
            orders: p.orders,
            itemSales: p.itemRevenue,
            avgPrice: r2(p.avgPrice),
            firstSold: p.firstSold,
            lastSold: p.lastSold,
            unitsPrevious90Days: p.priorUnits,
            unitsLast90Days: p.recentUnits,
            trend: p.trend,
          })),
        };
      },
    }),

    get_product_detail: tool({
      description:
        "Deep dive on one product: units and item sales by month, which options/variants buyers chose, number of buyers and repeat buyers, share of its orders that used a coupon, and its most recent orders. Use this to explain why a product rose or fell.",
      inputSchema: z.object({
        product: z.string().min(1).describe("Product name, or part of it"),
        ...dateRangeInput,
      }),
      execute: async ({ product, start, end }) => {
        const matches = findProducts(product);
        if (matches.length === 0) return { error: `No product matching "${product}".` };
        const p = matches[0];
        const scoped = inRange(start, end);
        const recent = scoped.filter((o) => o.rows.some((r) => r.productKey === p.key)).slice(-8);
        return {
          product: p.name,
          listingTitle: p.title,
          otherMatches: matches.slice(1, 6).map((m) => m.name),
          totals: { units: p.units, orders: p.orders, itemSales: p.itemRevenue, avgPrice: r2(p.avgPrice) },
          unitsPrevious90Days: p.priorUnits,
          unitsLast90Days: p.recentUnits,
          trend: p.trend,
          ...productDetail(scoped, p.key),
          recentOrders: recent.reverse().map(orderSummary),
        };
      },
    }),

    get_customer_insights: tool({
      description:
        "Buyer analytics: unique buyers, share who ordered more than once, revenue per buyer, typical days between repeat orders, new vs returning buyers by month, and the top buyers by revenue with their tier (Returning = 2+ orders, New = first order in the last 30 days, One-time).",
      inputSchema: z.object({
        ...dateRangeInput,
        topBuyers: z.number().int().min(0).max(30).default(10),
        repeatOnly: z.boolean().default(false).describe("List only buyers with 2+ orders"),
      }),
      execute: async ({ start, end, topBuyers, repeatOnly }) => {
        const subset = inRange(start, end);
        const k = customerKpis(subset);
        const buyers = buyerStats(subset, through, orders).filter((b) => !repeatOnly || b.orders > 1);
        return {
          ...k,
          avgLifetimeValue: r2(k.avgLifetimeValue),
          buyers: buyers.slice(0, topBuyers).map((b) => ({
            buyer: buyerLabel(b.key, b.name),
            tier: b.tier,
            orders: b.orders,
            units: b.units,
            revenue: b.revenue,
            firstOrder: b.firstOrder,
            lastOrder: b.lastOrder,
            usedCoupon: b.usedCoupon,
          })),
        };
      },
    }),

    get_geo_breakdown: tool({
      description: "Orders, units and revenue by US state or by country.",
      inputSchema: z.object({ ...dateRangeInput, by: z.enum(["state", "country"]).default("state") }),
      execute: async ({ start, end, by }) => geoBreakdown(inRange(start, end), by).slice(0, 30),
    }),

    get_breakdown: tool({
      description:
        "Distribution of sales by product type, order type (online vs in person), payment type, or by the options buyers chose (`variations` returns units per value for every option name, such as Size or Style).",
      inputSchema: z.object({
        ...dateRangeInput,
        field: z.enum(["variations", "productType", "orderType", "paymentType"]),
      }),
      execute: async ({ start, end, field }) => {
        const subset = inRange(start, end);
        if (field === "variations") {
          return variationBreakdown(subset).map((v) => ({ option: v.name, values: v.values.slice(0, 20) }));
        }
        return breakdownBy(subset, field).slice(0, 25);
      },
    }),

    get_coupon_performance: tool({
      description:
        "Coupon usage: per code the orders, discount given, revenue, first/last use and `lift` (shop-wide orders per day while the code was in use ÷ the same number of days before it; null when there is too little data). Also average order with vs without a coupon and the share of orders using one. Discounts are a cost to the seller; this data cannot prove a coupon caused sales.",
      inputSchema: z.object(dateRangeInput),
      execute: async ({ start, end }) => {
        const c = couponAnalysis(inRange(start, end), orders);
        return {
          ...c,
          coupons: c.coupons.slice(0, 40).map((x) => ({ ...x, avgOrderValue: r2(x.avgOrderValue), lift: r2(x.lift) })),
          withCoupon: { ...c.withCoupon, avgRevenue: r2(c.withCoupon.avgRevenue), avgUnits: r2(c.withCoupon.avgUnits) },
          withoutCoupon: {
            ...c.withoutCoupon,
            avgRevenue: r2(c.withoutCoupon.avgRevenue),
            avgUnits: r2(c.withoutCoupon.avgUnits),
          },
        };
      },
    }),

    get_fulfillment_stats: tool({
      description:
        "Shipping speed: average/median/90th-percentile days from payment to shipping, a histogram, a monthly trend and, when the seller has set a processing time, the on-time rate against it (Star Seller requires 95%).",
      inputSchema: z.object(dateRangeInput),
      execute: async ({ start, end }) => {
        const f = fulfillmentStats(inRange(start, end), ctx.processingDays);
        return {
          processingTimeBusinessDays: ctx.processingDays ?? "not set by the seller",
          ...f,
          avgDays: r2(f.avgDays),
          monthly: f.monthly.slice(-18),
        };
      },
    }),

    get_refunds: tool({
      description: "Refund totals and the refunded orders (fully or partly) in a date range.",
      inputSchema: z.object(dateRangeInput),
      execute: async ({ start, end }) => {
        const subset = inRange(start, end);
        const k = computeKpis(subset, orders);
        return {
          totalRefunds: k.totalRefunds,
          refundedOrders: k.refundedOrders,
          refundRate: k.refundRate,
          fullyRefundedOrders: k.cancelledOrders,
          orders: subset
            .filter((o) => o.money.refund > 0)
            .slice(-25)
            .reverse()
            .map(orderSummary),
        };
      },
    }),

    get_day_of_week_pattern: tool({
      description: "Orders and revenue by day of week (Mon–Sun).",
      inputSchema: z.object(dateRangeInput),
      execute: async ({ start, end }) => dayOfWeekBreakdown(inRange(start, end)),
    }),

    get_forecast: tool({
      description:
        "The app's forecast for the month in progress and the next 3 months: expected orders and revenue with a likely range, the method used, how far off it was when tested on past months (`wape`, 0.3 = off by 30%), and next month's expected units by product. Always quote the range and the accuracy, and round the figures.",
      inputSchema: z.object({}),
      execute: async () => {
        const f = shopForecast(orders, { through });
        const round = (p: { month: string; value: number; lower: number; upper: number }) => ({
          month: p.month,
          expected: Math.round(p.value),
          low: Math.round(p.lower),
          high: Math.round(p.upper),
        });
        return {
          method: MODEL_LABELS[f.orders.model],
          notes: f.orders.notes,
          avgOrderValueUsed: r2(f.avgOrderValue),
          accuracy: { orders: f.orders.accuracy, revenue: f.revenue.accuracy },
          monthInProgress: f.revenue.current
            ? { ...round(f.revenue.current), soFar: f.revenue.current.partial }
            : null,
          revenue: f.revenue.forecast.map(round),
          orders: f.orders.forecast.map(round),
          nextMonthByProduct: itemForecasts(orders, { through }),
        };
      },
    }),

    project_scenario: tool({
      description:
        "What-if and goal planning. Projects monthly revenue and net after fees for the coming months from the forecast baseline, with optional changes to order volume and average order value. With `targetRevenue` it also returns how many orders per month that target needs. Use for 'if the rest of the year looks like X' or 'what would it take to reach $Y' questions.",
      inputSchema: z.object({
        months: z.number().int().min(1).max(12).default(3),
        orderChangePercent: z.number().min(-100).max(1000).default(0).describe("Change in monthly orders vs the forecast, e.g. 20 for +20%"),
        aovChangePercent: z.number().min(-100).max(1000).default(0).describe("Change in average order value, e.g. -10 for a 10% discount"),
        targetRevenue: z.number().positive().nullish().describe("Monthly revenue goal"),
      }),
      execute: async ({ months, orderChangePercent, aovChangePercent, targetRevenue }) => {
        const f = shopForecast(orders, { through, horizon: months });
        const recent = computeKpis(filterOrders(orders, addDays(through ?? ctx.today, -89), through), orders);
        const keepRate = recent.revenue > 0 ? recent.netAfterFees / recent.revenue : 0.89;
        const aov = (f.avgOrderValue ?? 0) * (1 + aovChangePercent / 100);
        return {
          assumptions: {
            baselineOrdersPerMonth: f.orders.forecast[0]?.value ?? 0,
            baselineAvgOrderValue: r2(f.avgOrderValue),
            scenarioAvgOrderValue: r2(aov),
            shareOfRevenueKeptAfterEtsyFees: r2(keepRate),
            note: "Baseline is the app's forecast; its accuracy is limited (see get_forecast).",
          },
          months: f.orders.forecast.map((p) => {
            const scenarioOrders = p.value * (1 + orderChangePercent / 100);
            return {
              month: p.month,
              orders: Math.round(scenarioOrders),
              revenue: Math.round(scenarioOrders * aov),
              netAfterFees: Math.round(scenarioOrders * aov * keepRate),
            };
          }),
          target:
            targetRevenue && aov > 0
              ? {
                  monthlyRevenue: targetRevenue,
                  ordersNeededPerMonth: Math.ceil(targetRevenue / aov),
                  vsBaselineOrders: f.orders.forecast[0]?.value ?? 0,
                }
              : null,
        };
      },
    }),

    get_insights: tool({
      description:
        "The app's 'Needs attention' list: notable changes computed from the shop's own history, each with supporting numbers. Good for 'what should I look at' or 'what changed' questions.",
      inputSchema: z.object({}),
      execute: async () =>
        buildInsights(orders, { through, today: ctx.today, processingDays: ctx.processingDays }).map(
          (i) => ({ tone: i.tone, title: i.title, detail: i.detail })
        ),
    }),

    search_orders: tool({
      description:
        "Find individual orders. `query` matches buyer, item title, option, SKU, coupon code, order number or location. Filters can be combined. Returns the most recent matches first, plus how many matched in total.",
      inputSchema: z.object({
        query: z.string().nullish(),
        ...dateRangeInput,
        coupon: z.string().nullish().describe("Only orders that used this coupon code"),
        refundedOnly: z.boolean().default(false),
        unshippedOnly: z.boolean().default(false),
        limit: z.number().int().min(1).max(50).default(15),
      }),
      execute: async ({ query, start, end, coupon, refundedOnly, unshippedOnly, limit }) => {
        const q = query?.toLowerCase();
        const code = coupon?.toUpperCase();
        const matches = inRange(start, end).filter((o) => {
          if (code && !o.coupons.includes(code)) return false;
          if (refundedOnly && o.money.refund <= 0) return false;
          if (unshippedOnly && (o.dateShipped || o.cancelled)) return false;
          if (!q) return true;
          return (
            o.key.toLowerCase().includes(q) ||
            (ctx.shareBuyerNames && o.buyerName.toLowerCase().includes(q)) ||
            buyerLabel(o.buyerKey, o.buyerName).toLowerCase() === q ||
            o.coupons.some((c) => c.toLowerCase().includes(q)) ||
            (o.location ?? "").toLowerCase().includes(q) ||
            o.rows.some(
              (r) =>
                r.itemName?.toLowerCase().includes(q) ||
                r.sku?.toLowerCase().includes(q) ||
                r.style?.toLowerCase().includes(q)
            )
          );
        });
        return {
          matched: matches.length,
          totalRevenue: r2(matches.reduce((s, o) => s + (o.cancelled ? 0 : o.money.revenue), 0)),
          totalUnits: matches.reduce((s, o) => s + (o.cancelled ? 0 : o.units), 0),
          orders: matches.slice(-limit).reverse().map(orderSummary),
        };
      },
    }),
  };
}

export type AnalystTools = ReturnType<typeof buildAnalystTools>;

/** Short past-tense labels shown in the chat while a tool runs. */
export const TOOL_LABELS: Record<keyof AnalystTools, string> = {
  get_overview: "Shop overview",
  get_monthly_performance: "Monthly performance",
  get_products: "Products",
  get_product_detail: "Product detail",
  get_customer_insights: "Customers",
  get_geo_breakdown: "Sales by location",
  get_breakdown: "Breakdown",
  get_coupon_performance: "Coupons",
  get_fulfillment_stats: "Shipping speed",
  get_refunds: "Refunds",
  get_day_of_week_pattern: "Weekly pattern",
  get_forecast: "Forecast",
  project_scenario: "Scenario",
  get_insights: "What needs attention",
  search_orders: "Order search",
};
