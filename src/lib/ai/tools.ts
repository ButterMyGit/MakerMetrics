import { tool } from "ai";
import { z } from "zod";
import type { SaleRow } from "@/lib/types";
import {
  breakdownBy,
  buildOrderHistory,
  buyerStats,
  computeKpis,
  couponAnalysis,
  customerKpis,
  dateBounds,
  dayOfWeekBreakdown,
  filterByDateRange,
  fulfillmentStats,
  geoBreakdown,
  monthlySeries,
  productStats,
} from "@/lib/analytics/core";
import { itemForecasts, ordersForecast, revenueForecast } from "@/lib/analytics/forecast";

const dateRangeInput = {
  start: z
    .string()
    .regex(/^\d{4}-\d{2}-\d{2}$/)
    .optional()
    .describe("Start date (YYYY-MM-DD), inclusive"),
  end: z
    .string()
    .regex(/^\d{4}-\d{2}-\d{2}$/)
    .optional()
    .describe("End date (YYYY-MM-DD), inclusive"),
};

function round2(n: number | null): number | null {
  return n == null ? null : Math.round(n * 100) / 100;
}

/**
 * Tool set for the AI analyst. All tools close over the signed-in user's rows
 * (already scoped by RLS) and run the same deterministic analytics used by
 * the dashboard, so the model reports numbers instead of inventing them.
 */
export function buildAnalystTools(rows: SaleRow[]) {
  const inRange = (start?: string, end?: string) =>
    filterByDateRange(rows, start ?? null, end ?? null);

  return {
    get_overview: tool({
      description:
        "Headline KPIs (orders, units, net/gross revenue, AOV, repeat rate, fees, discounts) for an optional date range. Also returns the dataset's date bounds.",
      inputSchema: z.object(dateRangeInput),
      execute: async ({ start, end }) => {
        const subset = inRange(start, end);
        const k = computeKpis(subset);
        return {
          dateBounds: dateBounds(rows),
          rangeApplied: { start: start ?? null, end: end ?? null },
          ...k,
          netRevenue: round2(k.netRevenue),
          grossRevenue: round2(k.grossRevenue),
          avgOrderValue: round2(k.avgOrderValue),
          totalFees: round2(k.totalFees),
          totalDiscounts: round2(k.totalDiscounts),
          shippingCollected: round2(k.shippingCollected),
          totalRefunds: round2(k.totalRefunds),
        };
      },
    }),

    get_monthly_performance: tool({
      description:
        "Month-by-month orders, units, net and gross revenue. Use to discuss trends, seasonality, best/worst months, growth.",
      inputSchema: z.object(dateRangeInput),
      execute: async ({ start, end }) =>
        monthlySeries(inRange(start, end)).map((p) => ({
          ...p,
          netRevenue: round2(p.netRevenue),
          grossRevenue: round2(p.grossRevenue),
        })),
    }),

    get_top_products: tool({
      description:
        "Per-product stats: units, orders, item revenue, average price, first/last sold, and a rising/falling/new/dormant trend label (last 90 days vs prior 90).",
      inputSchema: z.object({
        ...dateRangeInput,
        limit: z.number().int().min(1).max(50).default(15),
      }),
      execute: async ({ start, end, limit }) =>
        productStats(inRange(start, end))
          .slice(0, limit)
          .map((p) => ({ ...p, itemRevenue: round2(p.itemRevenue), avgPrice: round2(p.avgPrice) })),
    }),

    get_customer_insights: tool({
      description:
        "Customer analytics: unique buyers, repeat rate, average lifetime value, median days between repeat orders, new-vs-returning by month, and the top buyers.",
      inputSchema: z.object({
        ...dateRangeInput,
        topBuyers: z.number().int().min(0).max(25).default(10),
      }),
      execute: async ({ start, end, topBuyers }) => {
        const subset = inRange(start, end);
        const k = customerKpis(subset);
        return {
          ...k,
          avgLifetimeValue: round2(k.avgLifetimeValue),
          topBuyers: buyerStats(subset)
            .slice(0, topBuyers)
            .map((b) => ({
              name: b.name,
              tier: b.tier,
              orders: b.orders,
              units: b.units,
              totalNet: round2(b.totalNet),
              lastOrder: b.lastOrder,
              location: b.location,
            })),
        };
      },
    }),

    get_geo_breakdown: tool({
      description: "Orders, units, and net revenue by US state or by country.",
      inputSchema: z.object({
        ...dateRangeInput,
        by: z.enum(["state", "country"]).default("state"),
      }),
      execute: async ({ start, end, by }) =>
        geoBreakdown(inRange(start, end), by)
          .slice(0, 30)
          .map((g) => ({ ...g, netRevenue: round2(g.netRevenue) })),
    }),

    get_breakdown: tool({
      description:
        "Distribution of sales by style, productType, orderType (online vs in-person), or paymentType.",
      inputSchema: z.object({
        ...dateRangeInput,
        field: z.enum(["style", "productType", "orderType", "paymentType"]),
        metric: z.enum(["units", "orders"]).default("units"),
      }),
      execute: async ({ start, end, field, metric }) =>
        breakdownBy(inRange(start, end), field, metric).slice(0, 25),
    }),

    get_coupon_performance: tool({
      description:
        "Coupon usage and effectiveness: per-code orders and discount cost, plus average order net/units with vs without a coupon.",
      inputSchema: z.object(dateRangeInput),
      execute: async ({ start, end }) => {
        const c = couponAnalysis(inRange(start, end));
        return {
          ...c,
          totalDiscountCost: round2(c.totalDiscountCost),
          coupons: c.coupons.slice(0, 20).map((x) => ({
            ...x,
            totalDiscount: round2(x.totalDiscount),
            netRevenue: round2(x.netRevenue),
            avgOrderNet: round2(x.avgOrderNet),
          })),
          withCoupon: { ...c.withCoupon, avgNet: round2(c.withCoupon.avgNet) },
          withoutCoupon: { ...c.withoutCoupon, avgNet: round2(c.withoutCoupon.avgNet) },
        };
      },
    }),

    get_fulfillment_stats: tool({
      description: "Shipping speed: average/median/p90 days from paid to shipped, plus a histogram.",
      inputSchema: z.object(dateRangeInput),
      execute: async ({ start, end }) => {
        const f = fulfillmentStats(inRange(start, end));
        return { ...f, avgDays: round2(f.avgDays), medianDays: round2(f.medianDays) };
      },
    }),

    get_day_of_week_pattern: tool({
      description: "Orders and net revenue by day of week (Mon-Sun).",
      inputSchema: z.object(dateRangeInput),
      execute: async ({ start, end }) =>
        dayOfWeekBreakdown(inRange(start, end)).map((d) => ({
          ...d,
          netRevenue: round2(d.netRevenue),
        })),
    }),

    get_forecast: tool({
      description:
        "6-month revenue or order forecast (seasonal decomposition + damped trend) with backtest accuracy, plus next-month per-product unit estimates. Always trained on full history.",
      inputSchema: z.object({
        metric: z.enum(["revenue", "orders"]).default("revenue"),
      }),
      execute: async ({ metric }) => {
        const result = metric === "revenue" ? revenueForecast(rows) : ordersForecast(rows);
        return {
          method: result.method,
          backtest: result.backtest,
          notes: result.notes,
          forecast: result.forecast.map((f) => ({
            month: f.month,
            value: round2(f.value),
            lower: round2(f.lower),
            upper: round2(f.upper),
          })),
          nextMonthByProduct: itemForecasts(rows, 10),
        };
      },
    }),

    search_orders: tool({
      description:
        "Search the order history by buyer name, item, SKU, coupon code, order ID, or location. Returns matching orders, most recent first.",
      inputSchema: z.object({
        query: z.string().min(1),
        limit: z.number().int().min(1).max(25).default(10),
      }),
      execute: async ({ query, limit }) => {
        const q = query.toLowerCase();
        return buildOrderHistory(rows)
          .filter(
            (o) =>
              o.orderId.toLowerCase().includes(q) ||
              o.buyer.toLowerCase().includes(q) ||
              o.couponCode?.toLowerCase().includes(q) ||
              o.shipLocation?.toLowerCase().includes(q) ||
              o.rows.some(
                (r) =>
                  r.itemName?.toLowerCase().includes(q) ||
                  r.sku?.toLowerCase().includes(q) ||
                  r.style?.toLowerCase().includes(q)
              )
          )
          .slice(0, limit)
          .map((o) => ({
            orderId: o.orderId,
            date: o.saleDate,
            buyer: o.buyer,
            items: o.rows.map((r) => ({
              name: r.itemName,
              style: r.style,
              qty: r.quantity,
              itemTotal: r.itemTotal,
            })),
            units: o.units,
            orderNet: round2(o.orderNet),
            coupon: o.couponCode,
            location: o.shipLocation,
          }));
      },
    }),
  };
}
