import { addDays, daysBetween } from "@/lib/dates";
import { formatDate, formatMoneyWhole, formatPercent } from "@/lib/format";
import {
  buyerStats,
  computeKpis,
  couponAnalysis,
  customerKpis,
  filterOrders,
  fulfillmentStats,
  productStats,
  sold,
  type Order,
} from "./core";

/**
 * "What needs attention": short, ranked observations computed from the
 * shop's own history. Every rule has a minimum sample size so that a change
 * from 2 units to 3 is never reported as a trend.
 */

export interface Insight {
  id: string;
  tone: "good" | "warn" | "info";
  title: string;
  detail: string;
  /** page that shows the underlying numbers */
  href?: string;
  /** a follow-up the AI analyst can answer */
  question?: string;
  /** higher = shown first */
  weight: number;
}

export interface InsightOptions {
  /** last date the data covers */
  through: string | null;
  /** today's date (to judge how stale the data is) */
  today?: string | null;
  /** the shop's processing time in business days, when set */
  processingDays?: number | null;
}

/** Star Seller requires 95% of orders shipped on time. */
const STAR_SELLER_ON_TIME = 0.95;

export function buildInsights(allOrders: Order[], options: InsightOptions): Insight[] {
  const live = sold(allOrders);
  const through = options.through;
  if (live.length === 0 || !through) return [];
  const out: Insight[] = [];

  const last30 = filterOrders(live, addDays(through, -29), through);
  const prev30 = filterOrders(live, addDays(through, -59), addDays(through, -30));
  const last90 = filterOrders(live, addDays(through, -89), through);
  const prev90 = filterOrders(live, addDays(through, -179), addDays(through, -90));

  // --- Sales momentum -------------------------------------------------------
  const now = computeKpis(last30, live);
  const before = computeKpis(prev30, live);
  if (now.orders + before.orders >= 8 && before.revenue > 0) {
    const change = (now.revenue - before.revenue) / before.revenue;
    if (Math.abs(change) >= 0.2) {
      const down = change < 0;
      out.push({
        id: "momentum",
        tone: down ? "warn" : "good",
        title: `Revenue is ${down ? "down" : "up"} ${formatPercent(Math.abs(change), 0)} over the last 30 days`,
        detail: `${formatMoneyWhole(now.revenue)} from ${now.orders} orders, vs ${formatMoneyWhole(before.revenue)} from ${before.orders} in the 30 days before.`,
        href: "/dashboard",
        question: `Why is my revenue ${down ? "down" : "up"} over the last 30 days compared with the 30 days before? Break it down by product and by orders vs order value.`,
        weight: 90 + Math.min(10, Math.abs(change) * 10),
      });
    }
  }

  // --- Average order value --------------------------------------------------
  if (now.orders >= 5 && before.orders >= 5 && now.avgOrderValue && before.avgOrderValue) {
    const change = (now.avgOrderValue - before.avgOrderValue) / before.avgOrderValue;
    if (Math.abs(change) >= 0.15) {
      out.push({
        id: "aov",
        tone: change > 0 ? "good" : "info",
        title: `Average order value ${change > 0 ? "rose" : "fell"} to ${formatMoneyWhole(now.avgOrderValue)}`,
        detail: `It was ${formatMoneyWhole(before.avgOrderValue)} in the 30 days before.`,
        href: "/orders",
        question: "What changed my average order value recently: discounts, product mix, or units per order?",
        weight: 50,
      });
    }
  }

  // --- Products -------------------------------------------------------------
  const products = productStats(live, through);
  const falling = products
    .filter((p) => p.trend === "falling")
    .sort((a, b) => b.priorUnits - b.recentUnits - (a.priorUnits - a.recentUnits));
  for (const p of falling.slice(0, 2)) {
    out.push({
      id: `falling:${p.key}`,
      tone: "warn",
      title: `${p.name} has slowed down`,
      detail: `${p.recentUnits} sold in the last 90 days, vs ${p.priorUnits} in the 90 days before.`,
      href: "/products",
      question: `Why did ${p.name} drop from ${p.priorUnits} to ${p.recentUnits} units?`,
      weight: 70 + Math.min(10, p.priorUnits - p.recentUnits),
    });
  }
  const rising = products
    .filter((p) => p.trend === "rising")
    .sort((a, b) => b.recentUnits - b.priorUnits - (a.recentUnits - a.priorUnits));
  for (const p of rising.slice(0, 1)) {
    out.push({
      id: `rising:${p.key}`,
      tone: "good",
      title: `${p.name} is picking up`,
      detail: `${p.recentUnits} sold in the last 90 days, vs ${p.priorUnits} in the 90 days before.`,
      href: "/products",
      question: `What is driving the increase in ${p.name} sales, and who is buying it?`,
      weight: 55,
    });
  }
  const formerBest = products
    .slice(0, 10)
    .filter((p) => p.trend === "dormant" && p.units >= 5 && p.lastSold);
  for (const p of formerBest.slice(0, 1)) {
    out.push({
      id: `dormant:${p.key}`,
      tone: "info",
      title: `${p.name} hasn't sold in ${daysBetween(p.lastSold as string, through)} days`,
      detail: `It is one of your top sellers overall (${p.units} units). Last sale: ${formatDate(p.lastSold)}.`,
      href: "/products",
      question: `${p.name} used to sell well but has stopped. What changed around its last sales?`,
      weight: 45,
    });
  }

  // --- Buyers ---------------------------------------------------------------
  const customers = customerKpis(live);
  const gap = customers.medianDaysBetweenOrders;
  if (gap != null && gap >= 7 && customers.repeatBuyers >= 5) {
    const due = buyerStats(live, through).filter((b) => {
      if (b.orders < 2 || !b.lastOrder) return false;
      const since = daysBetween(b.lastOrder, through);
      return since >= gap && since <= gap * 2;
    });
    if (due.length >= 2) {
      out.push({
        id: "reorder-due",
        tone: "info",
        title: `${due.length} repeat buyers are due to reorder`,
        detail: `Your repeat buyers typically come back after about ${Math.round(gap)} days, and these are past that point.`,
        href: "/customers",
        question: "Which repeat buyers are overdue to reorder, and what did each of them buy last?",
        weight: 48,
      });
    }
  }
  const repeatNow = computeKpis(last90, live);
  const repeatBefore = computeKpis(prev90, live);
  if (
    repeatNow.uniqueBuyers >= 10 &&
    repeatBefore.uniqueBuyers >= 10 &&
    repeatNow.repeatBuyerRate != null &&
    repeatBefore.repeatBuyerRate != null
  ) {
    const change = repeatNow.repeatBuyerRate - repeatBefore.repeatBuyerRate;
    if (Math.abs(change) >= 0.05) {
      out.push({
        id: "repeat-rate",
        tone: change > 0 ? "good" : "info",
        title: `${change > 0 ? "More" : "Fewer"} of your buyers are coming back`,
        detail: `${formatPercent(repeatNow.repeatBuyerRate, 0)} of buyers in the last 90 days had ordered before, vs ${formatPercent(repeatBefore.repeatBuyerRate, 0)} in the 90 days before.`,
        href: "/customers",
        question: "What do my returning buyers purchase, and how soon after their first order?",
        weight: 52,
      });
    }
  }

  // --- Shipping -------------------------------------------------------------
  const recentShipping = fulfillmentStats(last30, options.processingDays);
  if (recentShipping.onTimeRate != null && recentShipping.ordersMeasured >= 5) {
    if (recentShipping.onTimeRate < STAR_SELLER_ON_TIME) {
      out.push({
        id: "on-time",
        tone: "warn",
        title: `${formatPercent(recentShipping.onTimeRate, 0)} of recent orders shipped on time`,
        detail: `${recentShipping.lateOrders} of ${recentShipping.ordersMeasured} orders in the last 30 days went out after your ${options.processingDays}-business-day processing time. Star Seller needs 95%.`,
        href: "/orders?tab=fulfillment",
        weight: 80,
      });
    }
  } else {
    const earlier = fulfillmentStats(prev90);
    if (
      recentShipping.ordersMeasured >= 5 &&
      earlier.ordersMeasured >= 10 &&
      recentShipping.medianDays != null &&
      earlier.medianDays != null &&
      recentShipping.medianDays - earlier.medianDays >= 1
    ) {
      out.push({
        id: "slower-shipping",
        tone: "warn",
        title: "Orders are taking longer to ship",
        detail: `Typically ${recentShipping.medianDays} days from payment to shipping in the last 30 days, vs ${earlier.medianDays} before.`,
        href: "/orders?tab=fulfillment",
        weight: 60,
      });
    }
  }

  // --- Refunds --------------------------------------------------------------
  const refundsNow = computeKpis(filterOrders(allOrders, addDays(through, -89), through), allOrders);
  if (refundsNow.refundedOrders >= 2 && (refundsNow.refundRate ?? 0) >= 0.05) {
    out.push({
      id: "refunds",
      tone: "warn",
      title: `${refundsNow.refundedOrders} orders were refunded in the last 90 days`,
      detail: `That is ${formatPercent(refundsNow.refundRate, 0)} of orders and ${formatMoneyWhole(refundsNow.totalRefunds)} returned to buyers.`,
      href: "/orders?filter=refunded",
      question: "Which products and reasons are behind my recent refunds?",
      weight: 65,
    });
  }

  // --- Coupons --------------------------------------------------------------
  const coupons = couponAnalysis(last90, live);
  const recent = computeKpis(last90, live);
  if (recent.orders >= 10 && (coupons.couponOrderShare ?? 0) >= 0.6 && recent.revenue > 0) {
    out.push({
      id: "coupon-reliance",
      tone: "info",
      title: `${formatPercent(coupons.couponOrderShare, 0)} of recent orders used a coupon`,
      detail: `Discounts took ${formatMoneyWhole(coupons.totalDiscountCost)} off in the last 90 days, about ${formatPercent(coupons.totalDiscountCost / (recent.revenue + coupons.totalDiscountCost), 0)} of what those orders would have brought in at full price.`,
      href: "/orders?tab=coupons",
      question: "Was the shop busier while my coupons were active than in the weeks before them?",
      weight: 40,
    });
  }

  // --- Data freshness -------------------------------------------------------
  if (options.today) {
    const age = daysBetween(through, options.today);
    if (age >= 14) {
      out.push({
        id: "stale-data",
        tone: "info",
        title: `Your data ends on ${formatDate(through)}`,
        detail: `That was ${age} days ago. Upload a fresh export so trends and the forecast reflect what is happening now.`,
        href: "/import",
        weight: 85,
      });
    }
  }

  return out.sort((a, b) => b.weight - a.weight);
}
