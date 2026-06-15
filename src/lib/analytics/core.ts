import type { SaleRow } from "@/lib/types";

/**
 * Pure analytics over flat SaleRow arrays. Everything here is deterministic
 * and shared between the dashboard UI and the AI assistant's tools.
 *
 * Money conventions:
 *  - "net revenue"  = sum of order_net over unique orders (what Etsy pays out
 *    before shipping label costs)
 *  - "gross revenue" = sum of order_total over unique orders (what buyers paid)
 *  - item-level revenue = item_total (pre-discount item value)
 */

// ---------------------------------------------------------------------------
// Basics
// ---------------------------------------------------------------------------

export function monthKey(date: string): string {
  return date.slice(0, 7);
}

export function buyerKey(r: SaleRow): string | null {
  if (r.buyerUserId) return `id:${r.buyerUserId}`;
  if (r.buyerUsername) return `u:${r.buyerUsername.toLowerCase()}`;
  if (r.fullName) return `n:${r.fullName.toLowerCase()}`;
  return null;
}

export function buyerDisplayName(r: SaleRow): string {
  return r.fullName ?? r.buyerUsername ?? "Unknown buyer";
}

/**
 * Etsy only fills the "Adjusted *" columns when an order was actually
 * adjusted (refund/cancellation); otherwise they are 0.00. So a 0 in an
 * adjusted field means "no adjustment", not "$0 of value" — only trust it
 * when it's non-zero.
 */
function adjustment(v: number | null | undefined): number | null {
  return v != null && v !== 0 ? v : null;
}

/** Net for a single order row, with fallbacks for partial data. */
export function orderNetOf(r: SaleRow): number {
  const adjusted = adjustment(r.adjustedNetOrderAmount);
  if (adjusted != null) return adjusted;
  if (r.orderNet != null) return r.orderNet;
  if (r.orderTotal != null) return r.orderTotal - (r.cardProcessingFees ?? 0);
  return r.itemTotal ?? 0;
}

export function orderGrossOf(r: SaleRow): number {
  const adjusted = adjustment(r.adjustedOrderTotal);
  if (adjusted != null) return adjusted;
  if (r.orderTotal != null) return r.orderTotal;
  return r.itemTotal ?? 0;
}

/** Etsy processing fees for one order, ignoring zero "adjusted" placeholders. */
export function orderFeesOf(r: SaleRow): number {
  return adjustment(r.adjustedCardProcessingFees) ?? r.cardProcessingFees ?? 0;
}

/** Refund total for one order (0 when none). */
export function orderRefundOf(r: SaleRow): number {
  return r.refundAmount ?? 0;
}

/** One representative row per order (first transaction of each order). */
export function orderLevel(rows: SaleRow[]): SaleRow[] {
  const seen = new Set<string>();
  const out: SaleRow[] = [];
  for (const r of rows) {
    const key = r.orderId ?? `t:${r.transactionId}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(r);
  }
  return out;
}

export function filterByDateRange(
  rows: SaleRow[],
  start: string | null,
  end: string | null
): SaleRow[] {
  if (!start && !end) return rows;
  return rows.filter((r) => {
    if (!r.saleDate) return false;
    if (start && r.saleDate < start) return false;
    if (end && r.saleDate > end) return false;
    return true;
  });
}

export function dateBounds(rows: SaleRow[]): { min: string; max: string } | null {
  let min: string | null = null;
  let max: string | null = null;
  for (const r of rows) {
    if (!r.saleDate) continue;
    if (min === null || r.saleDate < min) min = r.saleDate;
    if (max === null || r.saleDate > max) max = r.saleDate;
  }
  return min && max ? { min, max } : null;
}

function median(sorted: number[]): number | null {
  if (sorted.length === 0) return null;
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
}

function ratioDelta(current: number, previous: number): number | null {
  if (!Number.isFinite(previous) || previous === 0) return null;
  return (current - previous) / previous;
}

// ---------------------------------------------------------------------------
// KPIs
// ---------------------------------------------------------------------------

export interface Kpis {
  orders: number;
  units: number;
  netRevenue: number;
  grossRevenue: number;
  avgOrderValue: number | null;
  uniqueBuyers: number;
  repeatBuyerRate: number | null;
  uniqueProducts: number;
  totalFees: number;
  totalDiscounts: number;
  shippingCollected: number;
  totalRefunds: number;
  refundedOrders: number;
  /** share of orders with any refund */
  refundRate: number | null;
}

export function computeKpis(rows: SaleRow[]): Kpis {
  const orders = orderLevel(rows);
  const units = rows.reduce((s, r) => s + (r.quantity || 0), 0);
  const netRevenue = orders.reduce((s, r) => s + orderNetOf(r), 0);
  const grossRevenue = orders.reduce((s, r) => s + orderGrossOf(r), 0);

  const buyerOrderCounts = new Map<string, number>();
  for (const o of orders) {
    const key = buyerKey(o);
    if (!key) continue;
    buyerOrderCounts.set(key, (buyerOrderCounts.get(key) ?? 0) + 1);
  }
  const uniqueBuyers = buyerOrderCounts.size;
  const repeatBuyers = [...buyerOrderCounts.values()].filter((c) => c > 1).length;

  const products = new Set(rows.map((r) => r.cardName ?? r.itemName).filter(Boolean));

  const totalRefunds = orders.reduce((s, r) => s + orderRefundOf(r), 0);
  const refundedOrders = orders.filter((r) => orderRefundOf(r) > 0).length;

  return {
    orders: orders.length,
    units,
    netRevenue,
    grossRevenue,
    avgOrderValue: orders.length > 0 ? netRevenue / orders.length : null,
    uniqueBuyers,
    repeatBuyerRate: uniqueBuyers > 0 ? repeatBuyers / uniqueBuyers : null,
    uniqueProducts: products.size,
    totalFees: orders.reduce((s, r) => s + orderFeesOf(r), 0),
    totalDiscounts: orders.reduce(
      (s, r) => s + (r.discountAmount ?? 0) + (r.shippingDiscount ?? 0),
      0
    ),
    shippingCollected: orders.reduce((s, r) => s + (r.shipping ?? 0), 0),
    totalRefunds,
    refundedOrders,
    refundRate: orders.length > 0 ? refundedOrders / orders.length : null,
  };
}

export interface KpiComparison {
  current: Kpis;
  previous: Kpis;
  deltas: {
    orders: number | null;
    units: number | null;
    netRevenue: number | null;
    avgOrderValue: number | null;
  };
}

/** Compare a period against the period of equal length immediately before it. */
export function compareKpis(
  allRows: SaleRow[],
  start: string,
  end: string
): KpiComparison {
  const startD = new Date(start);
  const endD = new Date(end);
  const lengthMs = endD.getTime() - startD.getTime() + 86400000;
  const prevEnd = new Date(startD.getTime() - 86400000);
  const prevStart = new Date(startD.getTime() - lengthMs);
  const iso = (d: Date) => d.toISOString().slice(0, 10);

  const current = computeKpis(filterByDateRange(allRows, start, end));
  const previous = computeKpis(filterByDateRange(allRows, iso(prevStart), iso(prevEnd)));

  return {
    current,
    previous,
    deltas: {
      orders: ratioDelta(current.orders, previous.orders),
      units: ratioDelta(current.units, previous.units),
      netRevenue: ratioDelta(current.netRevenue, previous.netRevenue),
      avgOrderValue:
        current.avgOrderValue != null && previous.avgOrderValue != null
          ? ratioDelta(current.avgOrderValue, previous.avgOrderValue)
          : null,
    },
  };
}

// ---------------------------------------------------------------------------
// Time series
// ---------------------------------------------------------------------------

export interface MonthPoint {
  month: string; // YYYY-MM
  orders: number;
  units: number;
  netRevenue: number;
  grossRevenue: number;
}

export function monthlySeries(rows: SaleRow[]): MonthPoint[] {
  const byMonth = new Map<string, MonthPoint>();
  const ensure = (m: string) => {
    let p = byMonth.get(m);
    if (!p) {
      p = { month: m, orders: 0, units: 0, netRevenue: 0, grossRevenue: 0 };
      byMonth.set(m, p);
    }
    return p;
  };

  for (const r of rows) {
    if (!r.saleDate) continue;
    ensure(monthKey(r.saleDate)).units += r.quantity || 0;
  }
  for (const o of orderLevel(rows)) {
    if (!o.saleDate) continue;
    const p = ensure(monthKey(o.saleDate));
    p.orders += 1;
    p.netRevenue += orderNetOf(o);
    p.grossRevenue += orderGrossOf(o);
  }

  const points = [...byMonth.values()].sort((a, b) => a.month.localeCompare(b.month));

  // Fill gaps so charts and forecasts see a continuous series.
  if (points.length > 1) {
    const filled: MonthPoint[] = [];
    const [startY, startM] = points[0].month.split("-").map(Number);
    const [endY, endM] = points[points.length - 1].month.split("-").map(Number);
    const idx = new Map(points.map((p) => [p.month, p]));
    let y = startY;
    let m = startM;
    while (y < endY || (y === endY && m <= endM)) {
      const key = `${y}-${String(m).padStart(2, "0")}`;
      filled.push(
        idx.get(key) ?? { month: key, orders: 0, units: 0, netRevenue: 0, grossRevenue: 0 }
      );
      m += 1;
      if (m > 12) {
        m = 1;
        y += 1;
      }
    }
    return filled;
  }
  return points;
}

export interface DowPoint {
  day: string; // Mon..Sun
  orders: number;
  netRevenue: number;
}

const DOW_LABELS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

export function dayOfWeekBreakdown(rows: SaleRow[]): DowPoint[] {
  const counts = DOW_LABELS.map((day) => ({ day, orders: 0, netRevenue: 0 }));
  for (const o of orderLevel(rows)) {
    if (!o.saleDate) continue;
    const [y, m, d] = o.saleDate.split("-").map(Number);
    const dow = new Date(y, m - 1, d).getDay();
    counts[dow].orders += 1;
    counts[dow].netRevenue += orderNetOf(o);
  }
  // start week on Monday
  return [...counts.slice(1), counts[0]];
}

// ---------------------------------------------------------------------------
// Products
// ---------------------------------------------------------------------------

export interface ProductStat {
  name: string;
  units: number;
  orders: number;
  itemRevenue: number;
  avgPrice: number | null;
  firstSold: string | null;
  lastSold: string | null;
  /** units in trailing 90 days vs the 90 days before that */
  recentUnits: number;
  priorUnits: number;
  trend: "rising" | "falling" | "steady" | "new" | "dormant";
}

export function productStats(rows: SaleRow[], asOf?: string): ProductStat[] {
  const today = asOf ?? dateBounds(rows)?.max ?? new Date().toISOString().slice(0, 10);
  const t = new Date(today).getTime();
  const d90 = new Date(t - 90 * 86400000).toISOString().slice(0, 10);
  const d180 = new Date(t - 180 * 86400000).toISOString().slice(0, 10);

  const map = new Map<
    string,
    ProductStat & { orderIds: Set<string>; priceSum: number; priceN: number }
  >();

  for (const r of rows) {
    const name = r.cardName ?? r.itemName;
    if (!name) continue;
    let p = map.get(name);
    if (!p) {
      p = {
        name,
        units: 0,
        orders: 0,
        itemRevenue: 0,
        avgPrice: null,
        firstSold: null,
        lastSold: null,
        recentUnits: 0,
        priorUnits: 0,
        trend: "steady",
        orderIds: new Set(),
        priceSum: 0,
        priceN: 0,
      };
      map.set(name, p);
    }
    const qty = r.quantity || 0;
    p.units += qty;
    p.itemRevenue += r.itemTotal ?? (r.price ?? 0) * qty;
    if (r.price != null) {
      p.priceSum += r.price;
      p.priceN += 1;
    }
    if (r.orderId) p.orderIds.add(r.orderId);
    if (r.saleDate) {
      if (!p.firstSold || r.saleDate < p.firstSold) p.firstSold = r.saleDate;
      if (!p.lastSold || r.saleDate > p.lastSold) p.lastSold = r.saleDate;
      if (r.saleDate > d90) p.recentUnits += qty;
      else if (r.saleDate > d180) p.priorUnits += qty;
    }
  }

  return [...map.values()]
    .map((p) => {
      const stat: ProductStat = {
        name: p.name,
        units: p.units,
        orders: p.orderIds.size,
        itemRevenue: p.itemRevenue,
        avgPrice: p.priceN > 0 ? p.priceSum / p.priceN : null,
        firstSold: p.firstSold,
        lastSold: p.lastSold,
        recentUnits: p.recentUnits,
        priorUnits: p.priorUnits,
        trend: "steady",
      };
      if (p.firstSold && p.firstSold > d90) stat.trend = "new";
      else if (p.recentUnits === 0 && p.units > 0) stat.trend = "dormant";
      else if (p.priorUnits > 0 && p.recentUnits >= p.priorUnits * 1.25) stat.trend = "rising";
      else if (p.priorUnits > 0 && p.recentUnits <= p.priorUnits * 0.75) stat.trend = "falling";
      else if (p.priorUnits === 0 && p.recentUnits > 0) stat.trend = "rising";
      return stat;
    })
    .sort((a, b) => b.units - a.units);
}

export function productMonthlyUnits(
  rows: SaleRow[],
  productName: string
): { month: string; units: number; revenue: number }[] {
  const byMonth = new Map<string, { units: number; revenue: number }>();
  for (const r of rows) {
    if ((r.cardName ?? r.itemName) !== productName || !r.saleDate) continue;
    const m = monthKey(r.saleDate);
    const cur = byMonth.get(m) ?? { units: 0, revenue: 0 };
    cur.units += r.quantity || 0;
    cur.revenue += r.itemTotal ?? 0;
    byMonth.set(m, cur);
  }
  return [...byMonth.entries()]
    .map(([month, v]) => ({ month, ...v }))
    .sort((a, b) => a.month.localeCompare(b.month));
}

// ---------------------------------------------------------------------------
// Breakdowns
// ---------------------------------------------------------------------------

export interface NamedCount {
  name: string;
  value: number;
}

export function breakdownBy(
  rows: SaleRow[],
  field: "style" | "productType" | "orderType" | "paymentType",
  metric: "units" | "orders" = "units"
): NamedCount[] {
  const map = new Map<string, number>();
  const source = metric === "orders" ? orderLevel(rows) : rows;
  for (const r of source) {
    const name = r[field] ?? "Unspecified";
    const inc = metric === "units" ? r.quantity || 0 : 1;
    map.set(name, (map.get(name) ?? 0) + inc);
  }
  return [...map.entries()]
    .map(([name, value]) => ({ name, value }))
    .sort((a, b) => b.value - a.value);
}

export interface GeoStat {
  region: string;
  orders: number;
  units: number;
  netRevenue: number;
}

export function geoBreakdown(rows: SaleRow[], by: "state" | "country"): GeoStat[] {
  const map = new Map<string, GeoStat>();
  const orders = orderLevel(rows);
  const unitsByOrder = new Map<string, number>();
  for (const r of rows) {
    const key = r.orderId ?? `t:${r.transactionId}`;
    unitsByOrder.set(key, (unitsByOrder.get(key) ?? 0) + (r.quantity || 0));
  }
  for (const o of orders) {
    if (by === "state" && (o.shipCountry ?? "United States") !== "United States") continue;
    const region = (by === "state" ? o.shipState : o.shipCountry) ?? "Unknown";
    let g = map.get(region);
    if (!g) {
      g = { region, orders: 0, units: 0, netRevenue: 0 };
      map.set(region, g);
    }
    g.orders += 1;
    g.units += unitsByOrder.get(o.orderId ?? `t:${o.transactionId}`) ?? 0;
    g.netRevenue += orderNetOf(o);
  }
  return [...map.values()].sort((a, b) => b.orders - a.orders);
}

// ---------------------------------------------------------------------------
// Buyers
// ---------------------------------------------------------------------------

export interface BuyerStat {
  key: string;
  name: string;
  location: string | null;
  orders: number;
  units: number;
  totalNet: number;
  avgOrderValue: number;
  firstOrder: string | null;
  lastOrder: string | null;
  usedCoupon: boolean;
  tier: "VIP" | "Returning" | "New";
}

export function buyerStats(rows: SaleRow[]): BuyerStat[] {
  const unitsByOrder = new Map<string, number>();
  for (const r of rows) {
    const key = r.orderId ?? `t:${r.transactionId}`;
    unitsByOrder.set(key, (unitsByOrder.get(key) ?? 0) + (r.quantity || 0));
  }

  const map = new Map<string, BuyerStat>();
  for (const o of orderLevel(rows)) {
    const key = buyerKey(o);
    if (!key) continue;
    let b = map.get(key);
    if (!b) {
      b = {
        key,
        name: buyerDisplayName(o),
        location: o.shipState
          ? `${o.shipCity ?? ""}${o.shipCity ? ", " : ""}${o.shipState}`
          : o.shipCountry,
        orders: 0,
        units: 0,
        totalNet: 0,
        avgOrderValue: 0,
        firstOrder: null,
        lastOrder: null,
        usedCoupon: false,
        tier: "New",
      };
      map.set(key, b);
    }
    b.orders += 1;
    b.units += unitsByOrder.get(o.orderId ?? `t:${o.transactionId}`) ?? 0;
    b.totalNet += orderNetOf(o);
    if (o.couponCode) b.usedCoupon = true;
    if (o.saleDate) {
      if (!b.firstOrder || o.saleDate < b.firstOrder) b.firstOrder = o.saleDate;
      if (!b.lastOrder || o.saleDate > b.lastOrder) b.lastOrder = o.saleDate;
    }
  }

  for (const b of map.values()) {
    b.avgOrderValue = b.orders > 0 ? b.totalNet / b.orders : 0;
    b.tier = b.orders >= 3 ? "VIP" : b.orders === 2 ? "Returning" : "New";
  }

  return [...map.values()].sort((a, b) => b.totalNet - a.totalNet);
}

export interface CustomerKpis {
  uniqueBuyers: number;
  repeatRate: number | null;
  avgLifetimeValue: number | null;
  medianDaysBetweenOrders: number | null;
  newVsReturning: { month: string; newBuyers: number; returningBuyers: number }[];
}

export function customerKpis(rows: SaleRow[]): CustomerKpis {
  const buyers = buyerStats(rows);
  const repeat = buyers.filter((b) => b.orders > 1);

  // median days between consecutive orders across all repeat buyers
  const ordersByBuyer = new Map<string, string[]>();
  for (const o of orderLevel(rows)) {
    const key = buyerKey(o);
    if (!key || !o.saleDate) continue;
    const list = ordersByBuyer.get(key) ?? [];
    list.push(o.saleDate);
    ordersByBuyer.set(key, list);
  }
  const gaps: number[] = [];
  for (const dates of ordersByBuyer.values()) {
    if (dates.length < 2) continue;
    dates.sort();
    for (let i = 1; i < dates.length; i++) {
      gaps.push(
        (new Date(dates[i]).getTime() - new Date(dates[i - 1]).getTime()) / 86400000
      );
    }
  }
  gaps.sort((a, b) => a - b);

  // new vs returning buyers by month (first-ever order = new)
  const firstOrderMonth = new Map<string, string>();
  for (const b of buyers) {
    if (b.firstOrder) firstOrderMonth.set(b.key, monthKey(b.firstOrder));
  }
  const nvr = new Map<string, { newBuyers: Set<string>; returningBuyers: Set<string> }>();
  for (const o of orderLevel(rows)) {
    const key = buyerKey(o);
    if (!key || !o.saleDate) continue;
    const m = monthKey(o.saleDate);
    let bucket = nvr.get(m);
    if (!bucket) {
      bucket = { newBuyers: new Set(), returningBuyers: new Set() };
      nvr.set(m, bucket);
    }
    if (firstOrderMonth.get(key) === m) bucket.newBuyers.add(key);
    else bucket.returningBuyers.add(key);
  }

  return {
    uniqueBuyers: buyers.length,
    repeatRate: buyers.length > 0 ? repeat.length / buyers.length : null,
    avgLifetimeValue:
      buyers.length > 0
        ? buyers.reduce((s, b) => s + b.totalNet, 0) / buyers.length
        : null,
    medianDaysBetweenOrders: median(gaps),
    newVsReturning: [...nvr.entries()]
      .map(([month, v]) => ({
        month,
        newBuyers: v.newBuyers.size,
        returningBuyers: v.returningBuyers.size,
      }))
      .sort((a, b) => a.month.localeCompare(b.month)),
  };
}

// ---------------------------------------------------------------------------
// Coupons
// ---------------------------------------------------------------------------

export interface CouponStat {
  code: string;
  orders: number;
  totalDiscount: number;
  netRevenue: number;
  avgOrderNet: number;
}

export interface CouponAnalysis {
  coupons: CouponStat[];
  withCoupon: { orders: number; avgNet: number | null; avgUnits: number | null };
  withoutCoupon: { orders: number; avgNet: number | null; avgUnits: number | null };
  totalDiscountCost: number;
}

export function couponAnalysis(rows: SaleRow[]): CouponAnalysis {
  const unitsByOrder = new Map<string, number>();
  for (const r of rows) {
    const key = r.orderId ?? `t:${r.transactionId}`;
    unitsByOrder.set(key, (unitsByOrder.get(key) ?? 0) + (r.quantity || 0));
  }

  const orders = orderLevel(rows);
  const byCode = new Map<string, CouponStat>();
  let withN = 0,
    withNet = 0,
    withUnits = 0,
    withoutN = 0,
    withoutNet = 0,
    withoutUnits = 0,
    totalDiscount = 0;

  for (const o of orders) {
    const net = orderNetOf(o);
    const units = unitsByOrder.get(o.orderId ?? `t:${o.transactionId}`) ?? 0;
    const discount = (o.discountAmount ?? 0) + (o.shippingDiscount ?? 0);
    if (o.couponCode) {
      withN += 1;
      withNet += net;
      withUnits += units;
      totalDiscount += discount;
      let c = byCode.get(o.couponCode);
      if (!c) {
        c = { code: o.couponCode, orders: 0, totalDiscount: 0, netRevenue: 0, avgOrderNet: 0 };
        byCode.set(o.couponCode, c);
      }
      c.orders += 1;
      c.totalDiscount += discount;
      c.netRevenue += net;
    } else {
      withoutN += 1;
      withoutNet += net;
      withoutUnits += units;
    }
  }

  for (const c of byCode.values()) c.avgOrderNet = c.orders > 0 ? c.netRevenue / c.orders : 0;

  return {
    coupons: [...byCode.values()].sort((a, b) => b.orders - a.orders),
    withCoupon: {
      orders: withN,
      avgNet: withN > 0 ? withNet / withN : null,
      avgUnits: withN > 0 ? withUnits / withN : null,
    },
    withoutCoupon: {
      orders: withoutN,
      avgNet: withoutN > 0 ? withoutNet / withoutN : null,
      avgUnits: withoutN > 0 ? withoutUnits / withoutN : null,
    },
    totalDiscountCost: totalDiscount,
  };
}

// ---------------------------------------------------------------------------
// Fulfillment
// ---------------------------------------------------------------------------

export interface FulfillmentStats {
  avgDays: number | null;
  medianDays: number | null;
  p90Days: number | null;
  sameOrNextDayRate: number | null;
  histogram: { bucket: string; count: number }[];
  ordersMeasured: number;
}

export function fulfillmentStats(rows: SaleRow[]): FulfillmentStats {
  const days: number[] = [];
  for (const o of orderLevel(rows)) {
    if (!o.datePaid || !o.dateShipped) continue;
    const d =
      (new Date(o.dateShipped).getTime() - new Date(o.datePaid).getTime()) / 86400000;
    if (d >= 0 && d <= 60) days.push(d);
  }
  days.sort((a, b) => a - b);

  const buckets = [
    { label: "Same day", min: 0, max: 0 },
    { label: "1 day", min: 1, max: 1 },
    { label: "2-3 days", min: 2, max: 3 },
    { label: "4-7 days", min: 4, max: 7 },
    { label: "8-14 days", min: 8, max: 14 },
    { label: "15+ days", min: 15, max: Infinity },
  ];

  return {
    avgDays: days.length > 0 ? days.reduce((s, d) => s + d, 0) / days.length : null,
    medianDays: median(days),
    p90Days: days.length > 0 ? days[Math.min(days.length - 1, Math.floor(days.length * 0.9))] : null,
    sameOrNextDayRate:
      days.length > 0 ? days.filter((d) => d <= 1).length / days.length : null,
    histogram: buckets.map((b) => ({
      bucket: b.label,
      count: days.filter((d) => d >= b.min && d <= b.max).length,
    })),
    ordersMeasured: days.length,
  };
}

// ---------------------------------------------------------------------------
// Order history
// ---------------------------------------------------------------------------

export interface OrderSummary {
  orderId: string;
  saleDate: string | null;
  buyer: string;
  itemsLabel: string;
  units: number;
  orderTotal: number | null;
  orderNet: number;
  refund: number;
  couponCode: string | null;
  shipLocation: string | null;
  orderType: string | null;
  rows: SaleRow[];
}

export function buildOrderHistory(rows: SaleRow[]): OrderSummary[] {
  const byOrder = new Map<string, SaleRow[]>();
  for (const r of rows) {
    const key = r.orderId ?? `t:${r.transactionId}`;
    const list = byOrder.get(key) ?? [];
    list.push(r);
    byOrder.set(key, list);
  }

  const orders: OrderSummary[] = [];
  for (const [orderId, items] of byOrder.entries()) {
    const first = items[0];
    const names = [...new Set(items.map((i) => i.cardName ?? i.itemName).filter(Boolean))];
    orders.push({
      orderId,
      saleDate: first.saleDate,
      buyer: buyerDisplayName(first),
      itemsLabel:
        names.length <= 2
          ? names.join(", ")
          : `${names.slice(0, 2).join(", ")} +${names.length - 2} more`,
      units: items.reduce((s, i) => s + (i.quantity || 0), 0),
      orderTotal: first.orderTotal,
      orderNet: orderNetOf(first),
      refund: orderRefundOf(first),
      couponCode: first.couponCode,
      shipLocation: first.shipState
        ? `${first.shipCity ?? ""}${first.shipCity ? ", " : ""}${first.shipState}`
        : first.shipCountry,
      orderType: first.orderType,
      rows: items,
    });
  }

  return orders.sort((a, b) => (b.saleDate ?? "").localeCompare(a.saleDate ?? ""));
}
