import type { SaleRow } from "@/lib/types";
import { addDays, addMonths, dayOfMonth, dayOfWeek, daysBetween, daysInMonth, monthKey } from "@/lib/dates";
import { displayBuyerName, displayLocation } from "@/lib/format";

export { monthKey } from "@/lib/dates";

/**
 * Pure analytics over orders. Everything here is deterministic and shared by
 * the UI and the AI assistant's tools.
 *
 * Rows (one per line item) are first folded into `Order`s with a single money
 * waterfall per order (see `OrderMoney`). Every figure in the app comes from
 * that waterfall, so "revenue" means the same thing on every page:
 *
 *   item subtotal − discounts + shipping charged            = revenue
 *   revenue + tax collected by Etsy                         = buyer paid
 *   revenue − payment processing fee − refunds              = Etsy "order net"
 *   order net − est. transaction fee − est. listing fee     = net after Etsy fees
 */

// ---------------------------------------------------------------------------
// Fees
// ---------------------------------------------------------------------------

/**
 * Etsy's published seller fee rates, used to estimate the fees that the
 * order exports don't include. Offsite Ads, Etsy Ads and shipping labels
 * can't be estimated from order data and are left out.
 */
export interface FeeConfig {
  /** share of (item price − discounts + shipping) charged per sale */
  transactionRate: number;
  /** flat listing fee charged per unit sold */
  listingFee: number;
}

export const DEFAULT_FEES: FeeConfig = { transactionRate: 0.065, listingFee: 0.2 };

// ---------------------------------------------------------------------------
// Orders
// ---------------------------------------------------------------------------

export interface OrderMoney {
  /** list price × quantity for every line */
  itemSubtotal: number;
  /** seller-funded discounts (coupons, sales), including shipping discounts */
  discount: number;
  /** shipping the buyer was charged */
  shipping: number;
  /** itemSubtotal − discount + shipping. Excludes tax. */
  revenue: number;
  /** sales tax / VAT collected from the buyer (remitted by Etsy in most places) */
  tax: number;
  /** what the buyer's card was charged: revenue + tax */
  buyerPaid: number;
  /** Etsy Payments processing fee (actual, from the Sold Orders export) */
  processingFee: number;
  /** amount refunded to the buyer */
  refund: number;
  /** Etsy's "Order Net": revenue − processing fee − refunds */
  orderNet: number;
  /** estimated transaction fee (see FeeConfig) */
  transactionFeeEst: number;
  /** estimated listing fees (see FeeConfig) */
  listingFeeEst: number;
  /** orderNet − estimated transaction and listing fees */
  netAfterFees: number;
  /** true when the processing fee came from the export rather than being absent */
  hasProcessingFee: boolean;
}

export interface Order {
  /** order id, or a per-line key when the export had no order id */
  key: string;
  orderId: string | null;
  saleDate: string | null;
  datePaid: string | null;
  dateShipped: string | null;
  rows: SaleRow[];
  units: number;
  buyerKey: string | null;
  buyerName: string;
  /** every coupon code used on the order */
  coupons: string[];
  money: OrderMoney;
  /** "Painesville, OH" */
  location: string | null;
  shipState: string | null;
  shipCountry: string | null;
  orderType: string | null;
  paymentType: string | null;
  /** short "Item A, Item B +2 more" label */
  itemsLabel: string;
  /** fully refunded: excluded from sales figures, still listed under Orders */
  cancelled: boolean;
}

export function buyerKey(r: SaleRow): string | null {
  if (r.buyerUserId) return `id:${r.buyerUserId}`;
  if (r.buyerUsername) return `u:${r.buyerUsername.toLowerCase()}`;
  if (r.fullName) return `n:${r.fullName.toLowerCase()}`;
  return null;
}

export function buyerDisplayName(r: SaleRow): string {
  return displayBuyerName(r.fullName, r.buyerUsername);
}

export function productName(r: SaleRow): string | null {
  return r.cardName ?? r.itemName;
}

function round2(n: number): number {
  return Math.round(n * 100) / 100;
}

/**
 * Etsy only fills the "Adjusted *" columns when an order was actually
 * adjusted (refund/cancellation); otherwise they are 0.00. So a 0 means "no
 * adjustment", not "$0 of value".
 */
function adjustment(v: number | null | undefined): number | null {
  return v != null && v !== 0 ? v : null;
}

/**
 * An order-level amount as exported per line. Etsy either repeats the value
 * on every line or puts it on a single line and 0 on the rest, depending on
 * the export; never both. Identical values on every line are counted once.
 */
function orderLevelAmount(values: (number | null)[]): number {
  const present = values.filter((v): v is number => v != null && v !== 0);
  if (present.length === 0) return 0;
  const repeated =
    values.length > 1 &&
    present.length === values.length &&
    present.every((v) => v === present[0]);
  return repeated ? present[0] : present.reduce((s, v) => s + v, 0);
}

function first<T>(rows: SaleRow[], pick: (r: SaleRow) => T | null | undefined): T | null {
  for (const r of rows) {
    const v = pick(r);
    if (v != null) return v;
  }
  return null;
}

export function orderMoney(rows: SaleRow[], fees: FeeConfig = DEFAULT_FEES): OrderMoney {
  const units = rows.reduce((s, r) => s + (r.quantity || 0), 0);
  const lineSubtotal = rows.reduce(
    (s, r) => s + (r.itemTotal ?? (r.price ?? 0) * (r.quantity || 0)),
    0
  );
  const itemSubtotal = first(rows, (r) => r.orderValue) ?? lineSubtotal;

  // Discounts are exported per line (sum them); shipping and tax per order.
  const discount =
    rows.reduce((s, r) => s + (r.discountAmount ?? 0), 0) +
    orderLevelAmount(rows.map((r) => r.shippingDiscount));
  const shipping = orderLevelAmount(rows.map((r) => r.shipping));
  const revenue = itemSubtotal - discount + shipping;

  // Etsy leaves the tax column at 0 when it collects and remits the tax
  // itself, but still includes that tax in Order Total. Recover it.
  const statedTax = orderLevelAmount(rows.map((r) => r.salesTax));
  const orderTotal = first(rows, (r) => r.orderTotal);
  const impliedTax = orderTotal != null ? orderTotal - revenue : 0;
  const tax = Math.max(statedTax, impliedTax > 0.005 ? impliedTax : 0);
  const buyerPaid = orderTotal ?? revenue + tax;

  const cardFees = first(rows, (r) => r.cardProcessingFees);
  const adjustedFees = adjustment(first(rows, (r) => r.adjustedCardProcessingFees));
  const processingFee = adjustedFees ?? cardFees ?? 0;

  const exportedNet = first(rows, (r) => r.orderNet);
  const adjustedNet = adjustment(first(rows, (r) => r.adjustedNetOrderAmount));

  // Refunds come from the payments export when it was imported; otherwise
  // they are inferred from the adjusted net Etsy reports for the order.
  const importedRefund = first(rows, (r) => r.refundAmount) ?? 0;
  const inferredRefund =
    adjustedNet != null && exportedNet != null
      ? Math.max(0, exportedNet + (cardFees ?? 0) - (adjustedNet + processingFee))
      : 0;
  const refund = importedRefund > 0 ? importedRefund : inferredRefund;

  let orderNet: number;
  if (adjustedNet != null) orderNet = adjustedNet;
  else if (exportedNet != null) orderNet = exportedNet - importedRefund;
  else orderNet = revenue - processingFee - importedRefund;

  // Etsy returns the transaction fee on refunded amounts.
  const transactionFeeEst = fees.transactionRate * Math.max(0, revenue - refund);
  const listingFeeEst = fees.listingFee * units;

  return {
    itemSubtotal: round2(itemSubtotal),
    discount: round2(discount),
    shipping: round2(shipping),
    revenue: round2(revenue),
    tax: round2(tax),
    buyerPaid: round2(buyerPaid),
    processingFee: round2(processingFee),
    refund: round2(refund),
    orderNet: round2(orderNet),
    transactionFeeEst: round2(transactionFeeEst),
    listingFeeEst: round2(listingFeeEst),
    netAfterFees: round2(orderNet - transactionFeeEst - listingFeeEst),
    hasProcessingFee: cardFees != null || adjustedFees != null,
  };
}

function buildOrder(key: string, rows: SaleRow[], fees: FeeConfig): Order {
  const head = rows[0];
  const money = orderMoney(rows, fees);
  const names = [...new Set(rows.map(productName).filter((n): n is string => Boolean(n)))];
  const coupons = [...new Set(rows.flatMap((r) => r.coupons))];
  return {
    key,
    orderId: head.orderId,
    saleDate: head.saleDate,
    datePaid: first(rows, (r) => r.datePaid),
    dateShipped: first(rows, (r) => r.dateShipped),
    rows,
    units: rows.reduce((s, r) => s + (r.quantity || 0), 0),
    buyerKey: buyerKey(head),
    buyerName: buyerDisplayName(head),
    coupons,
    money,
    location: displayLocation(head.shipCity, head.shipState, head.shipCountry),
    shipState: head.shipState ? head.shipState.trim().toUpperCase() : null,
    shipCountry: head.shipCountry,
    orderType: head.orderType,
    paymentType: head.paymentType,
    itemsLabel:
      names.length <= 2
        ? names.join(", ")
        : `${names.slice(0, 2).join(", ")} +${names.length - 2} more`,
    cancelled: money.refund > 0 && money.refund >= money.revenue - 0.01,
  };
}

const orderCache = new WeakMap<SaleRow[], Map<string, Order[]>>();

/** Fold line items into orders (oldest first). Memoized per rows array. */
export function buildOrders(rows: SaleRow[], fees: FeeConfig = DEFAULT_FEES): Order[] {
  const cacheKey = `${fees.transactionRate}:${fees.listingFee}`;
  let perRows = orderCache.get(rows);
  const hit = perRows?.get(cacheKey);
  if (hit) return hit;

  const groups = new Map<string, SaleRow[]>();
  for (const r of rows) {
    const key = r.orderId ?? `t:${r.transactionId}`;
    const list = groups.get(key);
    if (list) list.push(r);
    else groups.set(key, [r]);
  }
  const orders = [...groups.entries()]
    .map(([key, list]) => buildOrder(key, list, fees))
    .sort((a, b) => (a.saleDate ?? "").localeCompare(b.saleDate ?? ""));

  if (!perRows) {
    perRows = new Map();
    orderCache.set(rows, perRows);
  }
  perRows.set(cacheKey, orders);
  return orders;
}

/** Orders that count as sales (everything except full refunds). */
export function sold(orders: Order[]): Order[] {
  return orders.some((o) => o.cancelled) ? orders.filter((o) => !o.cancelled) : orders;
}

export function filterOrders(
  orders: Order[],
  start: string | null,
  end: string | null
): Order[] {
  if (!start && !end) return orders;
  return orders.filter((o) => {
    if (!o.saleDate) return false;
    if (start && o.saleDate < start) return false;
    if (end && o.saleDate > end) return false;
    return true;
  });
}

export function dateBounds(orders: Order[]): { min: string; max: string } | null {
  let min: string | null = null;
  let max: string | null = null;
  for (const o of orders) {
    if (!o.saleDate) continue;
    if (min === null || o.saleDate < min) min = o.saleDate;
    if (max === null || o.saleDate > max) max = o.saleDate;
  }
  return min && max ? { min, max } : null;
}

/** Latest date with any activity (a sale or a shipment): how fresh the data is. */
export function dataThrough(orders: Order[]): string | null {
  let latest: string | null = null;
  for (const o of orders) {
    for (const d of [o.saleDate, o.dateShipped]) {
      if (d && (latest === null || d > latest)) latest = d;
    }
  }
  return latest;
}

function median(sorted: number[]): number | null {
  if (sorted.length === 0) return null;
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
}

function ratioDelta(current: number | null, previous: number | null): number | null {
  if (current == null || previous == null) return null;
  if (!Number.isFinite(previous) || previous === 0) return null;
  return (current - previous) / previous;
}

// ---------------------------------------------------------------------------
// KPIs
// ---------------------------------------------------------------------------

export interface Kpis {
  orders: number;
  /** line items (Etsy "transactions"): one per product on an order */
  lineItems: number;
  units: number;
  revenue: number;
  netAfterFees: number;
  avgOrderValue: number | null;
  uniqueBuyers: number;
  /** buyers in this period who had ordered before (or ordered twice in it) */
  repeatBuyers: number;
  repeatBuyerRate: number | null;
  uniqueProducts: number;
  processingFees: number;
  /** estimated transaction + listing fees */
  etsyFeesEst: number;
  /** processing (actual) + estimated transaction and listing fees */
  totalFees: number;
  /** totalFees ÷ revenue */
  feeRate: number | null;
  totalDiscounts: number;
  shippingCharged: number;
  taxCollected: number;
  totalRefunds: number;
  refundedOrders: number;
  /** share of orders with any refund (full refunds included) */
  refundRate: number | null;
  cancelledOrders: number;
}

/**
 * @param orders  orders in the period
 * @param history every order of the shop, used to tell returning buyers from
 *                first-time ones. Defaults to `orders`.
 */
export function computeKpis(orders: Order[], history: Order[] = orders): Kpis {
  const live = sold(orders);

  const firstOrderByBuyer = new Map<string, string>();
  for (const o of sold(history)) {
    if (!o.buyerKey || !o.saleDate) continue;
    const prev = firstOrderByBuyer.get(o.buyerKey);
    if (!prev || o.saleDate < prev) firstOrderByBuyer.set(o.buyerKey, o.saleDate);
  }
  const buyerOrders = new Map<string, { count: number; earliest: string | null }>();
  for (const o of live) {
    if (!o.buyerKey) continue;
    const b = buyerOrders.get(o.buyerKey) ?? { count: 0, earliest: null };
    b.count += 1;
    if (o.saleDate && (!b.earliest || o.saleDate < b.earliest)) b.earliest = o.saleDate;
    buyerOrders.set(o.buyerKey, b);
  }
  let repeatBuyers = 0;
  for (const [key, b] of buyerOrders) {
    const firstEver = firstOrderByBuyer.get(key);
    if (b.count > 1 || (firstEver && b.earliest && firstEver < b.earliest)) repeatBuyers += 1;
  }

  const products = new Set<string>();
  let units = 0;
  let lineItems = 0;
  for (const o of live) {
    units += o.units;
    lineItems += o.rows.length;
    for (const r of o.rows) if (r.productKey) products.add(r.productKey);
  }

  const sum = (pick: (m: OrderMoney) => number, list: Order[] = live) =>
    round2(list.reduce((s, o) => s + pick(o.money), 0));

  const revenue = sum((m) => m.revenue);
  const processingFees = sum((m) => m.processingFee);
  const etsyFeesEst = sum((m) => m.transactionFeeEst + m.listingFeeEst);
  const totalFees = round2(processingFees + etsyFeesEst);
  const refundedOrders = orders.filter((o) => o.money.refund > 0).length;

  return {
    orders: live.length,
    lineItems,
    units,
    revenue,
    netAfterFees: sum((m) => m.netAfterFees),
    avgOrderValue: live.length > 0 ? revenue / live.length : null,
    uniqueBuyers: buyerOrders.size,
    repeatBuyers,
    repeatBuyerRate: buyerOrders.size > 0 ? repeatBuyers / buyerOrders.size : null,
    uniqueProducts: products.size,
    processingFees,
    etsyFeesEst,
    totalFees,
    feeRate: revenue > 0 ? totalFees / revenue : null,
    totalDiscounts: sum((m) => m.discount),
    shippingCharged: sum((m) => m.shipping),
    taxCollected: sum((m) => m.tax),
    totalRefunds: sum((m) => m.refund, orders),
    refundedOrders,
    refundRate: orders.length > 0 ? refundedOrders / orders.length : null,
    cancelledOrders: orders.length - live.length,
  };
}

export interface KpiComparison {
  current: Kpis;
  previous: Kpis;
  /** the comparison window */
  previousRange: { start: string; end: string };
  deltas: {
    orders: number | null;
    units: number | null;
    revenue: number | null;
    netAfterFees: number | null;
    avgOrderValue: number | null;
    /** percentage-point change, e.g. 0.04 = +4 points */
    repeatBuyerRate: number | null;
  };
}

/** Compare a period against the period of equal length immediately before it. */
export function compareKpis(allOrders: Order[], start: string, end: string): KpiComparison {
  const length = daysBetween(start, end) + 1;
  const prevEnd = addDays(start, -1);
  const prevStart = addDays(start, -length);

  const current = computeKpis(filterOrders(allOrders, start, end), allOrders);
  const previous = computeKpis(filterOrders(allOrders, prevStart, prevEnd), allOrders);

  return {
    current,
    previous,
    previousRange: { start: prevStart, end: prevEnd },
    deltas: {
      orders: ratioDelta(current.orders, previous.orders),
      units: ratioDelta(current.units, previous.units),
      revenue: ratioDelta(current.revenue, previous.revenue),
      netAfterFees: ratioDelta(current.netAfterFees, previous.netAfterFees),
      avgOrderValue: ratioDelta(current.avgOrderValue, previous.avgOrderValue),
      repeatBuyerRate:
        current.repeatBuyerRate != null && previous.repeatBuyerRate != null
          ? current.repeatBuyerRate - previous.repeatBuyerRate
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
  revenue: number;
  netAfterFees: number;
  /** set on the month that the data only partly covers */
  partial?: { day: number; days: number };
}

/**
 * Month-by-month totals with gaps filled. When `through` (the data-through
 * date) falls inside the last month, that month is flagged as partial so
 * charts and forecasts don't read it as a collapse.
 */
export function monthlySeries(orders: Order[], through?: string | null): MonthPoint[] {
  const byMonth = new Map<string, MonthPoint>();
  for (const o of sold(orders)) {
    if (!o.saleDate) continue;
    const m = monthKey(o.saleDate);
    let p = byMonth.get(m);
    if (!p) {
      p = { month: m, orders: 0, units: 0, revenue: 0, netAfterFees: 0 };
      byMonth.set(m, p);
    }
    p.orders += 1;
    p.units += o.units;
    p.revenue += o.money.revenue;
    p.netAfterFees += o.money.netAfterFees;
  }
  if (byMonth.size === 0) return [];

  const months = [...byMonth.keys()].sort();
  const firstMonth = months[0];
  let lastMonth = months[months.length - 1];
  if (through && monthKey(through) > lastMonth) lastMonth = monthKey(through);

  const filled: MonthPoint[] = [];
  for (let m = firstMonth; m <= lastMonth; m = addMonths(m, 1)) {
    const p = byMonth.get(m) ?? { month: m, orders: 0, units: 0, revenue: 0, netAfterFees: 0 };
    p.revenue = round2(p.revenue);
    p.netAfterFees = round2(p.netAfterFees);
    filled.push(p);
  }

  if (through && monthKey(through) === lastMonth) {
    const day = dayOfMonth(through);
    const days = daysInMonth(lastMonth);
    if (day < days) filled[filled.length - 1].partial = { day, days };
  }
  return filled;
}

export interface DowPoint {
  day: string; // Mon..Sun
  orders: number;
  revenue: number;
}

const DOW_LABELS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

export function dayOfWeekBreakdown(orders: Order[]): DowPoint[] {
  const counts = DOW_LABELS.map((day) => ({ day, orders: 0, revenue: 0 }));
  for (const o of sold(orders)) {
    if (!o.saleDate) continue;
    const dow = dayOfWeek(o.saleDate);
    counts[dow].orders += 1;
    counts[dow].revenue += o.money.revenue;
  }
  counts.forEach((c) => (c.revenue = round2(c.revenue)));
  // start week on Monday
  return [...counts.slice(1), counts[0]];
}

// ---------------------------------------------------------------------------
// Products
// ---------------------------------------------------------------------------

export type ProductTrend = "rising" | "falling" | "new" | "dormant" | null;

/** Below this many units across both 90-day windows, a trend is just noise. */
export const TREND_MIN_UNITS = 6;
/** ...and the change itself must be at least this many units. */
export const TREND_MIN_CHANGE = 3;

export interface ProductStat {
  /** stable identity: Etsy listing id when known, otherwise the title */
  key: string;
  listingId: string | null;
  name: string;
  /** most recent full listing title */
  title: string | null;
  productType: string | null;
  units: number;
  orders: number;
  /** list price × quantity, before order-level discounts */
  itemRevenue: number;
  avgPrice: number | null;
  firstSold: string | null;
  lastSold: string | null;
  /** units in the trailing 90 days vs the 90 days before that */
  recentUnits: number;
  priorUnits: number;
  /** null when volume is too low to call a direction */
  trend: ProductTrend;
  /** units per month for the last six months, oldest first */
  spark: number[];
}

/**
 * @param orders  orders in the period: units, orders and item sales come from these
 * @param asOf    the "today" for 90-day windows; defaults to the latest data
 * @param history every order of the shop. When given, first sold, the 90-day
 *                change, the trend and the sparkline describe the product's
 *                whole history rather than just the selected period.
 */
export function productStats(
  orders: Order[],
  asOf?: string | null,
  history?: Order[]
): ProductStat[] {
  if (history && history !== orders) {
    const today = asOf ?? dataThrough(sold(history));
    const lifetime = new Map(productStats(history, today).map((p) => [p.key, p]));
    return productStats(orders, today).map((p) => {
      const full = lifetime.get(p.key);
      return full
        ? {
            ...p,
            name: full.name,
            title: full.title,
            firstSold: full.firstSold,
            recentUnits: full.recentUnits,
            priorUnits: full.priorUnits,
            trend: full.trend,
            spark: full.spark,
          }
        : p;
    });
  }
  const live = sold(orders);
  const today = asOf ?? dataThrough(live) ?? dateBounds(live)?.max;
  if (!today) return [];
  const d90 = addDays(today, -90);
  const d180 = addDays(today, -180);
  const sparkMonths = Array.from({ length: 6 }, (_, i) => addMonths(monthKey(today), i - 5));

  interface Acc extends ProductStat {
    orderKeys: Set<string>;
    priceSum: number;
    priceN: number;
    latest: string;
  }
  const map = new Map<string, Acc>();

  for (const o of live) {
    for (const r of o.rows) {
      const key = r.productKey;
      const name = productName(r);
      if (!key || !name) continue;
      let p = map.get(key);
      if (!p) {
        p = {
          key,
          listingId: r.listingId,
          name,
          title: r.itemName,
          productType: r.productType,
          units: 0,
          orders: 0,
          itemRevenue: 0,
          avgPrice: null,
          firstSold: null,
          lastSold: null,
          recentUnits: 0,
          priorUnits: 0,
          trend: null,
          spark: sparkMonths.map(() => 0),
          orderKeys: new Set(),
          priceSum: 0,
          priceN: 0,
          latest: "",
        };
        map.set(key, p);
      }
      const qty = r.quantity || 0;
      p.units += qty;
      p.itemRevenue += r.itemTotal ?? (r.price ?? 0) * qty;
      if (r.price != null) {
        p.priceSum += r.price;
        p.priceN += 1;
      }
      p.orderKeys.add(o.key);
      const date = r.saleDate;
      if (date) {
        if (!p.firstSold || date < p.firstSold) p.firstSold = date;
        if (!p.lastSold || date > p.lastSold) p.lastSold = date;
        if (date > d90) p.recentUnits += qty;
        else if (date > d180) p.priorUnits += qty;
        const si = sparkMonths.indexOf(monthKey(date));
        if (si >= 0) p.spark[si] += qty;
        // Listings get retitled; show the most recent title.
        if (date >= p.latest) {
          p.latest = date;
          p.name = name;
          p.title = r.itemName;
          p.productType = r.productType;
        }
      }
    }
  }

  const stats = [...map.values()].map((p): ProductStat => {
    let trend: ProductTrend = null;
    const enough = p.recentUnits + p.priorUnits >= TREND_MIN_UNITS;
    const change = p.recentUnits - p.priorUnits;
    if (p.firstSold && p.firstSold > d90) trend = "new";
    else if (p.recentUnits === 0) trend = "dormant";
    else if (enough && change >= TREND_MIN_CHANGE && p.recentUnits >= p.priorUnits * 1.25)
      trend = "rising";
    else if (enough && -change >= TREND_MIN_CHANGE && p.recentUnits <= p.priorUnits * 0.75)
      trend = "falling";
    return {
      key: p.key,
      listingId: p.listingId,
      name: p.name,
      title: p.title,
      productType: p.productType,
      units: p.units,
      orders: p.orderKeys.size,
      itemRevenue: round2(p.itemRevenue),
      avgPrice: p.priceN > 0 ? p.priceSum / p.priceN : null,
      firstSold: p.firstSold,
      lastSold: p.lastSold,
      recentUnits: p.recentUnits,
      priorUnits: p.priorUnits,
      trend,
      spark: p.spark,
    };
  });

  // Two listings can share a display name; tell them apart.
  const byName = new Map<string, ProductStat[]>();
  for (const s of stats) byName.set(s.name, [...(byName.get(s.name) ?? []), s]);
  for (const group of byName.values()) {
    if (group.length < 2) continue;
    const types = new Set(group.map((g) => g.productType ?? ""));
    for (const g of group) {
      if (types.size === group.length && g.productType) g.name = `${g.name} · ${g.productType}`;
      else if (g.listingId) g.name = `${g.name} · #${g.listingId.slice(-4)}`;
    }
  }

  return stats.sort((a, b) => b.units - a.units || b.itemRevenue - a.itemRevenue);
}

export interface ProductDetail {
  monthly: { month: string; units: number; revenue: number }[];
  /** units by option value, grouped by option name ("Size", "Style", …) */
  variations: { name: string; values: { value: string; units: number }[] }[];
  buyers: number;
  repeatBuyers: number;
  /** share of this product's orders that used a coupon */
  couponOrderShare: number | null;
  /** average order discount as a share of the order's item subtotal */
  avgDiscountRate: number | null;
}

export function productDetail(orders: Order[], key: string): ProductDetail {
  const byMonth = new Map<string, { units: number; revenue: number }>();
  const variations = new Map<string, Map<string, number>>();
  const buyerOrders = new Map<string, number>();
  let productOrders = 0;
  let couponOrders = 0;
  let discountRateSum = 0;

  for (const o of sold(orders)) {
    const lines = o.rows.filter((r) => r.productKey === key);
    if (lines.length === 0) continue;
    productOrders += 1;
    if (o.coupons.length > 0) couponOrders += 1;
    if (o.money.itemSubtotal > 0) discountRateSum += o.money.discount / o.money.itemSubtotal;
    if (o.buyerKey) buyerOrders.set(o.buyerKey, (buyerOrders.get(o.buyerKey) ?? 0) + 1);
    for (const r of lines) {
      const qty = r.quantity || 0;
      if (r.saleDate) {
        const m = monthKey(r.saleDate);
        const cur = byMonth.get(m) ?? { units: 0, revenue: 0 };
        cur.units += qty;
        cur.revenue += r.itemTotal ?? (r.price ?? 0) * qty;
        byMonth.set(m, cur);
      }
      for (const v of r.variationList) {
        const values = variations.get(v.name) ?? new Map<string, number>();
        values.set(v.value, (values.get(v.value) ?? 0) + qty);
        variations.set(v.name, values);
      }
    }
  }

  return {
    monthly: [...byMonth.entries()]
      .map(([month, v]) => ({ month, units: v.units, revenue: round2(v.revenue) }))
      .sort((a, b) => a.month.localeCompare(b.month)),
    variations: [...variations.entries()].map(([name, values]) => ({
      name,
      values: [...values.entries()]
        .map(([value, units]) => ({ value, units }))
        .sort((a, b) => b.units - a.units),
    })),
    buyers: buyerOrders.size,
    repeatBuyers: [...buyerOrders.values()].filter((c) => c > 1).length,
    couponOrderShare: productOrders > 0 ? couponOrders / productOrders : null,
    avgDiscountRate: productOrders > 0 ? discountRateSum / productOrders : null,
  };
}

// ---------------------------------------------------------------------------
// Breakdowns
// ---------------------------------------------------------------------------

export interface NamedCount {
  name: string;
  value: number;
}

function toSortedCounts(map: Map<string, number>): NamedCount[] {
  return [...map.entries()]
    .map(([name, value]) => ({ name, value }))
    .sort((a, b) => b.value - a.value);
}

export function breakdownBy(
  orders: Order[],
  field: "productType" | "orderType" | "paymentType",
  metric: "units" | "orders" = "units"
): NamedCount[] {
  const map = new Map<string, number>();
  for (const o of sold(orders)) {
    if (metric === "orders") {
      const name = (field === "productType" ? o.rows[0].productType : o[field]) ?? "Unspecified";
      map.set(name, (map.get(name) ?? 0) + 1);
    } else {
      for (const r of o.rows) {
        const name = r[field] ?? "Unspecified";
        map.set(name, (map.get(name) ?? 0) + (r.quantity || 0));
      }
    }
  }
  return toSortedCounts(map);
}

/** Units sold per option value, for every option name the shop uses. */
export function variationBreakdown(orders: Order[]): { name: string; values: NamedCount[] }[] {
  const byName = new Map<string, Map<string, number>>();
  for (const o of sold(orders)) {
    for (const r of o.rows) {
      for (const v of r.variationList) {
        const values = byName.get(v.name) ?? new Map<string, number>();
        values.set(v.value, (values.get(v.value) ?? 0) + (r.quantity || 0));
        byName.set(v.name, values);
      }
    }
  }
  return [...byName.entries()]
    .map(([name, values]) => ({ name, values: toSortedCounts(values) }))
    .sort(
      (a, b) =>
        b.values.reduce((s, v) => s + v.value, 0) - a.values.reduce((s, v) => s + v.value, 0)
    );
}

export interface GeoStat {
  region: string;
  orders: number;
  units: number;
  revenue: number;
}

export function geoBreakdown(orders: Order[], by: "state" | "country"): GeoStat[] {
  const map = new Map<string, GeoStat>();
  for (const o of sold(orders)) {
    if (by === "state" && (o.shipCountry ?? "United States") !== "United States") continue;
    const region = (by === "state" ? o.shipState : o.shipCountry) ?? "Unknown";
    let g = map.get(region);
    if (!g) {
      g = { region, orders: 0, units: 0, revenue: 0 };
      map.set(region, g);
    }
    g.orders += 1;
    g.units += o.units;
    g.revenue += o.money.revenue;
  }
  const out = [...map.values()];
  out.forEach((g) => (g.revenue = round2(g.revenue)));
  return out.sort((a, b) => b.orders - a.orders);
}

// ---------------------------------------------------------------------------
// Buyers
// ---------------------------------------------------------------------------

/**
 * VIP: 3+ orders. Returning: 2 orders. New: one order, placed in the last
 * 30 days. One-time: one order, longer ago.
 */
export type BuyerTier = "VIP" | "Returning" | "New" | "One-time";

export const NEW_BUYER_DAYS = 30;

export interface BuyerStat {
  key: string;
  name: string;
  location: string | null;
  orders: number;
  units: number;
  revenue: number;
  avgOrderValue: number;
  firstOrder: string | null;
  lastOrder: string | null;
  usedCoupon: boolean;
  tier: BuyerTier;
}

/**
 * @param history every order of the shop. When given, a buyer's tier reflects
 *                all of their orders, not only those in the selected period.
 */
export function buyerStats(orders: Order[], asOf?: string | null, history?: Order[]): BuyerStat[] {
  const live = sold(orders);
  const today = asOf ?? dataThrough(history ? sold(history) : live);
  const lifetime =
    history && history !== orders
      ? new Map(buyerStats(history, today).map((b) => [b.key, b]))
      : null;
  const map = new Map<string, BuyerStat>();
  for (const o of live) {
    if (!o.buyerKey) continue;
    let b = map.get(o.buyerKey);
    if (!b) {
      b = {
        key: o.buyerKey,
        name: o.buyerName,
        location: o.location,
        orders: 0,
        units: 0,
        revenue: 0,
        avgOrderValue: 0,
        firstOrder: null,
        lastOrder: null,
        usedCoupon: false,
        tier: "One-time",
      };
      map.set(o.buyerKey, b);
    }
    b.orders += 1;
    b.units += o.units;
    b.revenue += o.money.revenue;
    if (o.coupons.length > 0) b.usedCoupon = true;
    if (o.saleDate) {
      if (!b.firstOrder || o.saleDate < b.firstOrder) b.firstOrder = o.saleDate;
      if (!b.lastOrder || o.saleDate > b.lastOrder) {
        b.lastOrder = o.saleDate;
        b.location = o.location ?? b.location;
      }
    }
  }

  for (const b of map.values()) {
    b.revenue = round2(b.revenue);
    b.avgOrderValue = b.orders > 0 ? b.revenue / b.orders : 0;
    const full = lifetime?.get(b.key);
    if (full) {
      b.tier = full.tier;
      b.firstOrder = full.firstOrder;
      continue;
    }
    if (b.orders >= 3) b.tier = "VIP";
    else if (b.orders === 2) b.tier = "Returning";
    else if (today && b.firstOrder && daysBetween(b.firstOrder, today) <= NEW_BUYER_DAYS)
      b.tier = "New";
    else b.tier = "One-time";
  }

  return [...map.values()].sort((a, b) => b.revenue - a.revenue);
}

export interface CustomerKpis {
  uniqueBuyers: number;
  repeatBuyers: number;
  /** buyers with 2+ orders ÷ all buyers */
  repeatRate: number | null;
  /** revenue per buyer */
  avgLifetimeValue: number | null;
  medianDaysBetweenOrders: number | null;
  newVsReturning: { month: string; newBuyers: number; returningBuyers: number }[];
}

export function customerKpis(orders: Order[]): CustomerKpis {
  const live = sold(orders);
  const buyers = buyerStats(live);
  const repeat = buyers.filter((b) => b.orders > 1);

  const datesByBuyer = new Map<string, string[]>();
  for (const o of live) {
    if (!o.buyerKey || !o.saleDate) continue;
    const list = datesByBuyer.get(o.buyerKey) ?? [];
    list.push(o.saleDate);
    datesByBuyer.set(o.buyerKey, list);
  }
  const gaps: number[] = [];
  for (const dates of datesByBuyer.values()) {
    if (dates.length < 2) continue;
    dates.sort();
    for (let i = 1; i < dates.length; i++) gaps.push(daysBetween(dates[i - 1], dates[i]));
  }
  gaps.sort((a, b) => a - b);

  const firstOrderMonth = new Map<string, string>();
  for (const b of buyers) if (b.firstOrder) firstOrderMonth.set(b.key, monthKey(b.firstOrder));
  const nvr = new Map<string, { newBuyers: Set<string>; returningBuyers: Set<string> }>();
  for (const o of live) {
    if (!o.buyerKey || !o.saleDate) continue;
    const m = monthKey(o.saleDate);
    let bucket = nvr.get(m);
    if (!bucket) {
      bucket = { newBuyers: new Set(), returningBuyers: new Set() };
      nvr.set(m, bucket);
    }
    if (firstOrderMonth.get(o.buyerKey) === m) bucket.newBuyers.add(o.buyerKey);
    else bucket.returningBuyers.add(o.buyerKey);
  }

  return {
    uniqueBuyers: buyers.length,
    repeatBuyers: repeat.length,
    repeatRate: buyers.length > 0 ? repeat.length / buyers.length : null,
    avgLifetimeValue:
      buyers.length > 0 ? buyers.reduce((s, b) => s + b.revenue, 0) / buyers.length : null,
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
  revenue: number;
  avgOrderValue: number;
  firstUsed: string | null;
  lastUsed: string | null;
  /**
   * Shop-wide orders per day while the code was in use, relative to the same
   * number of days just before it (1.3 = 30% more). Null when the window or
   * the sample is too small to say anything.
   */
  lift: number | null;
}

export interface CouponAnalysis {
  coupons: CouponStat[];
  withCoupon: { orders: number; avgRevenue: number | null; avgUnits: number | null };
  withoutCoupon: { orders: number; avgRevenue: number | null; avgUnits: number | null };
  totalDiscountCost: number;
  /** share of orders that used any coupon */
  couponOrderShare: number | null;
}

/**
 * @param orders  orders in the period
 * @param history all orders, used for the before/during comparison of each
 *                code's window. Defaults to `orders`.
 */
export function couponAnalysis(orders: Order[], history: Order[] = orders): CouponAnalysis {
  const live = sold(orders);
  const byCode = new Map<string, CouponStat>();
  let withN = 0,
    withRev = 0,
    withUnits = 0,
    withoutN = 0,
    withoutRev = 0,
    withoutUnits = 0,
    totalDiscount = 0;

  for (const o of live) {
    if (o.coupons.length === 0) {
      withoutN += 1;
      withoutRev += o.money.revenue;
      withoutUnits += o.units;
      continue;
    }
    withN += 1;
    withRev += o.money.revenue;
    withUnits += o.units;
    totalDiscount += o.money.discount;
    for (const code of o.coupons) {
      let c = byCode.get(code);
      if (!c) {
        c = {
          code,
          orders: 0,
          totalDiscount: 0,
          revenue: 0,
          avgOrderValue: 0,
          firstUsed: null,
          lastUsed: null,
          lift: null,
        };
        byCode.set(code, c);
      }
      c.orders += 1;
      // An order with several codes splits its discount evenly between them.
      c.totalDiscount += o.money.discount / o.coupons.length;
      c.revenue += o.money.revenue;
      if (o.saleDate) {
        if (!c.firstUsed || o.saleDate < c.firstUsed) c.firstUsed = o.saleDate;
        if (!c.lastUsed || o.saleDate > c.lastUsed) c.lastUsed = o.saleDate;
      }
    }
  }

  const all = sold(history);
  const firstSale = dateBounds(all)?.min ?? null;
  for (const c of byCode.values()) {
    c.totalDiscount = round2(c.totalDiscount);
    c.revenue = round2(c.revenue);
    c.avgOrderValue = c.orders > 0 ? c.revenue / c.orders : 0;
    if (!c.firstUsed || !c.lastUsed || !firstSale || c.orders < 5) continue;
    const days = daysBetween(c.firstUsed, c.lastUsed) + 1;
    const beforeStart = addDays(c.firstUsed, -days);
    if (days < 7 || beforeStart < firstSale) continue;
    const during = filterOrders(all, c.firstUsed, c.lastUsed).length;
    const before = filterOrders(all, beforeStart, addDays(c.firstUsed, -1)).length;
    if (before >= 3) c.lift = during / before;
  }

  return {
    coupons: [...byCode.values()].sort((a, b) => b.orders - a.orders),
    withCoupon: {
      orders: withN,
      avgRevenue: withN > 0 ? withRev / withN : null,
      avgUnits: withN > 0 ? withUnits / withN : null,
    },
    withoutCoupon: {
      orders: withoutN,
      avgRevenue: withoutN > 0 ? withoutRev / withoutN : null,
      avgUnits: withoutN > 0 ? withoutUnits / withoutN : null,
    },
    totalDiscountCost: round2(totalDiscount),
    couponOrderShare: live.length > 0 ? withN / live.length : null,
  };
}

// ---------------------------------------------------------------------------
// Fulfillment
// ---------------------------------------------------------------------------

/** Business days (Mon–Fri) after `from`, up to and including `to`. */
export function businessDaysBetween(from: string, to: string): number {
  const total = daysBetween(from, to);
  if (total <= 0) return 0;
  let count = 0;
  for (let i = 1; i <= total; i++) {
    const dow = dayOfWeek(addDays(from, i));
    if (dow !== 0 && dow !== 6) count += 1;
  }
  return count;
}

export interface FulfillmentStats {
  avgDays: number | null;
  medianDays: number | null;
  p90Days: number | null;
  histogram: { bucket: string; count: number }[];
  ordersMeasured: number;
  /** share shipped within the shop's processing time (business days); null until one is set */
  onTimeRate: number | null;
  lateOrders: number;
  monthly: { month: string; orders: number; medianDays: number; onTimeRate: number | null }[];
}

export function fulfillmentStats(
  orders: Order[],
  processingDays?: number | null
): FulfillmentStats {
  interface Shipment {
    month: string;
    days: number;
    onTime: boolean | null;
  }
  const shipments: Shipment[] = [];
  for (const o of sold(orders)) {
    const paid = o.datePaid ?? o.saleDate;
    if (!paid || !o.dateShipped) continue;
    const days = daysBetween(paid, o.dateShipped);
    if (days < 0 || days > 60) continue;
    shipments.push({
      month: monthKey(o.saleDate ?? paid),
      days,
      onTime:
        processingDays != null ? businessDaysBetween(paid, o.dateShipped) <= processingDays : null,
    });
  }

  const days = shipments.map((s) => s.days).sort((a, b) => a - b);
  const buckets = [
    { label: "Same day", min: 0, max: 0 },
    { label: "1 day", min: 1, max: 1 },
    { label: "2-3 days", min: 2, max: 3 },
    { label: "4-7 days", min: 4, max: 7 },
    { label: "8-14 days", min: 8, max: 14 },
    { label: "15+ days", min: 15, max: Infinity },
  ];

  const rate = (list: Shipment[]): number | null =>
    processingDays != null && list.length > 0
      ? list.filter((s) => s.onTime).length / list.length
      : null;

  const byMonth = new Map<string, Shipment[]>();
  for (const s of shipments) byMonth.set(s.month, [...(byMonth.get(s.month) ?? []), s]);

  return {
    avgDays: days.length > 0 ? days.reduce((s, d) => s + d, 0) / days.length : null,
    medianDays: median(days),
    p90Days:
      days.length > 0 ? days[Math.min(days.length - 1, Math.floor(days.length * 0.9))] : null,
    histogram: buckets.map((b) => ({
      bucket: b.label,
      count: days.filter((d) => d >= b.min && d <= b.max).length,
    })),
    ordersMeasured: days.length,
    onTimeRate: rate(shipments),
    lateOrders: shipments.filter((s) => s.onTime === false).length,
    monthly: [...byMonth.entries()]
      .map(([month, list]) => ({
        month,
        orders: list.length,
        medianDays: median(list.map((s) => s.days).sort((a, b) => a - b)) ?? 0,
        onTimeRate: rate(list),
      }))
      .sort((a, b) => a.month.localeCompare(b.month)),
  };
}
