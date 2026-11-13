/**
 * One definition per metric. The same text explains a number in the UI
 * (info tooltips) and tells the AI analyst what each figure means, so the
 * two can't drift apart.
 */

export interface MetricDef {
  label: string;
  /** how it is calculated, in plain words */
  formula: string;
}

export const METRICS = {
  revenue: {
    label: "Revenue",
    formula:
      "Item prices − your discounts + shipping you charged. Sales tax collected by Etsy is not included, and fully refunded orders are left out.",
  },
  netAfterFees: {
    label: "Net after Etsy fees",
    formula:
      "Revenue − payment processing fees (actual, from your Sold Orders export) − refunds − estimated transaction fee (6.5% of revenue) − estimated listing fee ($0.20 per unit). Etsy Ads, Offsite Ads fees, shipping labels and your own costs are not included yet.",
  },
  orders: {
    label: "Orders",
    formula:
      "Checkouts. One order can contain several line items (different products), and each line item can be more than one unit. Fully refunded orders are not counted.",
  },
  units: {
    label: "Units sold",
    formula: "Total quantity across all line items.",
  },
  avgOrderValue: {
    label: "Avg order value",
    formula: "Revenue ÷ orders.",
  },
  repeatBuyerRate: {
    label: "Repeat buyers",
    formula:
      "Share of the period's buyers who had ordered from you before, or ordered more than once in the period.",
  },
  fees: {
    label: "Etsy fees",
    formula:
      "Payment processing fees (actual) + estimated transaction and listing fees. Shown as a share of revenue.",
  },
  discounts: {
    label: "Discounts given",
    formula: "Coupon and sale discounts you funded, including shipping discounts.",
  },
  refunds: {
    label: "Refunds",
    formula:
      "Amounts returned to buyers. Taken from the Etsy Payments export when imported, otherwise from the adjusted totals in the Sold Orders export.",
  },
  lifetimeValue: {
    label: "Avg revenue per buyer",
    formula: "Revenue ÷ unique buyers, over the selected period.",
  },
  reorderGap: {
    label: "Typical time to reorder",
    formula: "Median days between one order and the next, across buyers who ordered more than once.",
  },
  itemRevenue: {
    label: "Item sales",
    formula:
      "List price × quantity for this product, before order-level discounts and without shipping.",
  },
  onTimeRate: {
    label: "Shipped on time",
    formula:
      "Share of orders shipped within your processing time, counted in business days from the day the order was paid.",
  },
  couponLift: {
    label: "Orders while active",
    formula:
      "Shop-wide orders per day between a code's first and last use, compared with the same number of days just before it. It shows whether the shop was busier during the promotion, not what the code caused.",
  },
} as const satisfies Record<string, MetricDef>;

export type MetricId = keyof typeof METRICS;

/** Glossary text for the AI analyst's system prompt. */
export function metricGlossary(): string {
  return Object.values(METRICS)
    .map((m) => `- ${m.label}: ${m.formula}`)
    .join("\n");
}
