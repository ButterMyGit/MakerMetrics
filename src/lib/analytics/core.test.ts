import { describe, expect, it } from "vitest";
import type { SaleRow } from "@/lib/types";
import {
  buildOrderHistory,
  buyerStats,
  computeKpis,
  couponAnalysis,
  customerKpis,
  fulfillmentStats,
  geoBreakdown,
  monthlySeries,
  orderLevel,
  productStats,
} from "./core";

function row(overrides: Partial<SaleRow>): SaleRow {
  return {
    transactionId: Math.random().toString(36).slice(2),
    orderId: null,
    listingId: null,
    saleDate: "2025-06-01",
    datePaid: null,
    dateShipped: null,
    itemName: null,
    cardName: null,
    productType: null,
    variations: null,
    style: null,
    sku: null,
    quantity: 1,
    price: null,
    itemTotal: null,
    currency: "USD",
    discountAmount: null,
    shippingDiscount: null,
    shipping: null,
    salesTax: null,
    orderValue: null,
    orderTotal: null,
    cardProcessingFees: null,
    orderNet: null,
    adjustedOrderTotal: null,
    adjustedCardProcessingFees: null,
    adjustedNetOrderAmount: null,
    vatPaidByBuyer: null,
    inPersonDiscount: null,
    inPersonLocation: null,
    orderType: null,
    paymentType: null,
    couponCode: null,
    couponDetails: null,
    buyerUsername: null,
    buyerUserId: null,
    fullName: null,
    shipCity: null,
    shipState: null,
    shipZipcode: null,
    shipCountry: null,
    ...overrides,
  };
}

// Two-line-item order + two single orders (one repeat buyer)
const ROWS: SaleRow[] = [
  row({
    transactionId: "1",
    orderId: "A",
    saleDate: "2025-05-10",
    cardName: "Fox",
    quantity: 2,
    itemTotal: 10,
    orderNet: 16.3,
    orderTotal: 18.12,
    cardProcessingFees: 1.2,
    buyerUsername: "jane",
    fullName: "Jane Doe",
    shipState: "TX",
    shipCountry: "United States",
    datePaid: "2025-05-10",
    dateShipped: "2025-05-11",
  }),
  row({
    transactionId: "2",
    orderId: "A",
    saleDate: "2025-05-10",
    cardName: "Bear",
    quantity: 1,
    itemTotal: 3,
    orderNet: 16.3,
    orderTotal: 18.12,
    cardProcessingFees: 1.2,
    buyerUsername: "jane",
    fullName: "Jane Doe",
    shipState: "TX",
    shipCountry: "United States",
  }),
  row({
    transactionId: "3",
    orderId: "B",
    saleDate: "2025-06-20",
    cardName: "Fox",
    quantity: 1,
    itemTotal: 5,
    orderNet: 8.25,
    orderTotal: 9,
    cardProcessingFees: 0.75,
    buyerUsername: "jane",
    fullName: "Jane Doe",
    couponCode: "WELCOME10",
    discountAmount: 0.5,
    shipState: "TX",
    shipCountry: "United States",
    datePaid: "2025-06-20",
    dateShipped: "2025-06-24",
  }),
  row({
    transactionId: "4",
    orderId: "C",
    saleDate: "2025-06-25",
    cardName: "Moon",
    quantity: 3,
    itemTotal: 15,
    orderNet: 14,
    orderTotal: 16,
    cardProcessingFees: 2,
    buyerUsername: "bob",
    fullName: "Bob Roe",
    shipState: "CO",
    shipCountry: "United States",
  }),
];

describe("orderLevel", () => {
  it("dedupes line items to one row per order", () => {
    expect(orderLevel(ROWS)).toHaveLength(3);
  });
});

describe("computeKpis", () => {
  it("computes order-level revenue without double counting", () => {
    const k = computeKpis(ROWS);
    expect(k.orders).toBe(3);
    expect(k.units).toBe(7);
    expect(k.netRevenue).toBeCloseTo(16.3 + 8.25 + 14);
    expect(k.grossRevenue).toBeCloseTo(18.12 + 9 + 16);
    expect(k.uniqueBuyers).toBe(2);
    expect(k.repeatBuyerRate).toBeCloseTo(0.5); // jane has 2 orders
    expect(k.totalFees).toBeCloseTo(1.2 + 0.75 + 2);
  });
});

describe("monthlySeries", () => {
  it("aggregates by month with order-level revenue", () => {
    const series = monthlySeries(ROWS);
    expect(series.map((p) => p.month)).toEqual(["2025-05", "2025-06"]);
    expect(series[0].orders).toBe(1);
    expect(series[0].units).toBe(3);
    expect(series[0].netRevenue).toBeCloseTo(16.3);
    expect(series[1].orders).toBe(2);
    expect(series[1].netRevenue).toBeCloseTo(8.25 + 14);
  });

  it("fills gap months with zeros", () => {
    const series = monthlySeries([
      row({ saleDate: "2025-01-15", orderId: "X", orderNet: 10 }),
      row({ saleDate: "2025-04-15", orderId: "Y", orderNet: 10 }),
    ]);
    expect(series.map((p) => p.month)).toEqual(["2025-01", "2025-02", "2025-03", "2025-04"]);
    expect(series[1].orders).toBe(0);
  });
});

describe("productStats", () => {
  it("ranks products and counts distinct orders", () => {
    const stats = productStats(ROWS);
    const fox = stats.find((p) => p.name === "Fox")!;
    expect(fox.units).toBe(3);
    expect(fox.orders).toBe(2);
    expect(fox.itemRevenue).toBeCloseTo(15);
  });
});

describe("buyers", () => {
  it("tiers buyers by order count", () => {
    const buyers = buyerStats(ROWS);
    const jane = buyers.find((b) => b.name === "Jane Doe")!;
    expect(jane.orders).toBe(2);
    expect(jane.tier).toBe("Returning");
    expect(jane.totalNet).toBeCloseTo(16.3 + 8.25);
    expect(jane.units).toBe(4);
  });

  it("computes repeat rate and reorder gap", () => {
    const k = customerKpis(ROWS);
    expect(k.uniqueBuyers).toBe(2);
    expect(k.repeatRate).toBeCloseTo(0.5);
    expect(k.medianDaysBetweenOrders).toBeCloseTo(41); // May 10 -> Jun 20
  });
});

describe("couponAnalysis", () => {
  it("separates with/without coupon", () => {
    const c = couponAnalysis(ROWS);
    expect(c.withCoupon.orders).toBe(1);
    expect(c.withoutCoupon.orders).toBe(2);
    expect(c.coupons[0].code).toBe("WELCOME10");
    expect(c.totalDiscountCost).toBeCloseTo(0.5);
  });
});

describe("geoBreakdown", () => {
  it("aggregates orders by state", () => {
    const geo = geoBreakdown(ROWS, "state");
    expect(geo[0].region).toBe("TX");
    expect(geo[0].orders).toBe(2);
    expect(geo[0].units).toBe(4);
  });
});

describe("fulfillmentStats", () => {
  it("measures paid-to-shipped days", () => {
    const f = fulfillmentStats(ROWS);
    expect(f.ordersMeasured).toBe(2);
    expect(f.medianDays).toBeCloseTo(2.5); // 1 and 4 days
  });
});

describe("buildOrderHistory", () => {
  it("groups line items into orders, newest first", () => {
    const history = buildOrderHistory(ROWS);
    expect(history).toHaveLength(3);
    expect(history[0].orderId).toBe("C");
    const orderA = history.find((o) => o.orderId === "A")!;
    expect(orderA.units).toBe(3);
    expect(orderA.rows).toHaveLength(2);
  });
});
