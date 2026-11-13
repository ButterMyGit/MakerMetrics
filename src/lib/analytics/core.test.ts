import { describe, expect, it } from "vitest";
import { row, simpleOrder } from "@/lib/testing/fixtures";
import {
  buildOrders,
  businessDaysBetween,
  buyerStats,
  compareKpis,
  computeKpis,
  couponAnalysis,
  customerKpis,
  dataThrough,
  fulfillmentStats,
  geoBreakdown,
  monthlySeries,
  orderMoney,
  productDetail,
  productStats,
  variationBreakdown,
} from "./core";

describe("orderMoney", () => {
  it("reproduces a real single-item Etsy order top to bottom", () => {
    // $36.99 item, 20% coupon, $6.35 shipping, $1.57 marketplace tax.
    const m = orderMoney([
      row({
        price: 36.99,
        itemTotal: 36.99,
        discountAmount: 7.4,
        shipping: 6.35,
        salesTax: 0, // Etsy exports 0 when it remits the tax itself
        orderValue: 36.99,
        orderTotal: 37.51,
        cardProcessingFees: 1.38,
        orderNet: 34.56,
      }),
    ]);
    expect(m.itemSubtotal).toBe(36.99);
    expect(m.discount).toBe(7.4);
    expect(m.shipping).toBe(6.35);
    expect(m.revenue).toBe(35.94);
    expect(m.tax).toBe(1.57);
    expect(m.buyerPaid).toBe(37.51);
    expect(m.processingFee).toBe(1.38);
    expect(m.orderNet).toBe(34.56);
    // revenue − processing = Etsy's order net
    expect(m.revenue - m.processingFee).toBeCloseTo(m.orderNet, 2);
    // 6.5% of revenue + $0.20 for one unit
    expect(m.transactionFeeEst).toBe(2.34);
    expect(m.listingFeeEst).toBe(0.2);
    expect(m.netAfterFees).toBe(32.02);
  });

  it("sums discount and shipping that Etsy puts on a single line of a multi-item order", () => {
    const shared = { orderId: "A", orderValue: 69.98, orderTotal: 63.52, cardProcessingFees: 2.16, orderNet: 55.94 };
    const m = orderMoney([
      row({ ...shared, price: 34.99, itemTotal: 34.99, discountAmount: 0, shipping: 0 }),
      row({ ...shared, price: 34.99, itemTotal: 34.99, discountAmount: 17.5, shipping: 5.62 }),
    ]);
    expect(m.discount).toBe(17.5);
    expect(m.shipping).toBe(5.62);
    expect(m.revenue).toBe(58.1);
    expect(m.tax).toBe(5.42);
    expect(m.revenue - m.processingFee).toBeCloseTo(m.orderNet, 2);
  });

  it("counts shipping once when the export repeats it on every line", () => {
    const m = orderMoney([
      row({ orderId: "B", price: 10, itemTotal: 10, shipping: 4, salesTax: 1 }),
      row({ orderId: "B", price: 10, itemTotal: 10, shipping: 4, salesTax: 1 }),
    ]);
    expect(m.shipping).toBe(4);
    expect(m.tax).toBe(1);
    expect(m.revenue).toBe(24);
  });

  it("ignores zero 'Adjusted' placeholders but honors real adjustments", () => {
    const plain = orderMoney([
      row({ price: 40, itemTotal: 40, orderValue: 40, orderTotal: 40, cardProcessingFees: 1.45, orderNet: 38.55, adjustedNetOrderAmount: 0 }),
    ]);
    expect(plain.orderNet).toBe(38.55);
    expect(plain.refund).toBe(0);

    // Partial refund: Etsy reports the reduced net and fee.
    const adjusted = orderMoney([
      row({
        price: 32.44, itemTotal: 32.44, shipping: 5.29, orderValue: 32.44, orderTotal: 41.01,
        cardProcessingFees: 1.48, orderNet: 36.25,
        adjustedNetOrderAmount: 34.4, adjustedCardProcessingFees: 1.32,
      }),
    ]);
    expect(adjusted.orderNet).toBe(34.4);
    expect(adjusted.processingFee).toBe(1.32);
    expect(adjusted.refund).toBe(2.01);
  });

  it("works from an items-only import (no order financials)", () => {
    const m = orderMoney([row({ price: 20, quantity: 2, itemTotal: 40, discountAmount: 4, shipping: 5 })]);
    expect(m.revenue).toBe(41);
    expect(m.orderNet).toBe(41);
    expect(m.hasProcessingFee).toBe(false);
    expect(m.listingFeeEst).toBe(0.4);
  });
});

describe("buildOrders", () => {
  it("groups line items, unions coupons and flags full refunds", () => {
    const orders = buildOrders([
      row({ orderId: "1", saleDate: "2025-06-02", cardName: "A", price: 10, itemTotal: 10, couponCode: "OCT20OFF;OCT20OFF" }),
      row({ orderId: "1", saleDate: "2025-06-02", cardName: "B", price: 5, itemTotal: 5, couponCode: "OCT20OFF;OCT20OFF" }),
      row({ orderId: "2", saleDate: "2025-06-01", cardName: "A", price: 10, itemTotal: 10, refundAmount: 10 }),
    ]);
    expect(orders.map((o) => o.key)).toEqual(["2", "1"]); // oldest first
    expect(orders[1].coupons).toEqual(["OCT20OFF"]);
    expect(orders[1].units).toBe(2);
    expect(orders[1].itemsLabel).toBe("A, B");
    expect(orders[0].cancelled).toBe(true);
  });

  it("cleans up buyer names and locations for display", () => {
    const [shouting, relay] = buildOrders([
      row({ orderId: "1", fullName: "john vitiello", shipCity: "PAINESVILLE", shipState: "oh", shipCountry: "United States" }),
      row({ orderId: "2", fullName: "abc123@mail.etsy.com", buyerUsername: "craftfan" }),
    ]);
    expect(shouting.buyerName).toBe("John Vitiello");
    expect(shouting.location).toBe("Painesville, OH");
    expect(relay.buyerName).toBe("craftfan");
  });
});

describe("computeKpis", () => {
  const orders = buildOrders([
    simpleOrder("1", "2025-06-01", 40, { buyerUserId: "amy" }),
    simpleOrder("2", "2025-06-10", 20, { buyerUserId: "bob", quantity: 2, price: 10 }),
    simpleOrder("3", "2025-07-05", 30, { buyerUserId: "amy" }),
    simpleOrder("4", "2025-07-06", 50, { buyerUserId: "cat", refundAmount: 50 }),
  ]);

  it("totals revenue per order and leaves fully refunded orders out of sales", () => {
    const k = computeKpis(orders);
    expect(k.orders).toBe(3);
    expect(k.cancelledOrders).toBe(1);
    expect(k.units).toBe(4);
    expect(k.lineItems).toBe(3);
    expect(k.revenue).toBe(90);
    expect(k.avgOrderValue).toBe(30);
    expect(k.totalRefunds).toBe(50);
    expect(k.refundedOrders).toBe(1);
  });

  it("separates actual processing fees from estimated Etsy fees", () => {
    const k = computeKpis(orders);
    expect(k.processingFees).toBeCloseTo(1.45 + 0.85 + 1.15, 2);
    expect(k.etsyFeesEst).toBeCloseTo(90 * 0.065 + 4 * 0.2, 1);
    expect(k.totalFees).toBeCloseTo(k.processingFees + k.etsyFeesEst, 2);
    expect(k.netAfterFees).toBeCloseTo(90 - k.totalFees, 1);
  });

  it("counts a buyer as repeat when they ordered before the period", () => {
    const july = orders.filter((o) => o.saleDate! >= "2025-07-01");
    expect(computeKpis(july).repeatBuyers).toBe(0); // no history given
    const k = computeKpis(july, orders);
    expect(k.uniqueBuyers).toBe(1);
    expect(k.repeatBuyers).toBe(1);
    expect(k.repeatBuyerRate).toBe(1);
  });

  it("compares against the previous period of equal length", () => {
    const c = compareKpis(orders, "2025-07-01", "2025-07-30");
    expect(c.previousRange).toEqual({ start: "2025-06-01", end: "2025-06-30" });
    expect(c.current.revenue).toBe(30);
    expect(c.previous.revenue).toBe(60);
    expect(c.deltas.revenue).toBeCloseTo(-0.5, 5);
  });
});

describe("monthlySeries", () => {
  const orders = buildOrders([
    simpleOrder("1", "2025-01-15", 10),
    simpleOrder("2", "2025-03-20", 30),
  ]);

  it("fills gap months with zeros", () => {
    const s = monthlySeries(orders);
    expect(s.map((p) => p.month)).toEqual(["2025-01", "2025-02", "2025-03"]);
    expect(s[1].orders).toBe(0);
    expect(s[2].revenue).toBe(30);
  });

  it("flags the month the data only partly covers", () => {
    const s = monthlySeries(orders, "2025-03-20");
    expect(s[2].partial).toEqual({ day: 20, days: 31 });
    expect(s[0].partial).toBeUndefined();
    expect(monthlySeries(orders, "2025-03-31")[2].partial).toBeUndefined();
  });

  it("extends to the data-through month even without sales in it", () => {
    const s = monthlySeries(orders, "2025-04-10");
    expect(s.map((p) => p.month)).toEqual(["2025-01", "2025-02", "2025-03", "2025-04"]);
    expect(s[3].partial).toEqual({ day: 10, days: 30 });
  });
});

describe("productStats", () => {
  it("keys products on the listing, so a retitled listing stays one product", () => {
    const orders = buildOrders([
      row({ orderId: "1", saleDate: "2025-05-01", listingId: "111", cardName: "Old title", quantity: 2 }),
      row({ orderId: "2", saleDate: "2025-06-01", listingId: "111", cardName: "New title" }),
      row({ orderId: "3", saleDate: "2025-06-02", listingId: "222", cardName: "Other" }),
    ]);
    const stats = productStats(orders);
    expect(stats).toHaveLength(2);
    expect(stats[0]).toMatchObject({ key: "l:111", name: "New title", units: 3, orders: 2 });
  });

  it("tells apart two listings that share a display name", () => {
    const orders = buildOrders([
      row({ orderId: "1", listingId: "111", cardName: "Charizard", productType: "Print" }),
      row({ orderId: "2", listingId: "222", cardName: "Charizard", productType: "Acrylic" }),
    ]);
    expect(productStats(orders).map((p) => p.name).sort()).toEqual([
      "Charizard · Acrylic",
      "Charizard · Print",
    ]);
  });

  it("does not call a trend on tiny volumes", () => {
    const mk = (id: string, date: string, name: string, qty = 1) =>
      row({ orderId: id, saleDate: date, cardName: name, quantity: qty });
    const orders = buildOrders([
      // 2 -> 3 units: noise
      mk("1", "2025-02-01", "Small", 2),
      mk("2", "2025-06-01", "Small", 3),
      // 7 -> 1 units: a real drop
      mk("3", "2025-02-10", "Big", 7),
      mk("4", "2025-06-10", "Big", 1),
      // 1 -> 9 units: a real rise
      mk("5", "2025-02-15", "Hot", 1),
      mk("6", "2025-06-15", "Hot", 9),
      // long ago only
      mk("7", "2024-01-01", "Gone", 4),
    ]);
    const byName = Object.fromEntries(productStats(orders, "2025-06-30").map((p) => [p.name, p]));
    expect(byName.Small.trend).toBeNull();
    expect(byName.Big.trend).toBe("falling");
    expect(byName.Big).toMatchObject({ priorUnits: 7, recentUnits: 1 });
    expect(byName.Hot.trend).toBe("rising");
    expect(byName.Gone.trend).toBe("dormant");
  });

  it("breaks a product down by month, option and buyer", () => {
    const orders = buildOrders([
      row({ orderId: "1", saleDate: "2025-05-03", cardName: "Art", variations: "Style: Art only", price: 30, itemTotal: 30, buyerUserId: "a" }),
      row({ orderId: "2", saleDate: "2025-06-03", cardName: "Art", variations: "Style: Art + Case", price: 40, itemTotal: 80, quantity: 2, buyerUserId: "a", couponCode: "SAVE" , discountAmount: 8, orderValue: 80}),
      row({ orderId: "3", saleDate: "2025-06-04", cardName: "Else" }),
    ]);
    const d = productDetail(orders, "n:Art");
    expect(d.monthly).toEqual([
      { month: "2025-05", units: 1, revenue: 30 },
      { month: "2025-06", units: 2, revenue: 80 },
    ]);
    expect(d.variations).toEqual([
      { name: "Style", values: [{ value: "Art + Case", units: 2 }, { value: "Art only", units: 1 }] },
    ]);
    expect(d.buyers).toBe(1);
    expect(d.repeatBuyers).toBe(1);
    expect(d.couponOrderShare).toBe(0.5);
  });
});

describe("variationBreakdown", () => {
  it("groups units by every option name the shop uses", () => {
    const orders = buildOrders([
      row({ orderId: "1", variations: "Size: 8x10, Color: Black", quantity: 2 }),
      row({ orderId: "2", variations: "Size: 5x7" }),
    ]);
    const b = variationBreakdown(orders);
    expect(b.find((v) => v.name === "Size")?.values).toEqual([
      { name: "8x10", value: 2 },
      { name: "5x7", value: 1 },
    ]);
    expect(b.find((v) => v.name === "Color")?.values).toEqual([{ name: "Black", value: 2 }]);
  });
});

describe("buyers", () => {
  const orders = buildOrders([
    simpleOrder("1", "2025-01-01", 10, { buyerUserId: "vip" }),
    simpleOrder("2", "2025-02-01", 10, { buyerUserId: "vip" }),
    simpleOrder("3", "2025-03-01", 10, { buyerUserId: "vip" }),
    simpleOrder("4", "2025-01-05", 10, { buyerUserId: "twice" }),
    simpleOrder("5", "2025-01-25", 10, { buyerUserId: "twice" }),
    simpleOrder("6", "2025-01-10", 10, { buyerUserId: "old" }),
    simpleOrder("7", "2025-03-20", 10, { buyerUserId: "fresh" }),
  ]);

  it("labels single-order buyers as New only when the order is recent", () => {
    const tiers = Object.fromEntries(buyerStats(orders, "2025-03-31").map((b) => [b.key, b.tier]));
    expect(tiers).toEqual({
      "id:vip": "VIP",
      "id:twice": "Returning",
      "id:old": "One-time",
      "id:fresh": "New",
    });
  });

  it("computes repeat rate and the typical reorder gap", () => {
    const k = customerKpis(orders);
    expect(k.uniqueBuyers).toBe(4);
    expect(k.repeatBuyers).toBe(2);
    expect(k.repeatRate).toBe(0.5);
    expect(k.medianDaysBetweenOrders).toBe(28); // gaps: 31, 28, 20
  });
});

describe("couponAnalysis", () => {
  it("splits joined codes and counts each order once per code", () => {
    const orders = buildOrders([
      simpleOrder("1", "2025-06-01", 40, { couponCode: "OCT20OFF", discountAmount: 8 }),
      row({ orderId: "2", saleDate: "2025-06-02", price: 20, itemTotal: 20, couponCode: "OCT20OFF;OCT20OFF", discountAmount: 0 }),
      row({ orderId: "2", saleDate: "2025-06-02", price: 20, itemTotal: 20, couponCode: "OCT20OFF;OCT20OFF", discountAmount: 8 }),
      simpleOrder("3", "2025-06-03", 30),
    ]);
    const c = couponAnalysis(orders);
    expect(c.coupons).toHaveLength(1);
    expect(c.coupons[0]).toMatchObject({ code: "OCT20OFF", orders: 2, totalDiscount: 16 });
    expect(c.withCoupon.orders).toBe(2);
    expect(c.withoutCoupon.orders).toBe(1);
    expect(c.couponOrderShare).toBeCloseTo(2 / 3, 5);
  });

  it("compares shop activity during a code's window with the days before it", () => {
    const rows = [];
    // 5 orders in the 10 days before the promo, 10 orders during its 10 days
    for (let d = 1; d <= 10; d += 2) rows.push(simpleOrder(`pre${d}`, `2025-05-${String(d).padStart(2, "0")}`, 20));
    for (let d = 11; d <= 20; d++) {
      rows.push(simpleOrder(`promo${d}`, `2025-05-${d}`, 20, d % 2 ? { couponCode: "MAY" , discountAmount: 2} : {}));
    }
    // earlier history so the "before" window is covered
    rows.push(simpleOrder("old", "2025-04-01", 20));
    const c = couponAnalysis(buildOrders(rows));
    expect(c.coupons[0].code).toBe("MAY");
    // Code used May 11–19 (9 orders in 9 days); the 9 days before had 4 orders.
    expect(c.coupons[0].lift).toBeCloseTo(9 / 4, 5);
  });
});

describe("geoBreakdown", () => {
  it("aggregates orders by state, ignoring casing", () => {
    const orders = buildOrders([
      simpleOrder("1", "2025-06-01", 10, { shipState: "oh", shipCountry: "United States" }),
      simpleOrder("2", "2025-06-02", 20, { shipState: "OH", shipCountry: "United States" }),
      simpleOrder("3", "2025-06-03", 30, { shipState: "ON", shipCountry: "Canada" }),
    ]);
    expect(geoBreakdown(orders, "state")).toEqual([{ region: "OH", orders: 2, units: 2, revenue: 30 }]);
    expect(geoBreakdown(orders, "country").map((g) => g.region)).toEqual(["United States", "Canada"]);
  });
});

describe("fulfillment", () => {
  it("counts business days, skipping weekends", () => {
    expect(businessDaysBetween("2025-06-06", "2025-06-06")).toBe(0); // same day
    expect(businessDaysBetween("2025-06-06", "2025-06-09")).toBe(1); // Fri -> Mon
    expect(businessDaysBetween("2025-06-02", "2025-06-05")).toBe(3); // Mon -> Thu
  });

  it("measures shipping speed and on-time rate against the processing time", () => {
    const orders = buildOrders([
      simpleOrder("1", "2025-06-02", 10, { datePaid: "2025-06-02", dateShipped: "2025-06-03" }),
      simpleOrder("2", "2025-06-06", 10, { datePaid: "2025-06-06", dateShipped: "2025-06-09" }),
      simpleOrder("3", "2025-06-09", 10, { datePaid: "2025-06-09", dateShipped: "2025-06-16" }),
      simpleOrder("4", "2025-06-10", 10, { datePaid: "2025-06-10" }), // not shipped yet
    ]);
    const noTarget = fulfillmentStats(orders);
    expect(noTarget.ordersMeasured).toBe(3);
    expect(noTarget.medianDays).toBe(3);
    expect(noTarget.onTimeRate).toBeNull();

    const f = fulfillmentStats(orders, 2);
    expect(f.onTimeRate).toBeCloseTo(2 / 3, 5); // Fri->Mon is one business day
    expect(f.lateOrders).toBe(1);
    expect(f.monthly).toEqual([{ month: "2025-06", orders: 3, medianDays: 3, onTimeRate: 2 / 3 }]);
  });
});

describe("dataThrough", () => {
  it("is the latest sale or shipment date", () => {
    const orders = buildOrders([
      simpleOrder("1", "2025-06-02", 10, { dateShipped: "2025-06-12" }),
      simpleOrder("2", "2025-06-08", 10),
    ]);
    expect(dataThrough(orders)).toBe("2025-06-12");
  });
});

describe("productStats with history", () => {
  it("takes sales from the period but the trend from the whole history", () => {
    const mk = (id: string, date: string, qty: number) =>
      row({ orderId: id, saleDate: date, cardName: "Big", quantity: qty });
    const all = buildOrders([mk("1", "2025-02-10", 7), mk("2", "2025-06-10", 1)]);
    const june = all.filter((o) => o.saleDate! >= "2025-06-01");
    // On its own, a short range makes an old product look brand new.
    expect(productStats(june, "2025-06-30")[0].trend).toBe("new");
    const [p] = productStats(june, "2025-06-30", all);
    expect(p.units).toBe(1);
    expect(p).toMatchObject({ priorUnits: 7, recentUnits: 1, trend: "falling", firstSold: "2025-02-10" });
  });
});
