import { describe, expect, it } from "vitest";
import { row, simpleOrder } from "@/lib/testing/fixtures";
import { buildOrders } from "./core";
import { buildInsights } from "./insights";

const day = (n: number) => {
  const d = new Date(Date.UTC(2025, 0, 1) + n * 86400000);
  return d.toISOString().slice(0, 10);
};

describe("buildInsights", () => {
  it("stays quiet when there is too little data to say anything", () => {
    const orders = buildOrders([
      simpleOrder("1", day(10), 20),
      simpleOrder("2", day(50), 20),
      simpleOrder("3", day(80), 20),
    ]);
    expect(buildInsights(orders, { through: day(80) })).toEqual([]);
  });

  it("reports a real revenue drop with both numbers", () => {
    const rows = [];
    for (let i = 0; i < 12; i++) rows.push(simpleOrder(`a${i}`, day(100 + i * 2), 50)); // days 100–122
    for (let i = 0; i < 4; i++) rows.push(simpleOrder(`b${i}`, day(135 + i * 5), 50)); // days 135–150
    const insights = buildInsights(buildOrders(rows), { through: day(155) });
    const momentum = insights.find((i) => i.id === "momentum");
    expect(momentum?.tone).toBe("warn");
    expect(momentum?.title).toMatch(/down/);
    expect(momentum?.detail).toContain("$200");
    expect(momentum?.question).toBeTruthy();
  });

  it("flags a product only when the change clears the minimum volume", () => {
    const rows = [
      row({ orderId: "1", saleDate: day(20), cardName: "Big", quantity: 7 }),
      row({ orderId: "2", saleDate: day(150), cardName: "Big", quantity: 1 }),
      row({ orderId: "3", saleDate: day(20), cardName: "Small", quantity: 2 }),
      row({ orderId: "4", saleDate: day(150), cardName: "Small", quantity: 1 }),
    ];
    const ids = buildInsights(buildOrders(rows), { through: day(170) }).map((i) => i.id);
    expect(ids).toContain("falling:n:Big");
    expect(ids).not.toContain("falling:n:Small");
  });

  it("warns about late shipping only when a processing time is set", () => {
    const rows = [];
    for (let i = 0; i < 6; i++) {
      rows.push(
        simpleOrder(`o${i}`, day(200 + i), 20, {
          datePaid: day(200 + i),
          dateShipped: day(200 + i + (i < 3 ? 1 : 9)),
        })
      );
    }
    const orders = buildOrders(rows);
    expect(buildInsights(orders, { through: day(215) }).some((i) => i.id === "on-time")).toBe(false);
    const late = buildInsights(orders, { through: day(215), processingDays: 3 }).find((i) => i.id === "on-time");
    expect(late?.title).toMatch(/50%/);
    expect(late?.detail).toMatch(/3 of 6/);
  });

  it("points out stale data and ranks it near the top", () => {
    const rows = [];
    for (let i = 0; i < 10; i++) rows.push(simpleOrder(`o${i}`, day(i * 3), 20));
    const insights = buildInsights(buildOrders(rows), { through: day(27), today: day(60) });
    expect(insights[0].id).toBe("stale-data");
    expect(insights[0].detail).toMatch(/33 days ago/);
  });

  it("notes heavy reliance on coupons", () => {
    const rows = [];
    for (let i = 0; i < 12; i++) {
      rows.push(simpleOrder(`o${i}`, day(300 + i * 3), 40, i < 9 ? { couponCode: "SAVE20", discountAmount: 8 } : {}));
    }
    const insight = buildInsights(buildOrders(rows), { through: day(340) }).find((i) => i.id === "coupon-reliance");
    expect(insight?.title).toMatch(/75%/);
  });
});
