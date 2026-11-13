import { describe, expect, it } from "vitest";
import { simpleOrder } from "@/lib/testing/fixtures";
import { buildOrders } from "./core";
import { forecastSeries, itemForecasts, shopForecast, type SeriesPoint } from "./forecast";

function series(start: string, values: number[]): SeriesPoint[] {
  const [y, m] = start.split("-").map(Number);
  return values.map((value, i) => {
    const total = y * 12 + (m - 1) + i;
    return {
      month: `${Math.floor(total / 12)}-${String((total % 12) + 1).padStart(2, "0")}`,
      value,
    };
  });
}

/** 3 years with a December spike on a flat base. */
function seasonal(): SeriesPoint[] {
  const values: number[] = [];
  for (let year = 0; year < 3; year++) {
    for (let m = 1; m <= 12; m++) values.push(m === 12 ? 300 : m === 11 ? 180 : 100);
  }
  return series("2023-01", values);
}

describe("forecastSeries", () => {
  it("never produces a zero-width range", () => {
    for (const input of [
      seasonal(),
      series("2025-01", [100, 120, 90, 110, 105, 95, 100, 130, 80, 100, 110, 90, 100, 120]),
      series("2025-01", [50, 60]),
    ]) {
      const r = forecastSeries(input);
      expect(r.forecast.length).toBe(3);
      for (const f of r.forecast) {
        expect(f.lower).toBeGreaterThanOrEqual(0);
        expect(f.lower).toBeLessThan(f.value);
        expect(f.upper).toBeGreaterThan(f.value);
      }
    }
  });

  it("widens the range further out", () => {
    const r = forecastSeries(series("2025-01", [100, 120, 90, 110, 105, 95, 100, 130]));
    const widths = r.forecast.map((f) => f.upper - f.lower);
    expect(widths[1]).toBeGreaterThan(widths[0]);
    expect(widths[2]).toBeGreaterThan(widths[1]);
  });

  it("does not read a first-year growth ramp as seasonality", () => {
    // Launch ramp Jan–May, then a decline. With one year of data the model
    // must not project next spring back up to the ramp's peak.
    const r = forecastSeries(
      series("2025-06", [50, 80, 120, 200, 300, 500, 900, 1200, 1300, 1350, 1100, 800, 600, 450, 400, 370]),
      { horizon: 6 }
    );
    expect(r.model).not.toBe("seasonal");
    expect(r.model).not.toBe("lastYear");
    for (const f of r.forecast) expect(f.value).toBeLessThan(700);
    expect(r.notes.join(" ")).toMatch(/two full years/);
  });

  it("uses a seasonal model only when it backtests better, given two years", () => {
    const r = forecastSeries(seasonal(), { horizon: 12 });
    expect(["seasonal", "lastYear"]).toContain(r.model);
    const december = r.forecast.find((f) => f.month.endsWith("-12"));
    const june = r.forecast.find((f) => f.month.endsWith("-06"));
    expect(december!.value).toBeGreaterThan(june!.value * 2);
  });

  it("falls back to 'same as last month' when that guess wins the backtest", () => {
    // A steady climb: last month is always closer than a lagging average.
    const r = forecastSeries(series("2025-01", [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]));
    expect(r.model).toBe("last");
    expect(r.forecast[0].value).toBe(100);
    expect(r.accuracy.wape).toBeCloseTo(r.accuracy.baselineWape!, 5);
  });

  it("reports error as total miss over total actual, so near-zero months can't blow it up", () => {
    const r = forecastSeries(series("2025-01", [100, 100, 100, 100, 1, 100, 100, 100]));
    expect(r.accuracy.monthsTested).toBe(5);
    expect(r.accuracy.wape).toBeLessThan(1);
  });

  it("estimates the month in progress from the data-through date, never below actuals", () => {
    const input = series("2025-01", [100, 100, 100, 100, 100, 40]);
    const r = forecastSeries(input, { through: "2025-06-10" });
    expect(r.history).toHaveLength(5);
    expect(r.current?.partial).toEqual({ actual: 40, day: 10, days: 30 });
    // 40 so far + two thirds of a ~100 month
    expect(r.current!.value).toBeCloseTo(40 + 100 * (20 / 30), 0);
    expect(r.current!.lower).toBeGreaterThanOrEqual(40);
    expect(r.forecast[0].month).toBe("2025-07");
    // early in the month, the partial month doesn't drag later months down
    expect(r.forecast[0].value).toBeCloseTo(100, 0);
  });

  it("treats a month as complete when the data runs to its last day", () => {
    const r = forecastSeries(series("2025-01", [100, 100, 100]), { through: "2025-03-31" });
    expect(r.current).toBeNull();
    expect(r.history).toHaveLength(3);
    expect(r.forecast[0].month).toBe("2025-04");
  });

  it("handles tiny and empty histories", () => {
    expect(forecastSeries([]).forecast).toEqual([]);
    const one = forecastSeries(series("2025-01", [30]), { through: "2025-01-10" });
    expect(one.current!.value).toBeCloseTo(93, 0); // pace: 30 in 10 of 31 days
    expect(one.forecast).toHaveLength(3);
    for (const f of one.forecast) expect(f.value).toBeGreaterThanOrEqual(0);
  });
});

describe("shopForecast", () => {
  const rows = [];
  let id = 0;
  // 8 months, 10 orders a month at $40
  for (let m = 1; m <= 8; m++) {
    for (let d = 1; d <= 10; d++) {
      rows.push(simpleOrder(`o${++id}`, `2025-${String(m).padStart(2, "0")}-${String(d * 2).padStart(2, "0")}`, 40));
    }
  }
  const orders = buildOrders(rows);

  it("derives revenue from forecast orders so the two always agree", () => {
    const f = shopForecast(orders, { through: "2025-08-31" });
    expect(f.avgOrderValue).toBe(40);
    f.orders.forecast.forEach((o, i) => {
      expect(Number.isInteger(o.value)).toBe(true);
      expect(f.revenue.forecast[i].value / o.value).toBeCloseTo(40, 0);
    });
    expect(f.revenue.accuracy.monthsTested).toBeGreaterThan(0);
    expect(f.revenue.accuracy.wape).toBeCloseTo(0, 5);
  });

  it("allocates next month's units to products by recent mix, with the tail rolled up", () => {
    const f = itemForecasts(orders, { through: "2025-08-31", limit: 5 });
    expect(f.month).toBe("2025-09");
    expect(f.items).toHaveLength(1);
    expect(f.items[0].name).toBe("Widget");
    expect(f.items[0].share).toBe(1);
    expect(f.items[0].units).toBeCloseTo(10, 0);
    expect(f.other).toEqual({ products: 0, units: 0 });
  });
});
