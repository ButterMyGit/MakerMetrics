import { describe, expect, it } from "vitest";
import { forecastSeries, type SeriesPoint } from "./forecast";

/** 3 years of synthetic seasonal data: December ~3x baseline, slight growth. */
function seasonalSeries(years = 3): SeriesPoint[] {
  const points: SeriesPoint[] = [];
  const seasonal = [1, 0.8, 0.9, 1, 1, 0.9, 0.8, 0.9, 1, 1.3, 1.8, 3];
  for (let y = 0; y < years; y++) {
    for (let m = 0; m < 12; m++) {
      const t = y * 12 + m;
      points.push({
        month: `${2022 + y}-${String(m + 1).padStart(2, "0")}`,
        value: (100 + t * 1.5) * seasonal[m],
      });
    }
  }
  return points;
}

describe("forecastSeries", () => {
  it("learns the December spike from history", () => {
    const result = forecastSeries(seasonalSeries(), 12);
    expect(result.forecast).toHaveLength(12);

    const december = result.forecast.find((f) => f.month.endsWith("-12"))!;
    const february = result.forecast.find((f) => f.month.endsWith("-02"))!;
    // seasonal ratio Dec/Feb is 3/0.8 = 3.75; demand at least 2.5x
    expect(december.value).toBeGreaterThan(february.value * 2.5);
  });

  it("is reasonably accurate on clean seasonal data", () => {
    const result = forecastSeries(seasonalSeries(), 6);
    expect(result.backtest.monthsTested).toBeGreaterThan(0);
    expect(result.backtest.mape).not.toBeNull();
    expect(result.backtest.mape!).toBeLessThan(0.15);
  });

  it("never forecasts negative values", () => {
    const declining: SeriesPoint[] = Array.from({ length: 18 }, (_, i) => ({
      month: `2024-${String((i % 12) + 1).padStart(2, "0")}`,
      value: Math.max(0, 100 - i * 10),
    }));
    // fix months to be sequential across years
    const fixed = declining.map((p, i) => ({
      ...p,
      month: `${2024 + Math.floor(i / 12)}-${String((i % 12) + 1).padStart(2, "0")}`,
    }));
    const result = forecastSeries(fixed, 6);
    for (const f of result.forecast) {
      expect(f.value).toBeGreaterThanOrEqual(0);
      expect(f.lower).toBeGreaterThanOrEqual(0);
    }
  });

  it("handles tiny histories without crashing", () => {
    const result = forecastSeries(
      [
        { month: "2025-01", value: 10 },
        { month: "2025-02", value: 12 },
      ],
      3
    );
    expect(result.forecast).toHaveLength(3);
    expect(result.method).toContain("not enough history");
  });

  it("handles empty input", () => {
    const result = forecastSeries([], 6);
    expect(result.forecast).toHaveLength(0);
  });

  it("produces widening uncertainty bands", () => {
    const result = forecastSeries(seasonalSeries(), 6);
    const spreads = result.forecast.map((f) => f.upper - f.lower);
    expect(spreads[spreads.length - 1]).toBeGreaterThan(spreads[0]);
  });
});
