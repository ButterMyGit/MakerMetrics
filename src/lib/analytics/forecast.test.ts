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

/**
 * Short sparse history mimicking a typical new Etsy seller:
 * strong first month, zero-gap months, then recovery.
 * The old Theil-Sen approach produced a highly negative slope on this data.
 */
function sparseShortSeries(): SeriesPoint[] {
  return [
    { month: "2026-01", value: 1500 },
    { month: "2026-02", value: 0 },    // gap month (zero-padded by monthlySeries)
    { month: "2026-03", value: 0 },
    { month: "2026-04", value: 900 },
    { month: "2026-05", value: 800 },
  ];
}

describe("forecastSeries – long history (seasonal decomposition)", () => {
  it("learns the December spike from 3 years of data", () => {
    const result = forecastSeries(seasonalSeries(), 12);
    expect(result.forecast).toHaveLength(12);
    const december = result.forecast.find((f) => f.month.endsWith("-12"))!;
    const february = result.forecast.find((f) => f.month.endsWith("-02"))!;
    expect(december.value).toBeGreaterThan(february.value * 2.5);
  });

  it("is reasonably accurate on clean seasonal data", () => {
    const result = forecastSeries(seasonalSeries(), 6);
    expect(result.backtest.monthsTested).toBeGreaterThan(0);
    expect(result.backtest.mape).not.toBeNull();
    expect(result.backtest.mape!).toBeLessThan(0.15);
  });

  it("produces non-negative uncertainty bands that widen or stay flat", () => {
    const result = forecastSeries(seasonalSeries(), 6);
    const spreads = result.forecast.map((f) => f.upper - f.lower);
    for (const s of spreads) expect(s).toBeGreaterThanOrEqual(0);
    expect(spreads[spreads.length - 1]).toBeGreaterThanOrEqual(spreads[0]);
  });

  it("never forecasts negative values on a declining series", () => {
    const fixed = Array.from({ length: 18 }, (_, i) => ({
      month: `${2024 + Math.floor(i / 12)}-${String((i % 12) + 1).padStart(2, "0")}`,
      value: Math.max(0, 100 - i * 8),
    }));
    const result = forecastSeries(fixed, 6);
    for (const f of result.forecast) {
      expect(f.value).toBeGreaterThanOrEqual(0);
      expect(f.lower).toBeGreaterThanOrEqual(0);
    }
  });
});

describe("forecastSeries – short history (SES)", () => {
  it("does not produce laughably low forecasts on zero-gap sparse data", () => {
    const result = forecastSeries(sparseShortSeries(), 3);
    expect(result.forecast).toHaveLength(3);
    // With SES the forecast should be in the ballpark of the non-zero months
    // (~800-1500). The old model produced near-zero from negative Theil-Sen slope.
    for (const f of result.forecast) {
      expect(f.value).toBeGreaterThan(300);
    }
  });

  it("uses exponential smoothing method for < 12 months", () => {
    const result = forecastSeries(sparseShortSeries(), 3);
    expect(result.method).toContain("exponential smoothing");
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
});
