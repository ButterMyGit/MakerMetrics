import type { SaleRow } from "@/lib/types";
import { monthlySeries, productStats } from "./core";

/**
 * Monthly forecasting with two regimes:
 *
 * SHORT HISTORY (< 12 complete months):
 *   Simple exponential smoothing (level only, no trend). Robust when data is
 *   sparse and month-to-month swings are large. The previous Theil-Sen+trend
 *   approach computed a heavily negative slope on zero-padded gap months (e.g.
 *   [1500, 0, 0, 900, 800]), producing "laughably low" forecasts.
 *
 * LONG HISTORY (12+ complete months):
 *   Classical multiplicative decomposition:
 *     value(t) = level(t) × seasonalIndex(month) + noise
 *   Seasonal indices from ratio-to-moving-average (24+ months) or ratio-to-
 *   median (12-23 months). Level/trend estimated by Theil-Sen on non-zero
 *   deseasonalized values only, with a mild damping factor.
 *
 * Current partial month: scaled to a full-month estimate (actuals ×
 * daysInMonth / dayOfMonth) rather than dropped, so recent momentum is
 * reflected. The chart labels it as an estimate.
 *
 * Accuracy is measured with a walk-forward backtest vs. a seasonal-naive
 * benchmark and shown on the forecast page.
 */

export interface SeriesPoint {
  month: string; // YYYY-MM
  value: number;
  estimated?: boolean; // true for the scaled current partial month
}

export interface ForecastPoint extends SeriesPoint {
  lower: number;
  upper: number;
}

export interface BacktestResult {
  monthsTested: number;
  /** mean absolute percentage error of this model (0.25 = 25% off on average) */
  mape: number | null;
  /** MAPE of "same month last year" benchmark over the same window */
  seasonalNaiveMape: number | null;
}

export interface ForecastResult {
  history: SeriesPoint[];
  forecast: ForecastPoint[];
  method: string;
  backtest: BacktestResult;
  notes: string[];
}

// ---------------------------------------------------------------------------

function monthOfYear(ym: string): number {
  return Number(ym.split("-")[1]); // 1..12
}

function addMonths(ym: string, k: number): string {
  const [y, m] = ym.split("-").map(Number);
  const total = y * 12 + (m - 1) + k;
  const ny = Math.floor(total / 12);
  const nm = (total % 12) + 1;
  return `${ny}-${String(nm).padStart(2, "0")}`;
}

function daysInMonth(ym: string): number {
  const [y, m] = ym.split("-").map(Number);
  return new Date(y, m, 0).getDate();
}

function theilSen(values: number[]): { slope: number; intercept: number } {
  const n = values.length;
  if (n === 1) return { slope: 0, intercept: values[0] };
  const slopes: number[] = [];
  for (let i = 0; i < n; i++) {
    for (let j = i + 1; j < n; j++) {
      slopes.push((values[j] - values[i]) / (j - i));
    }
  }
  slopes.sort((a, b) => a - b);
  const slope = slopes[Math.floor(slopes.length / 2)];
  const intercepts = values.map((v, i) => v - slope * i).sort((a, b) => a - b);
  const intercept = intercepts[Math.floor(intercepts.length / 2)];
  return { slope, intercept };
}

/** Seasonal indices (length 13, 1-indexed by calendar month), normalized to mean 1. */
function seasonalIndices(series: SeriesPoint[]): { indices: number[]; source: string } {
  const n = series.length;
  const flat = { indices: new Array(13).fill(1), source: "none" };
  if (n < 12) return flat;

  const ratiosByMonth: number[][] = Array.from({ length: 13 }, () => []);

  if (n >= 24) {
    for (let i = 6; i < n - 6; i++) {
      let sum = 0;
      for (let j = i - 6; j < i + 6; j++) sum += series[j].value;
      const ma = (sum + (series[i + 6].value - series[i - 6].value) / 2) / 12;
      if (ma > 0) ratiosByMonth[monthOfYear(series[i].month)].push(series[i].value / ma);
    }
  } else {
    const sorted = series.map((p) => p.value).sort((a, b) => a - b);
    const med = sorted[Math.floor(sorted.length / 2)];
    if (med <= 0) return flat;
    for (const p of series) ratiosByMonth[monthOfYear(p.month)].push(p.value / med);
  }

  const indices = new Array(13).fill(1);
  const observed: number[] = [];
  for (let m = 1; m <= 12; m++) {
    const r = ratiosByMonth[m];
    if (r.length > 0) {
      r.sort((a, b) => a - b);
      indices[m] = r[Math.floor(r.length / 2)];
      observed.push(indices[m]);
    }
  }
  const mean = observed.reduce((s, v) => s + v, 0) / observed.length;
  if (mean > 0) for (let m = 1; m <= 12; m++) indices[m] /= mean;

  return { indices, source: n >= 24 ? "ratio-to-moving-average" : "ratio-to-median" };
}

// ---------------------------------------------------------------------------
// Short-history model: simple exponential smoothing (level only)
// ---------------------------------------------------------------------------

function fitSES(
  series: SeriesPoint[],
  horizon: number,
  alpha = 0.3
): { forecast: ForecastPoint[]; fitted: number[]; method: string } {
  const n = series.length;
  const lastMonth = series[n - 1].month;

  let level = series[0].value;
  const fitted = [level];
  for (let i = 1; i < n; i++) {
    level = alpha * series[i].value + (1 - alpha) * level;
    fitted.push(level);
  }

  const forecast: ForecastPoint[] = Array.from({ length: horizon }, (_, h) => ({
    month: addMonths(lastMonth, h + 1),
    value: Math.max(0, level),
    lower: Math.max(0, level),
    upper: Math.max(0, level),
  }));

  return { forecast, fitted, method: `exponential smoothing (α=${alpha}, short history)` };
}

// ---------------------------------------------------------------------------
// Long-history model: seasonal decomposition + damped Theil-Sen
// ---------------------------------------------------------------------------

function fitDecomposition(
  series: SeriesPoint[],
  horizon: number
): { forecast: ForecastPoint[]; fitted: number[]; method: string } {
  const n = series.length;
  const lastMonth = series[n - 1].month;

  const { indices, source } = seasonalIndices(series);
  const deseason = series.map((p) => p.value / (indices[monthOfYear(p.month)] || 1));

  // Only include non-zero months in the slope estimate — zeros are structural
  // gap-fills, not real data points, and dominate Theil-Sen negatively.
  const nonZeroIdxs = deseason
    .map((v, i) => (v > 0 ? i : -1))
    .filter((i) => i >= 0);

  let slope = 0;
  let level = deseason[n - 1];

  if (nonZeroIdxs.length >= 2) {
    const nonZeroValues = nonZeroIdxs.map((i) => deseason[i]);
    // Re-index so positions are 0..k for Theil-Sen
    const reindexed = nonZeroIdxs.map((absIdx, k) => ({ absIdx, k, v: nonZeroValues[k] }));
    const ts = theilSen(reindexed.map((r) => r.v));
    slope = ts.slope;
    // Level at the last data point's position in the non-zero subsequence
    const lastNonZeroK = reindexed[reindexed.length - 1].k;
    level = ts.intercept + ts.slope * lastNonZeroK;
  }

  const fitted = series.map((p, i) => {
    const s = indices[monthOfYear(p.month)] || 1;
    return Math.max(0, deseason[i] * s);
  });

  const damp = 0.95;
  const forecast: ForecastPoint[] = [];
  let cumSlope = 0;
  for (let h = 1; h <= horizon; h++) {
    cumSlope += slope * Math.pow(damp, h);
    const month = addMonths(lastMonth, h);
    const value = Math.max(0, (level + cumSlope) * (indices[monthOfYear(month)] || 1));
    forecast.push({ month, value, lower: value, upper: value });
  }

  const method =
    source === "none"
      ? "exponential smoothing (under 12 months)"
      : `seasonal decomposition (${source}) + damped trend`;

  return { forecast, fitted, method };
}

// ---------------------------------------------------------------------------

function fitAndForecast(
  series: SeriesPoint[],
  horizon: number
): { forecast: ForecastPoint[]; fitted: number[]; method: string } {
  const n = series.length;

  if (n < 4) {
    const avg = series.reduce((s, p) => s + p.value, 0) / Math.max(n, 1);
    const lastMonth = series[n - 1]?.month ?? "2000-01";
    return {
      forecast: Array.from({ length: horizon }, (_, h) => ({
        month: addMonths(lastMonth, h + 1),
        value: avg,
        lower: 0,
        upper: avg * 2,
      })),
      fitted: series.map(() => avg),
      method: "flat average (not enough history)",
    };
  }

  // Short history: SES avoids the negative-slope trap from zero-padded gaps.
  if (n < 12) {
    return fitSES(series, horizon);
  }

  return fitDecomposition(series, horizon);
}

function applyIntervals(
  series: SeriesPoint[],
  fitted: number[],
  forecast: ForecastPoint[]
): void {
  const residuals = series
    .map((p, i) => p.value - fitted[i])
    .filter((r) => Number.isFinite(r));
  const sd =
    residuals.length > 1
      ? Math.sqrt(residuals.reduce((s, r) => s + r * r, 0) / (residuals.length - 1))
      : 0;
  forecast.forEach((f, h) => {
    const spread = 1.28 * sd * Math.sqrt(1 + h * 0.35);
    f.lower = Math.max(0, f.value - spread);
    f.upper = f.value + spread;
  });
}

function backtest(series: SeriesPoint[]): BacktestResult {
  const n = series.length;
  const k = Math.min(6, n - 4); // hold out up to 6 months, keep >= 4 to train
  if (k < 2) return { monthsTested: 0, mape: null, seasonalNaiveMape: null };

  const modelErrors: number[] = [];
  const naiveErrors: number[] = [];

  for (let i = n - k; i < n; i++) {
    const train = series.slice(0, i);
    const actual = series[i].value;
    if (actual <= 0) continue;

    const { forecast } = fitAndForecast(train, 1);
    modelErrors.push(Math.abs(forecast[0].value - actual) / actual);

    const lastYear = series.find((p) => p.month === addMonths(series[i].month, -12));
    if (lastYear && lastYear.value > 0) {
      naiveErrors.push(Math.abs(lastYear.value - actual) / actual);
    }
  }

  const mean = (xs: number[]) =>
    xs.length > 0 ? xs.reduce((s, v) => s + v, 0) / xs.length : null;

  return {
    monthsTested: modelErrors.length,
    mape: mean(modelErrors),
    seasonalNaiveMape: mean(naiveErrors),
  };
}

export function forecastSeries(series: SeriesPoint[], horizon = 6): ForecastResult {
  const notes: string[] = [];

  // Scale the current partial month up to a projected full-month value instead
  // of dropping it. This keeps recent momentum in the model — e.g. if this
  // month is tracking above average, the forecast should reflect that.
  const nowYm = new Date().toISOString().slice(0, 7);
  const today = new Date().getDate();
  const training = series.map((p) => {
    if (p.month !== nowYm) return p;
    const days = daysInMonth(p.month);
    if (today < days && today > 0) {
      const scaled = Math.round((p.value / today) * days * 100) / 100;
      notes.push(
        `${nowYm} is incomplete (day ${today} of ${days}). The current month is scaled to an estimated full-month value of ${scaled.toFixed(0)} for model training, shown as a dotted bar on the chart.`
      );
      return { month: p.month, value: scaled, estimated: true };
    }
    return p;
  });

  if (training.length === 0) {
    return {
      history: series,
      forecast: [],
      method: "no data",
      backtest: { monthsTested: 0, mape: null, seasonalNaiveMape: null },
      notes: ["Not enough history to forecast."],
    };
  }

  const { forecast, fitted, method } = fitAndForecast(training, horizon);
  applyIntervals(training, fitted, forecast);

  if (training.length < 12) {
    notes.push(
      "Under 12 months of history — seasonal patterns can't be learned yet. Forecasts reflect recent average momentum."
    );
  } else if (training.length < 24) {
    notes.push(
      "Seasonality estimated from one year of history; accuracy improves with a second year."
    );
  }

  return { history: series, forecast, method, backtest: backtest(training), notes };
}

// ---------------------------------------------------------------------------
// Shop-level entry points
// ---------------------------------------------------------------------------

export function revenueForecast(rows: SaleRow[], horizon = 6): ForecastResult {
  const series = monthlySeries(rows).map((p) => ({
    month: p.month,
    value: p.netRevenue,
  }));
  return forecastSeries(series, horizon);
}

export function ordersForecast(rows: SaleRow[], horizon = 6): ForecastResult {
  const series = monthlySeries(rows).map((p) => ({
    month: p.month,
    value: p.orders,
  }));
  const result = forecastSeries(series, horizon);
  result.forecast.forEach((f) => {
    f.value = Math.round(f.value);
    f.lower = Math.floor(f.lower);
    f.upper = Math.ceil(f.upper);
  });
  return result;
}

export interface ItemForecast {
  name: string;
  nextMonthUnits: number;
  shareOfUnits: number;
  basis: "last-year + recent mix" | "recent mix";
}

export function itemForecasts(rows: SaleRow[], limit = 15): ItemForecast[] {
  const unitSeries = monthlySeries(rows).map((p) => ({
    month: p.month,
    value: p.units,
  }));
  const total = forecastSeries(unitSeries, 1);
  if (total.forecast.length === 0) return [];
  const nextMonth = total.forecast[0];
  const totalUnits = Math.max(0, nextMonth.value);

  const stats = productStats(rows);
  const active = stats.filter((p) => p.trend !== "dormant");
  if (active.length === 0) return [];

  const recentTotal = active.reduce((s, p) => s + p.recentUnits, 0);

  const targetMonth = addMonths(nextMonth.month, -12);
  const lyUnits = new Map<string, number>();
  let lyTotal = 0;
  for (const r of rows) {
    if (!r.saleDate || r.saleDate.slice(0, 7) !== targetMonth) continue;
    const name = r.cardName ?? r.itemName;
    if (!name) continue;
    lyUnits.set(name, (lyUnits.get(name) ?? 0) + (r.quantity || 0));
    lyTotal += r.quantity || 0;
  }

  const hasLy = lyTotal > 0;
  const results: ItemForecast[] = active.map((p) => {
    const recentShare = recentTotal > 0 ? p.recentUnits / recentTotal : 0;
    const lyShare = hasLy ? (lyUnits.get(p.name) ?? 0) / lyTotal : 0;
    const share = hasLy ? 0.5 * lyShare + 0.5 * recentShare : recentShare;
    return {
      name: p.name,
      shareOfUnits: share,
      nextMonthUnits: Math.round(totalUnits * share * 10) / 10,
      basis: hasLy ? "last-year + recent mix" : "recent mix",
    };
  });

  return results
    .filter((r) => r.shareOfUnits > 0)
    .sort((a, b) => b.nextMonthUnits - a.nextMonthUnits)
    .slice(0, limit);
}
