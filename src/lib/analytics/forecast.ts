import type { SaleRow } from "@/lib/types";
import { monthlySeries, productStats } from "./core";

/**
 * Monthly forecasting via classical multiplicative decomposition:
 *
 *   value(t) = level(t) x seasonalIndex(monthOfYear) + noise
 *
 * - Seasonal indices come from ratio-to-moving-average (needs 24+ months) or
 *   ratio-to-median (12-23 months). Under 12 months no seasonality is used.
 * - The level/trend is a Theil-Sen robust line over the deseasonalized series,
 *   with the slope damped as the horizon grows (long-range extrapolation of a
 *   small shop's trend is mostly noise).
 * - Accuracy is reported honestly: a walk-forward backtest over the last
 *   months of history, compared against a seasonal-naive benchmark.
 *
 * This consistently behaves better than daily-grain Holt-Winters on sparse
 * Etsy sales (the legacy approach), which over-fits weekly seasonality and
 * explodes on zero-heavy days.
 */

export interface SeriesPoint {
  month: string; // YYYY-MM
  value: number;
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

/** Seasonal indices (length 13, 1-indexed by calendar month), normalized to mean 1. */
function seasonalIndices(series: SeriesPoint[]): { indices: number[]; source: string } {
  const n = series.length;
  const flat = { indices: new Array(13).fill(1), source: "none" };
  if (n < 12) return flat;

  const ratiosByMonth: number[][] = Array.from({ length: 13 }, () => []);

  if (n >= 24) {
    // ratio to 12-month centered moving average
    for (let i = 6; i < n - 6; i++) {
      let sum = 0;
      for (let j = i - 6; j < i + 6; j++) sum += series[j].value;
      const ma = (sum + (series[i + 6].value - series[i - 6].value) / 2) / 12;
      if (ma > 0) ratiosByMonth[monthOfYear(series[i].month)].push(series[i].value / ma);
    }
  } else {
    // ratio to overall median (shorter histories)
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
      indices[m] = r[Math.floor(r.length / 2)]; // median ratio
      observed.push(indices[m]);
    }
  }
  // normalize observed indices to mean 1
  const mean = observed.reduce((s, v) => s + v, 0) / observed.length;
  if (mean > 0) for (let m = 1; m <= 12; m++) indices[m] /= mean;

  return { indices, source: n >= 24 ? "ratio-to-moving-average" : "ratio-to-median" };
}

function fitAndForecast(
  series: SeriesPoint[],
  horizon: number
): { forecast: ForecastPoint[]; fitted: number[]; method: string } {
  const n = series.length;
  const lastMonth = series[n - 1].month;

  if (n < 4) {
    // too little data: flat average
    const avg = series.reduce((s, p) => s + p.value, 0) / Math.max(n, 1);
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

  const { indices, source } = seasonalIndices(series);
  const deseason = series.map((p) => p.value / (indices[monthOfYear(p.month)] || 1));

  // Robust trend on the recent window (older regime changes shouldn't dominate)
  const window = Math.min(deseason.length, 24);
  const recent = deseason.slice(-window);
  const { slope, intercept } = theilSen(recent);
  const level = intercept + slope * (recent.length - 1);

  const fitted = series.map((p, i) => {
    const offset = i - (n - window);
    const lv = offset >= 0 ? intercept + slope * offset : deseason[i];
    return Math.max(0, lv * (indices[monthOfYear(p.month)] || 1));
  });

  const damp = 0.92; // each extra month forward trusts the trend less
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
      ? "damped trend (under 12 months of history; no seasonality)"
      : `seasonal decomposition (${source}) + damped robust trend`;

  return { forecast, fitted, method };
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
      ? Math.sqrt(
          residuals.reduce((s, r) => s + r * r, 0) / (residuals.length - 1)
        )
      : 0;
  forecast.forEach((f, h) => {
    const spread = 1.28 * sd * Math.sqrt(1 + h * 0.35); // ~80% interval, widening
    f.lower = Math.max(0, f.value - spread);
    f.upper = f.value + spread;
  });
}

function backtest(series: SeriesPoint[]): BacktestResult {
  const n = series.length;
  const k = Math.min(6, n - 8); // hold out up to 6 months, keep >= 8 to train
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

  // drop the current partial month from training if it's clearly incomplete
  const trimmed = [...series];
  const nowYm = new Date().toISOString().slice(0, 7);
  if (trimmed.length > 1 && trimmed[trimmed.length - 1].month === nowYm) {
    notes.push("The current (incomplete) month is excluded from model training.");
    trimmed.pop();
  }

  if (trimmed.length === 0) {
    return {
      history: series,
      forecast: [],
      method: "no data",
      backtest: { monthsTested: 0, mape: null, seasonalNaiveMape: null },
      notes: ["Not enough history to forecast."],
    };
  }

  const { forecast, fitted, method } = fitAndForecast(trimmed, horizon);
  applyIntervals(trimmed, fitted, forecast);

  if (trimmed.length < 12) {
    notes.push(
      "Less than 12 months of history — seasonal patterns (e.g. holiday spikes) can't be learned yet."
    );
  } else if (trimmed.length < 24) {
    notes.push(
      "Seasonality is estimated from a single year of history; expect it to improve with a second year of data."
    );
  }

  return { history: series, forecast, method, backtest: backtest(trimmed), notes };
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

/**
 * Per-product next-month estimates. Total units are forecast with the shop
 * model, then allocated by a blend of (a) each product's unit share in the
 * same calendar month last year and (b) its share over the trailing 90 days.
 * This avoids the legacy failure mode of fitting an independent noisy model
 * per product.
 */
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

  // share over trailing 90 days
  const recentTotal = active.reduce((s, p) => s + p.recentUnits, 0);

  // share in the same calendar month last year
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
