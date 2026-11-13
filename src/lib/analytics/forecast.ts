import { addMonths, dayOfMonth, daysInMonth, monthKey } from "@/lib/dates";
import { monthlySeries, productStats, sold, type Order } from "./core";

/**
 * Monthly forecasting that is honest about what a small shop's history can
 * support.
 *
 *  - Several simple models are tried in a walk-forward backtest (each month
 *    is predicted using only the months before it). The one with the lowest
 *    error is used; if "same as last month" wins, that is the forecast.
 *  - Seasonal models are only considered with two full years of history. One
 *    year can't tell a seasonal peak from a shop that was simply growing.
 *  - Ranges come from how far off the backtest actually was, widening with
 *    the horizon. They are never zero-width.
 *  - The month in progress is estimated as "actual so far + expected rest of
 *    month", using the data-through date rather than today's date.
 *  - Orders are forecast; revenue is derived from them with the recent
 *    average order value, so the two can't disagree.
 */

export interface SeriesPoint {
  month: string; // YYYY-MM
  value: number;
}

export interface ForecastPoint extends SeriesPoint {
  lower: number;
  upper: number;
  /** present on the month in progress */
  partial?: { actual: number; day: number; days: number };
}

export type ModelId = "recent" | "last" | "lastYear" | "seasonal";

export const MODEL_LABELS: Record<ModelId, string> = {
  recent: "Recent average, weighted toward the latest months",
  last: "Same as last month",
  lastYear: "Same month last year",
  seasonal: "Recent level adjusted for the time of year",
};

export interface Accuracy {
  monthsTested: number;
  /** total absolute error ÷ total actual over the tested months (0.3 = off by 30%) */
  wape: number | null;
  /** the same measure for a plain "same as last month" guess */
  baselineWape: number | null;
  /** average absolute miss per month, in the series' own units */
  avgError: number | null;
}

export interface ForecastResult {
  /** complete months only */
  history: SeriesPoint[];
  /** estimate for the month in progress, when the data ends mid-month */
  current: ForecastPoint | null;
  /** months after the current one */
  forecast: ForecastPoint[];
  model: ModelId;
  accuracy: Accuracy;
  notes: string[];
}

export interface ForecastOptions {
  /** number of future months to project (after the month in progress) */
  horizon?: number;
  /** last date the data covers; decides whether the final month is partial */
  through?: string | null;
}

/** Complete months needed before a seasonal pattern is considered. */
export const SEASONAL_MIN_MONTHS = 24;

const MIN_TRAIN = 3;
const MAX_TEST_MONTHS = 12;

// ---------------------------------------------------------------------------
// Models
// ---------------------------------------------------------------------------

function monthOfYear(ym: string): number {
  return Number(ym.split("-")[1]); // 1..12
}

function smoothedLevel(values: number[], alpha = 0.4): number {
  if (values.length === 0) return 0;
  let level = values[0];
  for (let i = 1; i < values.length; i++) level = alpha * values[i] + (1 - alpha) * level;
  return level;
}

/** Seasonal indices (1-indexed by calendar month, mean 1) from a centered 12-month average. */
function seasonalIndices(series: SeriesPoint[]): number[] {
  const n = series.length;
  const ratios: number[][] = Array.from({ length: 13 }, () => []);
  for (let i = 6; i < n - 6; i++) {
    let sum = 0;
    for (let j = i - 6; j < i + 6; j++) sum += series[j].value;
    const ma = (sum + (series[i + 6].value - series[i - 6].value) / 2) / 12;
    if (ma > 0) ratios[monthOfYear(series[i].month)].push(series[i].value / ma);
  }
  const indices = new Array<number>(13).fill(1);
  const observed: number[] = [];
  for (let m = 1; m <= 12; m++) {
    const r = ratios[m].sort((a, b) => a - b);
    if (r.length > 0) {
      indices[m] = r[Math.floor(r.length / 2)];
      observed.push(indices[m]);
    }
  }
  const mean = observed.length > 0 ? observed.reduce((s, v) => s + v, 0) / observed.length : 1;
  if (mean > 0) for (let m = 1; m <= 12; m++) indices[m] /= mean;
  return indices;
}

function eligible(model: ModelId, trainLength: number): boolean {
  if (model === "lastYear") return trainLength >= 12;
  if (model === "seasonal") return trainLength >= SEASONAL_MIN_MONTHS;
  return trainLength >= 1;
}

/** Predict the month `h` steps after the end of `train`. */
function predict(model: ModelId, train: SeriesPoint[], h: number): number {
  const n = train.length;
  if (n === 0) return 0;
  const values = train.map((p) => p.value);
  switch (model) {
    case "last":
      return values[n - 1];
    case "lastYear": {
      const idx = n + h - 1 - 12;
      return idx >= 0 && idx < n ? values[idx] : smoothedLevel(values);
    }
    case "seasonal": {
      const indices = seasonalIndices(train);
      const level = smoothedLevel(train.map((p) => p.value / (indices[monthOfYear(p.month)] || 1)));
      const target = addMonths(train[n - 1].month, h);
      return Math.max(0, level * (indices[monthOfYear(target)] || 1));
    }
    default:
      return Math.max(0, smoothedLevel(values));
  }
}

// ---------------------------------------------------------------------------
// Backtest and model choice
// ---------------------------------------------------------------------------

interface BacktestPoint {
  actual: number;
  predicted: Partial<Record<ModelId, number>>;
}

function walkForward(series: SeriesPoint[]): BacktestPoint[] {
  const n = series.length;
  const start = Math.max(MIN_TRAIN, n - MAX_TEST_MONTHS);
  const points: BacktestPoint[] = [];
  for (let i = start; i < n; i++) {
    const train = series.slice(0, i);
    const predicted: Partial<Record<ModelId, number>> = {};
    for (const model of ["recent", "last", "lastYear", "seasonal"] as ModelId[]) {
      if (eligible(model, train.length)) predicted[model] = predict(model, train, 1);
    }
    points.push({ actual: series[i].value, predicted });
  }
  return points;
}

function wapeOf(points: BacktestPoint[], model: ModelId): number | null {
  let err = 0;
  let total = 0;
  for (const p of points) {
    const pred = p.predicted[model];
    if (pred == null) return null;
    err += Math.abs(pred - p.actual);
    total += p.actual;
  }
  return total > 0 ? err / total : null;
}

/**
 * Start from the recent-average model and switch only when another model was
 * clearly better over the months where both could be tested.
 */
function chooseModel(points: BacktestPoint[]): ModelId {
  let best: ModelId = "recent";
  let bestScore = 0.95; // a challenger must beat "recent" by at least 5%
  for (const model of ["last", "lastYear", "seasonal"] as ModelId[]) {
    const window = points.filter((p) => p.predicted[model] != null);
    if (window.length < 3) continue;
    const challenger = wapeOf(window, model);
    const incumbent = wapeOf(window, "recent");
    if (challenger == null || incumbent == null || incumbent === 0) continue;
    const score = challenger / incumbent;
    if (score < bestScore) {
      best = model;
      bestScore = score;
    }
  }
  return best;
}

function accuracyOf(points: BacktestPoint[], model: ModelId): Accuracy {
  const window = points.filter((p) => p.predicted[model] != null);
  if (window.length === 0) {
    return { monthsTested: 0, wape: null, baselineWape: null, avgError: null };
  }
  const absErr = window.reduce((s, p) => s + Math.abs((p.predicted[model] as number) - p.actual), 0);
  return {
    monthsTested: window.length,
    wape: wapeOf(window, model),
    baselineWape: wapeOf(window, "last"),
    avgError: absErr / window.length,
  };
}

function quantile(sorted: number[], q: number): number {
  const pos = (sorted.length - 1) * q;
  const lo = Math.floor(pos);
  const hi = Math.ceil(pos);
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo);
}

/**
 * How far actual results landed from the prediction in the backtest, as
 * low/high multipliers covering roughly 80% of months.
 */
function errorBand(points: BacktestPoint[], model: ModelId): { low: number; high: number } {
  const ratios = points
    .filter((p) => (p.predicted[model] ?? 0) > 0)
    .map((p) => p.actual / (p.predicted[model] as number))
    .sort((a, b) => a - b);
  let low = 0.5;
  let high = 1.6;
  if (ratios.length >= 4) {
    low = quantile(ratios, 0.1);
    high = quantile(ratios, 0.9);
  }
  // Never claim more certainty than ±15%, and keep the band around the estimate.
  return { low: Math.min(0.85, Math.max(0, low)), high: Math.max(1.15, high) };
}

function withBand(
  month: string,
  value: number,
  band: { low: number; high: number },
  h: number
): ForecastPoint {
  const widen = Math.sqrt(h);
  return {
    month,
    value,
    lower: Math.max(0, value * (1 - (1 - band.low) * widen)),
    upper: value * (1 + (band.high - 1) * widen),
  };
}

// ---------------------------------------------------------------------------
// Series forecast
// ---------------------------------------------------------------------------

function splitPartial(
  series: SeriesPoint[],
  through?: string | null
): { complete: SeriesPoint[]; partial: { point: SeriesPoint; day: number; days: number } | null } {
  const last = series[series.length - 1];
  if (!last || !through || monthKey(through) !== last.month) return { complete: series, partial: null };
  const day = dayOfMonth(through);
  const days = daysInMonth(last.month);
  if (day >= days) return { complete: series, partial: null };
  return { complete: series.slice(0, -1), partial: { point: last, day, days } };
}

export function forecastSeries(series: SeriesPoint[], options: ForecastOptions = {}): ForecastResult {
  const horizon = options.horizon ?? 3;
  const { complete, partial } = splitPartial(series, options.through);
  const notes: string[] = [];

  if (complete.length === 0 && !partial) {
    return {
      history: [],
      current: null,
      forecast: [],
      model: "recent",
      accuracy: { monthsTested: 0, wape: null, baselineWape: null, avgError: null },
      notes: ["Not enough history to forecast."],
    };
  }

  const points = walkForward(complete);
  const model = chooseModel(points);
  const band = errorBand(points, model);

  let current: ForecastPoint | null = null;
  let train = complete;
  if (partial) {
    const { point, day, days } = partial;
    const remaining = (days - day) / days;
    // With no complete month yet, the only signal is this month's pace.
    const fullMonth = complete.length > 0 ? predict(model, complete, 1) : (point.value / day) * days;
    const rest = withBand(point.month, fullMonth * remaining, band, 1);
    current = {
      month: point.month,
      value: point.value + rest.value,
      lower: point.value + rest.lower,
      upper: point.value + rest.upper,
      partial: { actual: point.value, day, days },
    };
    // Once most of the month is in, let it inform the months that follow.
    if (day / days >= 0.5 || complete.length === 0) {
      train = [...complete, { month: point.month, value: current.value }];
    }
  }

  const lastMonth = (partial?.point ?? complete[complete.length - 1]).month;
  const offset = train.length > 0 ? monthsApart(train[train.length - 1].month, lastMonth) : 0;
  const forecast: ForecastPoint[] = [];
  for (let h = 1; h <= horizon; h++) {
    const month = addMonths(lastMonth, h);
    const value = predict(model, train, h + offset);
    forecast.push(withBand(month, value, band, h + (partial ? 1 : 0)));
  }

  if (complete.length < MIN_TRAIN + 1) {
    notes.push(
      `Only ${complete.length} complete month${complete.length === 1 ? "" : "s"} of history, so treat this as a rough guide.`
    );
  } else if (complete.length < SEASONAL_MIN_MONTHS) {
    notes.push(
      `Seasonal patterns need two full years of history. With ${complete.length} months, this follows your recent level and won't anticipate a holiday rush or a slow season.`
    );
  }

  return { history: complete, current, forecast, model, accuracy: accuracyOf(points, model), notes };
}

function monthsApart(from: string, to: string): number {
  const [fy, fm] = from.split("-").map(Number);
  const [ty, tm] = to.split("-").map(Number);
  return (ty - fy) * 12 + (tm - fm);
}

// ---------------------------------------------------------------------------
// Shop-level forecast: orders, with revenue derived from them
// ---------------------------------------------------------------------------

export interface ShopForecast {
  orders: ForecastResult;
  revenue: ForecastResult;
  /** recent average order value used to turn orders into revenue */
  avgOrderValue: number | null;
}

/** Revenue ÷ orders over the last six months of `months` that had orders. */
function trailingAov(months: { orders: number; revenue: number }[]): number | null {
  const recent = months.slice(-6);
  const orders = recent.reduce((s, m) => s + m.orders, 0);
  if (orders > 0) return recent.reduce((s, m) => s + m.revenue, 0) / orders;
  const all = months.reduce((s, m) => s + m.orders, 0);
  return all > 0 ? months.reduce((s, m) => s + m.revenue, 0) / all : null;
}

export function shopForecast(orders: Order[], options: ForecastOptions = {}): ShopForecast {
  const months = monthlySeries(orders, options.through);
  const orderSeries = months.map((m) => ({ month: m.month, value: m.orders }));
  const result = forecastSeries(orderSeries, options);

  const completeMonths = months.slice(0, result.history.length);
  const aov = trailingAov(completeMonths.length > 0 ? completeMonths : months);
  const scale = aov ?? 0;

  // Revenue accuracy: replay the same forecast (orders × the AOV known then).
  const start = Math.max(MIN_TRAIN, completeMonths.length - MAX_TEST_MONTHS);
  let err = 0;
  let baseErr = 0;
  let total = 0;
  let tested = 0;
  for (let i = start; i < completeMonths.length; i++) {
    const train = orderSeries.slice(0, i);
    if (!eligible(result.model, train.length)) continue;
    const pastAov = trailingAov(completeMonths.slice(0, i)) ?? 0;
    const predicted = predict(result.model, train, 1) * pastAov;
    err += Math.abs(predicted - completeMonths[i].revenue);
    baseErr += Math.abs(completeMonths[i - 1].revenue - completeMonths[i].revenue);
    total += completeMonths[i].revenue;
    tested += 1;
  }

  const toRevenue = (p: ForecastPoint): ForecastPoint => ({
    month: p.month,
    value: p.value * scale,
    lower: p.lower * scale,
    upper: p.upper * scale,
  });

  let current: ForecastPoint | null = null;
  if (result.current?.partial) {
    const actual = months[months.length - 1].revenue;
    const restOrders = result.current.value - result.current.partial.actual;
    const restLower = result.current.lower - result.current.partial.actual;
    const restUpper = result.current.upper - result.current.partial.actual;
    current = {
      month: result.current.month,
      value: actual + restOrders * scale,
      lower: actual + restLower * scale,
      upper: actual + restUpper * scale,
      partial: { ...result.current.partial, actual },
    };
  }

  const revenue: ForecastResult = {
    history: completeMonths.map((m) => ({ month: m.month, value: m.revenue })),
    current,
    forecast: result.forecast.map(toRevenue),
    model: result.model,
    accuracy: {
      monthsTested: tested,
      wape: tested > 0 && total > 0 ? err / total : null,
      baselineWape: tested > 0 && total > 0 ? baseErr / total : null,
      avgError: tested > 0 ? err / tested : null,
    },
    notes: result.notes,
  };

  // Whole orders only.
  const roundPoint = (p: ForecastPoint): ForecastPoint => ({
    ...p,
    value: Math.round(p.value),
    lower: Math.floor(p.lower),
    upper: Math.ceil(p.upper),
  });
  const ordersResult: ForecastResult = {
    ...result,
    current: result.current ? roundPoint(result.current) : null,
    forecast: result.forecast.map(roundPoint),
  };

  return { orders: ordersResult, revenue, avgOrderValue: aov };
}

// ---------------------------------------------------------------------------
// Next month by product
// ---------------------------------------------------------------------------

export interface ItemForecast {
  /** first full month after the data ends */
  month: string | null;
  totalUnits: number;
  items: { key: string; name: string; units: number; share: number }[];
  /** everything outside the listed items, rolled up */
  other: { products: number; units: number };
}

/**
 * Next month's expected units split by each product's share of the last 90
 * days. Per-product models would just be fitting noise at this volume.
 */
export function itemForecasts(orders: Order[], options: ForecastOptions & { limit?: number } = {}): ItemForecast {
  const limit = options.limit ?? 5;
  const live = sold(orders);
  const forecast = shopForecast(live, { ...options, horizon: 1 });
  const next = forecast.orders.forecast[0];
  const empty: ItemForecast = { month: next?.month ?? null, totalUnits: 0, items: [], other: { products: 0, units: 0 } };
  if (!next) return empty;

  const months = monthlySeries(live, options.through).slice(-6);
  const orderCount = months.reduce((s, m) => s + m.orders, 0);
  const unitsPerOrder = orderCount > 0 ? months.reduce((s, m) => s + m.units, 0) / orderCount : 1;
  const totalUnits = next.value * unitsPerOrder;

  const active = productStats(live, options.through).filter((p) => p.recentUnits > 0);
  const recentTotal = active.reduce((s, p) => s + p.recentUnits, 0);
  if (recentTotal === 0) return empty;

  const ranked = active
    .map((p) => ({
      key: p.key,
      name: p.name,
      share: p.recentUnits / recentTotal,
      units: (totalUnits * p.recentUnits) / recentTotal,
    }))
    .sort((a, b) => b.units - a.units);
  const rest = ranked.slice(limit);

  return {
    month: next.month,
    totalUnits,
    items: ranked.slice(0, limit),
    other: { products: rest.length, units: rest.reduce((s, r) => s + r.units, 0) },
  };
}
