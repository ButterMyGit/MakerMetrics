"use client";

import { addDays } from "@/lib/dates";

/**
 * The date range shared by every analytics page. Kept in a tiny external
 * store so it survives navigation, is remembered between visits
 * (localStorage) and can be shared as a link (?range=90d or ?from=…&to=…).
 */

export type RangePreset = "30d" | "90d" | "12m" | "ytd" | "lastyear" | "all" | "custom";

export interface RangeChoice {
  preset: RangePreset;
  /** only for preset "custom" (YYYY-MM-DD, inclusive) */
  from?: string;
  to?: string;
}

export const DEFAULT_RANGE: RangeChoice = { preset: "30d" };

export const PRESET_LABELS: Record<Exclude<RangePreset, "custom">, string> = {
  "30d": "Last 30 days",
  "90d": "Last 90 days",
  "12m": "Last 12 months",
  ytd: "This year",
  lastyear: "Last year",
  all: "All time",
};

const STORAGE_KEY = "makermetrics-range";
const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/;
const listeners = new Set<() => void>();
let current: RangeChoice | null = null;

function isPreset(v: unknown): v is Exclude<RangePreset, "custom"> {
  return typeof v === "string" && v in PRESET_LABELS;
}

function parse(params: URLSearchParams): RangeChoice | null {
  const from = params.get("from");
  const to = params.get("to");
  if (from && to && ISO_DATE.test(from) && ISO_DATE.test(to) && from <= to) {
    return { preset: "custom", from, to };
  }
  const preset = params.get("range");
  return isPreset(preset) ? { preset } : null;
}

function read(): RangeChoice {
  try {
    const fromUrl = parse(new URLSearchParams(window.location.search));
    if (fromUrl) return fromUrl;
    const saved = localStorage.getItem(STORAGE_KEY);
    if (saved) {
      const fromStorage = parse(new URLSearchParams(saved));
      if (fromStorage) return fromStorage;
    }
  } catch {
    // Storage can be unavailable; fall through to the default.
  }
  return DEFAULT_RANGE;
}

function toQuery(choice: RangeChoice): string {
  if (choice.preset === "custom" && choice.from && choice.to) {
    return `from=${choice.from}&to=${choice.to}`;
  }
  return choice.preset === DEFAULT_RANGE.preset ? "" : `range=${choice.preset}`;
}

export function getRangeChoice(): RangeChoice {
  if (typeof window === "undefined") return DEFAULT_RANGE;
  if (!current) current = read();
  return current;
}

export function getServerRangeChoice(): RangeChoice {
  return DEFAULT_RANGE;
}

/** Keep the address bar in step with the active range (no navigation). */
export function syncRangeToUrl(): void {
  if (typeof window === "undefined") return;
  const params = new URLSearchParams(window.location.search);
  params.delete("range");
  params.delete("from");
  params.delete("to");
  const rest = params.toString();
  const query = [toQuery(getRangeChoice()), rest].filter(Boolean).join("&");
  const next = `${window.location.pathname}${query ? `?${query}` : ""}${window.location.hash}`;
  if (next !== `${window.location.pathname}${window.location.search}${window.location.hash}`) {
    window.history.replaceState(window.history.state, "", next);
  }
}

export function setRangeChoice(choice: RangeChoice): void {
  current = choice;
  try {
    localStorage.setItem(STORAGE_KEY, toQuery(choice) || `range=${choice.preset}`);
  } catch {
    // Not remembered between visits; still applied for this one.
  }
  syncRangeToUrl();
  listeners.forEach((l) => l());
}

export function subscribeRange(listener: () => void): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

/**
 * Turn a choice into concrete dates. Relative presets end at `anchor` (the
 * last date the data covers, or today), so "last 30 days" is never mostly
 * empty just because the latest export is a few days old.
 */
export function resolveRange(
  choice: RangeChoice,
  anchor: string
): { start: string | null; end: string | null } {
  const year = Number(anchor.slice(0, 4));
  switch (choice.preset) {
    case "30d":
      return { start: addDays(anchor, -29), end: anchor };
    case "90d":
      return { start: addDays(anchor, -89), end: anchor };
    case "12m":
      return { start: addDays(anchor, -364), end: anchor };
    case "ytd":
      return { start: `${year}-01-01`, end: anchor };
    case "lastyear":
      return { start: `${year - 1}-01-01`, end: `${year - 1}-12-31` };
    case "custom":
      return { start: choice.from ?? null, end: choice.to ?? null };
    default:
      return { start: null, end: null };
  }
}

export function rangeLabel(choice: RangeChoice): string {
  return choice.preset === "custom" ? "Custom range" : PRESET_LABELS[choice.preset];
}
