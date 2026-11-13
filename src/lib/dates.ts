/**
 * Calendar-date helpers over ISO strings (YYYY-MM-DD / YYYY-MM). All math is
 * done in UTC on the date parts, so results never shift with the viewer's
 * timezone or daylight saving changes.
 */

const DAY_MS = 86400000;

function toUtc(iso: string): number {
  const [y, m, d] = iso.slice(0, 10).split("-").map(Number);
  return Date.UTC(y, m - 1, d);
}

function fromUtc(ms: number): string {
  return new Date(ms).toISOString().slice(0, 10);
}

/** Today's date in the viewer's local timezone. */
export function todayIso(now: Date = new Date()): string {
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, "0");
  const d = String(now.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

export function addDays(iso: string, days: number): string {
  return fromUtc(toUtc(iso) + days * DAY_MS);
}

/** Whole days from `a` to `b` (positive when b is later). */
export function daysBetween(a: string, b: string): number {
  return Math.round((toUtc(b) - toUtc(a)) / DAY_MS);
}

export function monthKey(iso: string): string {
  return iso.slice(0, 7);
}

export function addMonths(ym: string, k: number): string {
  const [y, m] = ym.split("-").map(Number);
  const total = y * 12 + (m - 1) + k;
  const ny = Math.floor(total / 12);
  const nm = (total % 12) + 1;
  return `${ny}-${String(nm).padStart(2, "0")}`;
}

export function daysInMonth(ym: string): number {
  const [y, m] = ym.split("-").map(Number);
  return new Date(Date.UTC(y, m, 0)).getUTCDate();
}

export function dayOfMonth(iso: string): number {
  return Number(iso.slice(8, 10));
}

/** 0 = Sunday … 6 = Saturday. */
export function dayOfWeek(iso: string): number {
  return new Date(toUtc(iso)).getUTCDay();
}

export function minIso(a: string, b: string): string {
  return a < b ? a : b;
}
