let currencyCode = "USD";
let money = makeMoney(currencyCode, 2);
let moneyWhole = makeMoney(currencyCode, 0);

function makeMoney(code: string, digits: number): Intl.NumberFormat {
  try {
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: code,
      minimumFractionDigits: digits,
      maximumFractionDigits: digits,
    });
  } catch {
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      minimumFractionDigits: digits,
      maximumFractionDigits: digits,
    });
  }
}

/** Use the shop's currency (from the imported rows) for all money formatting. */
export function setCurrency(code: string | null | undefined): void {
  const next = (code ?? "USD").toUpperCase();
  if (next === currencyCode) return;
  currencyCode = next;
  money = makeMoney(next, 2);
  moneyWhole = makeMoney(next, 0);
}

export function getCurrency(): string {
  return currencyCode;
}

const num = new Intl.NumberFormat("en-US");

export function formatMoney(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return "—";
  return money.format(v);
}

/** Whole units once the amount is large enough that cents are noise. */
export function formatMoneyCompact(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return "—";
  return Math.abs(v) >= 10000 ? moneyWhole.format(v) : money.format(v);
}

export function formatMoneyWhole(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return "—";
  return moneyWhole.format(v);
}

/** Round to two significant figures: 1906.35 -> 1900, 250.46 -> 250, 43.2 -> 43. */
export function roundApprox(v: number): number {
  if (!Number.isFinite(v) || v === 0) return 0;
  const magnitude = Math.pow(10, Math.max(0, Math.floor(Math.log10(Math.abs(v))) - 1));
  return Math.round(v / magnitude) * magnitude;
}

/** Money for estimates and forecasts: rounded, no cents, so it doesn't imply precision. */
export function formatMoneyApprox(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return "—";
  return moneyWhole.format(roundApprox(v));
}

export function formatNumber(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return "—";
  return num.format(v);
}

export function formatPercent(v: number | null | undefined, digits = 1): string {
  if (v == null || !Number.isFinite(v)) return "—";
  return `${(v * 100).toFixed(digits)}%`;
}

export function formatDelta(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return "—";
  const sign = v >= 0 ? "+" : "";
  return `${sign}${(v * 100).toFixed(1)}%`;
}

/** "2026-03" -> "Mar 2026" */
export function formatMonth(ym: string): string {
  const [y, m] = ym.split("-").map(Number);
  return new Date(y, m - 1, 1).toLocaleDateString("en-US", {
    month: "short",
    year: "numeric",
  });
}

/** "2026-03-14" -> "Mar 14, 2026" */
export function formatDate(iso: string | null | undefined): string {
  if (!iso) return "—";
  const [y, m, d] = iso.slice(0, 10).split("-").map(Number);
  if (!y || !m || !d) return "—";
  return new Date(y, m - 1, d).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

/** "2026-03-14" -> "Mar 14" */
export function formatDateShort(iso: string | null | undefined): string {
  if (!iso) return "—";
  const [y, m, d] = iso.slice(0, 10).split("-").map(Number);
  if (!y || !m || !d) return "—";
  return new Date(y, m - 1, d).toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

// ---------------------------------------------------------------------------
// Display cleanup for buyer-entered text
// ---------------------------------------------------------------------------

/**
 * Buyers type names and cities in all caps or all lowercase. Only those are
 * re-cased; mixed-case input ("McDonald", "DeKalb") is left as typed.
 */
export function titleCase(s: string): string {
  const letters = s.replace(/[^\p{L}]/gu, "");
  if (!letters) return s;
  const allLower = letters === letters.toLowerCase();
  const allUpper = letters === letters.toUpperCase();
  if (!allLower && !allUpper) return s;
  return s
    .toLowerCase()
    .replace(/(^|[\s\-'’.(])(\p{L})/gu, (_, sep: string, ch: string) => sep + ch.toUpperCase());
}

function looksLikeEmail(s: string): boolean {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(s.trim());
}

/** Buyer name for display. An Etsy relay email is never shown as a name. */
export function displayBuyerName(
  fullName: string | null | undefined,
  username?: string | null
): string {
  const name = fullName?.trim();
  if (name && !looksLikeEmail(name)) return titleCase(name);
  const user = username?.trim();
  if (user && !looksLikeEmail(user)) return user;
  return "Etsy buyer";
}

/** "PAINESVILLE", "OH" -> "Painesville, OH"; non-US orders show the country. */
export function displayLocation(
  city: string | null | undefined,
  state: string | null | undefined,
  country: string | null | undefined
): string | null {
  const c = city?.trim() ? titleCase(city.trim()) : null;
  const st = state?.trim()
    ? state.trim().length <= 3
      ? state.trim().toUpperCase()
      : titleCase(state.trim())
    : null;
  const domestic = !country || country === "United States";
  const parts = [c, st, domestic ? null : country].filter(Boolean);
  return parts.length > 0 ? parts.join(", ") : (country ?? null);
}

const PAYMENT_WORDS: Record<string, string> = {
  cc: "Credit card",
  dc: "Debit card",
  pp: "PayPal",
  paypal: "PayPal",
  gc: "Gift card",
  applepay: "Apple Pay",
  apple_pay: "Apple Pay",
  googlepay: "Google Pay",
  google_pay: "Google Pay",
  klarna: "Klarna",
  ideal: "iDEAL",
  sofort: "Sofort",
  inperson: "In person",
  online: "Online",
};

/** "online_cc" -> "Credit card (online)"; unknown codes become readable words. */
export function humanizeCode(code: string | null | undefined): string | null {
  if (!code) return null;
  const raw = code.trim().toLowerCase();
  if (!raw) return null;
  if (PAYMENT_WORDS[raw]) return PAYMENT_WORDS[raw];
  const m = raw.match(/^(online|inperson)_(.+)$/);
  if (m) {
    const method = PAYMENT_WORDS[m[2]] ?? titleCase(m[2].replace(/_/g, " "));
    return `${method} (${m[1] === "online" ? "online" : "in person"})`;
  }
  return titleCase(raw.replace(/_/g, " "));
}

/** Privacy mode: "John Vitiello" -> "J. V." */
export function maskName(name: string): string {
  const initials = name
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((w) => `${w[0].toUpperCase()}.`);
  return initials.length > 0 ? initials.join(" ") : "Buyer";
}

/** Axis labels: "$1.5k", "$500". */
export function formatMoneyAxis(v: number): string {
  try {
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: currencyCode,
      notation: "compact",
      maximumFractionDigits: 1,
    })
      .format(v)
      .replace("K", "k");
  } catch {
    return String(Math.round(v));
  }
}

/** Round axis ticks from 0 up to at least `max`: 0, 500, 1000, 1500. */
export function niceTicks(max: number, target = 4): number[] {
  if (!Number.isFinite(max) || max <= 0) return [0, 1];
  const rough = max / target;
  const magnitude = Math.pow(10, Math.floor(Math.log10(rough)));
  const step = [1, 2, 2.5, 5, 10].map((m) => m * magnitude).find((s) => s >= rough) ?? rough;
  const ticks: number[] = [];
  for (let v = 0; v < max + step - 1e-9; v += step) ticks.push(Math.round(v * 1e6) / 1e6);
  return ticks;
}
