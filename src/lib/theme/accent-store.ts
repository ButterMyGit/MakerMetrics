"use client";

/**
 * The dashboard accent color drives the chart palette (`--chart-1..5`).
 * Stored per account in localStorage with a tiny external store so React stays
 * in sync without effects/hydration hacks. The authenticated layout seeds it
 * from Supabase so the setting follows the signed-in account.
 */

const LEGACY_STORAGE_KEY = "makermetrics-accent";
const STORAGE_PREFIX = "makermetrics-accent:";
export const DEFAULT_ACCENT = "#2563eb"; // blue-600

export const ACCENT_PRESETS: { name: string; value: string }[] = [
  { name: "Blue", value: "#2563eb" },
  { name: "Violet", value: "#7c3aed" },
  { name: "Emerald", value: "#059669" },
  { name: "Rose", value: "#e11d48" },
  { name: "Amber", value: "#d97706" },
  { name: "Cyan", value: "#0891b2" },
  { name: "Fuchsia", value: "#c026d3" },
  { name: "Slate", value: "#475569" },
];

const listeners = new Set<() => void>();
let activeUserId: string | null = null;
let cached: string | null = null;

function isHex(v: string): boolean {
  return /^#[0-9a-fA-F]{6}$/.test(v);
}

function storageKey(): string | null {
  return activeUserId ? `${STORAGE_PREFIX}${activeUserId}` : null;
}

export function setAccentUser(userId: string): void {
  if (activeUserId === userId) return;
  activeUserId = userId;
  cached = null;
  if (typeof window !== "undefined") {
    localStorage.removeItem(LEGACY_STORAGE_KEY);
  }
  listeners.forEach((l) => l());
}

export function seedAccent(hex: string): void {
  if (!isHex(hex)) return;
  const key = storageKey();
  if (typeof window !== "undefined" && key) {
    localStorage.setItem(key, hex);
  }
  cached = hex;
  applyAccent(hex);
  listeners.forEach((l) => l());
}

export function getAccent(): string {
  if (typeof window === "undefined") return DEFAULT_ACCENT;
  if (cached === null) {
    const key = storageKey();
    const raw = key ? localStorage.getItem(key) : null;
    cached = raw && isHex(raw) ? raw : DEFAULT_ACCENT;
  }
  return cached;
}

export function getServerAccent(): string {
  return DEFAULT_ACCENT;
}

export function setAccent(hex: string): void {
  if (!isHex(hex)) return;
  cached = hex;
  const key = storageKey();
  if (key) localStorage.setItem(key, hex);
  applyAccent(hex);
  listeners.forEach((l) => l());
}

export function subscribeAccent(listener: () => void): () => void {
  listeners.add(listener);
  window.addEventListener("storage", listener);
  return () => {
    listeners.delete(listener);
    window.removeEventListener("storage", listener);
  };
}

/** Write the accent + a derived mono ramp onto the document root. */
export function applyAccent(hex: string): void {
  if (typeof document === "undefined" || !isHex(hex)) return;
  const root = document.documentElement;
  root.style.setProperty("--chart-1", hex);
  root.style.setProperty("--chart-2", `color-mix(in oklab, ${hex}, white 24%)`);
  root.style.setProperty("--chart-3", `color-mix(in oklab, ${hex}, white 46%)`);
  root.style.setProperty("--chart-4", `color-mix(in oklab, ${hex}, black 18%)`);
  root.style.setProperty("--chart-5", `color-mix(in oklab, ${hex}, black 38%)`);
}
