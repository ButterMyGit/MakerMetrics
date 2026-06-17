"use client";

/**
 * AI provider settings are persisted per signed-in user in Supabase. The raw
 * API key is encrypted server-side; this client store only keeps non-secret
 * metadata for fast UI hydration.
 */

export interface AiSettings {
  provider: "openai" | "anthropic" | "google";
  model: string;
  hasApiKey: boolean;
}

export interface AiSettingsInput {
  provider: AiSettings["provider"];
  apiKey: string;
  model: string;
}

const LEGACY_STORAGE_KEY = "makermetrics-ai-settings";
const STORAGE_PREFIX = "makermetrics-ai-settings:";
const listeners = new Set<() => void>();

let activeUserId: string | null = null;
let cachedRaw: string | null = null;
let cachedValue: AiSettings | null = null;

function storageKey(): string | null {
  return activeUserId ? `${STORAGE_PREFIX}${activeUserId}` : null;
}

function notify(): void {
  listeners.forEach((l) => l());
}

export function setAiSettingsUser(userId: string): void {
  if (activeUserId === userId) return;
  activeUserId = userId;
  cachedRaw = null;
  cachedValue = null;
  if (typeof window !== "undefined") {
    localStorage.removeItem(LEGACY_STORAGE_KEY);
  }
  notify();
}

export function getAiSettings(): AiSettings | null {
  if (typeof window === "undefined") return null;
  const key = storageKey();
  if (!key) return null;
  const raw = localStorage.getItem(key);
  if (raw !== cachedRaw) {
    cachedRaw = raw;
    try {
      cachedValue = raw ? (JSON.parse(raw) as AiSettings) : null;
    } catch {
      cachedValue = null;
    }
  }
  return cachedValue;
}

export function getServerAiSettings(): AiSettings | null {
  return null;
}

function cacheAiSettings(settings: AiSettings | null): void {
  if (typeof window === "undefined") return;
  const key = storageKey();
  if (!key) return;
  if (settings) {
    const raw = JSON.stringify(settings);
    cachedValue = settings;
    cachedRaw = raw;
    localStorage.setItem(key, raw);
  } else {
    cachedValue = null;
    cachedRaw = null;
    localStorage.removeItem(key);
  }
  notify();
}

export async function loadAiSettings(): Promise<void> {
  const res = await fetch("/api/ai/settings", { method: "GET" });
  const body = (await res.json()) as { settings?: AiSettings | null; error?: string };
  if (!res.ok) throw new Error(body.error ?? "Failed to load AI settings.");
  cacheAiSettings(body.settings ?? null);
}

export async function saveAiSettings(settings: AiSettingsInput): Promise<AiSettings> {
  const res = await fetch("/api/ai/settings", {
    method: "PUT",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(settings),
  });
  const body = (await res.json()) as { settings?: AiSettings; error?: string };
  if (!res.ok || !body.settings) throw new Error(body.error ?? "Failed to save AI settings.");
  cacheAiSettings(body.settings);
  return body.settings;
}

export async function deleteAiSettings(): Promise<void> {
  const res = await fetch("/api/ai/settings", { method: "DELETE" });
  const body = (await res.json()) as { error?: string };
  if (!res.ok) throw new Error(body.error ?? "Failed to delete AI settings.");
  cacheAiSettings(null);
}

export function subscribeAiSettings(listener: () => void): () => void {
  listeners.add(listener);
  window.addEventListener("storage", listener);
  return () => {
    listeners.delete(listener);
    window.removeEventListener("storage", listener);
  };
}
