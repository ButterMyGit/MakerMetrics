"use client";

/**
 * BYOK settings live in localStorage only. A tiny external store keeps React
 * in sync (useSyncExternalStore) without effects or hydration hacks.
 */

export interface AiSettings {
  provider: "openai" | "anthropic" | "google";
  apiKey: string;
  model: string;
}

const STORAGE_KEY = "makermetrics-ai-settings";
const listeners = new Set<() => void>();

let cachedRaw: string | null = null;
let cachedValue: AiSettings | null = null;

export function getAiSettings(): AiSettings | null {
  if (typeof window === "undefined") return null;
  const raw = localStorage.getItem(STORAGE_KEY);
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

export function saveAiSettings(settings: AiSettings): void {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(settings));
  listeners.forEach((l) => l());
}

export function subscribeAiSettings(listener: () => void): () => void {
  listeners.add(listener);
  window.addEventListener("storage", listener);
  return () => {
    listeners.delete(listener);
    window.removeEventListener("storage", listener);
  };
}
