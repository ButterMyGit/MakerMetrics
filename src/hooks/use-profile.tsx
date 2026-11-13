"use client";

import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
  useSyncExternalStore,
  type ReactNode,
} from "react";
import { createClient } from "@/lib/supabase/client";
import { parsePrefs, type Prefs } from "@/lib/prefs";

interface ProfileContextValue {
  userId: string;
  shopName: string | null;
  setShopName: (name: string | null) => void;
  email: string | null;
  isDemo: boolean;
  prefs: Prefs;
  /** Apply a change now and save it to the account in the background. */
  updatePrefs: (patch: Partial<Record<keyof Prefs, Prefs[keyof Prefs] | null>>) => void;
}

const ProfileContext = createContext<ProfileContextValue | null>(null);

// ---------------------------------------------------------------------------
// Changes that haven't reached the account yet (offline, the read-only demo
// account, or a database without the prefs function) are kept on this device
// so they still apply here.
// ---------------------------------------------------------------------------

const EMPTY: Record<string, unknown> = {};
const listeners = new Set<() => void>();
let pendingKey: string | null = null;
let pendingRaw: string | null = null;
let pendingValue: Record<string, unknown> = EMPTY;

function readPending(key: string): Record<string, unknown> {
  let raw: string | null = null;
  try {
    raw = localStorage.getItem(key);
  } catch {
    return EMPTY;
  }
  if (key === pendingKey && raw === pendingRaw) return pendingValue;
  pendingKey = key;
  pendingRaw = raw;
  try {
    const parsed = raw ? (JSON.parse(raw) as unknown) : null;
    pendingValue = parsed && typeof parsed === "object" ? (parsed as Record<string, unknown>) : EMPTY;
  } catch {
    pendingValue = EMPTY;
  }
  return pendingValue;
}

function writePending(key: string, value: Record<string, unknown>): void {
  try {
    if (Object.keys(value).length === 0) localStorage.removeItem(key);
    else localStorage.setItem(key, JSON.stringify(value));
  } catch {
    // Applies for this visit only.
  }
  listeners.forEach((l) => l());
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

function applyPatch(base: Prefs, patch: Record<string, unknown>): Prefs {
  const merged: Record<string, unknown> = { ...base };
  for (const [k, v] of Object.entries(patch)) {
    if (v === null) delete merged[k];
    else merged[k] = v;
  }
  return parsePrefs(merged);
}

export function ProfileProvider({
  userId,
  initialShopName,
  initialPrefs,
  email,
  isDemo,
  children,
}: {
  userId: string;
  initialShopName: string | null;
  initialPrefs: Prefs;
  email: string | null;
  isDemo: boolean;
  children: ReactNode;
}) {
  const [shopName, setShopName] = useState<string | null>(initialShopName);
  const [saved, setSaved] = useState<Prefs>(initialPrefs);
  const storageKey = `makermetrics-prefs:${userId}`;
  const pending = useSyncExternalStore(
    subscribe,
    () => readPending(storageKey),
    () => EMPTY
  );

  const prefs = useMemo(() => applyPatch(saved, pending), [saved, pending]);

  const updatePrefs = useCallback<ProfileContextValue["updatePrefs"]>(
    (patch) => {
      const next = { ...readPending(storageKey), ...patch };
      writePending(storageKey, next);
      if (isDemo) return;
      void (async () => {
        const supabase = createClient();
        const { data, error } = await supabase.rpc("merge_profile_prefs", { prefs: patch });
        if (error) return; // stays on this device
        setSaved(parsePrefs(data));
        const remaining = { ...readPending(storageKey) };
        for (const [k, v] of Object.entries(patch)) {
          if (remaining[k] === v) delete remaining[k];
        }
        writePending(storageKey, remaining);
      })();
    },
    [storageKey, isDemo]
  );

  const value = useMemo(
    () => ({ userId, shopName, setShopName, email, isDemo, prefs, updatePrefs }),
    [userId, shopName, email, isDemo, prefs, updatePrefs]
  );

  return <ProfileContext.Provider value={value}>{children}</ProfileContext.Provider>;
}

export function useProfile(): ProfileContextValue {
  const ctx = useContext(ProfileContext);
  if (!ctx) throw new Error("useProfile must be used within ProfileProvider");
  return ctx;
}
