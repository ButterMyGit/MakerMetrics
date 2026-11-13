"use client";

import type { SaleRow } from "@/lib/types";

/**
 * Per-account copy of the sales rows in IndexedDB, so repeat visits render
 * immediately while the server copy is checked in the background. The cache
 * stays on this device and is cleared on sign-out and account deletion.
 */

const DB_NAME = "makermetrics";
const STORE = "sales";
const VERSION = 1;

export interface SalesCacheEntry {
  /** `${rowCount}:${latestUpdatedAt}` — changes whenever rows are added, edited or removed */
  signature: string;
  rows: SaleRow[];
}

function open(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    if (typeof indexedDB === "undefined") {
      reject(new Error("IndexedDB unavailable"));
      return;
    }
    const req = indexedDB.open(DB_NAME, VERSION);
    req.onupgradeneeded = () => {
      if (!req.result.objectStoreNames.contains(STORE)) req.result.createObjectStore(STORE);
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error ?? new Error("IndexedDB open failed"));
    req.onblocked = () => reject(new Error("IndexedDB blocked"));
  });
}

function run<T>(
  mode: IDBTransactionMode,
  action: (store: IDBObjectStore) => IDBRequest<T>
): Promise<T> {
  return open().then(
    (db) =>
      new Promise<T>((resolve, reject) => {
        const tx = db.transaction(STORE, mode);
        const req = action(tx.objectStore(STORE));
        tx.oncomplete = () => {
          db.close();
          resolve(req.result);
        };
        tx.onerror = tx.onabort = () => {
          db.close();
          reject(tx.error ?? new Error("IndexedDB transaction failed"));
        };
      })
  );
}

export async function readSalesCache(userId: string): Promise<SalesCacheEntry | null> {
  try {
    const entry = await run<SalesCacheEntry | undefined>("readonly", (s) => s.get(userId));
    if (!entry || typeof entry.signature !== "string" || !Array.isArray(entry.rows)) return null;
    return entry;
  } catch {
    return null;
  }
}

export async function writeSalesCache(userId: string, entry: SalesCacheEntry): Promise<void> {
  try {
    await run("readwrite", (s) => s.put(entry, userId));
  } catch {
    // Private windows and full disks can refuse writes; the app works without the cache.
  }
}

/** Remove one account's cached rows, or everything when no id is given. */
export async function clearSalesCache(userId?: string): Promise<void> {
  try {
    await run("readwrite", (s) => (userId ? s.delete(userId) : s.clear()));
  } catch {
    // Nothing cached, or storage unavailable.
  }
}
