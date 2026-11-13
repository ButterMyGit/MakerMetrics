"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
  type ReactNode,
} from "react";
import { createClient } from "@/lib/supabase/client";
import { dbRowToSaleRow, type SaleItemDbRow, type SaleRow } from "@/lib/types";
import { buildOrders, dataThrough, dateBounds, filterOrders, type Order } from "@/lib/analytics/core";
import { readSalesCache, writeSalesCache } from "@/lib/data/sales-cache";
import { minIso, todayIso } from "@/lib/dates";
import { setCurrency } from "@/lib/format";
import {
  getRangeChoice,
  getServerRangeChoice,
  rangeLabel,
  resolveRange,
  setRangeChoice,
  subscribeRange,
  type RangeChoice,
} from "@/lib/range-store";
import { useProfile } from "@/hooks/use-profile";

export interface ActiveRange extends RangeChoice {
  /** resolved, inclusive dates; null on both for "all time" */
  start: string | null;
  end: string | null;
  label: string;
}

interface SalesDataContextValue {
  /** every order, oldest first */
  allOrders: Order[];
  /** orders within the active date range */
  orders: Order[];
  /** number of imported line items */
  lineItems: number;
  /** true only until the first rows (cached or fetched) are available */
  loading: boolean;
  /** true while the server copy is being checked or re-downloaded */
  syncing: boolean;
  error: string | null;
  range: ActiveRange;
  setRange: (choice: RangeChoice) => void;
  bounds: { min: string; max: string } | null;
  /** last date the data covers (latest sale or shipment), never later than today */
  through: string | null;
  today: string;
  refresh: () => Promise<void>;
  hasData: boolean;
}

const SalesDataContext = createContext<SalesDataContextValue | null>(null);

const PAGE_SIZE = 1000;
/** Bump when the cached row shape changes so old device caches are ignored. */
const CACHE_VERSION = 2;

/**
 * A cheap fingerprint of the account's rows: how many there are and when one
 * last changed. Inserts, edits (updated_at trigger) and deletes all change it.
 */
async function fetchSignature(): Promise<{ signature: string; count: number }> {
  const supabase = createClient();
  const { data, count, error } = await supabase
    .from("sale_items")
    .select("updated_at", { count: "exact" })
    .order("updated_at", { ascending: false })
    .limit(1);
  if (error) throw new Error(error.message);
  const latest = (data?.[0] as { updated_at?: string } | undefined)?.updated_at ?? "";
  const total = count ?? 0;
  return { signature: `v${CACHE_VERSION}:${total}:${latest}`, count: total };
}

async function fetchPage(from: number): Promise<SaleItemDbRow[]> {
  const supabase = createClient();
  const { data, error } = await supabase
    .from("sale_items")
    .select("*")
    .order("sale_date", { ascending: true, nullsFirst: false })
    .order("transaction_id", { ascending: true })
    .range(from, from + PAGE_SIZE - 1);
  if (error) throw new Error(error.message);
  return (data ?? []) as SaleItemDbRow[];
}

/** Download every row, requesting all pages at once instead of one by one. */
async function fetchAllRows(count: number): Promise<SaleRow[]> {
  const pages = Math.max(1, Math.ceil(count / PAGE_SIZE));
  const batches = await Promise.all(
    Array.from({ length: pages }, (_, i) => fetchPage(i * PAGE_SIZE))
  );
  const rows = batches.flat();
  // Rows added between the count and the download land on an extra page.
  let from = pages * PAGE_SIZE;
  while (rows.length === from) {
    const extra = await fetchPage(from);
    rows.push(...extra);
    if (extra.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }
  return rows.map(dbRowToSaleRow);
}

interface LoadUpdate {
  rows?: SaleRow[];
  error?: string;
  /** set on the final update of a load */
  done?: boolean;
}

/**
 * Show the device copy first (if any), then compare fingerprints with the
 * server and download only when something changed. `force` skips the
 * comparison, e.g. right after an import. Progress is reported through
 * `onUpdate`, which may fire more than once.
 */
async function loadSales(
  userId: string,
  force: boolean,
  onUpdate: (update: LoadUpdate) => void
): Promise<void> {
  try {
    const cached = await readSalesCache(userId);
    if (cached && !force) onUpdate({ rows: cached.rows });
    const { signature, count } = await fetchSignature();
    if (force || !cached || cached.signature !== signature) {
      const rows = await fetchAllRows(count);
      onUpdate({ rows });
      void writeSalesCache(userId, { signature, rows });
    }
    onUpdate({ done: true });
  } catch (e) {
    onUpdate({ error: e instanceof Error ? e.message : "Failed to load data", done: true });
  }
}

export function SalesDataProvider({ children }: { children: ReactNode }) {
  const { userId } = useProfile();
  const [allRows, setAllRows] = useState<SaleRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [syncing, setSyncing] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const choice = useSyncExternalStore(subscribeRange, getRangeChoice, getServerRangeChoice);
  const [today] = useState(() => todayIso());
  const generation = useRef(0);

  const applyUpdate = useCallback((update: LoadUpdate) => {
    if (update.rows) {
      setAllRows(update.rows);
      setLoading(false);
    }
    if (update.error) setError(update.error);
    if (update.done) {
      setLoading(false);
      setSyncing(false);
    }
  }, []);

  // Initial load. State only changes from the loader's callbacks.
  useEffect(() => {
    const id = ++generation.current;
    void loadSales(userId, false, (update) => {
      if (id === generation.current) applyUpdate(update);
    });
    return () => {
      generation.current += 1;
    };
  }, [userId, applyUpdate]);

  const refresh = useCallback(() => {
    const id = ++generation.current;
    setSyncing(true);
    setError(null);
    return loadSales(userId, true, (update) => {
      if (id === generation.current) applyUpdate(update);
    });
  }, [userId, applyUpdate]);

  const allOrders = useMemo(() => {
    // The shop's currency is whatever most rows are in.
    const counts = new Map<string, number>();
    for (const r of allRows) if (r.currency) counts.set(r.currency, (counts.get(r.currency) ?? 0) + 1);
    setCurrency([...counts.entries()].sort((a, b) => b[1] - a[1])[0]?.[0]);
    return buildOrders(allRows);
  }, [allRows]);

  const bounds = useMemo(() => dateBounds(allOrders), [allOrders]);
  const through = useMemo(() => {
    const latest = dataThrough(allOrders);
    return latest ? minIso(latest, today) : null;
  }, [allOrders, today]);

  const range = useMemo<ActiveRange>(
    () => ({ ...choice, ...resolveRange(choice, through ?? today), label: rangeLabel(choice) }),
    [choice, through, today]
  );

  const orders = useMemo(
    () => filterOrders(allOrders, range.start, range.end),
    [allOrders, range.start, range.end]
  );

  const value = useMemo(
    () => ({
      allOrders,
      orders,
      lineItems: allRows.length,
      loading,
      syncing,
      error,
      range,
      setRange: setRangeChoice,
      bounds,
      through,
      today,
      refresh,
      hasData: allRows.length > 0,
    }),
    [allOrders, orders, allRows.length, loading, syncing, error, range, bounds, through, today, refresh]
  );

  return <SalesDataContext.Provider value={value}>{children}</SalesDataContext.Provider>;
}

export function useSalesData(): SalesDataContextValue {
  const ctx = useContext(SalesDataContext);
  if (!ctx) throw new Error("useSalesData must be used within SalesDataProvider");
  return ctx;
}
