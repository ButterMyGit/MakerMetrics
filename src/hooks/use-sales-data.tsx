"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { createClient } from "@/lib/supabase/client";
import { dbRowToSaleRow, type SaleItemDbRow, type SaleRow } from "@/lib/types";
import { dateBounds, filterByDateRange } from "@/lib/analytics/core";

export type RangePreset = "all" | "30d" | "90d" | "12m" | "ytd" | "lastyear";

export interface DateRange {
  preset: RangePreset;
  start: string | null;
  end: string | null;
}

interface SalesDataContextValue {
  /** all rows, unfiltered, sorted by saleDate ascending */
  allRows: SaleRow[];
  /** rows within the active date range */
  rows: SaleRow[];
  loading: boolean;
  error: string | null;
  range: DateRange;
  setPreset: (preset: RangePreset) => void;
  bounds: { min: string; max: string } | null;
  refresh: () => Promise<void>;
  hasData: boolean;
}

const SalesDataContext = createContext<SalesDataContextValue | null>(null);

const PAGE_SIZE = 1000;

async function fetchAllRows(): Promise<SaleRow[]> {
  const supabase = createClient();
  const rows: SaleRow[] = [];
  let from = 0;
  for (;;) {
    const { data, error } = await supabase
      .from("sale_items")
      .select("*")
      .order("sale_date", { ascending: true, nullsFirst: false })
      .order("transaction_id", { ascending: true })
      .range(from, from + PAGE_SIZE - 1);
    if (error) throw new Error(error.message);
    const batch = (data ?? []) as SaleItemDbRow[];
    rows.push(...batch.map(dbRowToSaleRow));
    if (batch.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }
  return rows;
}

function presetToRange(preset: RangePreset): DateRange {
  const today = new Date();
  const iso = (d: Date) => d.toISOString().slice(0, 10);
  switch (preset) {
    case "30d":
      return { preset, start: iso(new Date(today.getTime() - 30 * 86400000)), end: null };
    case "90d":
      return { preset, start: iso(new Date(today.getTime() - 90 * 86400000)), end: null };
    case "12m":
      return { preset, start: iso(new Date(today.getTime() - 365 * 86400000)), end: null };
    case "ytd":
      return { preset, start: `${today.getFullYear()}-01-01`, end: null };
    case "lastyear": {
      const y = today.getFullYear() - 1;
      return { preset, start: `${y}-01-01`, end: `${y}-12-31` };
    }
    default:
      return { preset: "all", start: null, end: null };
  }
}

export function SalesDataProvider({ children }: { children: ReactNode }) {
  const [allRows, setAllRows] = useState<SaleRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [range, setRange] = useState<DateRange>({ preset: "all", start: null, end: null });

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      setAllRows(await fetchAllRows());
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load data");
    } finally {
      setLoading(false);
    }
  }, []);

  // Initial load. State updates happen after the fetch resolves (never
  // synchronously inside the effect body).
  useEffect(() => {
    let cancelled = false;
    fetchAllRows()
      .then((rows) => {
        if (!cancelled) setAllRows(rows);
      })
      .catch((e: unknown) => {
        if (!cancelled) setError(e instanceof Error ? e.message : "Failed to load data");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const setPreset = useCallback((preset: RangePreset) => {
    setRange(presetToRange(preset));
  }, []);

  const bounds = useMemo(() => dateBounds(allRows), [allRows]);

  const rows = useMemo(
    () => filterByDateRange(allRows, range.start, range.end),
    [allRows, range.start, range.end]
  );

  const value = useMemo(
    () => ({
      allRows,
      rows,
      loading,
      error,
      range,
      setPreset,
      bounds,
      refresh,
      hasData: allRows.length > 0,
    }),
    [allRows, rows, loading, error, range, setPreset, bounds, refresh]
  );

  return <SalesDataContext.Provider value={value}>{children}</SalesDataContext.Provider>;
}

export function useSalesData(): SalesDataContextValue {
  const ctx = useContext(SalesDataContext);
  if (!ctx) throw new Error("useSalesData must be used within SalesDataProvider");
  return ctx;
}
