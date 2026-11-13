"use client";

import dynamic from "next/dynamic";
import { formatMoneyCompact, formatNumber } from "@/lib/format";

/**
 * Chart components. The Recharts-based ones live in charts-impl.tsx and are
 * loaded on demand, so pages become interactive before the chart library
 * arrives. BarList is plain CSS and renders immediately.
 */

function ChartSkeleton() {
  return <div className="h-[260px] w-full animate-pulse rounded-lg bg-muted/60" />;
}

export const MonthlyTrendChart = dynamic(
  () => import("./charts-impl").then((m) => m.MonthlyTrendChart),
  { ssr: false, loading: ChartSkeleton }
);
export const DonutChart = dynamic(() => import("./charts-impl").then((m) => m.DonutChart), {
  ssr: false,
  loading: ChartSkeleton,
});
export const ForecastChart = dynamic(
  () => import("./charts-impl").then((m) => m.ForecastChart),
  { ssr: false, loading: ChartSkeleton }
);
export const StackedMonthlyChart = dynamic(
  () => import("./charts-impl").then((m) => m.StackedMonthlyChart),
  { ssr: false, loading: ChartSkeleton }
);
export const SimpleBarChart = dynamic(
  () => import("./charts-impl").then((m) => m.SimpleBarChart),
  { ssr: false, loading: ChartSkeleton }
);

/** CSS horizontal bar list — crisp on mobile, no SVG needed. */
export function BarList({
  data,
  money = false,
  maxItems = 10,
}: {
  data: { name: string; value: number }[];
  money?: boolean;
  maxItems?: number;
}) {
  const items = data.slice(0, maxItems);
  const max = Math.max(...items.map((d) => d.value), 1);
  return (
    <ul className="grid gap-2.5">
      {items.map((d) => (
        <li key={d.name} className="grid gap-1">
          <div className="flex items-baseline justify-between gap-3 text-sm">
            <span className="min-w-0 truncate">{d.name}</span>
            <span className="shrink-0 tabular-nums font-medium">
              {money ? formatMoneyCompact(d.value) : formatNumber(d.value)}
            </span>
          </div>
          <div className="h-2 overflow-hidden rounded-full bg-muted">
            <div
              className="h-full rounded-full bg-[var(--chart-1)]"
              style={{ width: `${(d.value / max) * 100}%` }}
            />
          </div>
        </li>
      ))}
    </ul>
  );
}
