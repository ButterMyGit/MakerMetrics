"use client";

import dynamic from "next/dynamic";
import { formatMoneyCompact, formatNumber } from "@/lib/format";

/**
 * Chart components. The Recharts-based ones live in charts-impl.tsx and are
 * loaded on demand, so pages become interactive before the chart library
 * arrives. BarList and Sparkline are plain CSS/SVG and render immediately.
 */

export type { ForecastChartRow, TrendPoint } from "./charts-impl";

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
  data: { name: string; value: number; sub?: string }[];
  money?: boolean;
  maxItems?: number;
}) {
  const items = data.slice(0, maxItems);
  const max = Math.max(...items.map((d) => d.value), 1);
  return (
    <ul className="grid grid-cols-[minmax(0,1fr)] gap-2.5">
      {items.map((d) => (
        <li key={d.name} className="grid gap-1">
          <div className="flex items-baseline justify-between gap-3 text-sm">
            <span className="min-w-0 truncate">{d.name}</span>
            <span className="shrink-0 tabular-nums font-medium">
              {money ? formatMoneyCompact(d.value) : formatNumber(d.value)}
              {d.sub && <span className="ml-1.5 text-xs font-normal text-muted-foreground">{d.sub}</span>}
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

/** Tiny inline trend: one bar per month, oldest first. */
export function Sparkline({ values, className }: { values: number[]; className?: string }) {
  const max = Math.max(...values, 1);
  const width = 6;
  const gap = 2;
  const height = 20;
  return (
    <svg
      viewBox={`0 0 ${values.length * (width + gap) - gap} ${height}`}
      width={values.length * (width + gap) - gap}
      height={height}
      className={className}
      role="img"
      aria-label={`Units per month, oldest first: ${values.join(", ")}`}
    >
      {values.map((v, i) => {
        const h = v === 0 ? 1.5 : Math.max(3, (v / max) * height);
        return (
          <rect
            key={i}
            x={i * (width + gap)}
            y={height - h}
            width={width}
            height={h}
            rx={1}
            fill={v === 0 ? "var(--border)" : "var(--chart-1)"}
            opacity={v === 0 ? 1 : 0.35 + 0.65 * (i / Math.max(1, values.length - 1))}
          />
        );
      })}
    </svg>
  );
}
