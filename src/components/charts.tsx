"use client";

import {
  Area,
  Bar,
  CartesianGrid,
  Cell,
  ComposedChart,
  Line,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { formatMoneyCompact, formatMonth, formatNumber } from "@/lib/format";

export const CHART_COLORS = [
  "var(--chart-1)",
  "var(--chart-2)",
  "var(--chart-3)",
  "var(--chart-4)",
  "var(--chart-5)",
];

function compactValue(v: number, money: boolean): string {
  if (money) {
    return Math.abs(v) >= 1000
      ? `$${(v / 1000).toFixed(v >= 10000 ? 0 : 1)}k`
      : `$${Math.round(v)}`;
  }
  return Math.abs(v) >= 1000 ? `${(v / 1000).toFixed(1)}k` : String(Math.round(v));
}

interface TooltipPayloadEntry {
  name?: string;
  value?: number | string;
  color?: string;
  dataKey?: string | number;
}

function ChartTooltip({
  active,
  payload,
  label,
  money,
  labelFormatter,
}: {
  active?: boolean;
  payload?: TooltipPayloadEntry[];
  label?: string | number;
  money?: boolean;
  labelFormatter?: (label: string) => string;
}) {
  if (!active || !payload?.length) return null;
  return (
    <div className="rounded-lg border bg-popover px-3 py-2 text-xs shadow-md">
      <p className="mb-1 font-medium">
        {labelFormatter ? labelFormatter(String(label)) : String(label)}
      </p>
      {payload.map((entry, i) => (
        <p key={i} className="flex items-center gap-1.5 tabular-nums text-muted-foreground">
          <span
            className="inline-block size-2 rounded-full"
            style={{ background: entry.color }}
          />
          {entry.name}:{" "}
          <span className="font-medium text-foreground">
            {money
              ? formatMoneyCompact(Number(entry.value))
              : formatNumber(Number(entry.value))}
          </span>
        </p>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------

export function MonthlyTrendChart({
  data,
  dataKey,
  name,
  money = false,
  height = 280,
}: {
  data: object[];
  dataKey: string;
  name: string;
  money?: boolean;
  height?: number;
}) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <ComposedChart data={data} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
        <XAxis
          dataKey="month"
          tickFormatter={(m: string) => formatMonth(m).replace(" 20", " '")}
          tick={{ fontSize: 11 }}
          stroke="var(--muted-foreground)"
          tickLine={false}
          axisLine={false}
          minTickGap={24}
        />
        <YAxis
          tickFormatter={(v: number) => compactValue(v, money)}
          tick={{ fontSize: 11 }}
          stroke="var(--muted-foreground)"
          tickLine={false}
          axisLine={false}
          width={44}
        />
        <Tooltip
          content={<ChartTooltip money={money} labelFormatter={formatMonth} />}
          cursor={{ fill: "var(--muted)", opacity: 0.5 }}
        />
        <Bar dataKey={dataKey} name={name} fill="var(--chart-1)" radius={[4, 4, 0, 0]} />
      </ComposedChart>
    </ResponsiveContainer>
  );
}

export function DonutChart({
  data,
  height = 240,
}: {
  data: { name: string; value: number }[];
  height?: number;
}) {
  const top = data.slice(0, 5);
  const restTotal = data.slice(5).reduce((s, d) => s + d.value, 0);
  const display = restTotal > 0 ? [...top, { name: "Other", value: restTotal }] : top;
  const total = display.reduce((s, d) => s + d.value, 0);

  return (
    <div className="flex flex-col items-center gap-3 sm:flex-row">
      <ResponsiveContainer width="100%" height={height} className="max-w-[240px]">
        <PieChart>
          <Pie
            data={display}
            dataKey="value"
            nameKey="name"
            innerRadius="60%"
            outerRadius="90%"
            paddingAngle={2}
            strokeWidth={0}
          >
            {display.map((_, i) => (
              <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />
            ))}
          </Pie>
          <Tooltip content={<ChartTooltip />} />
        </PieChart>
      </ResponsiveContainer>
      <ul className="grid w-full gap-1.5 text-sm">
        {display.map((d, i) => (
          <li key={d.name} className="flex items-center gap-2">
            <span
              className="inline-block size-2.5 shrink-0 rounded-full"
              style={{ background: CHART_COLORS[i % CHART_COLORS.length] }}
            />
            <span className="min-w-0 flex-1 truncate text-muted-foreground">{d.name}</span>
            <span className="tabular-nums font-medium">
              {formatNumber(d.value)}
              <span className="ml-1 text-xs text-muted-foreground">
                {total > 0 ? `${((d.value / total) * 100).toFixed(0)}%` : ""}
              </span>
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}

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

export function ForecastChart({
  data,
  money = false,
  height = 300,
}: {
  /** rows with: month, actual?, estimated?, forecast?, lower?, upper? */
  data: Record<string, number | string | null>[];
  money?: boolean;
  height?: number;
}) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <ComposedChart data={data} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
        <XAxis
          dataKey="month"
          tickFormatter={(m: string) => formatMonth(m).replace(" 20", " '")}
          tick={{ fontSize: 11 }}
          stroke="var(--muted-foreground)"
          tickLine={false}
          axisLine={false}
          minTickGap={24}
        />
        <YAxis
          tickFormatter={(v: number) => compactValue(v, money)}
          tick={{ fontSize: 11 }}
          stroke="var(--muted-foreground)"
          tickLine={false}
          axisLine={false}
          width={44}
        />
        <Tooltip content={<ChartTooltip money={money} labelFormatter={formatMonth} />} />
        <Area
          dataKey="upper"
          name="Upper"
          stroke="none"
          fill="var(--chart-2)"
          fillOpacity={0.12}
          legendType="none"
          activeDot={false}
        />
        <Area
          dataKey="lower"
          name="Lower"
          stroke="none"
          fill="var(--background)"
          fillOpacity={1}
          legendType="none"
          activeDot={false}
        />
        <Line
          dataKey="actual"
          name="Actual"
          stroke="var(--chart-1)"
          strokeWidth={2}
          dot={false}
          connectNulls={false}
        />
        <Line
          dataKey="estimated"
          name="Est. this month"
          stroke="var(--chart-1)"
          strokeWidth={2}
          strokeDasharray="3 3"
          dot={false}
          connectNulls={false}
        />
        <Line
          dataKey="forecast"
          name="Forecast"
          stroke="var(--chart-2)"
          strokeWidth={2}
          strokeDasharray="6 4"
          dot={false}
          connectNulls
        />
      </ComposedChart>
    </ResponsiveContainer>
  );
}

export function StackedMonthlyChart({
  data,
  series,
  height = 260,
}: {
  data: object[];
  series: { dataKey: string; name: string }[];
  height?: number;
}) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <ComposedChart data={data} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
        <XAxis
          dataKey="month"
          tickFormatter={(m: string) => formatMonth(m).replace(" 20", " '")}
          tick={{ fontSize: 11 }}
          stroke="var(--muted-foreground)"
          tickLine={false}
          axisLine={false}
          minTickGap={24}
        />
        <YAxis
          tick={{ fontSize: 11 }}
          stroke="var(--muted-foreground)"
          tickLine={false}
          axisLine={false}
          width={36}
        />
        <Tooltip content={<ChartTooltip labelFormatter={formatMonth} />} />
        {series.map((s, i) => (
          <Bar
            key={s.dataKey}
            dataKey={s.dataKey}
            name={s.name}
            stackId="stack"
            fill={CHART_COLORS[i % CHART_COLORS.length]}
            radius={i === series.length - 1 ? [4, 4, 0, 0] : undefined}
          />
        ))}
      </ComposedChart>
    </ResponsiveContainer>
  );
}

export function SimpleBarChart({
  data,
  dataKey,
  name,
  labelKey = "day",
  money = false,
  height = 220,
}: {
  data: object[];
  dataKey: string;
  name: string;
  labelKey?: string;
  money?: boolean;
  height?: number;
}) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <ComposedChart data={data} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
        <XAxis
          dataKey={labelKey}
          tick={{ fontSize: 11 }}
          stroke="var(--muted-foreground)"
          tickLine={false}
          axisLine={false}
        />
        <YAxis
          tickFormatter={(v: number) => compactValue(v, money)}
          tick={{ fontSize: 11 }}
          stroke="var(--muted-foreground)"
          tickLine={false}
          axisLine={false}
          width={40}
        />
        <Tooltip
          content={<ChartTooltip money={money} />}
          cursor={{ fill: "var(--muted)", opacity: 0.5 }}
        />
        <Bar dataKey={dataKey} name={name} fill="var(--chart-1)" radius={[4, 4, 0, 0]} />
      </ComposedChart>
    </ResponsiveContainer>
  );
}
