"use client";

import { useMediaQuery } from "@/hooks/use-media-query";
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
import {
  formatMoneyAxis,
  formatMoneyCompact,
  formatMonth,
  formatNumber,
  niceTicks,
} from "@/lib/format";

export const CHART_COLORS = [
  "var(--chart-1)",
  "var(--chart-5)",
  "var(--chart-3)",
  "var(--chart-4)",
  "var(--chart-2)",
];

/**
 * On touch screens a tooltip opens on a tap, not while a finger passes over
 * the chart on its way down the page.
 */
function useTooltipTrigger(): "hover" | "click" {
  return useMediaQuery("(pointer: coarse)") ? "click" : "hover";
}

function axisValue(v: number, money: boolean): string {
  if (money) return formatMoneyAxis(v);
  return Math.abs(v) >= 1000 ? `${(v / 1000).toFixed(1)}k` : String(v);
}

function shortMonth(m: string): string {
  return formatMonth(m).replace(" 20", " '");
}

/** Whole-number ticks for counts, round money ticks otherwise. */
function yTicks(max: number, money: boolean): number[] {
  const ticks = niceTicks(max);
  return money ? ticks : [...new Set(ticks.map((t) => Math.round(t)))];
}

function maxOf(data: object[], keys: string[]): number {
  let max = 0;
  for (const d of data) {
    for (const k of keys) {
      const v = (d as Record<string, unknown>)[k];
      if (typeof v === "number" && v > max) max = v;
      if (Array.isArray(v)) for (const x of v) if (typeof x === "number" && x > max) max = x;
    }
  }
  return max;
}

interface TooltipPayloadEntry {
  name?: string;
  value?: number | string | (number | string)[];
  color?: string;
  dataKey?: string | number;
  payload?: Record<string, unknown>;
}

function ChartTooltip({
  active,
  payload,
  label,
  money,
  labelFormatter,
  note,
}: {
  active?: boolean;
  payload?: TooltipPayloadEntry[];
  label?: string | number;
  money?: boolean;
  labelFormatter?: (label: string) => string;
  /** extra line under the title, derived from the hovered row */
  note?: (row: Record<string, unknown>) => string | null;
}) {
  if (!active || !payload?.length) return null;
  const fmt = (v: number | string) =>
    money ? formatMoneyCompact(Number(v)) : formatNumber(Math.round(Number(v) * 10) / 10);
  const extra = note && payload[0].payload ? note(payload[0].payload) : null;
  return (
    <div className="rounded-lg border bg-popover px-3 py-2 text-xs shadow-md">
      <p className="mb-1 font-medium">
        {labelFormatter ? labelFormatter(String(label)) : String(label)}
      </p>
      {extra && <p className="mb-1 text-muted-foreground">{extra}</p>}
      {payload
        .filter((entry) => entry.value != null && entry.name)
        .map((entry, i) => (
          <p key={i} className="flex items-center gap-1.5 tabular-nums text-muted-foreground">
            <span
              className="inline-block size-2 rounded-full"
              style={{ background: entry.color }}
            />
            {entry.name}:{" "}
            <span className="font-medium text-foreground">
              {Array.isArray(entry.value)
                ? `${fmt(entry.value[0])} – ${fmt(entry.value[1])}`
                : fmt(entry.value as number | string)}
            </span>
          </p>
        ))}
    </div>
  );
}

function Legend({ items }: { items: { label: string; color: string; dashed?: boolean }[] }) {
  return (
    <ul className="mb-2 flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
      {items.map((item) => (
        <li key={item.label} className="flex items-center gap-1.5">
          <span
            className="inline-block h-2.5 w-2.5 rounded-sm"
            style={
              item.dashed
                ? { border: `1.5px dashed ${item.color}` }
                : { background: item.color }
            }
          />
          {item.label}
        </li>
      ))}
    </ul>
  );
}

// ---------------------------------------------------------------------------

export interface TrendPoint {
  month: string;
  /** set when the data only covers part of this month */
  partial?: { day: number; days: number };
  /** expected full-month value for a partial month */
  expected?: number | null;
}

/**
 * Monthly bars. A month the data only partly covers is drawn lighter, with
 * a dashed outline up to where it is expected to finish, so it doesn't read
 * as a collapse.
 */
export function MonthlyTrendChart({
  data,
  dataKey,
  name,
  money = false,
  height = 280,
}: {
  data: TrendPoint[];
  dataKey: string;
  name: string;
  money?: boolean;
  height?: number;
}) {
  const tooltipTrigger = useTooltipTrigger();
  const rows = data.map((d) => {
    const value = Number((d as unknown as Record<string, unknown>)[dataKey] ?? 0);
    const rest = d.partial && d.expected != null ? Math.max(0, d.expected - value) : 0;
    return { ...d, __value: value, __rest: rest };
  });
  const hasPartial = rows.some((r) => r.partial);
  const hasExpected = rows.some((r) => r.__rest > 0);
  const ticks = yTicks(Math.max(...rows.map((r) => r.__value + r.__rest), 0), money);

  return (
    <div>
      {hasPartial && (
        <Legend
          items={[
            { label: name, color: "var(--chart-1)" },
            {
              label: "Month in progress",
              color: "color-mix(in oklab, var(--chart-1), transparent 55%)",
            },
            ...(hasExpected
              ? [{ label: "Expected by month end", color: "var(--chart-1)", dashed: true }]
              : []),
          ]}
        />
      )}
      <ResponsiveContainer width="100%" height={height}>
        <ComposedChart data={rows} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
          <XAxis
            dataKey="month"
            tickFormatter={shortMonth}
            tick={{ fontSize: 11 }}
            stroke="var(--muted-foreground)"
            tickLine={false}
            axisLine={false}
            minTickGap={24}
          />
          <YAxis
            ticks={ticks}
            domain={[0, ticks[ticks.length - 1]]}
            tickFormatter={(v: number) => axisValue(v, money)}
            tick={{ fontSize: 11 }}
            stroke="var(--muted-foreground)"
            tickLine={false}
            axisLine={false}
            width={48}
          />
          <Tooltip
            trigger={tooltipTrigger}
            content={
              <ChartTooltip
                money={money}
                labelFormatter={formatMonth}
                note={(row) => {
                  const p = row.partial as TrendPoint["partial"];
                  return p ? `In progress: day ${p.day} of ${p.days}` : null;
                }}
              />
            }
            cursor={{ fill: "var(--muted)", opacity: 0.5 }}
          />
          <Bar dataKey="__value" name={name} stackId="m" fill="var(--chart-1)" radius={[4, 4, 0, 0]}>
            {rows.map((r) => (
              <Cell key={r.month} fillOpacity={r.partial ? 0.45 : 1} />
            ))}
          </Bar>
          {hasExpected && (
            <Bar
              dataKey="__rest"
              name="Expected rest of month"
              stackId="m"
              fill="transparent"
              stroke="var(--chart-1)"
              strokeDasharray="4 3"
              radius={[4, 4, 0, 0]}
            />
          )}
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
}

export function DonutChart({
  data,
  height = 240,
}: {
  data: { name: string; value: number }[];
  height?: number;
}) {
  const tooltipTrigger = useTooltipTrigger();
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
          <Tooltip trigger={tooltipTrigger} content={<ChartTooltip />} />
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

export interface ForecastChartRow {
  month: string;
  actual: number | null;
  forecast: number | null;
  /** [low, high] of the likely range */
  band: [number, number] | null;
}

export function ForecastChart({
  data,
  money = false,
  height = 300,
}: {
  data: ForecastChartRow[];
  money?: boolean;
  height?: number;
}) {
  const tooltipTrigger = useTooltipTrigger();
  const ticks = yTicks(maxOf(data, ["actual", "forecast", "band"]), money);
  return (
    <div>
      <Legend
        items={[
          { label: "Actual", color: "var(--chart-1)" },
          { label: "Forecast", color: "var(--chart-1)", dashed: true },
          {
            label: "Likely range (about 4 in 5 months)",
            color: "color-mix(in oklab, var(--chart-1), transparent 80%)",
          },
        ]}
      />
      <ResponsiveContainer width="100%" height={height}>
        <ComposedChart data={data} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
          <XAxis
            dataKey="month"
            tickFormatter={shortMonth}
            tick={{ fontSize: 11 }}
            stroke="var(--muted-foreground)"
            tickLine={false}
            axisLine={false}
            minTickGap={24}
          />
          <YAxis
            ticks={ticks}
            domain={[0, ticks[ticks.length - 1]]}
            tickFormatter={(v: number) => axisValue(v, money)}
            tick={{ fontSize: 11 }}
            stroke="var(--muted-foreground)"
            tickLine={false}
            axisLine={false}
            width={48}
          />
          <Tooltip trigger={tooltipTrigger} content={<ChartTooltip money={money} labelFormatter={formatMonth} />} />
          <Area
            dataKey="band"
            name="Likely range"
            stroke="none"
            fill="var(--chart-1)"
            fillOpacity={0.14}
            activeDot={false}
            connectNulls
            isAnimationActive={false}
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
            dataKey="forecast"
            name="Forecast"
            stroke="var(--chart-1)"
            strokeWidth={2}
            strokeDasharray="6 4"
            dot={{ r: 2.5, fill: "var(--chart-1)", strokeWidth: 0 }}
            connectNulls
          />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
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
  const tooltipTrigger = useTooltipTrigger();
  const totals = data.map((d) =>
    series.reduce((s, k) => s + Number((d as Record<string, unknown>)[k.dataKey] ?? 0), 0)
  );
  const ticks = yTicks(Math.max(...totals, 0), false);
  return (
    <div>
      <Legend
        items={series.map((s, i) => ({
          label: s.name,
          color: CHART_COLORS[i % CHART_COLORS.length],
        }))}
      />
      <ResponsiveContainer width="100%" height={height}>
        <ComposedChart data={data} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
          <XAxis
            dataKey="month"
            tickFormatter={shortMonth}
            tick={{ fontSize: 11 }}
            stroke="var(--muted-foreground)"
            tickLine={false}
            axisLine={false}
            minTickGap={24}
          />
          <YAxis
            ticks={ticks}
            domain={[0, ticks[ticks.length - 1]]}
            allowDecimals={false}
            tick={{ fontSize: 11 }}
            stroke="var(--muted-foreground)"
            tickLine={false}
            axisLine={false}
            width={36}
          />
          <Tooltip
            trigger={tooltipTrigger}
            content={<ChartTooltip labelFormatter={formatMonth} />}
            cursor={{ fill: "var(--muted)", opacity: 0.5 }}
          />
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
    </div>
  );
}

export function SimpleBarChart({
  data,
  dataKey,
  name,
  labelKey = "day",
  money = false,
  percent = false,
  monthLabels = false,
  height = 220,
}: {
  data: object[];
  dataKey: string;
  name: string;
  labelKey?: string;
  money?: boolean;
  /** values are 0–1 shares, shown as percentages */
  percent?: boolean;
  /** x values are YYYY-MM keys */
  monthLabels?: boolean;
  height?: number;
}) {
  const tooltipTrigger = useTooltipTrigger();
  const rows = percent
    ? data.map((d) => {
        const v = (d as Record<string, unknown>)[dataKey];
        return { ...d, [dataKey]: typeof v === "number" ? Math.round(v * 1000) / 10 : null };
      })
    : data;
  const ticks = percent ? [0, 25, 50, 75, 100] : yTicks(maxOf(rows, [dataKey]), money);
  return (
    <ResponsiveContainer width="100%" height={height}>
      <ComposedChart data={rows} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
        <XAxis
          dataKey={labelKey}
          tickFormatter={monthLabels ? shortMonth : undefined}
          tick={{ fontSize: 11 }}
          stroke="var(--muted-foreground)"
          tickLine={false}
          axisLine={false}
          minTickGap={monthLabels ? 24 : 5}
        />
        <YAxis
          ticks={ticks}
          domain={[0, ticks[ticks.length - 1]]}
          tickFormatter={(v: number) => (percent ? `${v}%` : axisValue(v, money))}
          tick={{ fontSize: 11 }}
          stroke="var(--muted-foreground)"
          tickLine={false}
          axisLine={false}
          width={44}
        />
        <Tooltip
            trigger={tooltipTrigger}
          content={
            <ChartTooltip money={money} labelFormatter={monthLabels ? formatMonth : undefined} />
          }
          cursor={{ fill: "var(--muted)", opacity: 0.5 }}
        />
        <Bar
          dataKey={dataKey}
          name={percent ? `${name} (%)` : name}
          fill="var(--chart-1)"
          radius={[4, 4, 0, 0]}
        />
      </ComposedChart>
    </ResponsiveContainer>
  );
}
