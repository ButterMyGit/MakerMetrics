"use client";

import { useMemo, useState } from "react";
import { useSalesData } from "@/hooks/use-sales-data";
import {
  itemForecasts,
  ordersForecast,
  revenueForecast,
  type ForecastResult,
} from "@/lib/analytics/forecast";
import { PageHeader } from "@/components/page-header";
import { EmptyState, LoadingState } from "@/components/empty-state";
import { ForecastChart } from "@/components/charts";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { formatMoneyCompact, formatMonth, formatNumber, formatPercent } from "@/lib/format";
import { Info } from "lucide-react";

function toChartData(result: ForecastResult) {
  const historyTail = result.history.slice(-18);
  const rows: Record<string, number | string | null>[] = historyTail.map((p) => ({
    month: p.month,
    // estimated = current partial month scaled up; shown differently
    actual: p.estimated ? null : p.value,
    estimated: p.estimated ? p.value : null,
    forecast: null,
    lower: null,
    upper: null,
  }));
  // bridge point so the dashed line connects to history
  if (rows.length > 0 && result.forecast.length > 0) {
    const last = historyTail[historyTail.length - 1];
    rows[rows.length - 1].forecast = last.estimated ? last.value : last.value;
  }
  for (const f of result.forecast) {
    rows.push({
      month: f.month,
      actual: null,
      estimated: null,
      forecast: f.value,
      lower: f.lower,
      upper: f.upper,
    });
  }
  return rows;
}

function BacktestSummary({ result }: { result: ForecastResult }) {
  const { backtest } = result;
  if (backtest.monthsTested === 0) {
    return (
      <p className="text-xs text-muted-foreground">
        Not enough history to measure accuracy yet.
      </p>
    );
  }
  return (
    <p className="text-xs text-muted-foreground">
      Backtest over the last {backtest.monthsTested} months: this model was off by{" "}
      <span className="font-medium text-foreground">{formatPercent(backtest.mape)}</span> on
      average
      {backtest.seasonalNaiveMape != null && (
        <>
          {" "}
          (vs {formatPercent(backtest.seasonalNaiveMape)} for a &ldquo;same month last
          year&rdquo; guess)
        </>
      )}
      .
    </p>
  );
}

export default function ForecastPage() {
  // Forecasts always train on full history; the global date filter is not applied.
  const { allRows, loading, hasData } = useSalesData();
  const [mode, setMode] = useState<"revenue" | "orders">("revenue");

  const revenue = useMemo(() => revenueForecast(allRows), [allRows]);
  const orders = useMemo(() => ordersForecast(allRows), [allRows]);
  const items = useMemo(() => itemForecasts(allRows), [allRows]);

  if (loading) return <LoadingState />;
  if (!hasData) return <EmptyState />;

  const active = mode === "revenue" ? revenue : orders;
  const chartData = toChartData(active);

  return (
    <div>
      <PageHeader
        title="Forecast"
        description="Trained on your full history, regardless of the date filter."
        showRange={false}
      />

      <Card>
        <CardHeader className="flex-row items-center justify-between gap-2">
          <div>
            <CardTitle>Next 6 months</CardTitle>
            <CardDescription>{active.method}</CardDescription>
          </div>
          <Tabs value={mode} onValueChange={(v) => setMode(v as "revenue" | "orders")}>
            <TabsList>
              <TabsTrigger value="revenue">Revenue</TabsTrigger>
              <TabsTrigger value="orders">Orders</TabsTrigger>
            </TabsList>
          </Tabs>
        </CardHeader>
        <CardContent className="grid gap-3">
          <ForecastChart data={chartData} money={mode === "revenue"} />
          <BacktestSummary result={active} />
          {active.notes.map((note) => (
            <Alert key={note}>
              <Info className="size-4" />
              <AlertTitle>Heads up</AlertTitle>
              <AlertDescription>{note}</AlertDescription>
            </Alert>
          ))}
        </CardContent>
      </Card>

      <div className="mt-4 grid gap-4 lg:grid-cols-2">
        <Card className="p-0">
          <CardHeader className="p-4 pb-0 sm:p-6 sm:pb-0">
            <CardTitle>Monthly projections</CardTitle>
            <CardDescription>Shaded band on the chart is an ~80% range.</CardDescription>
          </CardHeader>
          <div className="p-2">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Month</TableHead>
                  <TableHead className="text-right">Revenue</TableHead>
                  <TableHead className="text-right">Orders</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {revenue.forecast.map((f, i) => (
                  <TableRow key={f.month}>
                    <TableCell className="font-medium">{formatMonth(f.month)}</TableCell>
                    <TableCell className="text-right tabular-nums">
                      {formatMoneyCompact(f.value)}
                      <span className="ml-1 hidden text-xs text-muted-foreground sm:inline">
                        ({formatMoneyCompact(f.lower)}–{formatMoneyCompact(f.upper)})
                      </span>
                    </TableCell>
                    <TableCell className="text-right tabular-nums">
                      {orders.forecast[i] ? formatNumber(orders.forecast[i].value) : "—"}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        </Card>

        <Card className="p-0">
          <CardHeader className="p-4 pb-0 sm:p-6 sm:pb-0">
            <CardTitle>Next month by product</CardTitle>
            <CardDescription>
              Total units forecast, allocated by each product&apos;s recent sales mix
              {items[0]?.basis === "last-year + recent mix"
                ? " blended with the same month last year"
                : ""}
              .
            </CardDescription>
          </CardHeader>
          <div className="p-2">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Product</TableHead>
                  <TableHead className="text-right">Est. units</TableHead>
                  <TableHead className="text-right">Mix</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {items.map((it) => (
                  <TableRow key={it.name}>
                    <TableCell className="max-w-[200px] truncate font-medium">
                      {it.name}
                    </TableCell>
                    <TableCell className="text-right tabular-nums">
                      {it.nextMonthUnits.toFixed(1)}
                    </TableCell>
                    <TableCell className="text-right tabular-nums text-muted-foreground">
                      {formatPercent(it.shareOfUnits)}
                    </TableCell>
                  </TableRow>
                ))}
                {items.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={3} className="text-center text-muted-foreground">
                      Not enough recent sales to estimate the product mix.
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </div>
        </Card>
      </div>
    </div>
  );
}
