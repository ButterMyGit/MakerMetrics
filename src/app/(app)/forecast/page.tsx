"use client";

import { useMemo, useState } from "react";
import { AlertTriangle, Info } from "lucide-react";
import { useSalesData } from "@/hooks/use-sales-data";
import {
  MODEL_LABELS,
  itemForecasts,
  shopForecast,
  type ForecastPoint,
  type ForecastResult,
} from "@/lib/analytics/forecast";
import { PageHeader } from "@/components/page-header";
import { EmptyState, LoadingState } from "@/components/empty-state";
import { ForecastChart, type ForecastChartRow } from "@/components/charts";
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
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
import {
  formatDate,
  formatMoney,
  formatMoneyApprox,
  formatMonth,
  formatNumber,
  formatPercent,
  roundApprox,
} from "@/lib/format";

function toChartData(result: ForecastResult): ForecastChartRow[] {
  const rows: ForecastChartRow[] = result.history.slice(-18).map((p) => ({
    month: p.month,
    actual: p.value,
    forecast: null,
    band: null,
  }));
  // Start the dashed line at the last actual point so the two connect.
  if (rows.length > 0) {
    const last = rows[rows.length - 1];
    last.forecast = last.actual;
    last.band = [last.actual as number, last.actual as number];
  }
  for (const f of [...(result.current ? [result.current] : []), ...result.forecast]) {
    rows.push({ month: f.month, actual: null, forecast: f.value, band: [f.lower, f.upper] });
  }
  return rows;
}

/** How wrong the forecast has been, in plain words, shown before the numbers. */
function AccuracyNote({ revenue }: { revenue: ForecastResult }) {
  const a = revenue.accuracy;
  if (a.monthsTested === 0 || a.wape == null) {
    return (
      <Alert>
        <Info className="size-4" />
        <AlertTitle>Too little history to check this forecast</AlertTitle>
        <AlertDescription>
          Accuracy is measured by forecasting past months and comparing with what happened. That
          needs at least four complete months.
        </AlertDescription>
      </Alert>
    );
  }
  const rough = a.wape > 0.35;
  const Icon = rough ? AlertTriangle : Info;
  const beatsBaseline = a.baselineWape != null && a.wape < a.baselineWape - 0.005;
  return (
    <Alert>
      <Icon className="size-4" />
      <AlertTitle>
        {rough ? "Treat this as a rough guide" : "How reliable this is"}: off by about{" "}
        {formatPercent(a.wape, 0)} when tested on your last {a.monthsTested} months
      </AlertTitle>
      <AlertDescription>
        That is roughly {formatMoneyApprox(a.avgError)} a month.{" "}
        {revenue.model === "last"
          ? "Nothing more elaborate did better than assuming next month looks like last month, so that is what is shown."
          : beatsBaseline
            ? `Simply assuming each month repeats the one before would have been off by ${formatPercent(a.baselineWape, 0)}.`
            : "That is about as good as assuming each month repeats the one before."}{" "}
        {revenue.notes.join(" ")}
      </AlertDescription>
    </Alert>
  );
}

function rangeText(p: ForecastPoint, money: boolean): string {
  const fmt = (v: number) => (money ? formatMoneyApprox(v) : formatNumber(Math.round(roundApprox(v))));
  return `${fmt(p.lower)} – ${fmt(p.upper)}`;
}

export default function ForecastPage() {
  // The forecast always uses the full history; the date filter doesn't apply.
  const { allOrders, loading, hasData, through } = useSalesData();
  const [mode, setMode] = useState<"revenue" | "orders">("revenue");

  const forecast = useMemo(() => shopForecast(allOrders, { through }), [allOrders, through]);
  const items = useMemo(() => itemForecasts(allOrders, { through }), [allOrders, through]);

  if (loading) return <LoadingState />;
  if (!hasData) return <EmptyState />;

  const active = mode === "revenue" ? forecast.revenue : forecast.orders;
  const months = [
    ...(forecast.revenue.current ? [0] : []),
    ...forecast.revenue.forecast.map((_, i) => i + 1),
  ];

  return (
    <div>
      <PageHeader
        title="Outlook"
        description={`Based on your full history through ${formatDate(through)}.`}
        showRange={false}
      />

      <div className="grid gap-4">
        <AccuracyNote revenue={forecast.revenue} />

        <Card>
          <CardHeader>
            <CardTitle>Next 3 months</CardTitle>
            <CardDescription>{MODEL_LABELS[active.model]}</CardDescription>
            <CardAction>
              <Tabs value={mode} onValueChange={(v) => setMode(v as "revenue" | "orders")}>
                <TabsList>
                  <TabsTrigger value="revenue">Revenue</TabsTrigger>
                  <TabsTrigger value="orders">Orders</TabsTrigger>
                </TabsList>
              </Tabs>
            </CardAction>
          </CardHeader>
          <CardContent>
            <ForecastChart data={toChartData(active)} money={mode === "revenue"} />
          </CardContent>
        </Card>

        <div className="grid gap-4 lg:grid-cols-2 [&>*]:min-w-0">
          <Card className="p-0">
            <CardHeader className="p-4 pb-0">
              <CardTitle>Month by month</CardTitle>
              <CardDescription>
                Revenue is forecast orders × your recent average order value
                {forecast.avgOrderValue != null ? ` (${formatMoney(forecast.avgOrderValue)})` : ""}.
                Figures are rounded; the range is where about 4 in 5 months should land.
              </CardDescription>
            </CardHeader>
            <div className="overflow-x-auto p-2">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Month</TableHead>
                    <TableHead className="text-right">Orders</TableHead>
                    <TableHead className="text-right">Revenue</TableHead>
                    <TableHead className="text-right">Likely range</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {months.map((i) => {
                    const rev = i === 0 ? forecast.revenue.current! : forecast.revenue.forecast[i - 1];
                    const ord = i === 0 ? forecast.orders.current! : forecast.orders.forecast[i - 1];
                    return (
                      <TableRow key={rev.month}>
                        <TableCell className="font-medium">
                          {formatMonth(rev.month)}
                          {rev.partial && (
                            <span className="block text-xs font-normal text-muted-foreground">
                              {formatMoney(rev.partial.actual)} so far (day {rev.partial.day} of{" "}
                              {rev.partial.days})
                            </span>
                          )}
                        </TableCell>
                        <TableCell className="text-right tabular-nums">about {formatNumber(ord.value)}</TableCell>
                        <TableCell className="text-right tabular-nums">about {formatMoneyApprox(rev.value)}</TableCell>
                        <TableCell className="text-right tabular-nums text-muted-foreground">
                          {rangeText(rev, true)}
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </div>
          </Card>

          <Card className="p-0">
            <CardHeader className="p-4 pb-0">
              <CardTitle>
                {items.month ? `${formatMonth(items.month)} by product` : "Next month by product"}
              </CardTitle>
              <CardDescription>
                About {formatNumber(Math.round(items.totalUnits))} units expected, split by what
                sold in the last 90 days. Individual products are too small to forecast on their
                own, so read these as shares, not promises.
              </CardDescription>
            </CardHeader>
            <div className="overflow-x-auto p-2">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Product</TableHead>
                    <TableHead className="text-right">Share of sales</TableHead>
                    <TableHead className="text-right">Likely units</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {items.items.map((it) => (
                    <TableRow key={it.key}>
                      <TableCell className="max-w-[220px] truncate font-medium">{it.name}</TableCell>
                      <TableCell className="text-right tabular-nums text-muted-foreground">
                        {formatPercent(it.share, 0)}
                      </TableCell>
                      <TableCell className="text-right tabular-nums">
                        {it.units < 1 ? "less than 1" : `about ${Math.round(it.units)}`}
                      </TableCell>
                    </TableRow>
                  ))}
                  {items.other.products > 0 && (
                    <TableRow>
                      <TableCell className="text-muted-foreground">
                        {items.other.products} other products
                      </TableCell>
                      <TableCell className="text-right tabular-nums text-muted-foreground">
                        {formatPercent(items.totalUnits > 0 ? items.other.units / items.totalUnits : 0, 0)}
                      </TableCell>
                      <TableCell className="text-right tabular-nums">
                        about {Math.round(items.other.units)}
                      </TableCell>
                    </TableRow>
                  )}
                  {items.items.length === 0 && (
                    <TableRow>
                      <TableCell colSpan={3} className="py-8 text-center text-muted-foreground">
                        No sales in the last 90 days to base a product mix on.
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </div>
          </Card>
        </div>
      </div>
    </div>
  );
}
