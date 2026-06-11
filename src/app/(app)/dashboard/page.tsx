"use client";

import { useMemo, useState } from "react";
import Link from "next/link";
import { useSalesData } from "@/hooks/use-sales-data";
import {
  compareKpis,
  computeKpis,
  dayOfWeekBreakdown,
  monthlySeries,
  productStats,
  buildOrderHistory,
} from "@/lib/analytics/core";
import { PageHeader } from "@/components/page-header";
import { KpiCard } from "@/components/kpi-card";
import { EmptyState, LoadingState } from "@/components/empty-state";
import { BarList, MonthlyTrendChart, SimpleBarChart } from "@/components/charts";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { formatDate, formatMoney, formatMoneyCompact, formatNumber, formatPercent } from "@/lib/format";

type Metric = "netRevenue" | "orders" | "units";

export default function DashboardPage() {
  const { rows, allRows, loading, hasData, range } = useSalesData();
  const [metric, setMetric] = useState<Metric>("netRevenue");

  const kpis = useMemo(() => computeKpis(rows), [rows]);
  const comparison = useMemo(() => {
    if (!range.start) return null;
    const end = range.end ?? new Date().toISOString().slice(0, 10);
    return compareKpis(allRows, range.start, end);
  }, [allRows, range]);

  const series = useMemo(() => monthlySeries(rows), [rows]);
  const dow = useMemo(() => dayOfWeekBreakdown(rows), [rows]);
  const topProducts = useMemo(() => productStats(rows).slice(0, 8), [rows]);
  const recentOrders = useMemo(() => buildOrderHistory(rows).slice(0, 6), [rows]);

  if (loading) return <LoadingState />;
  if (!hasData) return <EmptyState />;

  const deltas = comparison?.deltas;

  return (
    <div>
      <PageHeader
        title="Dashboard"
        description={
          comparison
            ? "Change shown vs the previous period of equal length."
            : undefined
        }
      />

      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
        <KpiCard
          label="Net revenue"
          value={formatMoneyCompact(kpis.netRevenue)}
          delta={deltas?.netRevenue}
          hint="After Etsy fees"
        />
        <KpiCard label="Orders" value={formatNumber(kpis.orders)} delta={deltas?.orders} />
        <KpiCard label="Units sold" value={formatNumber(kpis.units)} delta={deltas?.units} />
        <KpiCard
          label="Avg order value"
          value={formatMoney(kpis.avgOrderValue)}
          delta={deltas?.avgOrderValue}
        />
        <KpiCard
          label="Repeat buyer rate"
          value={formatPercent(kpis.repeatBuyerRate)}
          hint={`${formatNumber(kpis.uniqueBuyers)} buyers`}
        />
        <KpiCard
          label="Etsy processing fees"
          value={formatMoneyCompact(kpis.totalFees)}
          hint={
            kpis.grossRevenue > 0
              ? `${formatPercent(kpis.totalFees / kpis.grossRevenue)} of gross`
              : undefined
          }
        />
      </div>

      <Card className="mt-4">
        <CardHeader className="flex-row items-center justify-between gap-2">
          <CardTitle>Performance by month</CardTitle>
          <Tabs value={metric} onValueChange={(v) => setMetric(v as Metric)}>
            <TabsList>
              <TabsTrigger value="netRevenue">Revenue</TabsTrigger>
              <TabsTrigger value="orders">Orders</TabsTrigger>
              <TabsTrigger value="units">Units</TabsTrigger>
            </TabsList>
          </Tabs>
        </CardHeader>
        <CardContent>
          <MonthlyTrendChart
            data={series}
            dataKey={metric}
            name={
              metric === "netRevenue" ? "Net revenue" : metric === "orders" ? "Orders" : "Units"
            }
            money={metric === "netRevenue"}
          />
        </CardContent>
      </Card>

      <div className="mt-4 grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader className="flex-row items-center justify-between">
            <CardTitle>Top products</CardTitle>
            <Link href="/products" className="text-xs font-medium text-primary hover:underline">
              View all
            </Link>
          </CardHeader>
          <CardContent>
            <BarList
              data={topProducts.map((p) => ({ name: p.name, value: p.units }))}
              maxItems={8}
            />
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Orders by day of week</CardTitle>
          </CardHeader>
          <CardContent>
            <SimpleBarChart data={dow} dataKey="orders" name="Orders" height={260} />
          </CardContent>
        </Card>
      </div>

      <Card className="mt-4">
        <CardHeader className="flex-row items-center justify-between">
          <CardTitle>Recent orders</CardTitle>
          <Link href="/orders" className="text-xs font-medium text-primary hover:underline">
            View all
          </Link>
        </CardHeader>
        <CardContent className="grid gap-1">
          {recentOrders.map((o) => (
            <div
              key={o.orderId}
              className="flex items-center justify-between gap-3 rounded-lg px-2 py-2 hover:bg-accent/50"
            >
              <div className="min-w-0">
                <p className="truncate text-sm font-medium">{o.buyer}</p>
                <p className="truncate text-xs text-muted-foreground">
                  {o.itemsLabel} · {formatDate(o.saleDate)}
                </p>
              </div>
              <div className="flex shrink-0 items-center gap-2">
                {o.couponCode && (
                  <Badge variant="secondary" className="hidden sm:inline-flex">
                    {o.couponCode}
                  </Badge>
                )}
                <span className="text-sm font-semibold tabular-nums">
                  {formatMoney(o.orderNet)}
                </span>
              </div>
            </div>
          ))}
        </CardContent>
      </Card>
    </div>
  );
}
