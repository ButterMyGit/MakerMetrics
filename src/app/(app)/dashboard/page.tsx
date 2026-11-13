"use client";

import { useMemo, useState } from "react";
import Link from "next/link";
import { AlertTriangle, ArrowRight, Info, Sparkles, TrendingUp } from "lucide-react";
import { useSalesData } from "@/hooks/use-sales-data";
import { useProfile } from "@/hooks/use-profile";
import { usePrivacy } from "@/hooks/use-privacy";
import { compareKpis, computeKpis, monthlySeries, productStats } from "@/lib/analytics/core";
import { shopForecast } from "@/lib/analytics/forecast";
import { buildInsights, type Insight } from "@/lib/analytics/insights";
import { EmptyRange, PageHeader } from "@/components/page-header";
import { KpiCard } from "@/components/kpi-card";
import { EmptyState, LoadingState } from "@/components/empty-state";
import { BarList, MonthlyTrendChart } from "@/components/charts";
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import {
  formatDate,
  formatDateShort,
  formatMoney,
  formatMoneyCompact,
  formatNumber,
  formatPercent,
} from "@/lib/format";
import { cn } from "@/lib/utils";

type Metric = "revenue" | "orders" | "units";

const TONE_ICON = { warn: AlertTriangle, good: TrendingUp, info: Info } as const;
const TONE_CLASS = {
  warn: "text-amber-600 dark:text-amber-400",
  good: "text-emerald-600 dark:text-emerald-400",
  info: "text-muted-foreground",
} as const;

function InsightRow({ insight }: { insight: Insight }) {
  const Icon = TONE_ICON[insight.tone];
  return (
    <li className="flex gap-3 py-3 first:pt-0 last:pb-0">
      <Icon className={cn("mt-0.5 size-4 shrink-0", TONE_CLASS[insight.tone])} />
      <div className="min-w-0 flex-1">
        <p className="text-sm font-medium">{insight.title}</p>
        <p className="mt-0.5 text-sm text-muted-foreground">{insight.detail}</p>
        <div className="mt-1.5 flex flex-wrap gap-x-4 gap-y-1 text-xs font-medium">
          {insight.href && (
            <Link href={insight.href} className="inline-flex items-center gap-1 text-primary hover:underline">
              View <ArrowRight className="size-3" />
            </Link>
          )}
          {insight.question && (
            <Link
              href={`/assistant?q=${encodeURIComponent(insight.question)}`}
              className="inline-flex items-center gap-1 text-primary hover:underline"
            >
              <Sparkles className="size-3" /> Ask the analyst
            </Link>
          )}
        </div>
      </div>
    </li>
  );
}

export default function DashboardPage() {
  const { orders, allOrders, loading, hasData, range, through, today } = useSalesData();
  const { prefs } = useProfile();
  const privacy = usePrivacy();
  const [metric, setMetric] = useState<Metric>("revenue");

  const kpis = useMemo(() => computeKpis(orders, allOrders), [orders, allOrders]);
  const comparison = useMemo(
    () => (range.start && range.end ? compareKpis(allOrders, range.start, range.end) : null),
    [allOrders, range.start, range.end]
  );
  const insights = useMemo(
    () => buildInsights(allOrders, { through, today, processingDays: prefs.processingDays }),
    [allOrders, through, today, prefs.processingDays]
  );

  // The trend always shows at least a year so a short range still has context.
  const series = useMemo(() => {
    const months = monthlySeries(allOrders, through);
    const forecast = shopForecast(allOrders, { through, horizon: 1 });
    const current = { revenue: forecast.revenue.current, orders: forecast.orders.current };
    const shown = range.preset === "all" ? months : months.slice(-13);
    return shown.map((m) => ({
      ...m,
      expected:
        m.partial && metric !== "units" && current[metric]?.month === m.month
          ? current[metric]!.value
          : null,
    }));
  }, [allOrders, through, range.preset, metric]);

  const topProducts = useMemo(() => productStats(orders, through, allOrders).slice(0, 8), [orders, through, allOrders]);
  const recentOrders = useMemo(() => [...orders].reverse().slice(0, 6), [orders]);

  if (loading) return <LoadingState />;
  if (!hasData) return <EmptyState />;

  const deltas = comparison?.deltas;
  const description = comparison
    ? `${formatDateShort(range.start)} – ${formatDate(range.end)}, compared with ${formatDateShort(
        comparison.previousRange.start
      )} – ${formatDateShort(comparison.previousRange.end)}.`
    : "Your whole history.";

  return (
    <div>
      <PageHeader title="Dashboard" description={description} />

      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 xl:grid-cols-6">
        <KpiCard
          label="Revenue"
          metric="revenue"
          value={formatMoneyCompact(kpis.revenue)}
          delta={deltas?.revenue}
        />
        <KpiCard
          label="Net after Etsy fees"
          metric="netAfterFees"
          value={formatMoneyCompact(kpis.netAfterFees)}
          delta={deltas?.netAfterFees}
          hint="Fees partly estimated"
        />
        <KpiCard
          label="Orders"
          metric="orders"
          value={formatNumber(kpis.orders)}
          delta={deltas?.orders}
          hint={`${formatNumber(kpis.units)} units`}
        />
        <KpiCard
          label="Avg order value"
          metric="avgOrderValue"
          value={formatMoney(kpis.avgOrderValue)}
          delta={deltas?.avgOrderValue}
        />
        <KpiCard
          label="Repeat buyers"
          metric="repeatBuyerRate"
          value={formatPercent(kpis.repeatBuyerRate)}
          delta={deltas?.repeatBuyerRate}
          deltaPoints
          hint={`${formatNumber(kpis.repeatBuyers)} of ${formatNumber(kpis.uniqueBuyers)} buyers`}
        />
        <KpiCard
          label="Etsy fees"
          metric="fees"
          value={formatMoneyCompact(kpis.totalFees)}
          hint={kpis.feeRate != null ? `${formatPercent(kpis.feeRate)} of revenue` : undefined}
        />
      </div>

      <div className="mt-4 grid gap-4 xl:grid-cols-3 [&>*]:min-w-0">
        <Card className="xl:col-span-2">
          <CardHeader>
            <CardTitle>By month</CardTitle>
            <CardDescription>
              {range.preset === "all" ? "All months" : "Last 13 months, whatever range is selected"}
            </CardDescription>
            <CardAction>
              <Tabs value={metric} onValueChange={(v) => setMetric(v as Metric)}>
                <TabsList>
                  <TabsTrigger value="revenue">Revenue</TabsTrigger>
                  <TabsTrigger value="orders">Orders</TabsTrigger>
                  <TabsTrigger value="units">Units</TabsTrigger>
                </TabsList>
              </Tabs>
            </CardAction>
          </CardHeader>
          <CardContent>
            <MonthlyTrendChart
              data={series}
              dataKey={metric}
              name={metric === "revenue" ? "Revenue" : metric === "orders" ? "Orders" : "Units"}
              money={metric === "revenue"}
              height={340}
            />
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Needs attention</CardTitle>
            <CardDescription>
              From your own history, as of {formatDateShort(through)}
            </CardDescription>
          </CardHeader>
          <CardContent>
            {insights.length === 0 ? (
              <p className="py-6 text-sm text-muted-foreground">
                Nothing stands out right now. Changes are only flagged when there are enough
                orders behind them to mean something.
              </p>
            ) : (
              <ul className="divide-y">
                {insights.slice(0, 5).map((insight) => (
                  <InsightRow key={insight.id} insight={insight} />
                ))}
              </ul>
            )}
          </CardContent>
        </Card>
      </div>

      {orders.length === 0 ? (
        <div className="mt-4">
          <EmptyRange />
        </div>
      ) : (
        <div className="mt-4 grid gap-4 lg:grid-cols-2 [&>*]:min-w-0">
          <Card>
            <CardHeader>
              <CardTitle>Top products</CardTitle>
              <CardDescription>Units sold in {range.label.toLowerCase()}</CardDescription>
              <CardAction>
                <Link href="/products" className="text-xs font-medium text-primary hover:underline">
                  View all
                </Link>
              </CardAction>
            </CardHeader>
            <CardContent>
              <BarList data={topProducts.map((p) => ({ name: p.name, value: p.units }))} maxItems={8} />
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Recent orders</CardTitle>
              <CardDescription>Revenue per order, before fees</CardDescription>
              <CardAction>
                <Link href="/orders" className="text-xs font-medium text-primary hover:underline">
                  View all
                </Link>
              </CardAction>
            </CardHeader>
            <CardContent className="grid grid-cols-[minmax(0,1fr)] gap-1">
              {recentOrders.map((o) => (
                <Link
                  key={o.key}
                  href={`/orders?order=${encodeURIComponent(o.key)}`}
                  className="flex min-w-0 items-center justify-between gap-3 rounded-lg py-2 hover:bg-accent/50"
                >
                  <div className="min-w-0">
                    <p className="truncate text-sm font-medium">{privacy.name(o.buyerName)}</p>
                    <p className="truncate text-xs text-muted-foreground">
                      {o.itemsLabel} · {formatDate(o.saleDate)}
                    </p>
                  </div>
                  <div className="flex shrink-0 items-center gap-2">
                    {o.coupons[0] && (
                      <Badge variant="secondary" className="hidden sm:inline-flex">
                        {o.coupons[0]}
                      </Badge>
                    )}
                    <span className="text-sm font-semibold tabular-nums">
                      {formatMoney(o.money.revenue)}
                    </span>
                  </div>
                </Link>
              ))}
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  );
}
