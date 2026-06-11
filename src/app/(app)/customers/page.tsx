"use client";

import { useMemo, useState } from "react";
import { useSalesData } from "@/hooks/use-sales-data";
import {
  buyerStats,
  couponAnalysis,
  customerKpis,
  fulfillmentStats,
  geoBreakdown,
} from "@/lib/analytics/core";
import { PageHeader } from "@/components/page-header";
import { KpiCard } from "@/components/kpi-card";
import { EmptyState, LoadingState } from "@/components/empty-state";
import { BarList, SimpleBarChart, StackedMonthlyChart } from "@/components/charts";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { formatDate, formatMoney, formatMoneyCompact, formatNumber, formatPercent } from "@/lib/format";

export default function CustomersPage() {
  const { rows, loading, hasData } = useSalesData();
  const [geoMode, setGeoMode] = useState<"state" | "country">("state");
  const [buyerFilter, setBuyerFilter] = useState<"all" | "repeat">("all");

  const kpis = useMemo(() => customerKpis(rows), [rows]);
  const buyers = useMemo(() => buyerStats(rows), [rows]);
  const geo = useMemo(() => geoBreakdown(rows, geoMode), [rows, geoMode]);
  const coupons = useMemo(() => couponAnalysis(rows), [rows]);
  const fulfillment = useMemo(() => fulfillmentStats(rows), [rows]);

  const visibleBuyers = useMemo(
    () => (buyerFilter === "repeat" ? buyers.filter((b) => b.orders > 1) : buyers).slice(0, 50),
    [buyers, buyerFilter]
  );

  if (loading) return <LoadingState />;
  if (!hasData) return <EmptyState />;

  return (
    <div>
      <PageHeader title="Customers" />

      <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        <KpiCard label="Unique buyers" value={formatNumber(kpis.uniqueBuyers)} />
        <KpiCard
          label="Repeat rate"
          value={formatPercent(kpis.repeatRate)}
          hint="Buyers with 2+ orders"
        />
        <KpiCard
          label="Avg lifetime value"
          value={formatMoney(kpis.avgLifetimeValue)}
          hint="Net revenue per buyer"
        />
        <KpiCard
          label="Median reorder gap"
          value={
            kpis.medianDaysBetweenOrders != null
              ? `${Math.round(kpis.medianDaysBetweenOrders)} days`
              : "—"
          }
          hint="Between repeat purchases"
        />
      </div>

      <div className="mt-4 grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>New vs returning buyers</CardTitle>
            <CardDescription>Distinct buyers placing orders each month</CardDescription>
          </CardHeader>
          <CardContent>
            <StackedMonthlyChart
              data={kpis.newVsReturning}
              series={[
                { dataKey: "newBuyers", name: "New" },
                { dataKey: "returningBuyers", name: "Returning" },
              ]}
            />
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex-row items-center justify-between">
            <CardTitle>Where buyers are</CardTitle>
            <Tabs value={geoMode} onValueChange={(v) => setGeoMode(v as "state" | "country")}>
              <TabsList>
                <TabsTrigger value="state">US states</TabsTrigger>
                <TabsTrigger value="country">Countries</TabsTrigger>
              </TabsList>
            </Tabs>
          </CardHeader>
          <CardContent>
            <BarList
              data={geo.map((g) => ({ name: g.region, value: g.orders }))}
              maxItems={8}
            />
          </CardContent>
        </Card>
      </div>

      <Card className="mt-4 p-0">
        <CardHeader className="flex-row items-center justify-between p-4 pb-0 sm:p-6 sm:pb-0">
          <CardTitle>Top buyers</CardTitle>
          <Tabs
            value={buyerFilter}
            onValueChange={(v) => setBuyerFilter(v as "all" | "repeat")}
          >
            <TabsList>
              <TabsTrigger value="all">All</TabsTrigger>
              <TabsTrigger value="repeat">Repeat only</TabsTrigger>
            </TabsList>
          </Tabs>
        </CardHeader>
        <div className="overflow-x-auto p-2">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Buyer</TableHead>
                <TableHead>Tier</TableHead>
                <TableHead className="text-right">Orders</TableHead>
                <TableHead className="hidden text-right sm:table-cell">Units</TableHead>
                <TableHead className="text-right">Lifetime net</TableHead>
                <TableHead className="hidden text-right md:table-cell">Avg order</TableHead>
                <TableHead className="hidden lg:table-cell">Last order</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {visibleBuyers.map((b) => (
                <TableRow key={b.key}>
                  <TableCell className="max-w-[160px] truncate font-medium sm:max-w-none">
                    {b.name}
                    {b.location && (
                      <span className="ml-2 hidden text-xs text-muted-foreground xl:inline">
                        {b.location}
                      </span>
                    )}
                  </TableCell>
                  <TableCell>
                    <Badge
                      variant={b.tier === "VIP" ? "default" : "secondary"}
                      className={
                        b.tier === "Returning"
                          ? "bg-blue-100 text-blue-700 dark:bg-blue-950 dark:text-blue-300"
                          : undefined
                      }
                    >
                      {b.tier}
                    </Badge>
                  </TableCell>
                  <TableCell className="text-right tabular-nums">{b.orders}</TableCell>
                  <TableCell className="hidden text-right tabular-nums sm:table-cell">
                    {b.units}
                  </TableCell>
                  <TableCell className="text-right tabular-nums font-medium">
                    {formatMoneyCompact(b.totalNet)}
                  </TableCell>
                  <TableCell className="hidden text-right tabular-nums md:table-cell">
                    {formatMoney(b.avgOrderValue)}
                  </TableCell>
                  <TableCell className="hidden text-muted-foreground lg:table-cell">
                    {formatDate(b.lastOrder)}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </Card>

      <div className="mt-4 grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Coupon effectiveness</CardTitle>
            <CardDescription>
              {formatNumber(coupons.withCoupon.orders)} of{" "}
              {formatNumber(coupons.withCoupon.orders + coupons.withoutCoupon.orders)} orders
              used a coupon · {formatMoneyCompact(coupons.totalDiscountCost)} given in discounts
            </CardDescription>
          </CardHeader>
          <CardContent className="grid gap-4">
            <div className="grid grid-cols-2 gap-2">
              <div className="rounded-lg border p-3">
                <p className="text-xs text-muted-foreground">Avg net (with coupon)</p>
                <p className="text-lg font-semibold tabular-nums">
                  {formatMoney(coupons.withCoupon.avgNet)}
                </p>
                <p className="text-xs text-muted-foreground">
                  {coupons.withCoupon.avgUnits != null
                    ? `${coupons.withCoupon.avgUnits.toFixed(1)} units/order`
                    : ""}
                </p>
              </div>
              <div className="rounded-lg border p-3">
                <p className="text-xs text-muted-foreground">Avg net (no coupon)</p>
                <p className="text-lg font-semibold tabular-nums">
                  {formatMoney(coupons.withoutCoupon.avgNet)}
                </p>
                <p className="text-xs text-muted-foreground">
                  {coupons.withoutCoupon.avgUnits != null
                    ? `${coupons.withoutCoupon.avgUnits.toFixed(1)} units/order`
                    : ""}
                </p>
              </div>
            </div>
            {coupons.coupons.length > 0 && (
              <div className="grid gap-1.5">
                {coupons.coupons.slice(0, 6).map((c) => (
                  <div key={c.code} className="flex items-center justify-between text-sm">
                    <Badge variant="secondary">{c.code}</Badge>
                    <span className="text-muted-foreground">
                      {c.orders} orders · {formatMoney(c.totalDiscount)} discounted
                    </span>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Fulfillment speed</CardTitle>
            <CardDescription>
              {fulfillment.ordersMeasured > 0
                ? `Median ${fulfillment.medianDays?.toFixed(0)} day(s) to ship · ${formatPercent(
                    fulfillment.sameOrNextDayRate
                  )} shipped within 1 day`
                : "No paid + shipped date pairs in this range"}
            </CardDescription>
          </CardHeader>
          <CardContent>
            <SimpleBarChart
              data={fulfillment.histogram}
              dataKey="count"
              name="Orders"
              labelKey="bucket"
            />
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
