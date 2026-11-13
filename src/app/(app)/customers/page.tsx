"use client";

import { useMemo, useState } from "react";
import Link from "next/link";
import { Download } from "lucide-react";
import { useSalesData } from "@/hooks/use-sales-data";
import { usePrivacy } from "@/hooks/use-privacy";
import { initialParam } from "@/hooks/use-media-query";
import {
  buyerStats,
  computeKpis,
  customerKpis,
  geoBreakdown,
  type BuyerStat,
  type BuyerTier,
} from "@/lib/analytics/core";
import { EmptyRange, PageHeader } from "@/components/page-header";
import { KpiCard } from "@/components/kpi-card";
import { EmptyState, LoadingState } from "@/components/empty-state";
import { BarList, StackedMonthlyChart } from "@/components/charts";
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { PanelHeader, SidePanel } from "@/components/side-panel";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { downloadCsv } from "@/lib/export";
import { formatDate, formatMoney, formatNumber, formatPercent } from "@/lib/format";

const TIER_STYLE: Record<BuyerTier, string> = {
  Returning: "bg-emerald-100 text-emerald-700 dark:bg-emerald-950 dark:text-emerald-300",
  New: "bg-blue-100 text-blue-700 dark:bg-blue-950 dark:text-blue-300",
  "One-time": "bg-muted text-muted-foreground",
};

const TIER_HELP = "Returning: 2 or more orders · New: first order in the last 30 days · One-time: a single order, longer ago";

function BuyerDetailPanel({ buyer, onClose }: { buyer: BuyerStat; onClose: () => void }) {
  const { allOrders } = useSalesData();
  const privacy = usePrivacy();
  const history = useMemo(
    () => allOrders.filter((o) => o.buyerKey === buyer.key).reverse(),
    [allOrders, buyer]
  );

  return (
    <div className="grid grid-cols-[minmax(0,1fr)] gap-4">
      <PanelHeader
        title={privacy.name(buyer.name)}
        subtitle={
          <>
            {privacy.location(buyer.location) ?? "Location unknown"} · customer since{" "}
            {formatDate(buyer.firstOrder)}
          </>
        }
        onClose={onClose}
      />
      <div className="flex items-center gap-2">
        <Badge className={TIER_STYLE[buyer.tier]}>{buyer.tier}</Badge>
        {buyer.usedCoupon && <Badge variant="secondary">Uses coupons</Badge>}
      </div>
      <dl className="grid grid-cols-3 gap-3 text-sm">
        <div>
          <dt className="text-xs text-muted-foreground">Orders</dt>
          <dd className="font-semibold tabular-nums">{buyer.orders}</dd>
        </div>
        <div>
          <dt className="text-xs text-muted-foreground">Revenue</dt>
          <dd className="font-semibold tabular-nums">{formatMoney(buyer.revenue)}</dd>
        </div>
        <div>
          <dt className="text-xs text-muted-foreground">Avg order</dt>
          <dd className="font-semibold tabular-nums">{formatMoney(buyer.avgOrderValue)}</dd>
        </div>
      </dl>
      <div>
        <h3 className="mb-2 text-sm font-medium">Order history</h3>
        <ul className="grid grid-cols-[minmax(0,1fr)] gap-2">
          {history.map((o) => (
            <li key={o.key}>
              <Link
                href={`/orders?order=${encodeURIComponent(o.key)}`}
                className="block rounded-lg border p-3 transition-colors hover:bg-accent/50"
              >
                <div className="flex justify-between gap-3 text-sm">
                  <span className="font-medium">{formatDate(o.saleDate)}</span>
                  <span className="tabular-nums">{formatMoney(o.money.revenue)}</span>
                </div>
                <p className="mt-0.5 truncate text-xs text-muted-foreground">
                  {o.itemsLabel}
                  {o.coupons.length > 0 ? ` · ${o.coupons.join(", ")}` : ""}
                  {o.money.refund > 0 ? " · refunded" : ""}
                </p>
              </Link>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}

export default function CustomersPage() {
  const { orders, allOrders, loading, hasData, through, range } = useSalesData();
  const privacy = usePrivacy();
  const [geoMode, setGeoMode] = useState<"state" | "country">("state");
  const [buyerFilter, setBuyerFilter] = useState<"all" | "repeat">("all");
  const [showAll, setShowAll] = useState(false);
  const [selectedKey, setSelectedKey] = useState<string | null>(() => initialParam("buyer"));

  const kpis = useMemo(() => customerKpis(orders), [orders]);
  const buyers = useMemo(() => buyerStats(orders, through, allOrders), [orders, through, allOrders]);
  const shop = useMemo(() => computeKpis(orders, allOrders), [orders, allOrders]);
  const geo = useMemo(() => geoBreakdown(orders, geoMode), [orders, geoMode]);

  const listed = useMemo(
    () => (buyerFilter === "repeat" ? buyers.filter((b) => b.tier === "Returning") : buyers),
    [buyers, buyerFilter]
  );
  const selected = useMemo(() => {
    if (!selectedKey) return null;
    return (
      buyers.find((b) => b.key === selectedKey) ??
      buyerStats(allOrders, through).find((b) => b.key === selectedKey) ??
      null
    );
  }, [selectedKey, buyers, allOrders, through]);

  if (loading) return <LoadingState />;
  if (!hasData) return <EmptyState />;

  const visible = showAll ? listed : listed.slice(0, 10);

  return (
    <div>
      <PageHeader title="Customers" description={`Buyers who ordered in ${range.label.toLowerCase()}`} />

      {orders.length === 0 ? (
        <EmptyRange />
      ) : (
        <div className="flex items-start gap-4">
        <div className="grid min-w-0 flex-1 gap-4">
          <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
            <KpiCard label="Buyers" value={formatNumber(kpis.uniqueBuyers)} />
            <KpiCard
              label="Repeat buyers"
              metric="repeatBuyerRate"
              value={formatPercent(shop.repeatBuyerRate)}
              hint={`${formatNumber(shop.repeatBuyers)} of ${formatNumber(shop.uniqueBuyers)} buyers`}
            />
            <KpiCard
              label="Avg revenue per buyer"
              metric="lifetimeValue"
              value={formatMoney(kpis.avgLifetimeValue)}
            />
            <KpiCard
              label="Typical time to reorder"
              metric="reorderGap"
              value={
                kpis.medianDaysBetweenOrders != null
                  ? `${Math.round(kpis.medianDaysBetweenOrders)} days`
                  : "—"
              }
            />
          </div>

          <div className="grid gap-4 lg:grid-cols-2 [&>*]:min-w-0">
            <Card>
              <CardHeader>
                <CardTitle>New vs returning buyers</CardTitle>
                <CardDescription>Buyers each month, split by whether it was their first order</CardDescription>
              </CardHeader>
              <CardContent>
                <StackedMonthlyChart
                  data={kpis.newVsReturning}
                  series={[
                    { dataKey: "newBuyers", name: "First order" },
                    { dataKey: "returningBuyers", name: "Returning" },
                  ]}
                />
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Where orders ship</CardTitle>
                <CardDescription>Orders by {geoMode === "state" ? "US state" : "country"}</CardDescription>
                <CardAction>
                  <Tabs value={geoMode} onValueChange={(v) => setGeoMode(v as "state" | "country")}>
                    <TabsList>
                      <TabsTrigger value="state">US states</TabsTrigger>
                      <TabsTrigger value="country">Countries</TabsTrigger>
                    </TabsList>
                  </Tabs>
                </CardAction>
              </CardHeader>
              <CardContent>
                <BarList
                  data={geo.map((g) => ({ name: g.region, value: g.orders, sub: formatMoney(g.revenue) }))}
                  maxItems={8}
                />
              </CardContent>
            </Card>
          </div>

          <Card className="p-0">
            <CardHeader className="p-4 pb-0">
              <CardTitle>Top buyers</CardTitle>
              <CardDescription title={TIER_HELP}>By revenue. {TIER_HELP}.</CardDescription>
              <CardAction className="flex items-center gap-2">
                <Tabs
                  value={buyerFilter}
                  onValueChange={(v) => {
                    setBuyerFilter(v as "all" | "repeat");
                    setShowAll(false);
                  }}
                >
                  <TabsList>
                    <TabsTrigger value="all">All</TabsTrigger>
                    <TabsTrigger value="repeat">Repeat</TabsTrigger>
                  </TabsList>
                </Tabs>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() =>
                    downloadCsv(
                      "makermetrics-buyers.csv",
                      ["Buyer", "Location", "Tier", "Orders", "Units", "Revenue", "Avg order", "First order", "Last order", "Used coupon"],
                      listed.map((b) => [
                        b.name, b.location, b.tier, b.orders, b.units, b.revenue,
                        Math.round(b.avgOrderValue * 100) / 100, b.firstOrder, b.lastOrder,
                        b.usedCoupon ? "yes" : "no",
                      ])
                    )
                  }
                >
                  <Download className="size-4" />
                  <span className="hidden sm:inline">Export</span>
                </Button>
              </CardAction>
            </CardHeader>
            <div className="overflow-x-auto p-2">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Buyer</TableHead>
                    <TableHead className="hidden md:table-cell">Location</TableHead>
                    <TableHead>Tier</TableHead>
                    <TableHead className="text-right">Orders</TableHead>
                    <TableHead className="text-right">Revenue</TableHead>
                    <TableHead className="hidden text-right sm:table-cell">Last order</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {visible.map((b) => (
                    <TableRow key={b.key} data-state={selected?.key === b.key ? "selected" : undefined} className="cursor-pointer" onClick={() => setSelectedKey((k) => (k === b.key ? null : b.key))}>
                      <TableCell className="max-w-[160px] truncate font-medium sm:max-w-none">
                        {privacy.name(b.name)}
                      </TableCell>
                      <TableCell className="hidden text-muted-foreground md:table-cell">
                        {privacy.location(b.location) ?? "—"}
                      </TableCell>
                      <TableCell>
                        <Badge className={TIER_STYLE[b.tier]}>{b.tier}</Badge>
                      </TableCell>
                      <TableCell className="text-right tabular-nums">{b.orders}</TableCell>
                      <TableCell className="text-right tabular-nums">{formatMoney(b.revenue)}</TableCell>
                      <TableCell className="hidden whitespace-nowrap text-right text-muted-foreground sm:table-cell">
                        {formatDate(b.lastOrder)}
                      </TableCell>
                    </TableRow>
                  ))}
                  {listed.length === 0 && (
                    <TableRow>
                      <TableCell colSpan={6} className="py-8 text-center text-muted-foreground">
                        No repeat buyers in this period yet.
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
              {!showAll && listed.length > 10 && (
                <div className="p-2 text-center">
                  <Button variant="ghost" size="sm" onClick={() => setShowAll(true)}>
                    View all {formatNumber(listed.length)} buyers
                  </Button>
                </div>
              )}
            </div>
          </Card>
        </div>
        <SidePanel open={selected !== null} onClose={() => setSelectedKey(null)} title="Buyer details">
          {selected && <BuyerDetailPanel buyer={selected} onClose={() => setSelectedKey(null)} />}
        </SidePanel>
        </div>
      )}

    </div>
  );
}
