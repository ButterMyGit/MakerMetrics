"use client";

import { useMemo, useState } from "react";
import { ArrowDown, ArrowUp, Download, Search } from "lucide-react";
import { useSalesData } from "@/hooks/use-sales-data";
import { useProfile } from "@/hooks/use-profile";
import { usePrivacy } from "@/hooks/use-privacy";
import { initialParam } from "@/hooks/use-media-query";
import {
  computeKpis,
  couponAnalysis,
  fulfillmentStats,
  type Order,
} from "@/lib/analytics/core";
import { EmptyRange, PageHeader } from "@/components/page-header";
import { KpiCard } from "@/components/kpi-card";
import { InfoTip } from "@/components/info-tip";
import { OrderDetail } from "@/components/orders/order-detail";
import { EmptyState, LoadingState } from "@/components/empty-state";
import { SimpleBarChart } from "@/components/charts";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { SidePanel, useWidePanel } from "@/components/side-panel";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
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
import { cn } from "@/lib/utils";

const PAGE_SIZE = 50;

type SortKey = "date" | "buyer" | "units" | "revenue" | "net";
type Filter = "all" | "coupon" | "nocoupon" | "refunded" | "unshipped" | "multi";

const FILTER_LABELS: Record<Filter, string> = {
  all: "All orders",
  coupon: "Used a coupon",
  nocoupon: "No coupon",
  refunded: "Refunded",
  unshipped: "Not shipped",
  multi: "Multiple items",
};

function matchesFilter(o: Order, filter: Filter): boolean {
  switch (filter) {
    case "coupon":
      return o.coupons.length > 0;
    case "nocoupon":
      return o.coupons.length === 0;
    case "refunded":
      return o.money.refund > 0;
    case "unshipped":
      return !o.dateShipped && !o.cancelled;
    case "multi":
      return o.units > 1;
    default:
      return true;
  }
}

function sortValue(o: Order, key: SortKey): string | number {
  switch (key) {
    case "buyer":
      return o.buyerName.toLowerCase();
    case "units":
      return o.units;
    case "revenue":
      return o.money.revenue;
    case "net":
      return o.money.netAfterFees;
    default:
      return o.saleDate ?? "";
  }
}

function SortHead({
  k,
  label,
  right,
  sort,
  onSort,
}: {
  k: SortKey;
  label: string;
  right?: boolean;
  sort: { key: SortKey; dir: 1 | -1 };
  onSort: (key: SortKey) => void;
}) {
  return (
    <TableHead className={cn(right && "text-right")}>
      <button
        type="button"
        onClick={() => onSort(k)}
        className={cn("inline-flex items-center gap-1 hover:text-foreground", right && "flex-row-reverse")}
      >
        {label}
        {sort.key === k &&
          (sort.dir === 1 ? <ArrowUp className="size-3" /> : <ArrowDown className="size-3" />)}
      </button>
    </TableHead>
  );
}

function OrdersTab() {
  const { orders, allOrders } = useSalesData();
  const privacy = usePrivacy();
  const wide = useWidePanel();
  const [query, setQuery] = useState(() => initialParam("q") ?? "");
  const [filter, setFilter] = useState<Filter>(() => {
    const f = initialParam("filter");
    return f && f in FILTER_LABELS ? (f as Filter) : "all";
  });
  const [sort, setSort] = useState<{ key: SortKey; dir: 1 | -1 }>({ key: "date", dir: -1 });
  const [page, setPage] = useState(0);
  const [selectedKey, setSelectedKey] = useState<string | null>(() => initialParam("order"));

  const buyerCounts = useMemo(() => {
    const counts = new Map<string, number>();
    for (const o of allOrders) {
      if (o.buyerKey) counts.set(o.buyerKey, (counts.get(o.buyerKey) ?? 0) + 1);
    }
    return counts;
  }, [allOrders]);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    const list = orders.filter((o) => {
      if (!matchesFilter(o, filter)) return false;
      if (!q) return true;
      return (
        o.key.toLowerCase().includes(q) ||
        o.buyerName.toLowerCase().includes(q) ||
        o.coupons.some((c) => c.toLowerCase().includes(q)) ||
        (o.location ?? "").toLowerCase().includes(q) ||
        o.rows.some(
          (r) =>
            r.itemName?.toLowerCase().includes(q) ||
            r.style?.toLowerCase().includes(q) ||
            r.sku?.toLowerCase().includes(q)
        )
      );
    });
    return list.sort((a, b) => {
      const av = sortValue(a, sort.key);
      const bv = sortValue(b, sort.key);
      return (av < bv ? -1 : av > bv ? 1 : 0) * sort.dir;
    });
  }, [orders, query, filter, sort]);

  // An order opened from a link may be outside the selected range.
  const selected = useMemo(
    () =>
      selectedKey
        ? (filtered.find((o) => o.key === selectedKey) ??
          allOrders.find((o) => o.key === selectedKey) ??
          null)
        : null,
    [selectedKey, filtered, allOrders]
  );
  const selectedIndex = selected ? filtered.findIndex((o) => o.key === selected.key) : -1;

  const pageCount = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const visible = filtered.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  function toggleSort(key: SortKey) {
    setSort((s) => (s.key === key ? { key, dir: s.dir === 1 ? -1 : 1 } : { key, dir: key === "buyer" ? 1 : -1 }));
    setPage(0);
  }

  function exportCsv() {
    downloadCsv(
      "makermetrics-orders.csv",
      [
        "Order ID", "Sale date", "Buyer", "Items", "Units", "Coupons", "Ships to",
        "Item subtotal", "Discount", "Shipping charged", "Revenue", "Sales tax", "Buyer paid",
        "Processing fee", "Refunded", "Est. transaction fee", "Est. listing fee", "Net after Etsy fees",
        "Date paid", "Date shipped",
      ],
      filtered.map((o) => [
        o.orderId, o.saleDate, o.buyerName, o.itemsLabel, o.units, o.coupons.join(" "), o.location,
        o.money.itemSubtotal, o.money.discount, o.money.shipping, o.money.revenue, o.money.tax,
        o.money.buyerPaid, o.money.processingFee, o.money.refund, o.money.transactionFeeEst,
        o.money.listingFeeEst, o.money.netAfterFees, o.datePaid, o.dateShipped,
      ])
    );
  }

  const detail = selected && (
    <OrderDetail
      order={selected}
      buyerOrderCount={selected.buyerKey ? (buyerCounts.get(selected.buyerKey) ?? 1) : 1}
      onClose={wide ? () => setSelectedKey(null) : undefined}
      onPrev={selectedIndex > 0 ? () => setSelectedKey(filtered[selectedIndex - 1].key) : undefined}
      onNext={
        selectedIndex >= 0 && selectedIndex < filtered.length - 1
          ? () => setSelectedKey(filtered[selectedIndex + 1].key)
          : undefined
      }
    />
  );

  return (
    <div className="flex items-start gap-4">
      <div className="min-w-0 flex-1">
        <div className="mb-3 flex flex-wrap items-center gap-2">
          <div className="relative min-w-48 flex-1">
            <Search className="absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              value={query}
              onChange={(e) => {
                setQuery(e.target.value);
                setPage(0);
              }}
              placeholder="Search buyer, item, order number, SKU, coupon, location…"
              className="pl-9"
            />
          </div>
          <Select
            value={filter}
            onValueChange={(v) => {
              setFilter(v as Filter);
              setPage(0);
            }}
          >
            <SelectTrigger className="w-44" aria-label="Filter orders">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {Object.entries(FILTER_LABELS).map(([value, label]) => (
                <SelectItem key={value} value={value}>
                  {label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Button variant="outline" onClick={exportCsv} disabled={filtered.length === 0}>
            <Download className="size-4" />
            <span className="hidden sm:inline">Export</span>
          </Button>
        </div>

        <p className="mb-2 text-xs text-muted-foreground">
          {formatNumber(filtered.length)} order{filtered.length === 1 ? "" : "s"} ·{" "}
          {formatMoney(filtered.reduce((s, o) => s + (o.cancelled ? 0 : o.money.revenue), 0))} revenue
        </p>

        {filtered.length === 0 ? (
          orders.length === 0 ? (
            <EmptyRange />
          ) : (
            <Card className="px-4 py-12 text-center text-sm text-muted-foreground">
              No orders match.
            </Card>
          )
        ) : (
          <>
            {/* Desktop: dense, sortable table */}
            <Card className="hidden p-0 md:block">
              <Table>
                <TableHeader>
                  <TableRow>
                    <SortHead k="date" label="Date" sort={sort} onSort={toggleSort} />
                    <SortHead k="buyer" label="Buyer" sort={sort} onSort={toggleSort} />
                    <TableHead>Items</TableHead>
                    <SortHead k="units" label="Units" right sort={sort} onSort={toggleSort} />
                    <TableHead>Coupon</TableHead>
                    <SortHead k="revenue" label="Revenue" right sort={sort} onSort={toggleSort} />
                    <SortHead k="net" label="Net after fees" right sort={sort} onSort={toggleSort} />
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {visible.map((o) => (
                    <TableRow
                      key={o.key}
                      onClick={() => setSelectedKey((k) => (k === o.key ? null : o.key))}
                      data-state={selected?.key === o.key ? "selected" : undefined}
                      className="cursor-pointer"
                    >
                      <TableCell className="whitespace-nowrap text-muted-foreground">
                        {formatDate(o.saleDate)}
                      </TableCell>
                      <TableCell className="max-w-40 truncate font-medium">
                        {privacy.name(o.buyerName)}
                      </TableCell>
                      <TableCell className="max-w-64 truncate">
                        {o.itemsLabel}
                        {o.money.refund > 0 && (
                          <Badge className="ml-2 bg-amber-100 text-amber-700 dark:bg-amber-950 dark:text-amber-300">
                            {o.cancelled ? "Refunded" : "Part refund"}
                          </Badge>
                        )}
                      </TableCell>
                      <TableCell className="text-right tabular-nums">{o.units}</TableCell>
                      <TableCell className="max-w-32 truncate text-muted-foreground">
                        {o.coupons.join(", ")}
                      </TableCell>
                      <TableCell className="text-right tabular-nums">
                        {formatMoney(o.money.revenue)}
                      </TableCell>
                      <TableCell className="text-right font-medium tabular-nums">
                        {formatMoney(o.money.netAfterFees)}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </Card>

            {/* Phone: compact cards */}
            <Card className="divide-y p-0 md:hidden">
              {visible.map((o) => (
                <button
                  key={o.key}
                  onClick={() => setSelectedKey((k) => (k === o.key ? null : o.key))}
                  className="flex w-full items-center justify-between gap-3 px-4 py-3 text-left transition-colors hover:bg-accent/50"
                >
                  <div className="min-w-0">
                    <p className="truncate text-sm font-medium">{privacy.name(o.buyerName)}</p>
                    <p className="mt-0.5 truncate text-xs text-muted-foreground">{o.itemsLabel}</p>
                  </div>
                  <div className="shrink-0 text-right">
                    <p className="text-sm font-semibold tabular-nums">{formatMoney(o.money.revenue)}</p>
                    <p className="text-xs text-muted-foreground">{formatDate(o.saleDate)}</p>
                  </div>
                </button>
              ))}
            </Card>
          </>
        )}

        {pageCount > 1 && (
          <div className="mt-4 flex items-center justify-between">
            <Button variant="outline" size="sm" disabled={page === 0} onClick={() => setPage((p) => p - 1)}>
              Previous
            </Button>
            <span className="text-sm text-muted-foreground">
              Page {page + 1} of {pageCount}
            </span>
            <Button
              variant="outline"
              size="sm"
              disabled={page >= pageCount - 1}
              onClick={() => setPage((p) => p + 1)}
            >
              Next
            </Button>
          </div>
        )}
      </div>

      <SidePanel open={selected !== null} onClose={() => setSelectedKey(null)} title="Order details">
        {detail}
      </SidePanel>
    </div>
  );
}

function FulfillmentTab() {
  const { orders } = useSalesData();
  const { prefs, updatePrefs } = useProfile();
  const stats = useMemo(() => fulfillmentStats(orders, prefs.processingDays), [orders, prefs.processingDays]);
  const unshipped = useMemo(() => orders.filter((o) => !o.dateShipped && !o.cancelled).length, [orders]);
  const [draft, setDraft] = useState(prefs.processingDays != null ? String(prefs.processingDays) : "");

  if (orders.length === 0) return <EmptyRange />;

  return (
    <div className="grid gap-4">
      <Card>
        <CardHeader>
          <CardTitle>Your processing time</CardTitle>
          <CardDescription>
            The number of business days you promise to ship within (the setting on your Etsy
            shipping profiles). On-time shipping is measured against it.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <form
            className="flex items-center gap-2"
            onSubmit={(e) => {
              e.preventDefault();
              const n = Number(draft);
              updatePrefs({ processingDays: draft.trim() === "" || !Number.isFinite(n) ? null : n });
            }}
          >
            <Input
              type="number"
              min={0}
              max={90}
              value={draft}
              onChange={(e) => setDraft(e.target.value)}
              placeholder="e.g. 3"
              className="w-24"
              aria-label="Processing time in business days"
            />
            <span className="text-sm text-muted-foreground">business days</span>
            <Button type="submit" variant="outline" size="sm">
              Save
            </Button>
          </form>
        </CardContent>
      </Card>

      <div className="stagger grid grid-cols-2 gap-3 lg:grid-cols-4">
        <KpiCard
          label="Shipped on time"
          metric="onTimeRate"
          value={stats.onTimeRate != null ? formatPercent(stats.onTimeRate, 0) : "—"}
          hint={
            stats.onTimeRate != null
              ? `${stats.lateOrders} late of ${stats.ordersMeasured} · Star Seller needs 95%`
              : "Set your processing time above"
          }
        />
        <KpiCard
          label="Typical days to ship"
          value={stats.medianDays != null ? String(stats.medianDays) : "—"}
          hint="Median, payment to shipping"
        />
        <KpiCard
          label="Slowest 10%"
          value={stats.p90Days != null ? `${stats.p90Days}+ days` : "—"}
          hint="9 in 10 orders ship faster"
        />
        <KpiCard label="Not shipped yet" value={formatNumber(unshipped)} hint="As of your latest export" />
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>{stats.onTimeRate != null ? "On-time rate by month" : "Days to ship by month"}</CardTitle>
            <CardDescription>
              {stats.onTimeRate != null
                ? "Share of each month's orders shipped within your processing time"
                : "Median days from payment to shipping"}
            </CardDescription>
          </CardHeader>
          <CardContent>
            <SimpleBarChart
              data={stats.monthly}
              dataKey={stats.onTimeRate != null ? "onTimeRate" : "medianDays"}
              name={stats.onTimeRate != null ? "On time" : "Median days"}
              labelKey="month"
              monthLabels
              percent={stats.onTimeRate != null}
              height={240}
            />
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>How fast orders ship</CardTitle>
            <CardDescription>Calendar days from payment to shipping</CardDescription>
          </CardHeader>
          <CardContent>
            <SimpleBarChart data={stats.histogram} dataKey="count" name="Orders" labelKey="bucket" height={240} />
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

function CouponsTab() {
  const { orders, allOrders } = useSalesData();
  const analysis = useMemo(() => couponAnalysis(orders, allOrders), [orders, allOrders]);
  const kpis = useMemo(() => computeKpis(orders, allOrders), [orders, allOrders]);

  if (orders.length === 0) return <EmptyRange />;

  const fullPrice = kpis.revenue + analysis.totalDiscountCost;

  return (
    <div className="grid gap-4">
      <div className="stagger grid grid-cols-2 gap-3 lg:grid-cols-4">
        <KpiCard
          label="Orders using a coupon"
          value={formatPercent(analysis.couponOrderShare, 0)}
          hint={`${analysis.withCoupon.orders} of ${kpis.orders} orders`}
        />
        <KpiCard
          label="Discounts given"
          metric="discounts"
          value={formatMoney(analysis.totalDiscountCost)}
          hint={fullPrice > 0 ? `${formatPercent(analysis.totalDiscountCost / fullPrice, 0)} off full price` : undefined}
        />
        <KpiCard
          label="Avg order with coupon"
          value={formatMoney(analysis.withCoupon.avgRevenue)}
          hint={analysis.withCoupon.avgUnits != null ? `${analysis.withCoupon.avgUnits.toFixed(2)} units per order` : undefined}
        />
        <KpiCard
          label="Avg order without"
          value={formatMoney(analysis.withoutCoupon.avgRevenue)}
          hint={analysis.withoutCoupon.avgUnits != null ? `${analysis.withoutCoupon.avgUnits.toFixed(2)} units per order` : undefined}
        />
      </div>

      <Card className="p-0">
        <CardHeader className="p-4 pb-0">
          <CardTitle>Every code</CardTitle>
          <CardDescription>
            Order size barely tells you whether a coupon worked. The last column compares how busy
            the whole shop was while each code was in use with the days just before it.
          </CardDescription>
        </CardHeader>
        <div className="overflow-x-auto p-2">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Code</TableHead>
                <TableHead>In use</TableHead>
                <TableHead className="text-right">Orders</TableHead>
                <TableHead className="text-right">Discount given</TableHead>
                <TableHead className="text-right">Revenue</TableHead>
                <TableHead className="text-right">
                  <span className="inline-flex items-center gap-1">
                    Orders while active <InfoTip metric="couponLift" />
                  </span>
                </TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {analysis.coupons.map((c) => (
                <TableRow key={c.code}>
                  <TableCell className="font-medium">{c.code}</TableCell>
                  <TableCell className="whitespace-nowrap text-muted-foreground">
                    {c.firstUsed === c.lastUsed
                      ? formatDate(c.firstUsed)
                      : `${formatDate(c.firstUsed)} – ${formatDate(c.lastUsed)}`}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">{c.orders}</TableCell>
                  <TableCell className="text-right tabular-nums">{formatMoney(c.totalDiscount)}</TableCell>
                  <TableCell className="text-right tabular-nums">{formatMoney(c.revenue)}</TableCell>
                  <TableCell className="text-right tabular-nums">
                    {c.lift == null ? (
                      <span className="text-muted-foreground">Too little data</span>
                    ) : (
                      <span
                        className={cn(
                          c.lift >= 1.15 && "text-emerald-600 dark:text-emerald-400",
                          c.lift <= 0.85 && "text-red-600 dark:text-red-400"
                        )}
                      >
                        {c.lift >= 1 ? "+" : "−"}
                        {formatPercent(Math.abs(c.lift - 1), 0)} vs before
                      </span>
                    )}
                  </TableCell>
                </TableRow>
              ))}
              {analysis.coupons.length === 0 && (
                <TableRow>
                  <TableCell colSpan={6} className="py-8 text-center text-muted-foreground">
                    No coupons were used in this period.
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </div>
      </Card>
    </div>
  );
}

export default function OrdersPage() {
  const { loading, hasData } = useSalesData();
  const [tab, setTab] = useState(() => {
    const t = initialParam("tab");
    return t === "fulfillment" || t === "coupons" ? t : "orders";
  });

  if (loading) return <LoadingState />;
  if (!hasData) return <EmptyState />;

  return (
    <div>
      <PageHeader title="Orders" />
      <Tabs value={tab} onValueChange={setTab}>
        <TabsList className="mb-4">
          <TabsTrigger value="orders">Orders</TabsTrigger>
          <TabsTrigger value="fulfillment">Fulfillment</TabsTrigger>
          <TabsTrigger value="coupons">Coupons</TabsTrigger>
        </TabsList>
        <TabsContent value="orders">
          <OrdersTab />
        </TabsContent>
        <TabsContent value="fulfillment">
          <FulfillmentTab />
        </TabsContent>
        <TabsContent value="coupons">
          <CouponsTab />
        </TabsContent>
      </Tabs>
    </div>
  );
}
