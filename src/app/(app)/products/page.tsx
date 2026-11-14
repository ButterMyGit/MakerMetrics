"use client";

import { useMemo, useState } from "react";
import { ArrowDown, ArrowUp, Download, ExternalLink, Search } from "lucide-react";
import { useSalesData } from "@/hooks/use-sales-data";
import { initialParam } from "@/hooks/use-media-query";
import {
  productDetail,
  productStats,
  variationBreakdown,
  type ProductStat,
  type ProductTrend,
} from "@/lib/analytics/core";
import { EmptyRange, PageHeader } from "@/components/page-header";
import { InfoTip } from "@/components/info-tip";
import { EmptyState, LoadingState } from "@/components/empty-state";
import { BarList, DonutChart, SimpleBarChart, Sparkline } from "@/components/charts";
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { PanelHeader, SidePanel, useWidePanel } from "@/components/side-panel";
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

type SortKey = "name" | "units" | "itemRevenue" | "avgPrice" | "lastSold" | "change";

const TREND_STYLE: Record<NonNullable<ProductTrend>, { label: string; className: string }> = {
  rising: { label: "Rising", className: "bg-emerald-100 text-emerald-700 dark:bg-emerald-950 dark:text-emerald-300" },
  falling: { label: "Falling", className: "bg-red-100 text-red-700 dark:bg-red-950 dark:text-red-300" },
  new: { label: "New", className: "bg-blue-100 text-blue-700 dark:bg-blue-950 dark:text-blue-300" },
  dormant: { label: "Dormant", className: "bg-muted text-muted-foreground" },
};

function TrendBadge({ trend }: { trend: ProductTrend }) {
  if (!trend) return null;
  const t = TREND_STYLE[trend];
  return <Badge className={t.className}>{t.label}</Badge>;
}

function sortValue(p: ProductStat, key: SortKey): string | number {
  switch (key) {
    case "name":
      return p.name.toLowerCase();
    case "avgPrice":
      return p.avgPrice ?? 0;
    case "lastSold":
      return p.lastSold ?? "";
    case "change":
      return p.recentUnits - p.priorUnits;
    default:
      return p[key];
  }
}

function ProductDetailPanel({ product, onClose }: { product: ProductStat; onClose: () => void }) {
  const { orders } = useSalesData();
  const detail = useMemo(
    () => productDetail(orders, product.key),
    [orders, product]
  );

  return (
    <div className="grid grid-cols-[minmax(0,1fr)] gap-5">
      <PanelHeader title={product.name} subtitle={product.title ?? undefined} onClose={onClose} />
      <div className="flex flex-wrap items-center gap-2">
        <TrendBadge trend={product.trend} />
        <span className="text-sm text-muted-foreground">
          {product.priorUnits} → {product.recentUnits} units (previous 90 days → last 90)
        </span>
      </div>

      <dl className="grid grid-cols-2 gap-x-4 gap-y-3 text-sm sm:grid-cols-3">
        <div>
          <dt className="text-xs text-muted-foreground">Units</dt>
          <dd className="font-semibold tabular-nums">
            {formatNumber(product.units)}
            <span className="ml-1 text-xs font-normal text-muted-foreground">
              in {formatNumber(product.orders)} orders
            </span>
          </dd>
        </div>
        <div>
          <dt className="flex items-center gap-1 text-xs text-muted-foreground">
            Item sales <InfoTip metric="itemRevenue" />
          </dt>
          <dd className="font-semibold tabular-nums">{formatMoney(product.itemRevenue)}</dd>
        </div>
        <div>
          <dt className="text-xs text-muted-foreground">Avg price</dt>
          <dd className="font-semibold tabular-nums">{formatMoney(product.avgPrice)}</dd>
        </div>
        <div>
          <dt className="text-xs text-muted-foreground">Buyers</dt>
          <dd className="font-semibold tabular-nums">
            {formatNumber(detail.buyers)}
            {detail.repeatBuyers > 0 && (
              <span className="ml-1 text-xs font-normal text-muted-foreground">
                {detail.repeatBuyers} bought it again
              </span>
            )}
          </dd>
        </div>
        <div>
          <dt className="text-xs text-muted-foreground">Orders with a coupon</dt>
          <dd className="font-semibold tabular-nums">{formatPercent(detail.couponOrderShare, 0)}</dd>
        </div>
        <div>
          <dt className="text-xs text-muted-foreground">Avg order discount</dt>
          <dd className="font-semibold tabular-nums">{formatPercent(detail.avgDiscountRate, 0)}</dd>
        </div>
        <div>
          <dt className="text-xs text-muted-foreground">First sold</dt>
          <dd>{formatDate(product.firstSold)}</dd>
        </div>
        <div>
          <dt className="text-xs text-muted-foreground">Last sold</dt>
          <dd>{formatDate(product.lastSold)}</dd>
        </div>
      </dl>

      <div>
        <h3 className="mb-2 text-sm font-medium">Units by month</h3>
        <SimpleBarChart
          data={detail.monthly}
          dataKey="units"
          name="Units"
          labelKey="month"
          monthLabels
          height={180}
        />
      </div>

      {detail.variations.map((v) => (
        <div key={v.name}>
          <h3 className="mb-2 text-sm font-medium">{v.name}</h3>
          <BarList data={v.values.map((x) => ({ name: x.value, value: x.units }))} maxItems={8} />
        </div>
      ))}

      {product.listingId && (
        <Button variant="outline" size="sm" className="w-fit" asChild>
          <a
            href={`https://www.etsy.com/listing/${encodeURIComponent(product.listingId)}`}
            target="_blank"
            rel="noopener noreferrer"
          >
            <ExternalLink className="size-3.5" /> Open listing on Etsy
          </a>
        </Button>
      )}
    </div>
  );
}

function SortHead({
  k,
  label,
  right,
  sort,
  onSort,
}: {
  k: SortKey;
  label: React.ReactNode;
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

export default function ProductsPage() {
  const { orders, allOrders, loading, hasData, through, range } = useSalesData();
  const [query, setQuery] = useState("");
  const [sort, setSort] = useState<{ key: SortKey; dir: 1 | -1 }>({ key: "units", dir: -1 });
  const [selectedKey, setSelectedKey] = useState<string | null>(() => initialParam("product"));
  const [variationName, setVariationName] = useState<string | null>(null);
  const [showAll, setShowAll] = useState(false);
  const wide = useWidePanel();

  const stats = useMemo(() => productStats(orders, through, allOrders), [orders, through, allOrders]);
  const variations = useMemo(() => variationBreakdown(orders), [orders]);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    const list = q
      ? stats.filter((p) => `${p.name} ${p.title ?? ""}`.toLowerCase().includes(q))
      : [...stats];
    return list.sort((a, b) => {
      const av = sortValue(a, sort.key);
      const bv = sortValue(b, sort.key);
      return (av < bv ? -1 : av > bv ? 1 : 0) * sort.dir;
    });
  }, [stats, query, sort]);

  // A product opened from search may not have sold in the selected range.
  const selected = useMemo(() => {
    if (!selectedKey) return null;
    return (
      stats.find((p) => p.key === selectedKey) ??
      productStats(allOrders, through).find((p) => p.key === selectedKey) ??
      null
    );
  }, [selectedKey, stats, allOrders, through]);

  if (loading) return <LoadingState />;
  if (!hasData) return <EmptyState />;

  const onSort = (key: SortKey) =>
    setSort((s) => (s.key === key ? { key, dir: s.dir === 1 ? -1 : 1 } : { key, dir: key === "name" ? 1 : -1 }));
  const activeVariation = variations.find((v) => v.name === variationName) ?? variations[0];
  const visible = showAll || query ? filtered : filtered.slice(0, 12);

  return (
    <div>
      <PageHeader
        title="Products"
        description={`${formatNumber(stats.length)} products sold in ${range.label.toLowerCase()}`}
      />

      {orders.length === 0 ? (
        <EmptyRange />
      ) : (
        <div className="flex flex-col gap-4 xl:flex-row xl:items-start">
          <div className="min-w-0 flex-1">
            <Card className="p-0">
              <div className="flex flex-wrap items-center gap-2 p-3 pb-0">
                <div className="relative min-w-48 flex-1">
                  <Search className="absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
                  <Input
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    placeholder="Search products…"
                    className="pl-9"
                  />
                </div>
                <Button
                  variant="outline"
                  onClick={() =>
                    downloadCsv(
                      "makermetrics-products.csv",
                      ["Product", "Listing ID", "Units", "Orders", "Item sales", "Avg price", "First sold", "Last sold", "Units previous 90 days", "Units last 90 days", "Trend"],
                      filtered.map((p) => [
                        p.name, p.listingId, p.units, p.orders, p.itemRevenue,
                        p.avgPrice != null ? Math.round(p.avgPrice * 100) / 100 : null,
                        p.firstSold, p.lastSold, p.priorUnits, p.recentUnits, p.trend ?? "",
                      ])
                    )
                  }
                >
                  <Download className="size-4" />
                  <span className="hidden sm:inline">Export</span>
                </Button>
              </div>
              <div className="overflow-x-auto p-2">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <SortHead k="name" label="Product" sort={sort} onSort={onSort} />
                      <TableHead className="hidden sm:table-cell">Last 6 months</TableHead>
                      <SortHead k="units" label="Units" right sort={sort} onSort={onSort} />
                      <SortHead
                        k="itemRevenue"
                        label="Item sales"
                        right
                        sort={sort}
                        onSort={onSort}
                      />
                      <SortHead k="avgPrice" label="Avg price" right sort={sort} onSort={onSort} />
                      <SortHead k="lastSold" label="Last sold" sort={sort} onSort={onSort} />
                      <SortHead
                        k="change"
                        label={
                          <span title="Units in the previous 90 days → units in the last 90 days">
                            90-day change
                          </span>
                        }
                        sort={sort}
                        onSort={onSort}
                      />
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {visible.map((p) => (
                      <TableRow key={p.key} data-state={selected?.key === p.key ? "selected" : undefined} className="cursor-pointer" onClick={() => setSelectedKey((k) => (k === p.key ? null : p.key))}>
                        <TableCell className="max-w-[220px] truncate font-medium lg:max-w-md">{p.name}</TableCell>
                        <TableCell className="hidden sm:table-cell">
                          <Sparkline values={p.spark} />
                        </TableCell>
                        <TableCell className="text-right tabular-nums">
                          {formatNumber(p.units)}
                          {p.units > p.orders && (
                            <span
                              className="ml-1 text-xs text-muted-foreground"
                              title={`${p.units} units across ${p.orders} orders`}
                            >
                              in {p.orders}
                            </span>
                          )}
                        </TableCell>
                        <TableCell className="text-right tabular-nums">{formatMoney(p.itemRevenue)}</TableCell>
                        <TableCell className="text-right tabular-nums">{formatMoney(p.avgPrice)}</TableCell>
                        <TableCell className="whitespace-nowrap text-muted-foreground">
                          {formatDate(p.lastSold)}
                        </TableCell>
                        <TableCell className="whitespace-nowrap">
                          <span className="mr-2 tabular-nums text-muted-foreground">
                            {p.priorUnits} → {p.recentUnits}
                          </span>
                          <TrendBadge trend={p.trend} />
                        </TableCell>
                      </TableRow>
                    ))}
                    {filtered.length === 0 && (
                      <TableRow>
                        <TableCell colSpan={7} className="py-8 text-center text-muted-foreground">
                          No products match.
                        </TableCell>
                      </TableRow>
                    )}
                  </TableBody>
                </Table>
                {!showAll && !query && filtered.length > 12 && (
                  <div className="p-2 text-center">
                    <Button variant="ghost" size="sm" onClick={() => setShowAll(true)}>
                      Show all {filtered.length} products
                    </Button>
                  </div>
                )}
              </div>
            </Card>
          </div>
          {/* Beside the table: the selected product, or how buyers choose options. */}
          {wide && selected ? null : (
            <>
              {activeVariation && (
                <Card className="w-full xl:w-[380px] xl:shrink-0">
                  <CardHeader>
                    <CardTitle>Options buyers choose</CardTitle>
                    <CardDescription>Units sold by {activeVariation.name.toLowerCase()}, across all products</CardDescription>
                    {variations.length > 1 && (
                      <CardAction>
                        <Select value={activeVariation.name} onValueChange={setVariationName}>
                          <SelectTrigger size="sm" aria-label="Option">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent align="end">
                            {variations.map((v) => (
                              <SelectItem key={v.name} value={v.name}>
                                {v.name}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </CardAction>
                    )}
                  </CardHeader>
                  <CardContent>
                    <DonutChart data={activeVariation.values} />
                  </CardContent>
                </Card>
              )}
            </>
          )}
          <SidePanel open={selected !== null} onClose={() => setSelectedKey(null)} title="Product details">
            {selected && <ProductDetailPanel product={selected} onClose={() => setSelectedKey(null)} />}
          </SidePanel>
        </div>
      )}
    </div>
  );
}
