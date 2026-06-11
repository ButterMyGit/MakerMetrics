"use client";

import { useMemo, useState } from "react";
import { useSalesData } from "@/hooks/use-sales-data";
import {
  breakdownBy,
  productMonthlyUnits,
  productStats,
  type ProductStat,
} from "@/lib/analytics/core";
import { PageHeader } from "@/components/page-header";
import { EmptyState, LoadingState } from "@/components/empty-state";
import { DonutChart, MonthlyTrendChart } from "@/components/charts";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { formatDate, formatMoney, formatMoneyCompact, formatNumber } from "@/lib/format";
import { ArrowDownRight, ArrowUpRight, Minus, Moon, Search, Sparkle } from "lucide-react";

function TrendBadge({ trend }: { trend: ProductStat["trend"] }) {
  switch (trend) {
    case "rising":
      return (
        <Badge className="gap-1 bg-emerald-100 text-emerald-700 dark:bg-emerald-950 dark:text-emerald-300">
          <ArrowUpRight className="size-3" /> Rising
        </Badge>
      );
    case "falling":
      return (
        <Badge className="gap-1 bg-red-100 text-red-700 dark:bg-red-950 dark:text-red-300">
          <ArrowDownRight className="size-3" /> Falling
        </Badge>
      );
    case "new":
      return (
        <Badge className="gap-1 bg-blue-100 text-blue-700 dark:bg-blue-950 dark:text-blue-300">
          <Sparkle className="size-3" /> New
        </Badge>
      );
    case "dormant":
      return (
        <Badge variant="secondary" className="gap-1">
          <Moon className="size-3" /> Dormant
        </Badge>
      );
    default:
      return (
        <Badge variant="secondary" className="gap-1">
          <Minus className="size-3" /> Steady
        </Badge>
      );
  }
}

export default function ProductsPage() {
  const { rows, loading, hasData } = useSalesData();
  const [query, setQuery] = useState("");
  const [selected, setSelected] = useState<ProductStat | null>(null);

  const stats = useMemo(() => productStats(rows), [rows]);
  const styles = useMemo(() => breakdownBy(rows, "style", "units"), [rows]);
  const types = useMemo(() => breakdownBy(rows, "productType", "units"), [rows]);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return stats;
    return stats.filter((p) => p.name.toLowerCase().includes(q));
  }, [stats, query]);

  const selectedSeries = useMemo(
    () => (selected ? productMonthlyUnits(rows, selected.name) : []),
    [rows, selected]
  );

  if (loading) return <LoadingState />;
  if (!hasData) return <EmptyState />;

  const totalUnits = stats.reduce((s, p) => s + p.units, 0);

  return (
    <div>
      <PageHeader
        title="Products"
        description="Trend compares the last 90 days against the 90 days before."
      />

      <div className="relative mb-4">
        <Search className="absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
        <Input
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Search products…"
          className="pl-9"
        />
      </div>

      <Card className="p-0">
        <div className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Product</TableHead>
                <TableHead className="text-right">Units</TableHead>
                <TableHead className="hidden text-right sm:table-cell">Orders</TableHead>
                <TableHead className="hidden text-right md:table-cell">Item revenue</TableHead>
                <TableHead className="hidden text-right md:table-cell">Share</TableHead>
                <TableHead className="hidden lg:table-cell">Last sold</TableHead>
                <TableHead>Trend</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filtered.map((p) => (
                <TableRow
                  key={p.name}
                  className="cursor-pointer"
                  onClick={() => setSelected(p)}
                >
                  <TableCell className="max-w-[200px] truncate font-medium sm:max-w-xs">
                    {p.name}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    {formatNumber(p.units)}
                  </TableCell>
                  <TableCell className="hidden text-right tabular-nums sm:table-cell">
                    {formatNumber(p.orders)}
                  </TableCell>
                  <TableCell className="hidden text-right tabular-nums md:table-cell">
                    {formatMoneyCompact(p.itemRevenue)}
                  </TableCell>
                  <TableCell className="hidden text-right tabular-nums md:table-cell">
                    {totalUnits > 0 ? `${((p.units / totalUnits) * 100).toFixed(1)}%` : "—"}
                  </TableCell>
                  <TableCell className="hidden text-muted-foreground lg:table-cell">
                    {formatDate(p.lastSold)}
                  </TableCell>
                  <TableCell>
                    <TrendBadge trend={p.trend} />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </Card>

      <div className="mt-4 grid gap-4 lg:grid-cols-2">
        {styles.length > 1 && (
          <Card>
            <CardHeader>
              <CardTitle>Units by style</CardTitle>
            </CardHeader>
            <CardContent>
              <DonutChart data={styles} />
            </CardContent>
          </Card>
        )}
        {types.length > 1 && (
          <Card>
            <CardHeader>
              <CardTitle>Units by product type</CardTitle>
            </CardHeader>
            <CardContent>
              <DonutChart data={types} />
            </CardContent>
          </Card>
        )}
      </div>

      <Sheet open={selected !== null} onOpenChange={(open) => !open && setSelected(null)}>
        <SheetContent side="right" className="w-full overflow-y-auto sm:max-w-md">
          {selected && (
            <>
              <SheetHeader>
                <SheetTitle>{selected.name}</SheetTitle>
                <SheetDescription>
                  First sold {formatDate(selected.firstSold)} · last sold{" "}
                  {formatDate(selected.lastSold)}
                </SheetDescription>
              </SheetHeader>
              <div className="grid gap-4 px-4 pb-8">
                <div className="grid grid-cols-3 gap-2">
                  <div className="rounded-lg border p-3 text-center">
                    <p className="text-lg font-semibold tabular-nums">
                      {formatNumber(selected.units)}
                    </p>
                    <p className="text-xs text-muted-foreground">Units</p>
                  </div>
                  <div className="rounded-lg border p-3 text-center">
                    <p className="text-lg font-semibold tabular-nums">
                      {formatNumber(selected.orders)}
                    </p>
                    <p className="text-xs text-muted-foreground">Orders</p>
                  </div>
                  <div className="rounded-lg border p-3 text-center">
                    <p className="text-lg font-semibold tabular-nums">
                      {formatMoney(selected.avgPrice)}
                    </p>
                    <p className="text-xs text-muted-foreground">Avg price</p>
                  </div>
                </div>
                <div>
                  <h3 className="mb-2 text-sm font-medium">Units by month</h3>
                  <MonthlyTrendChart
                    data={selectedSeries}
                    dataKey="units"
                    name="Units"
                    height={220}
                  />
                </div>
                <div className="flex items-center justify-between rounded-lg border p-3 text-sm">
                  <span className="text-muted-foreground">Momentum (90d vs prior 90d)</span>
                  <span className="tabular-nums font-medium">
                    {formatNumber(selected.priorUnits)} → {formatNumber(selected.recentUnits)}
                  </span>
                </div>
              </div>
            </>
          )}
        </SheetContent>
      </Sheet>
    </div>
  );
}
