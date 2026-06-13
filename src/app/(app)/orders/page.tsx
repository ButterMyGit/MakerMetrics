"use client";

import { useMemo, useState } from "react";
import { useSalesData } from "@/hooks/use-sales-data";
import { buildOrderHistory, orderNetOf, type OrderSummary } from "@/lib/analytics/core";
import { PageHeader } from "@/components/page-header";
import { EmptyState, LoadingState } from "@/components/empty-state";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { Separator } from "@/components/ui/separator";
import { formatDate, formatMoney, formatNumber } from "@/lib/format";
import { Search } from "lucide-react";

const PAGE_SIZE = 25;

export default function OrdersPage() {
  const { rows, loading, hasData } = useSalesData();
  const [query, setQuery] = useState("");
  const [page, setPage] = useState(0);
  const [selected, setSelected] = useState<OrderSummary | null>(null);

  const orders = useMemo(() => buildOrderHistory(rows), [rows]);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return orders;
    return orders.filter((o) => {
      if (o.orderId.toLowerCase().includes(q)) return true;
      if (o.buyer.toLowerCase().includes(q)) return true;
      if (o.couponCode?.toLowerCase().includes(q)) return true;
      if (o.shipLocation?.toLowerCase().includes(q)) return true;
      return o.rows.some(
        (r) =>
          r.itemName?.toLowerCase().includes(q) ||
          r.style?.toLowerCase().includes(q) ||
          r.sku?.toLowerCase().includes(q) ||
          r.transactionId.toLowerCase().includes(q)
      );
    });
  }, [orders, query]);

  const pageCount = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const visible = filtered.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  if (loading) return <LoadingState />;
  if (!hasData) return <EmptyState />;

  return (
    <div>
      <PageHeader title="Orders" description={`${formatNumber(filtered.length)} orders`} />

      <div className="relative mb-4">
        <Search className="absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
        <Input
          value={query}
          onChange={(e) => {
            setQuery(e.target.value);
            setPage(0);
          }}
          placeholder="Search by buyer, item, order ID, SKU, coupon, location…"
          className="pl-9"
        />
      </div>

      <Card className="divide-y p-0">
        {visible.length === 0 && (
          <p className="px-4 py-12 text-center text-sm text-muted-foreground">
            No orders match your search.
          </p>
        )}
        {visible.map((o) => (
          <button
            key={o.orderId}
            onClick={() => setSelected(o)}
            className="flex w-full items-center justify-between gap-3 px-4 py-3 text-left transition-colors hover:bg-accent/50"
          >
            <div className="min-w-0">
              <div className="flex items-center gap-2">
                <p className="truncate text-sm font-medium">{o.buyer}</p>
                {o.couponCode && (
                  <Badge variant="secondary" className="hidden sm:inline-flex">
                    {o.couponCode}
                  </Badge>
                )}
              </div>
              <p className="mt-0.5 truncate text-xs text-muted-foreground">
                {o.itemsLabel}
                {o.shipLocation ? ` · ${o.shipLocation}` : ""}
              </p>
            </div>
            <div className="shrink-0 text-right">
              <p className="text-sm font-semibold tabular-nums">{formatMoney(o.orderNet)}</p>
              <p className="text-xs text-muted-foreground">
                {formatDate(o.saleDate)} · {o.units} unit{o.units === 1 ? "" : "s"}
              </p>
            </div>
          </button>
        ))}
      </Card>

      {pageCount > 1 && (
        <div className="mt-4 flex items-center justify-between">
          <Button
            variant="outline"
            size="sm"
            disabled={page === 0}
            onClick={() => setPage((p) => p - 1)}
          >
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

      <Sheet open={selected !== null} onOpenChange={(open) => !open && setSelected(null)}>
        <SheetContent side="right" className="w-full overflow-y-auto sm:max-w-md">
          {selected && (
            <>
              <SheetHeader>
                <SheetTitle>Order #{selected.orderId}</SheetTitle>
                <SheetDescription>
                  {selected.buyer} · {formatDate(selected.saleDate)}
                </SheetDescription>
              </SheetHeader>
              <div className="grid gap-4 px-4 pb-8">
                <div className="grid gap-2">
                  {selected.rows.map((r) => (
                    <div key={r.transactionId} className="rounded-lg border p-3">
                      <p className="text-sm font-medium">{r.itemName ?? "Unknown item"}</p>
                      <p className="mt-0.5 text-xs text-muted-foreground">
                        {[r.style, r.sku && `SKU ${r.sku}`].filter(Boolean).join(" · ")}
                      </p>
                      <div className="mt-2 flex justify-between text-sm">
                        <span className="text-muted-foreground">
                          {r.quantity} × {formatMoney(r.price)}
                        </span>
                        <span className="font-medium tabular-nums">
                          {formatMoney(r.itemTotal)}
                        </span>
                      </div>
                    </div>
                  ))}
                </div>

                <Separator />

                <dl className="grid gap-1.5 text-sm">
                  {(
                    [
                      ["Order total", selected.rows[0].orderTotal],
                      ["Discount", selected.rows[0].discountAmount],
                      ["Shipping collected", selected.rows[0].shipping],
                      ["Sales tax", selected.rows[0].salesTax],
                      ["Processing fees", selected.rows[0].cardProcessingFees],
                    ] as const
                  ).map(
                    ([label, value]) =>
                      value != null &&
                      value !== 0 && (
                        <div key={label} className="flex justify-between">
                          <dt className="text-muted-foreground">{label}</dt>
                          <dd className="tabular-nums">{formatMoney(value)}</dd>
                        </div>
                      )
                  )}
                  <div className="flex justify-between font-semibold">
                    <dt>Net to you</dt>
                    <dd className="tabular-nums">{formatMoney(orderNetOf(selected.rows[0]))}</dd>
                  </div>
                </dl>

                <Separator />

                <dl className="grid gap-1.5 text-sm">
                  {selected.rows[0].paymentType && (
                    <div className="flex justify-between">
                      <dt className="text-muted-foreground">Payment</dt>
                      <dd>{selected.rows[0].paymentType}</dd>
                    </div>
                  )}
                  {selected.couponCode && (
                    <div className="flex justify-between">
                      <dt className="text-muted-foreground">Coupon</dt>
                      <dd>{selected.couponCode}</dd>
                    </div>
                  )}
                  {selected.shipLocation && (
                    <div className="flex justify-between">
                      <dt className="text-muted-foreground">Ships to</dt>
                      <dd>
                        {selected.shipLocation}
                        {selected.rows[0].shipCountry &&
                        selected.rows[0].shipCountry !== "United States"
                          ? ` (${selected.rows[0].shipCountry})`
                          : ""}
                      </dd>
                    </div>
                  )}
                  {selected.rows[0].datePaid && (
                    <div className="flex justify-between">
                      <dt className="text-muted-foreground">Paid</dt>
                      <dd>{formatDate(selected.rows[0].datePaid)}</dd>
                    </div>
                  )}
                  {selected.rows[0].dateShipped && (
                    <div className="flex justify-between">
                      <dt className="text-muted-foreground">Shipped</dt>
                      <dd>{formatDate(selected.rows[0].dateShipped)}</dd>
                    </div>
                  )}
                </dl>
              </div>
            </>
          )}
        </SheetContent>
      </Sheet>
    </div>
  );
}
