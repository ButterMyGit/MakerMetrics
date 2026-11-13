"use client";

import Link from "next/link";
import { ChevronLeft, ChevronRight, ExternalLink, X } from "lucide-react";
import type { Order } from "@/lib/analytics/core";
import { usePrivacy } from "@/hooks/use-privacy";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { InfoTip } from "@/components/info-tip";
import { formatDate, formatMoney, humanizeCode } from "@/lib/format";
import { cn } from "@/lib/utils";

function Line({
  label,
  value,
  sign,
  strong,
  muted,
  hint,
}: {
  label: string;
  value: number;
  sign?: "-" | "+";
  strong?: boolean;
  muted?: boolean;
  hint?: string;
}) {
  return (
    <div className={cn("flex justify-between gap-3", strong && "font-semibold")}>
      <dt className={cn(!strong && "text-muted-foreground")}>
        {label}
        {hint && <span className="ml-1 text-xs font-normal text-muted-foreground">{hint}</span>}
      </dt>
      <dd className={cn("tabular-nums", muted && "text-muted-foreground")}>
        {sign === "-" ? "−" : sign === "+" ? "+" : ""}
        {formatMoney(value)}
      </dd>
    </div>
  );
}

/** One order, top to bottom: what the buyer paid, what Etsy took, what is left. */
export function OrderDetail({
  order,
  buyerOrderCount,
  onClose,
  onPrev,
  onNext,
}: {
  order: Order;
  /** how many orders this buyer has placed in total */
  buyerOrderCount: number;
  onClose?: () => void;
  onPrev?: () => void;
  onNext?: () => void;
}) {
  const privacy = usePrivacy();
  const m = order.money;
  const head = order.rows[0];
  const payment = humanizeCode(order.paymentType);

  return (
    <div className="grid gap-4">
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <h2 className="truncate text-base font-semibold">
            Order {order.orderId ? `#${privacy.orderId(order.orderId)}` : "(no order number)"}
          </h2>
          <p className="truncate text-sm text-muted-foreground">
            {privacy.name(order.buyerName)} · {formatDate(order.saleDate)}
          </p>
        </div>
        <div className="flex shrink-0 items-center gap-1">
          <Button variant="ghost" size="icon" className="size-8" disabled={!onPrev} onClick={onPrev} aria-label="Previous order">
            <ChevronLeft className="size-4" />
          </Button>
          <Button variant="ghost" size="icon" className="size-8" disabled={!onNext} onClick={onNext} aria-label="Next order">
            <ChevronRight className="size-4" />
          </Button>
          {onClose && (
            <Button variant="ghost" size="icon" className="size-8" onClick={onClose} aria-label="Close">
              <X className="size-4" />
            </Button>
          )}
        </div>
      </div>

      <div className="flex flex-wrap gap-1.5">
        {order.cancelled && <Badge variant="destructive">Fully refunded</Badge>}
        {!order.cancelled && m.refund > 0 && (
          <Badge className="bg-amber-100 text-amber-700 dark:bg-amber-950 dark:text-amber-300">
            Partly refunded
          </Badge>
        )}
        {order.coupons.map((c) => (
          <Badge key={c} variant="secondary">
            {c}
          </Badge>
        ))}
      </div>

      <div className="grid gap-2">
        {order.rows.map((r) => (
          <div key={r.transactionId} className="rounded-lg border p-3">
            <p className="text-sm font-medium">{r.itemName ?? "Unknown item"}</p>
            <p className="mt-0.5 text-xs text-muted-foreground">
              {[...r.variationList.map((v) => `${v.name}: ${v.value}`), r.sku && `SKU ${r.sku}`]
                .filter(Boolean)
                .join(" · ")}
            </p>
            <div className="mt-2 flex justify-between text-sm">
              <span className="text-muted-foreground">
                {r.quantity} × {formatMoney(r.price)}
              </span>
              <span className="font-medium tabular-nums">
                {formatMoney(r.itemTotal ?? (r.price ?? 0) * r.quantity)}
              </span>
            </div>
          </div>
        ))}
      </div>

      <dl className="grid gap-1.5 text-sm">
        <Line label="Item subtotal" value={m.itemSubtotal} />
        {m.discount > 0 && <Line label="Discount" value={m.discount} sign="-" />}
        {m.shipping > 0 && <Line label="Shipping charged" value={m.shipping} sign="+" />}
        <Line label="Revenue" value={m.revenue} strong />
        {m.tax > 0 && (
          <Line label="Sales tax" hint="collected and remitted by Etsy" value={m.tax} sign="+" muted />
        )}
        {m.tax > 0 && <Line label="Buyer paid" value={m.buyerPaid} muted />}
        <Separator className="my-1" />
        {m.hasProcessingFee ? (
          <Line label="Payment processing fee" value={m.processingFee} sign="-" />
        ) : (
          <p className="text-xs text-muted-foreground">
            Processing fee unknown. Import the Sold Orders export to add it.
          </p>
        )}
        {m.refund > 0 && <Line label="Refunded" value={m.refund} sign="-" />}
        <Line label="Transaction fee" hint="est. 6.5%" value={m.transactionFeeEst} sign="-" />
        <Line label="Listing fee" hint="est." value={m.listingFeeEst} sign="-" />
        <div className="flex justify-between gap-3 font-semibold">
          <dt className="flex items-center gap-1.5">
            Net after Etsy fees <InfoTip metric="netAfterFees" />
          </dt>
          <dd className="tabular-nums">{formatMoney(m.netAfterFees)}</dd>
        </div>
      </dl>

      <Separator />

      <dl className="grid gap-1.5 text-sm">
        {payment && (
          <div className="flex justify-between gap-3">
            <dt className="text-muted-foreground">Payment</dt>
            <dd>{payment}</dd>
          </div>
        )}
        {order.location && (
          <div className="flex justify-between gap-3">
            <dt className="text-muted-foreground">Ships to</dt>
            <dd>{privacy.location(order.location)}</dd>
          </div>
        )}
        {order.datePaid && (
          <div className="flex justify-between gap-3">
            <dt className="text-muted-foreground">Paid</dt>
            <dd>{formatDate(order.datePaid)}</dd>
          </div>
        )}
        <div className="flex justify-between gap-3">
          <dt className="text-muted-foreground">Shipped</dt>
          <dd>{order.dateShipped ? formatDate(order.dateShipped) : "Not yet"}</dd>
        </div>
      </dl>

      <div className="flex flex-wrap gap-2">
        {order.orderId && (
          <Button variant="outline" size="sm" asChild>
            <a
              href={`https://www.etsy.com/your/orders/sold?order_id=${encodeURIComponent(order.orderId)}`}
              target="_blank"
              rel="noopener noreferrer"
            >
              <ExternalLink className="size-3.5" /> Open on Etsy
            </a>
          </Button>
        )}
        {order.buyerKey && buyerOrderCount > 1 && (
          <Button variant="outline" size="sm" asChild>
            <Link href={`/customers?buyer=${encodeURIComponent(order.buyerKey)}`}>
              All {buyerOrderCount} orders from this buyer
            </Link>
          </Button>
        )}
        {head.listingId && order.rows.length === 1 && (
          <Button variant="outline" size="sm" asChild>
            <a
              href={`https://www.etsy.com/listing/${encodeURIComponent(head.listingId)}`}
              target="_blank"
              rel="noopener noreferrer"
            >
              <ExternalLink className="size-3.5" /> Listing
            </a>
          </Button>
        )}
      </div>
    </div>
  );
}
