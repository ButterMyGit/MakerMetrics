import type { SaleItemDbRow } from "@/lib/types";
import { deriveItemParts, deriveStyle } from "./normalize";
import type { EtsyLedgerEntry, EtsyListing, EtsyMoney, EtsyPayment, EtsyReceipt } from "./api-types";

/**
 * Turns Etsy API objects into the same rows the CSV import produces, so the
 * whole app works identically on either source.
 */

export function money(m: EtsyMoney | null | undefined): number | null {
  if (!m || typeof m.amount !== "number" || !m.divisor) return null;
  return Math.round((m.amount / m.divisor) * 100) / 100;
}

/** Calendar date of a Unix timestamp in the seller's timezone. */
export function dateIn(timestamp: number | null | undefined, timeZone: string): string | null {
  if (!timestamp) return null;
  try {
    // en-CA formats as YYYY-MM-DD.
    return new Intl.DateTimeFormat("en-CA", { timeZone, year: "numeric", month: "2-digit", day: "2-digit" }).format(
      new Date(timestamp * 1000)
    );
  } catch {
    return new Date(timestamp * 1000).toISOString().slice(0, 10);
  }
}

/** Columns the API can fill. Coupon codes are not in the API, so the CSV's stay untouched. */
export type ApiSaleRow = Omit<
  SaleItemDbRow,
  "coupon_code" | "coupon_details" | "in_person_discount" | "in_person_location" | "order_type" | "buyer_username" | "shipping_discount" | "vat_paid_by_buyer"
>;

export function receiptToRows(
  receipt: EtsyReceipt,
  payment: EtsyPayment | null,
  timeZone: string
): ApiSaleRow[] {
  const shippedAt =
    receipt.shipments?.map((s) => s.shipment_notification_timestamp).find((t) => t) ?? null;
  let refund = (receipt.refunds ?? []).reduce((s, r) => s + (money(r.amount) ?? 0), 0);
  // A canceled order is not a sale: record it as fully refunded.
  if (receipt.status === "canceled" && refund === 0) {
    refund =
      (money(receipt.total_price) ?? 0) -
      (money(receipt.discount_amt) ?? 0) +
      (money(receipt.total_shipping_cost) ?? 0);
  }
  const adjusted = (m: EtsyMoney | null | undefined) => {
    const v = money(m);
    // Etsy reports adjusted values only after a refund; 0 otherwise means "none".
    return v != null && refund > 0 ? v : 0;
  };

  return receipt.transactions.map((t, index) => {
    const first = index === 0;
    const variations =
      t.variations && t.variations.length > 0
        ? t.variations.map((v) => `${v.formatted_name}: ${v.formatted_value}`).join(", ")
        : null;
    const { cardName, productType } = deriveItemParts(t.title);
    const price = money(t.price);
    return {
      transaction_id: String(t.transaction_id),
      order_id: String(receipt.receipt_id),
      listing_id: t.listing_id != null ? String(t.listing_id) : null,
      sale_date: dateIn(receipt.created_timestamp, timeZone),
      date_paid: dateIn(t.paid_timestamp, timeZone),
      date_shipped: dateIn(t.shipped_timestamp ?? shippedAt, timeZone),
      item_name: t.title,
      card_name: cardName,
      product_type: productType,
      variations,
      style: deriveStyle(variations),
      sku: t.sku || null,
      quantity: t.quantity,
      price,
      item_total: price != null ? Math.round(price * t.quantity * 100) / 100 : null,
      currency: t.price?.currency_code ?? receipt.grandtotal.currency_code,
      // Order-level amounts sit on the first line, like Etsy's own export.
      discount_amount: first ? money(receipt.discount_amt) : 0,
      shipping: first ? money(receipt.total_shipping_cost) : 0,
      sales_tax: first ? money(receipt.total_tax_cost) : 0,
      order_value: money(receipt.total_price),
      order_total: money(receipt.grandtotal),
      card_processing_fees: payment ? money(payment.amount_fees) : null,
      order_net: payment ? money(payment.amount_net) : null,
      adjusted_order_total: payment ? adjusted(payment.adjusted_gross) : 0,
      adjusted_card_processing_fees: payment ? adjusted(payment.adjusted_fees) : 0,
      adjusted_net_order_amount: payment ? adjusted(payment.adjusted_net) : 0,
      refund_amount: refund > 0 ? Math.round(refund * 100) / 100 : null,
      payment_type: receipt.payment_method,
      buyer_user_id: receipt.buyer_user_id != null ? String(receipt.buyer_user_id) : null,
      full_name: receipt.name,
      ship_city: receipt.city,
      ship_state: receipt.state,
      ship_zipcode: receipt.zip,
      ship_country: receipt.country_iso,
    };
  });
}

export function ledgerToRow(entry: EtsyLedgerEntry, timeZone: string) {
  return {
    entry_id: String(entry.entry_id),
    entry_date: dateIn(entry.created_timestamp, timeZone) ?? "1970-01-01",
    // Ledger amounts are in the currency's smallest unit.
    amount: Math.round(entry.amount) / 100,
    currency: entry.currency,
    description: entry.description,
    ledger_type: entry.ledger_type,
    reference_type: entry.reference_type,
    reference_id: entry.reference_id,
    source: "etsy_api",
  };
}

export function listingToRow(listing: EtsyListing) {
  const iso = (t: number | null | undefined) => (t ? new Date(t * 1000).toISOString() : null);
  return {
    listing_id: String(listing.listing_id),
    title: listing.title,
    state: listing.state,
    price: money(listing.price),
    currency: listing.price?.currency_code ?? null,
    quantity: listing.quantity,
    num_favorers: listing.num_favorers,
    tags: listing.tags ?? [],
    url: listing.url,
    listed_at: iso(listing.original_creation_timestamp),
    ends_at: iso(listing.ending_timestamp),
    synced_at: new Date().toISOString(),
  };
}
