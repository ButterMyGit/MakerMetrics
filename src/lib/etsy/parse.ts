import Papa from "papaparse";
import type { SaleItemDbRow } from "@/lib/types";

/**
 * TypeScript port of the legacy watcher.py ingestion pipeline.
 *
 * Etsy provides several relevant CSV exports:
 *  - "EtsySoldOrderItems*.csv"          -> one row per transaction (line item)
 *  - "EtsySoldOrders*.csv"              -> one row per order with financials
 *  - a combined export with both item and order columns
 *  - "EtsyDirectCheckoutPayments*.csv"  -> one row per payment, incl. refunds
 *
 * Items can be imported alone. Orders and payments only *enrich* existing item
 * rows (joined on Order ID); they never create rows. In particular, payments
 * only contribute a refund amount onto the order they belong to — so a refund
 * can never be mistaken for an extra purchase or bump a buyer's repeat status.
 */

export type CsvFormat = "items" | "orders" | "combined" | "payments" | "unknown";

export interface ParsedCsv {
  fileName: string;
  format: CsvFormat;
  rows: Record<string, string>[];
}

export interface BuildResult {
  rows: SaleItemDbRow[];
  warnings: string[];
  /** item rows that had no matching order row (still imported) */
  unmatchedItems: number;
  /** true when a payments file contributed refund data to this batch */
  includesRefunds: boolean;
}

// ---------------------------------------------------------------------------
// Parsing & format detection
// ---------------------------------------------------------------------------

export function parseCsvFile(file: File): Promise<ParsedCsv> {
  return new Promise((resolve, reject) => {
    Papa.parse<Record<string, string>>(file, {
      header: true,
      skipEmptyLines: "greedy",
      transformHeader: (h) => h.trim(),
      complete: (result) => {
        const rows = result.data;
        resolve({
          fileName: file.name,
          format: detectFormat(result.meta.fields ?? []),
          rows,
        });
      },
      error: (err) => reject(err),
    });
  });
}

export function detectFormat(headers: string[]): CsvFormat {
  const has = (name: string) =>
    headers.some((h) => h.toLowerCase() === name.toLowerCase());

  const hasTid = has("Transaction ID");
  const hasItem = has("Item Name");
  const hasFull = has("Full Name");
  const hasPaymentId = has("Payment ID");
  const hasRefund = has("Refund Amount");

  if (hasTid && hasFull) return "combined";
  if (hasTid && hasItem) return "items";
  if (hasPaymentId && hasRefund && !hasTid) return "payments";
  if (hasFull && !hasTid) return "orders";
  return "unknown";
}

// ---------------------------------------------------------------------------
// Value cleaning
// ---------------------------------------------------------------------------

function cleanString(v: string | undefined | null): string | null {
  if (v == null) return null;
  const s = String(v).trim();
  return s === "" ? null : s;
}

function cleanMoney(v: string | undefined | null): number | null {
  if (v == null) return null;
  const s = String(v).trim().replace(/[$,]/g, "").trim();
  if (s === "" || /^[-\s]*$/.test(s)) return 0;
  const n = Number(s);
  return Number.isFinite(n) ? Math.round(n * 100) / 100 : null;
}

function cleanInt(v: string | undefined | null): number | null {
  if (v == null) return null;
  const n = Number(String(v).trim());
  return Number.isFinite(n) ? Math.trunc(n) : null;
}

/** Etsy dates come as MM/DD/YY or MM/DD/YYYY (sometimes YYYY-MM-DD). */
function cleanDate(v: string | undefined | null): string | null {
  if (v == null) return null;
  const s = String(v).trim();
  if (s === "") return null;

  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);

  const m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/);
  if (m) {
    const [, mm, dd, yy] = m;
    const year = yy.length === 2 ? `20${yy}` : yy;
    return `${year}-${mm.padStart(2, "0")}-${dd.padStart(2, "0")}`;
  }
  return null;
}

/** Strip "#" prefixes and trailing ".0" from numeric IDs rendered by spreadsheets. */
function cleanId(v: string | undefined | null): string | null {
  if (v == null) return null;
  let s = String(v).trim().replace(/^#+/, "").trim();
  if (/^\d+\.0+$/.test(s)) s = s.split(".")[0];
  return s === "" ? null : s;
}

// ---------------------------------------------------------------------------
// Row access helpers (case-insensitive, alias-aware)
// ---------------------------------------------------------------------------

function makeGetter(row: Record<string, string>) {
  const lower = new Map<string, string>();
  for (const [k, v] of Object.entries(row)) lower.set(k.toLowerCase(), v);
  return (...names: string[]): string | null => {
    for (const name of names) {
      const v = lower.get(name.toLowerCase());
      if (v != null && String(v).trim() !== "") return v;
    }
    return null;
  };
}

// ---------------------------------------------------------------------------
// Enrichment (derived fields)
// ---------------------------------------------------------------------------

export function deriveItemParts(itemName: string | null): {
  cardName: string | null;
  productType: string | null;
} {
  if (!itemName) return { cardName: null, productType: null };
  const idx = itemName.indexOf("|");
  if (idx === -1) return { cardName: itemName.trim(), productType: null };
  return {
    cardName: itemName.slice(0, idx).trim() || null,
    productType: itemName.slice(idx + 1).trim() || null,
  };
}

export function deriveStyle(variations: string | null): string | null {
  if (!variations) return null;
  const s = variations.replace(/^(Style:|Custom Property:)\s*/i, "").trim();
  return s === "" ? null : s;
}

// ---------------------------------------------------------------------------
// Building DB rows
// ---------------------------------------------------------------------------

interface OrderInfo {
  fullName: string | null;
  orderValue: number | null;
  orderTotal: number | null;
  cardProcessingFees: number | null;
  orderNet: number | null;
  adjustedOrderTotal: number | null;
  adjustedCardProcessingFees: number | null;
  adjustedNetOrderAmount: number | null;
  datePaid: string | null;
}

function buildOrderIndex(orderRows: Record<string, string>[]): Map<string, OrderInfo> {
  const index = new Map<string, OrderInfo>();
  for (const row of orderRows) {
    const get = makeGetter(row);
    const orderId = cleanId(get("Order ID"));
    if (!orderId) continue;
    index.set(orderId, {
      fullName: cleanString(get("Full Name", "Buyer")),
      orderValue: cleanMoney(get("Order Value")),
      orderTotal: cleanMoney(get("Order Total")),
      cardProcessingFees: cleanMoney(get("Card Processing Fees")),
      orderNet: cleanMoney(get("Order Net")),
      adjustedOrderTotal: cleanMoney(get("Adjusted Order Total")),
      adjustedCardProcessingFees: cleanMoney(get("Adjusted Card Processing Fees")),
      adjustedNetOrderAmount: cleanMoney(get("Adjusted Net Order Amount")),
      datePaid: cleanDate(get("Date Paid")),
    });
  }
  return index;
}

/**
 * Map of Order ID -> total refunded, from payment exports. Multiple payments
 * for one order are summed. Only the refund amount is taken; everything else
 * in the payments file duplicates the orders export.
 */
function buildRefundIndex(paymentRows: Record<string, string>[]): Map<string, number> {
  const index = new Map<string, number>();
  for (const row of paymentRows) {
    const get = makeGetter(row);
    const orderId = cleanId(get("Order ID"));
    if (!orderId) continue;
    const refund = cleanMoney(get("Refund Amount")) ?? 0;
    if (refund <= 0) continue;
    index.set(orderId, (index.get(orderId) ?? 0) + refund);
  }
  return index;
}

function buildRowFromItem(
  row: Record<string, string>,
  order: OrderInfo | undefined,
  refund: number | null
): SaleItemDbRow | null {
  const get = makeGetter(row);

  const transactionId = cleanId(get("Transaction ID"));
  if (!transactionId) return null;

  const itemName = cleanString(get("Item Name"));
  const { cardName, productType } = deriveItemParts(itemName);
  const variations = cleanString(get("Variations"));

  return {
    transaction_id: transactionId,
    order_id: cleanId(get("Order ID")),
    listing_id: cleanId(get("Listing ID")),
    sale_date: cleanDate(get("Sale Date")),
    date_paid: order?.datePaid ?? cleanDate(get("Date Paid")),
    date_shipped: cleanDate(get("Date Shipped", "Item Date Shipped")),
    item_name: itemName,
    card_name: cardName,
    product_type: productType,
    variations,
    style: deriveStyle(variations),
    sku: cleanString(get("SKU", "Item SKU", "Order SKU")),
    quantity: cleanInt(get("Quantity")) ?? 1,
    price: cleanMoney(get("Price")),
    item_total: cleanMoney(get("Item Total")),
    currency: cleanString(get("Currency", "Item Currency")),
    discount_amount: cleanMoney(get("Discount Amount", "Order Discount Amount")),
    shipping_discount: cleanMoney(get("Shipping Discount", "Order Shipping Discount")),
    shipping: cleanMoney(get("Shipping", "Order Shipping", "Order Shipping Amount")),
    sales_tax: cleanMoney(get("Sales Tax", "Order Sales Tax", "Order Sales Tax Amount")),
    order_value: order?.orderValue ?? cleanMoney(get("Order Value")),
    order_total: order?.orderTotal ?? cleanMoney(get("Order Total")),
    card_processing_fees:
      order?.cardProcessingFees ?? cleanMoney(get("Card Processing Fees")),
    order_net: order?.orderNet ?? cleanMoney(get("Order Net")),
    adjusted_order_total:
      order?.adjustedOrderTotal ?? cleanMoney(get("Adjusted Order Total")),
    adjusted_card_processing_fees:
      order?.adjustedCardProcessingFees ??
      cleanMoney(get("Adjusted Card Processing Fees")),
    adjusted_net_order_amount:
      order?.adjustedNetOrderAmount ??
      cleanMoney(get("Adjusted Net Order Amount")),
    vat_paid_by_buyer: cleanMoney(get("VAT Paid by Buyer")),
    in_person_discount: cleanMoney(
      get("InPerson Discount", "Item InPerson Discount", "Order InPerson Discount")
    ),
    in_person_location: cleanString(
      get("InPerson Location", "Item InPerson Location", "Order InPerson Location")
    ),
    refund_amount: refund,
    order_type: cleanString(get("Order Type", "Item Order Type")),
    payment_type: cleanString(get("Payment Type", "Item Payment Type", "Payment Method")),
    coupon_code: cleanString(get("Coupon Code", "Order Coupon Code")),
    coupon_details: cleanString(get("Coupon Details", "Order Coupon Details")),
    buyer_username: cleanString(get("Buyer Username", "Buyer", "Buyer_x")),
    buyer_user_id: cleanId(get("Buyer User ID")),
    full_name: order?.fullName ?? cleanString(get("Full Name", "Buyer_y")),
    ship_city: cleanString(get("Ship City", "Item Ship City")),
    ship_state: cleanString(get("Ship State", "Item Ship State")),
    ship_zipcode: cleanString(get("Ship Zipcode", "Item Ship Zipcode")),
    ship_country: cleanString(get("Ship Country", "Item Ship Country")),
  };
}

/**
 * Combine any mix of parsed CSVs into upsert-ready sale_items rows.
 * Order files enrich item rows via Order ID; later duplicates of the same
 * Transaction ID win (re-importing is always safe).
 */
export function buildSaleItems(files: ParsedCsv[]): BuildResult {
  const warnings: string[] = [];

  const orderRows = files
    .filter((f) => f.format === "orders")
    .flatMap((f) => f.rows);
  const paymentRows = files
    .filter((f) => f.format === "payments")
    .flatMap((f) => f.rows);
  const itemFiles = files.filter(
    (f) => f.format === "items" || f.format === "combined"
  );

  for (const f of files) {
    if (f.format === "unknown") {
      warnings.push(
        `"${f.fileName}" doesn't look like a supported Etsy export and was skipped.`
      );
    }
  }
  if (orderRows.length > 0 && itemFiles.length === 0) {
    warnings.push(
      "An orders CSV was provided without an order *items* CSV. Orders alone can't be imported — also upload the matching EtsySoldOrderItems export."
    );
  }
  const includesRefunds = paymentRows.length > 0;
  if (includesRefunds && itemFiles.length === 0) {
    warnings.push(
      "A payments CSV was provided without an order *items* CSV. Refunds attach to existing orders — also upload the matching EtsySoldOrderItems export."
    );
  }

  const orderIndex = buildOrderIndex(orderRows);
  const refundIndex = buildRefundIndex(paymentRows);
  const byTransaction = new Map<string, SaleItemDbRow>();
  let unmatchedItems = 0;
  let skippedNoTid = 0;

  for (const file of itemFiles) {
    for (const raw of file.rows) {
      const get = makeGetter(raw);
      const orderId = cleanId(get("Order ID"));
      const order = orderId ? orderIndex.get(orderId) : undefined;
      if (orderRows.length > 0 && !order) unmatchedItems++;

      // Refund is an order-level value; every line item of the order carries
      // it (read at order level), mirroring how order_net is denormalized.
      const refund = includesRefunds
        ? (orderId && refundIndex.get(orderId)) || null
        : null;

      const built = buildRowFromItem(raw, order, refund);
      if (!built) {
        skippedNoTid++;
        continue;
      }
      byTransaction.set(built.transaction_id, built);
    }
  }

  if (skippedNoTid > 0) {
    warnings.push(`${skippedNoTid} row(s) skipped — missing Transaction ID.`);
  }
  if (orderRows.length > 0 && unmatchedItems > 0) {
    warnings.push(
      `${unmatchedItems} item row(s) had no matching order row. Check that both exports cover the same period.`
    );
  }

  return { rows: [...byTransaction.values()], warnings, unmatchedItems, includesRefunds };
}
