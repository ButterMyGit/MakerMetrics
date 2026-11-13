import { describe, expect, it } from "vitest";
import { buildOrders } from "@/lib/analytics/core";
import { dbRowToSaleRow, type SaleItemDbRow } from "@/lib/types";
import type { EtsyReceipt } from "./api-types";
import { dateIn, ledgerToRow, money, receiptToRows } from "./map";

const usd = (amount: number) => ({ amount, divisor: 100, currency_code: "USD" });

const receipt: EtsyReceipt = {
  receipt_id: 4229082383,
  buyer_user_id: 55,
  name: "jane doe",
  city: "RICHMOND",
  state: "VA",
  zip: "23220",
  country_iso: "US",
  payment_method: "cc",
  created_timestamp: 1794456000, // 2026-11-12T04:00:00Z = Nov 11 evening in New York
  grandtotal: usd(6894),
  total_price: usd(6998),
  total_shipping_cost: usd(562),
  total_tax_cost: usd(334),
  discount_amt: usd(1000),
  shipments: [{ shipment_notification_timestamp: 1794542400 }],
  transactions: [
    { transaction_id: 1, title: "Fox &amp; Hare | Print", quantity: 1, price: usd(3499), listing_id: 111, sku: "", paid_timestamp: 1794456000, shipped_timestamp: null, variations: [{ formatted_name: "Size", formatted_value: "8x10" }] },
    { transaction_id: 2, title: "Owl", quantity: 1, price: usd(3499), listing_id: 222, sku: "OWL-1", paid_timestamp: 1794456000, shipped_timestamp: null },
  ],
};
const payment = { receipt_id: 4229082383, amount_gross: usd(6894), amount_fees: usd(232), amount_net: usd(6328) };

describe("Etsy API mapping", () => {
  it("converts money and dates in the seller's timezone", () => {
    expect(money(usd(3499))).toBe(34.99);
    expect(money(null)).toBeNull();
    expect(dateIn(1794456000, "America/New_York")).toBe("2026-11-11");
    expect(dateIn(1794456000, "UTC")).toBe("2026-11-12");
  });

  it("produces rows that the analytics read exactly like a CSV import", () => {
    const rows = receiptToRows(receipt, payment, "America/New_York");
    expect(rows).toHaveLength(2);
    expect(rows[0]).toMatchObject({
      transaction_id: "1", order_id: "4229082383", listing_id: "111", sale_date: "2026-11-11",
      variations: "Size: 8x10", style: "8x10", discount_amount: 10, shipping: 5.62, order_total: 68.94,
      card_processing_fees: 2.32, order_net: 63.28, refund_amount: null,
    });
    expect(rows[1]).toMatchObject({ discount_amount: 0, shipping: 0, sku: "OWL-1" });
    expect("coupon_code" in rows[0]).toBe(false); // never overwrites the CSV's coupon

    const [order] = buildOrders(rows.map((r) => dbRowToSaleRow(r as SaleItemDbRow)));
    expect(order.money).toMatchObject({
      itemSubtotal: 69.98, discount: 10, shipping: 5.62, revenue: 65.6, tax: 3.34,
      buyerPaid: 68.94, processingFee: 2.32, orderNet: 63.28,
    });
    expect(order.dateShipped).toBe("2026-11-12");
    expect(order.buyerName).toBe("Jane Doe");
  });

  it("carries refunds and adjusted amounts", () => {
    const refunded = { ...receipt, refunds: [{ amount: usd(500) }] };
    const rows = receiptToRows(refunded, { ...payment, adjusted_net: usd(5843), adjusted_fees: usd(217) }, "UTC");
    expect(rows[0]).toMatchObject({ refund_amount: 5, adjusted_net_order_amount: 58.43, adjusted_card_processing_fees: 2.17 });
  });

  it("maps ledger entries from minor units", () => {
    expect(ledgerToRow({ entry_id: 9, amount: -234, currency: "USD", description: "transaction", created_timestamp: 1794456000, ledger_type: "transaction", reference_type: "receipt", reference_id: "4229082383" }, "UTC"))
      .toMatchObject({ entry_id: "9", amount: -2.34, entry_date: "2026-11-12", reference_id: "4229082383" });
  });
});
