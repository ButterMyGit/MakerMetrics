import type { SaleRow } from "@/lib/types";
import { parseCoupons, parseVariations, variationLabel } from "@/lib/etsy/normalize";

let seq = 0;

/** A SaleRow with sensible blanks; derived fields follow the overrides. */
export function row(overrides: Partial<SaleRow> = {}): SaleRow {
  seq += 1;
  const base: SaleRow = {
    transactionId: `t${seq}`,
    orderId: null,
    listingId: null,
    saleDate: "2025-06-01",
    datePaid: null,
    dateShipped: null,
    itemName: null,
    cardName: null,
    productType: null,
    variations: null,
    style: null,
    sku: null,
    quantity: 1,
    price: null,
    itemTotal: null,
    currency: "USD",
    discountAmount: null,
    shippingDiscount: null,
    shipping: null,
    salesTax: null,
    orderValue: null,
    orderTotal: null,
    cardProcessingFees: null,
    orderNet: null,
    adjustedOrderTotal: null,
    adjustedCardProcessingFees: null,
    adjustedNetOrderAmount: null,
    vatPaidByBuyer: null,
    inPersonDiscount: null,
    inPersonLocation: null,
    refundAmount: null,
    orderType: null,
    paymentType: null,
    couponCode: null,
    couponDetails: null,
    buyerUsername: null,
    buyerUserId: null,
    fullName: null,
    shipCity: null,
    shipState: null,
    shipZipcode: null,
    shipCountry: null,
    coupons: [],
    variationList: [],
    productKey: null,
    ...overrides,
  };
  if (overrides.coupons === undefined) base.coupons = parseCoupons(base.couponCode);
  if (overrides.variationList === undefined) {
    base.variationList = parseVariations(base.variations);
    if (overrides.style === undefined) base.style = variationLabel(base.variationList);
  }
  if (overrides.productKey === undefined) {
    const name = base.cardName ?? base.itemName;
    base.productKey = base.listingId ? `l:${base.listingId}` : name ? `n:${name}` : null;
  }
  return base;
}

/** A single-line order priced at `price`, with Etsy's usual order columns filled in. */
export function simpleOrder(
  orderId: string,
  saleDate: string,
  price: number,
  overrides: Partial<SaleRow> = {}
): SaleRow {
  const fee = Math.round((price * 0.03 + 0.25) * 100) / 100;
  return row({
    orderId,
    saleDate,
    cardName: "Widget",
    price,
    itemTotal: price,
    orderValue: price,
    orderTotal: price,
    cardProcessingFees: fee,
    orderNet: Math.round((price - fee) * 100) / 100,
    buyerUserId: `b-${orderId}`,
    ...overrides,
  });
}
