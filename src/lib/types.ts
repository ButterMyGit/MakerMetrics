/** One Etsy transaction (line item). Mirrors public.sale_items. */
export interface SaleRow {
  transactionId: string;
  orderId: string | null;
  listingId: string | null;

  saleDate: string | null; // YYYY-MM-DD
  datePaid: string | null;
  dateShipped: string | null;

  itemName: string | null;
  cardName: string | null;
  productType: string | null;
  variations: string | null;
  style: string | null;
  sku: string | null;
  quantity: number;
  price: number | null;
  itemTotal: number | null;
  currency: string | null;

  discountAmount: number | null;
  shippingDiscount: number | null;
  shipping: number | null;
  salesTax: number | null;
  orderValue: number | null;
  orderTotal: number | null;
  cardProcessingFees: number | null;
  orderNet: number | null;
  adjustedOrderTotal: number | null;
  adjustedCardProcessingFees: number | null;
  adjustedNetOrderAmount: number | null;
  vatPaidByBuyer: number | null;
  inPersonDiscount: number | null;
  inPersonLocation: string | null;
  /** Order-level refund total (from EtsyDirectCheckoutPayments). */
  refundAmount: number | null;

  orderType: string | null;
  paymentType: string | null;
  couponCode: string | null;
  couponDetails: string | null;

  buyerUsername: string | null;
  buyerUserId: string | null;
  fullName: string | null;

  shipCity: string | null;
  shipState: string | null;
  shipZipcode: string | null;
  shipCountry: string | null;
}

/** snake_case shape stored in / returned by Supabase. */
export interface SaleItemDbRow {
  transaction_id: string;
  order_id: string | null;
  listing_id: string | null;
  sale_date: string | null;
  date_paid: string | null;
  date_shipped: string | null;
  item_name: string | null;
  card_name: string | null;
  product_type: string | null;
  variations: string | null;
  style: string | null;
  sku: string | null;
  quantity: number | null;
  price: number | null;
  item_total: number | null;
  currency: string | null;
  discount_amount: number | null;
  shipping_discount: number | null;
  shipping: number | null;
  sales_tax: number | null;
  order_value: number | null;
  order_total: number | null;
  card_processing_fees: number | null;
  order_net: number | null;
  adjusted_order_total: number | null;
  adjusted_card_processing_fees: number | null;
  adjusted_net_order_amount: number | null;
  vat_paid_by_buyer: number | null;
  in_person_discount: number | null;
  in_person_location: string | null;
  refund_amount: number | null;
  order_type: string | null;
  payment_type: string | null;
  coupon_code: string | null;
  coupon_details: string | null;
  buyer_username: string | null;
  buyer_user_id: string | null;
  full_name: string | null;
  ship_city: string | null;
  ship_state: string | null;
  ship_zipcode: string | null;
  ship_country: string | null;
}

export function dbRowToSaleRow(r: SaleItemDbRow): SaleRow {
  return {
    transactionId: r.transaction_id,
    orderId: r.order_id,
    listingId: r.listing_id,
    saleDate: r.sale_date,
    datePaid: r.date_paid,
    dateShipped: r.date_shipped,
    itemName: r.item_name,
    cardName: r.card_name,
    productType: r.product_type,
    variations: r.variations,
    style: r.style,
    sku: r.sku,
    quantity: r.quantity ?? 1,
    price: r.price,
    itemTotal: r.item_total,
    currency: r.currency,
    discountAmount: r.discount_amount,
    shippingDiscount: r.shipping_discount,
    shipping: r.shipping,
    salesTax: r.sales_tax,
    orderValue: r.order_value,
    orderTotal: r.order_total,
    cardProcessingFees: r.card_processing_fees,
    orderNet: r.order_net,
    adjustedOrderTotal: r.adjusted_order_total,
    adjustedCardProcessingFees: r.adjusted_card_processing_fees,
    adjustedNetOrderAmount: r.adjusted_net_order_amount,
    vatPaidByBuyer: r.vat_paid_by_buyer,
    inPersonDiscount: r.in_person_discount,
    inPersonLocation: r.in_person_location,
    refundAmount: r.refund_amount,
    orderType: r.order_type,
    paymentType: r.payment_type,
    couponCode: r.coupon_code,
    couponDetails: r.coupon_details,
    buyerUsername: r.buyer_username,
    buyerUserId: r.buyer_user_id,
    fullName: r.full_name,
    shipCity: r.ship_city,
    shipState: r.ship_state,
    shipZipcode: r.ship_zipcode,
    shipCountry: r.ship_country,
  };
}
