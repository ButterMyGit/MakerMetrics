/** The parts of Etsy Open API v3 responses that MakerMetrics reads. */

export interface EtsyMoney {
  amount: number;
  divisor: number;
  currency_code: string;
}

export interface EtsyTransaction {
  transaction_id: number;
  title: string | null;
  quantity: number;
  price: EtsyMoney;
  listing_id: number | null;
  sku: string | null;
  variations?: { formatted_name: string; formatted_value: string }[];
  paid_timestamp: number | null;
  shipped_timestamp: number | null;
  expected_ship_date?: number | null;
}

export interface EtsyReceipt {
  receipt_id: number;
  buyer_user_id: number | null;
  name: string | null;
  city: string | null;
  state: string | null;
  zip: string | null;
  country_iso: string | null;
  payment_method: string | null;
  status?: string | null;
  is_paid?: boolean;
  created_timestamp: number;
  updated_timestamp?: number;
  grandtotal: EtsyMoney;
  total_price: EtsyMoney;
  total_shipping_cost: EtsyMoney;
  total_tax_cost: EtsyMoney;
  total_vat_cost?: EtsyMoney;
  discount_amt: EtsyMoney;
  shipments?: { shipment_notification_timestamp: number | null }[];
  transactions: EtsyTransaction[];
  refunds?: { amount: EtsyMoney; status?: string }[];
}

export interface EtsyPayment {
  receipt_id: number;
  amount_gross: EtsyMoney;
  amount_fees: EtsyMoney;
  amount_net: EtsyMoney;
  adjusted_gross?: EtsyMoney | null;
  adjusted_fees?: EtsyMoney | null;
  adjusted_net?: EtsyMoney | null;
}

export interface EtsyLedgerEntry {
  entry_id: number;
  amount: number;
  currency: string;
  description: string | null;
  created_timestamp: number;
  ledger_type: string | null;
  reference_type: string | null;
  reference_id: string | null;
}

export interface EtsyListing {
  listing_id: number;
  title: string | null;
  state: string | null;
  price: EtsyMoney | null;
  quantity: number | null;
  num_favorers: number | null;
  tags?: string[];
  url: string | null;
  original_creation_timestamp?: number | null;
  ending_timestamp?: number | null;
}

export interface EtsyPage<T> {
  count: number;
  results: T[];
}
