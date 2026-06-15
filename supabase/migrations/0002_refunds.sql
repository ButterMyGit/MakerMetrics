-- Add order-level refund tracking.
-- Sourced from the EtsyDirectCheckoutPayments export ("Refund Amount"), joined
-- onto existing sale_items by Order ID. Refunds only enrich existing rows;
-- they never create new sale_items, so a refunded order cannot inflate a
-- buyer's order count or repeat-buyer status.

alter table public.sale_items
  add column if not exists refund_amount numeric(12, 2);
