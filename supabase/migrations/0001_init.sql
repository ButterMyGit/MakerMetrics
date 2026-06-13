-- MakerMetrics initial schema
-- One row per Etsy transaction (line item), with order-level financials denormalized.
-- All access is scoped per user via RLS.

create table public.profiles (
  user_id uuid primary key references auth.users (id) on delete cascade,
  shop_name text,
  settings jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table public.profiles enable row level security;

create policy "profiles_select_own" on public.profiles
  for select using ((select auth.uid()) = user_id);
create policy "profiles_insert_own" on public.profiles
  for insert with check ((select auth.uid()) = user_id);
create policy "profiles_update_own" on public.profiles
  for update using ((select auth.uid()) = user_id);
create policy "profiles_delete_own" on public.profiles
  for delete using ((select auth.uid()) = user_id);

create table public.sale_items (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users (id) on delete cascade,

  -- identity
  transaction_id text not null,
  order_id text,
  listing_id text,

  -- dates (stored as dates; Etsy exports carry no time component)
  sale_date date,
  date_paid date,
  date_shipped date,

  -- item
  item_name text,
  card_name text,      -- derived: portion of item_name before "|"
  product_type text,   -- derived: portion of item_name after "|"
  variations text,
  style text,          -- derived from variations
  sku text,
  quantity integer,
  price numeric(12, 2),
  item_total numeric(12, 2),
  currency text,

  -- order-level financials (denormalized; aggregate with distinct order_id)
  discount_amount numeric(12, 2),
  shipping_discount numeric(12, 2),
  shipping numeric(12, 2),
  sales_tax numeric(12, 2),
  order_value numeric(12, 2),
  order_total numeric(12, 2),
  card_processing_fees numeric(12, 2),
  order_net numeric(12, 2),
  adjusted_order_total numeric(12, 2),
  adjusted_card_processing_fees numeric(12, 2),
  adjusted_net_order_amount numeric(12, 2),
  vat_paid_by_buyer numeric(12, 2),
  in_person_discount numeric(12, 2),
  in_person_location text,

  -- order metadata
  order_type text,
  payment_type text,
  coupon_code text,
  coupon_details text,

  -- buyer
  buyer_username text,
  buyer_user_id text,
  full_name text,

  -- shipping destination (no street address is stored by design)
  ship_city text,
  ship_state text,
  ship_zipcode text,
  ship_country text,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),

  unique (user_id, transaction_id)
);

create index sale_items_user_sale_date_idx on public.sale_items (user_id, sale_date);
create index sale_items_user_order_idx on public.sale_items (user_id, order_id);

alter table public.sale_items enable row level security;

create policy "sale_items_select_own" on public.sale_items
  for select using ((select auth.uid()) = user_id);
create policy "sale_items_insert_own" on public.sale_items
  for insert with check ((select auth.uid()) = user_id);
create policy "sale_items_update_own" on public.sale_items
  for update using ((select auth.uid()) = user_id);
create policy "sale_items_delete_own" on public.sale_items
  for delete using ((select auth.uid()) = user_id);

-- keep updated_at fresh on upserts
create or replace function public.set_updated_at()
returns trigger
language plpgsql
security invoker
set search_path = ''
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

create trigger sale_items_set_updated_at
  before update on public.sale_items
  for each row execute function public.set_updated_at();

create trigger profiles_set_updated_at
  before update on public.profiles
  for each row execute function public.set_updated_at();
