-- MakerMetrics baseline schema.
--
-- Fresh projects: run every file in supabase/migrations in order (SQL editor
-- or `supabase db push`). This file is idempotent, so it is also safe on a
-- database that already has these objects.

-- ---------------------------------------------------------------------------
-- Helper functions
-- ---------------------------------------------------------------------------

create or replace function public.set_updated_at()
returns trigger
language plpgsql
set search_path to ''
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

-- The read-only demo account is flagged in auth.users.raw_app_meta_data,
-- which only the service role can write:
--   update auth.users
--      set raw_app_meta_data = coalesce(raw_app_meta_data, '{}'::jsonb) || '{"is_demo": true}'
--    where email = '<demo account email>';
create or replace function public.is_demo_user()
returns boolean
language sql
stable
set search_path to ''
as $$
  select coalesce((auth.jwt() -> 'app_metadata' ->> 'is_demo')::boolean, false)
$$;

-- ---------------------------------------------------------------------------
-- profiles: one row per account (shop name + settings JSON)
-- ---------------------------------------------------------------------------

create table if not exists public.profiles (
  user_id uuid primary key references auth.users (id) on delete cascade,
  shop_name text,
  settings jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table public.profiles enable row level security;

drop policy if exists profiles_select_own on public.profiles;
create policy profiles_select_own on public.profiles
  for select using ((select auth.uid()) = user_id);

drop policy if exists profiles_insert_own on public.profiles;
create policy profiles_insert_own on public.profiles
  for insert with check ((select auth.uid()) = user_id and not public.is_demo_user());

drop policy if exists profiles_update_own on public.profiles;
create policy profiles_update_own on public.profiles
  for update using ((select auth.uid()) = user_id and not public.is_demo_user())
  with check ((select auth.uid()) = user_id and not public.is_demo_user());

drop policy if exists profiles_delete_own on public.profiles;
create policy profiles_delete_own on public.profiles
  for delete using ((select auth.uid()) = user_id and not public.is_demo_user());

drop trigger if exists profiles_set_updated_at on public.profiles;
create trigger profiles_set_updated_at
  before update on public.profiles
  for each row execute function public.set_updated_at();

-- ---------------------------------------------------------------------------
-- sale_items: one row per Etsy transaction (line item). Order-level values
-- are denormalized onto every line of the order.
-- ---------------------------------------------------------------------------

create table if not exists public.sale_items (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users (id) on delete cascade,
  transaction_id text not null,
  order_id text,
  listing_id text,
  sale_date date,
  date_paid date,
  date_shipped date,
  item_name text,
  card_name text,
  product_type text,
  variations text,
  style text,
  sku text,
  quantity integer,
  price numeric,
  item_total numeric,
  currency text,
  discount_amount numeric,
  shipping_discount numeric,
  shipping numeric,
  sales_tax numeric,
  order_value numeric,
  order_total numeric,
  card_processing_fees numeric,
  order_net numeric,
  adjusted_order_total numeric,
  adjusted_card_processing_fees numeric,
  adjusted_net_order_amount numeric,
  vat_paid_by_buyer numeric,
  in_person_discount numeric,
  in_person_location text,
  order_type text,
  payment_type text,
  coupon_code text,
  coupon_details text,
  buyer_username text,
  buyer_user_id text,
  full_name text,
  ship_city text,
  ship_state text,
  ship_zipcode text,
  ship_country text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  refund_amount numeric,
  constraint sale_items_user_id_transaction_id_key unique (user_id, transaction_id)
);

create index if not exists sale_items_user_sale_date_idx
  on public.sale_items (user_id, sale_date);
create index if not exists sale_items_user_order_idx
  on public.sale_items (user_id, order_id);

alter table public.sale_items enable row level security;

drop policy if exists sale_items_select_own on public.sale_items;
create policy sale_items_select_own on public.sale_items
  for select using ((select auth.uid()) = user_id);

drop policy if exists sale_items_insert_own on public.sale_items;
create policy sale_items_insert_own on public.sale_items
  for insert with check ((select auth.uid()) = user_id and not public.is_demo_user());

drop policy if exists sale_items_update_own on public.sale_items;
create policy sale_items_update_own on public.sale_items
  for update using ((select auth.uid()) = user_id and not public.is_demo_user())
  with check ((select auth.uid()) = user_id and not public.is_demo_user());

drop policy if exists sale_items_delete_own on public.sale_items;
create policy sale_items_delete_own on public.sale_items
  for delete using ((select auth.uid()) = user_id and not public.is_demo_user());

drop trigger if exists sale_items_set_updated_at on public.sale_items;
create trigger sale_items_set_updated_at
  before update on public.sale_items
  for each row execute function public.set_updated_at();
