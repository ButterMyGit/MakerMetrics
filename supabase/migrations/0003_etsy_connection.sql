-- Etsy API connection (optional). Credentials and tokens are stored only as
-- AES-256-GCM ciphertext produced by the server; the database never sees them
-- in the clear, and the browser never receives the `secrets` column.

create table if not exists public.etsy_connections (
  user_id uuid primary key references auth.users (id) on delete cascade,
  -- 'needs_auth' (key saved, shop not authorized yet), 'connected', 'error'
  status text not null default 'needs_auth',
  shop_id text,
  shop_name text,
  etsy_user_id text,
  -- encrypted { keystring, sharedSecret, accessToken, refreshToken, expiresAt }
  secrets jsonb not null,
  last_synced_at timestamptz,
  last_error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table public.etsy_connections enable row level security;

drop policy if exists etsy_connections_select_own on public.etsy_connections;
create policy etsy_connections_select_own on public.etsy_connections
  for select using ((select auth.uid()) = user_id);
drop policy if exists etsy_connections_insert_own on public.etsy_connections;
create policy etsy_connections_insert_own on public.etsy_connections
  for insert with check ((select auth.uid()) = user_id and not public.is_demo_user());
drop policy if exists etsy_connections_update_own on public.etsy_connections;
create policy etsy_connections_update_own on public.etsy_connections
  for update using ((select auth.uid()) = user_id and not public.is_demo_user())
  with check ((select auth.uid()) = user_id and not public.is_demo_user());
drop policy if exists etsy_connections_delete_own on public.etsy_connections;
create policy etsy_connections_delete_own on public.etsy_connections
  for delete using ((select auth.uid()) = user_id and not public.is_demo_user());

drop trigger if exists etsy_connections_set_updated_at on public.etsy_connections;
create trigger etsy_connections_set_updated_at
  before update on public.etsy_connections
  for each row execute function public.set_updated_at();

-- Etsy payment-account ledger: every fee, ad charge, shipping label, refund
-- and deposit. Filled by the Etsy API sync (and later the statement CSV).
create table if not exists public.ledger_entries (
  user_id uuid not null references auth.users (id) on delete cascade,
  entry_id text not null,
  entry_date date not null,
  amount numeric not null,
  currency text,
  description text,
  ledger_type text,
  reference_type text,
  reference_id text,
  source text not null default 'etsy_api',
  created_at timestamptz not null default now(),
  primary key (user_id, entry_id)
);

create index if not exists ledger_entries_user_date_idx
  on public.ledger_entries (user_id, entry_date);

alter table public.ledger_entries enable row level security;

drop policy if exists ledger_entries_select_own on public.ledger_entries;
create policy ledger_entries_select_own on public.ledger_entries
  for select using ((select auth.uid()) = user_id);
drop policy if exists ledger_entries_insert_own on public.ledger_entries;
create policy ledger_entries_insert_own on public.ledger_entries
  for insert with check ((select auth.uid()) = user_id and not public.is_demo_user());
drop policy if exists ledger_entries_update_own on public.ledger_entries;
create policy ledger_entries_update_own on public.ledger_entries
  for update using ((select auth.uid()) = user_id and not public.is_demo_user())
  with check ((select auth.uid()) = user_id and not public.is_demo_user());
drop policy if exists ledger_entries_delete_own on public.ledger_entries;
create policy ledger_entries_delete_own on public.ledger_entries
  for delete using ((select auth.uid()) = user_id and not public.is_demo_user());

-- Current state of each listing, refreshed on every sync.
create table if not exists public.listings (
  user_id uuid not null references auth.users (id) on delete cascade,
  listing_id text not null,
  title text,
  state text,
  price numeric,
  currency text,
  quantity integer,
  num_favorers integer,
  tags jsonb not null default '[]'::jsonb,
  url text,
  listed_at timestamptz,
  ends_at timestamptz,
  synced_at timestamptz not null default now(),
  primary key (user_id, listing_id)
);

alter table public.listings enable row level security;

drop policy if exists listings_select_own on public.listings;
create policy listings_select_own on public.listings
  for select using ((select auth.uid()) = user_id);
drop policy if exists listings_insert_own on public.listings;
create policy listings_insert_own on public.listings
  for insert with check ((select auth.uid()) = user_id and not public.is_demo_user());
drop policy if exists listings_update_own on public.listings;
create policy listings_update_own on public.listings
  for update using ((select auth.uid()) = user_id and not public.is_demo_user())
  with check ((select auth.uid()) = user_id and not public.is_demo_user());
drop policy if exists listings_delete_own on public.listings;
create policy listings_delete_own on public.listings
  for delete using ((select auth.uid()) = user_id and not public.is_demo_user());
