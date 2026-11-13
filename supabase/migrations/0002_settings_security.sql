-- Settings functions run as the caller, so row level security (including the
-- demo account's read-only rule) applies to them. Previously
-- set_profile_setting was SECURITY DEFINER and callable without signing in.

create or replace function public.is_demo_user()
returns boolean
language sql
stable
set search_path to ''
as $$
  select coalesce((auth.jwt() -> 'app_metadata' ->> 'is_demo')::boolean, false)
$$;

-- Admin accounts are flagged the same way as the demo account, in
-- auth.users.raw_app_meta_data (service role only):
--   update auth.users
--      set raw_app_meta_data = coalesce(raw_app_meta_data, '{}'::jsonb) || '{"role": "admin"}'
--    where email = '<your email>';
create or replace function public.is_admin()
returns boolean
language sql
stable
set search_path to ''
as $$
  select coalesce(auth.jwt() -> 'app_metadata' ->> 'role', '') = 'admin'
$$;

-- Accent color only. Other keys in profiles.settings (such as the encrypted
-- AI key) are written by server routes, never through this function.
create or replace function public.set_profile_setting(key text, value text)
returns void
language plpgsql
security invoker
set search_path to ''
as $$
begin
  if key <> 'accent' or value !~ '^#[0-9a-fA-F]{6}$' then
    raise exception 'Unsupported profile setting';
  end if;
  insert into public.profiles (user_id, settings)
    values ((select auth.uid()), jsonb_build_object(key, value))
  on conflict (user_id)
    do update set settings = public.profiles.settings || jsonb_build_object(key, value);
end;
$$;

-- Merges display/analysis preferences into profiles.settings.prefs and
-- returns the merged object. A null value removes that preference.
create or replace function public.merge_profile_prefs(prefs jsonb)
returns jsonb
language plpgsql
security invoker
set search_path to ''
as $$
declare
  merged jsonb;
begin
  if prefs is null or jsonb_typeof(prefs) <> 'object' then
    raise exception 'prefs must be a JSON object';
  end if;
  if pg_column_size(prefs) > 16384 then
    raise exception 'prefs is too large';
  end if;
  insert into public.profiles (user_id, settings)
    values ((select auth.uid()), jsonb_build_object('prefs', jsonb_strip_nulls(prefs)))
  on conflict (user_id)
    do update set settings = jsonb_set(
      public.profiles.settings,
      '{prefs}',
      jsonb_strip_nulls(coalesce(public.profiles.settings -> 'prefs', '{}'::jsonb) || prefs)
    )
  returning settings -> 'prefs' into merged;
  return merged;
end;
$$;

revoke execute on function public.set_profile_setting(text, text) from public, anon;
grant execute on function public.set_profile_setting(text, text) to authenticated;

revoke execute on function public.merge_profile_prefs(jsonb) from public, anon;
grant execute on function public.merge_profile_prefs(jsonb) to authenticated;
