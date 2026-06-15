-- Helper to merge a single key into profiles.settings JSONB without
-- overwriting other keys. Used by the accent color picker (and any future
-- per-user settings).
create or replace function public.set_profile_setting(key text, value text)
returns void
language plpgsql
security definer
set search_path = ''
as $$
begin
  insert into public.profiles (user_id, settings)
    values (auth.uid(), jsonb_build_object(key, value))
  on conflict (user_id)
    do update set
      settings = public.profiles.settings || jsonb_build_object(key, value),
      updated_at = now();
end;
$$;
