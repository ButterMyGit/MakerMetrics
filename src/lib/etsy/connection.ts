import type { SupabaseClient } from "@supabase/supabase-js";
import { open, seal } from "@/lib/crypto/secret-box";
import type { EtsySecrets } from "./client";

export interface EtsyConnectionRow {
  status: string;
  shop_id: string | null;
  shop_name: string | null;
  secrets: unknown;
  last_synced_at: string | null;
  last_error: string | null;
}

/** What the browser may know about the connection. Never includes secrets. */
export interface EtsyConnectionInfo {
  status: "none" | "needs_auth" | "connected" | "error";
  shopName: string | null;
  lastSyncedAt: string | null;
  lastError: string | null;
  /** last characters of the saved keystring, to recognize it */
  keyHint: string | null;
}

export async function loadConnection(
  supabase: SupabaseClient,
  userId: string
): Promise<{ row: EtsyConnectionRow; secrets: EtsySecrets } | null> {
  const { data, error } = await supabase
    .from("etsy_connections")
    .select("status, shop_id, shop_name, secrets, last_synced_at, last_error")
    .eq("user_id", userId)
    .maybeSingle();
  if (error) throw new Error(error.message);
  if (!data) return null;
  const secrets = open<EtsySecrets>(data.secrets);
  if (!secrets?.keystring || !secrets.sharedSecret) return null;
  return { row: data as EtsyConnectionRow, secrets };
}

export async function saveSecrets(
  supabase: SupabaseClient,
  userId: string,
  secrets: EtsySecrets,
  extra: Record<string, unknown> = {}
): Promise<void> {
  const { error } = await supabase
    .from("etsy_connections")
    .upsert({ user_id: userId, secrets: seal(secrets), ...extra }, { onConflict: "user_id" });
  if (error) throw new Error(error.message);
}

export function publicInfo(
  loaded: { row: EtsyConnectionRow; secrets: EtsySecrets } | null
): EtsyConnectionInfo {
  if (!loaded) return { status: "none", shopName: null, lastSyncedAt: null, lastError: null, keyHint: null };
  const status = loaded.row.status;
  return {
    status: status === "connected" || status === "error" ? status : "needs_auth",
    shopName: loaded.row.shop_name,
    lastSyncedAt: loaded.row.last_synced_at,
    lastError: loaded.row.last_error,
    keyHint: loaded.secrets.keystring.slice(-4),
  };
}

/** The callback URL the seller must register on their Etsy app, exactly. */
export function callbackUrl(req: Request): string {
  const base = process.env.NEXT_PUBLIC_SITE_URL?.replace(/\/$/, "") ?? new URL(req.url).origin;
  return `${base}/api/etsy/callback`;
}
