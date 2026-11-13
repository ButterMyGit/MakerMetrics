import { createClient } from "@/lib/supabase/server";
import { isDemoEmail } from "@/lib/demo-account";
import { etsyClient } from "@/lib/etsy/client";
import { loadConnection, saveSecrets } from "@/lib/etsy/connection";
import { syncShop } from "@/lib/etsy/sync";

export const maxDuration = 300;

/** Leave time to finish the page in progress and save state before the limit. */
const BUDGET_MS = 220_000;

export async function POST(req: Request) {
  const started = Date.now();
  const supabase = await createClient();
  const {
    data: { user },
  } = await supabase.auth.getUser();
  if (!user) return Response.json({ error: "Not authenticated" }, { status: 401 });
  if (isDemoEmail(user.email)) {
    return Response.json({ error: "The demo account is read-only." }, { status: 403 });
  }

  const body = (await req.json().catch(() => ({}))) as { timeZone?: unknown };
  const timeZone = typeof body.timeZone === "string" && body.timeZone ? body.timeZone : "UTC";

  let loaded;
  try {
    loaded = await loadConnection(supabase, user.id);
  } catch (error) {
    return Response.json({ error: error instanceof Error ? error.message : "Not connected." }, { status: 500 });
  }
  if (!loaded || !loaded.row.shop_id || !loaded.secrets.refreshToken) {
    return Response.json({ error: "Connect your Etsy shop in Settings first." }, { status: 400 });
  }

  let secrets = loaded.secrets;
  const etsy = etsyClient(secrets, async (tokens) => {
    secrets = { ...secrets, ...tokens };
    await saveSecrets(supabase, user.id, secrets);
  });

  try {
    const result = await syncShop({
      etsy,
      supabase,
      userId: user.id,
      shopId: loaded.row.shop_id,
      since: loaded.row.last_synced_at ? new Date(loaded.row.last_synced_at) : null,
      timeZone,
      deadline: started + BUDGET_MS,
    });
    await supabase
      .from("etsy_connections")
      .update({
        status: "connected",
        last_error: null,
        last_synced_at: (result.resumeFrom ?? new Date(started)).toISOString(),
      })
      .eq("user_id", user.id);
    return Response.json({ ...result, more: result.resumeFrom != null });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Sync failed.";
    await supabase.from("etsy_connections").update({ last_error: message }).eq("user_id", user.id);
    return Response.json({ error: message }, { status: 502 });
  }
}
