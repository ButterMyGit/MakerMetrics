import { cookies } from "next/headers";
import { NextResponse } from "next/server";
import { createClient } from "@/lib/supabase/server";
import { etsyClient, exchangeCode } from "@/lib/etsy/client";
import { callbackUrl, loadConnection, saveSecrets } from "@/lib/etsy/connection";

/** Etsy sends the seller back here after they approve (or decline) access. */
export async function GET(req: Request) {
  const url = new URL(req.url);
  const settings = new URL("/settings", req.url);
  const done = (result: string) => {
    settings.searchParams.set("etsy", result);
    return NextResponse.redirect(settings);
  };

  const supabase = await createClient();
  const {
    data: { user },
  } = await supabase.auth.getUser();
  if (!user) return NextResponse.redirect(new URL("/login", req.url));

  const jar = await cookies();
  const saved = jar.get("etsy_oauth")?.value;
  jar.delete({ name: "etsy_oauth", path: "/api/etsy" });

  if (url.searchParams.get("error")) return done("declined");
  const code = url.searchParams.get("code");
  let pending: { state?: string; verifier?: string } = {};
  try {
    pending = saved ? JSON.parse(saved) : {};
  } catch {
    // Treated as a state mismatch below.
  }
  // The state must be the one this browser was sent out with.
  if (!code || !pending.state || !pending.verifier || url.searchParams.get("state") !== pending.state) {
    return done("failed");
  }

  try {
    const loaded = await loadConnection(supabase, user.id);
    if (!loaded) return done("missing-key");
    const tokens = await exchangeCode(loaded.secrets.keystring, code, pending.verifier, callbackUrl(req));
    const secrets = { ...loaded.secrets, ...tokens };

    const etsy = etsyClient(secrets, async () => undefined);
    const me = await etsy.get<{ user_id: number; shop_id: number }>("/users/me");
    const shop = await etsy.get<{ shop_name?: string }>(`/shops/${me.shop_id}`);

    await saveSecrets(supabase, user.id, secrets, {
      status: "connected",
      shop_id: String(me.shop_id),
      etsy_user_id: String(me.user_id),
      shop_name: shop.shop_name ?? null,
      last_error: null,
    });
    return done("connected");
  } catch (error) {
    await supabase
      .from("etsy_connections")
      .update({ status: "error", last_error: error instanceof Error ? error.message : "Authorization failed." })
      .eq("user_id", user.id);
    return done("failed");
  }
}
