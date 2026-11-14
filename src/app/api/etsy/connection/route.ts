import { createClient } from "@/lib/supabase/server";
import { isDemoEmail } from "@/lib/demo-account";
import { callbackUrl, loadConnection, publicInfo, saveSecrets } from "@/lib/etsy/connection";

async function signedIn() {
  const supabase = await createClient();
  const {
    data: { user },
  } = await supabase.auth.getUser();
  return { supabase, user };
}

/** Connection status (no secrets) and the callback URL to register on Etsy. */
export async function GET(req: Request) {
  const { supabase, user } = await signedIn();
  if (!user) return Response.json({ error: "Not authenticated" }, { status: 401 });
  try {
    const loaded = await loadConnection(supabase, user.id);
    return Response.json({
      connection: publicInfo(loaded),
      callbackUrl: callbackUrl(req),
      // preview deployments have their own address, so their callback URL differs from the live site's
      preview: process.env.VERCEL_ENV === "preview",
    });
  } catch (error) {
    // Most likely the Etsy tables haven't been created yet.
    return Response.json({
      connection: publicInfo(null),
      callbackUrl: callbackUrl(req),
      unavailable: error instanceof Error ? error.message : "Etsy connection is not set up.",
    });
  }
}

/** Save the seller's own Etsy app key. Authorization happens next, at /api/etsy/connect. */
export async function PUT(req: Request) {
  const { supabase, user } = await signedIn();
  if (!user) return Response.json({ error: "Not authenticated" }, { status: 401 });
  if (isDemoEmail(user.email)) {
    return Response.json({ error: "The demo account is read-only." }, { status: 403 });
  }
  const body = (await req.json().catch(() => ({}))) as { keystring?: unknown; sharedSecret?: unknown };
  const keystring = typeof body.keystring === "string" ? body.keystring.trim() : "";
  const sharedSecret = typeof body.sharedSecret === "string" ? body.sharedSecret.trim() : "";
  if (!/^[a-z0-9]{16,64}$/i.test(keystring) || !/^[a-z0-9]{6,64}$/i.test(sharedSecret)) {
    return Response.json(
      { error: "Enter the keystring and shared secret exactly as shown on your Etsy app." },
      { status: 400 }
    );
  }
  try {
    await saveSecrets(supabase, user.id, { keystring, sharedSecret }, {
      status: "needs_auth",
      shop_id: null,
      shop_name: null,
      last_error: null,
    });
  } catch (error) {
    return Response.json(
      { error: error instanceof Error ? error.message : "Could not save the key." },
      { status: 500 }
    );
  }
  return Response.json({ ok: true });
}

/** Disconnect: forget the key and tokens. Imported orders stay. */
export async function DELETE() {
  const { supabase, user } = await signedIn();
  if (!user) return Response.json({ error: "Not authenticated" }, { status: 401 });
  const { error } = await supabase.from("etsy_connections").delete().eq("user_id", user.id);
  if (error) return Response.json({ error: error.message }, { status: 500 });
  return Response.json({ ok: true });
}
