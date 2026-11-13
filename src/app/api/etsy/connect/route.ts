import { cookies } from "next/headers";
import { NextResponse } from "next/server";
import { createClient } from "@/lib/supabase/server";
import { authorizeUrl, createPkce } from "@/lib/etsy/client";
import { callbackUrl, loadConnection } from "@/lib/etsy/connection";

/** Starts Etsy's OAuth flow: remembers the PKCE verifier and sends the seller to Etsy. */
export async function GET(req: Request) {
  const supabase = await createClient();
  const {
    data: { user },
  } = await supabase.auth.getUser();
  const settings = new URL("/settings", req.url);
  if (!user) return NextResponse.redirect(new URL("/login", req.url));

  const loaded = await loadConnection(supabase, user.id).catch(() => null);
  if (!loaded) {
    settings.searchParams.set("etsy", "missing-key");
    return NextResponse.redirect(settings);
  }

  const pkce = createPkce();
  const jar = await cookies();
  jar.set("etsy_oauth", JSON.stringify({ state: pkce.state, verifier: pkce.verifier }), {
    httpOnly: true,
    secure: process.env.NODE_ENV === "production",
    sameSite: "lax",
    path: "/api/etsy",
    maxAge: 600,
  });
  return NextResponse.redirect(
    authorizeUrl(loaded.secrets.keystring, callbackUrl(req), pkce.challenge, pkce.state)
  );
}
