import { createClient as createAdminClient } from "@supabase/supabase-js";
import { createClient } from "@/lib/supabase/server";
import { isDemoEmail } from "@/lib/demo-account";

export const maxDuration = 30;

/**
 * Permanently deletes the signed-in user's auth account. Because both
 * `profiles` and `sale_items` reference `auth.users (id) on delete cascade`,
 * removing the auth user also removes all of their imported data.
 *
 * Requires the service-role key (added by the Supabase–Vercel integration as
 * SUPABASE_SERVICE_ROLE_KEY). It is only ever read server-side here.
 */
export async function POST() {
  const supabase = await createClient();
  const {
    data: { user },
  } = await supabase.auth.getUser();

  if (!user) {
    return Response.json({ error: "Not authenticated" }, { status: 401 });
  }

  if (isDemoEmail(user.email)) {
    return Response.json({ error: "The demo account is read-only." }, { status: 403 });
  }

  const url = process.env.NEXT_PUBLIC_SUPABASE_URL ?? process.env.SUPABASE_URL;
  const serviceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

  if (!url || !serviceKey) {
    return Response.json(
      {
        error:
          "Account deletion isn't configured. Set SUPABASE_SERVICE_ROLE_KEY in the environment.",
      },
      { status: 500 }
    );
  }

  const admin = createAdminClient(url, serviceKey, {
    auth: { autoRefreshToken: false, persistSession: false },
  });

  // Delete data explicitly first (defensive — cascade should also cover this),
  // then remove the auth user.
  await admin.from("sale_items").delete().eq("user_id", user.id);
  await admin.from("profiles").delete().eq("user_id", user.id);

  const { error } = await admin.auth.admin.deleteUser(user.id);
  if (error) {
    return Response.json({ error: error.message }, { status: 500 });
  }

  // Clear the session cookies for this browser.
  await supabase.auth.signOut();

  return Response.json({ ok: true });
}
