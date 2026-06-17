import { createClient } from "@/lib/supabase/server";

export async function POST() {
  const email =
    process.env.DEMO_EMAIL?.trim() ?? process.env.NEXT_PUBLIC_DEMO_EMAIL?.trim() ?? "";
  const password = process.env.DEMO_PASSWORD ?? "";

  if (!email || !password) {
    return Response.json({ error: "Demo account is not configured." }, { status: 500 });
  }

  const supabase = await createClient();
  const { error } = await supabase.auth.signInWithPassword({ email, password });
  if (error) {
    return Response.json({ error: error.message }, { status: 401 });
  }

  return Response.json({ ok: true });
}
