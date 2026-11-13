import { createClient } from "@/lib/supabase/server";
import { decryptAiSettings, isAiProvider } from "@/lib/ai/encrypted-settings";
import { listModels } from "@/lib/ai/models";

export const maxDuration = 30;

/**
 * Lists the models an API key can use. Send `{ provider, apiKey }` to check a
 * key before saving it, or `{}` to list models for the saved key.
 */
export async function POST(req: Request) {
  const supabase = await createClient();
  const {
    data: { user },
  } = await supabase.auth.getUser();
  if (!user) return Response.json({ error: "Not authenticated" }, { status: 401 });

  const body = (await req.json().catch(() => ({}))) as { provider?: unknown; apiKey?: unknown };
  let provider = isAiProvider(body.provider) ? body.provider : null;
  let apiKey = typeof body.apiKey === "string" ? body.apiKey.trim() : "";

  if (!apiKey) {
    const { data: profile } = await supabase
      .from("profiles")
      .select("settings")
      .eq("user_id", user.id)
      .maybeSingle();
    try {
      const saved = decryptAiSettings(profile?.settings);
      // A saved key only works with the provider it was saved for.
      if (saved && (!provider || provider === saved.provider)) {
        provider = saved.provider;
        apiKey = saved.apiKey;
      }
    } catch (error) {
      return Response.json(
        { error: error instanceof Error ? error.message : "Could not read the saved key." },
        { status: 500 }
      );
    }
  }

  if (!provider || !apiKey) {
    return Response.json({ error: "Enter an API key first." }, { status: 400 });
  }

  try {
    return Response.json({ provider, models: await listModels(provider, apiKey) });
  } catch (error) {
    return Response.json(
      { error: error instanceof Error ? error.message : "Could not list models." },
      { status: 400 }
    );
  }
}
