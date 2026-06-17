import { createClient } from "@/lib/supabase/server";
import {
  encryptAiSettings,
  isAiProvider,
  publicAiSettingsFromProfileSettings,
} from "@/lib/ai/encrypted-settings";

export async function GET() {
  const supabase = await createClient();
  const {
    data: { user },
  } = await supabase.auth.getUser();
  if (!user) {
    return Response.json({ error: "Not authenticated" }, { status: 401 });
  }

  const { data, error } = await supabase
    .from("profiles")
    .select("settings")
    .eq("user_id", user.id)
    .maybeSingle();
  if (error) {
    return Response.json({ error: error.message }, { status: 500 });
  }

  return Response.json({ settings: publicAiSettingsFromProfileSettings(data?.settings) });
}

export async function PUT(req: Request) {
  const supabase = await createClient();
  const {
    data: { user },
  } = await supabase.auth.getUser();
  if (!user) {
    return Response.json({ error: "Not authenticated" }, { status: 401 });
  }

  const body = (await req.json()) as {
    provider?: unknown;
    model?: unknown;
    apiKey?: unknown;
  };
  if (
    !isAiProvider(body.provider) ||
    typeof body.model !== "string" ||
    !body.model.trim() ||
    typeof body.apiKey !== "string" ||
    !body.apiKey.trim()
  ) {
    return Response.json({ error: "Provider, model, and API key are required." }, { status: 400 });
  }

  const { data: current, error: readError } = await supabase
    .from("profiles")
    .select("settings")
    .eq("user_id", user.id)
    .maybeSingle();
  if (readError) {
    return Response.json({ error: readError.message }, { status: 500 });
  }

  let encrypted;
  try {
    encrypted = encryptAiSettings({
      provider: body.provider,
      model: body.model.trim(),
      apiKey: body.apiKey.trim(),
    });
  } catch (error) {
    return Response.json(
      { error: error instanceof Error ? error.message : "Failed to encrypt AI settings." },
      { status: 500 }
    );
  }

  const existingSettings =
    current?.settings && typeof current.settings === "object"
      ? (current.settings as Record<string, unknown>)
      : {};
  const settings = { ...existingSettings, ai: encrypted };
  const { error } = await supabase
    .from("profiles")
    .upsert({ user_id: user.id, settings }, { onConflict: "user_id" });

  if (error) {
    return Response.json({ error: error.message }, { status: 500 });
  }

  return Response.json({
    settings: {
      provider: encrypted.provider,
      model: encrypted.model,
      hasApiKey: true,
    },
  });
}

export async function DELETE() {
  const supabase = await createClient();
  const {
    data: { user },
  } = await supabase.auth.getUser();
  if (!user) {
    return Response.json({ error: "Not authenticated" }, { status: 401 });
  }

  const { data: current, error: readError } = await supabase
    .from("profiles")
    .select("settings")
    .eq("user_id", user.id)
    .maybeSingle();
  if (readError) {
    return Response.json({ error: readError.message }, { status: 500 });
  }

  const settings =
    current?.settings && typeof current.settings === "object"
      ? { ...(current.settings as Record<string, unknown>) }
      : {};
  delete settings.ai;

  const { error } = await supabase
    .from("profiles")
    .upsert({ user_id: user.id, settings }, { onConflict: "user_id" });
  if (error) {
    return Response.json({ error: error.message }, { status: 500 });
  }

  return Response.json({ settings: null });
}
