import type { SavedAiSettings } from "./encrypted-settings";

export interface ModelOption {
  id: string;
  label: string;
}

type Provider = SavedAiSettings["provider"];

async function getJson(url: string, headers: Record<string, string>): Promise<unknown> {
  const res = await fetch(url, { headers, signal: AbortSignal.timeout(15000) });
  const body = (await res.json().catch(() => null)) as
    | { error?: { message?: string } | string; data?: unknown; models?: unknown }
    | null;
  if (!res.ok) {
    const detail =
      typeof body?.error === "string" ? body.error : (body?.error?.message ?? res.statusText);
    if (res.status === 401 || res.status === 403) {
      throw new Error(`The provider rejected this API key (${detail}).`);
    }
    throw new Error(`Could not reach the provider: ${detail}`);
  }
  return body;
}

/** Text-generation models only: no embeddings, audio, image, moderation or realtime. */
const NOT_CHAT = /embed|whisper|tts|audio|realtime|image|dall-e|moderation|transcribe|search|instruct|babbage|davinci|computer-use|codex|sora|aqa|imagen|veo|robotics|live|native/i;

/**
 * Ask the provider which models this key can use. Doubles as a key check:
 * a bad key fails here with the provider's own message. Newest first.
 */
export async function listModels(provider: Provider, apiKey: string): Promise<ModelOption[]> {
  if (provider === "anthropic") {
    const body = (await getJson("https://api.anthropic.com/v1/models?limit=100", {
      "x-api-key": apiKey,
      "anthropic-version": "2023-06-01",
    })) as { data?: { id: string; display_name?: string; created_at?: string }[] };
    return (body.data ?? [])
      .sort((a, b) => (b.created_at ?? "").localeCompare(a.created_at ?? ""))
      .map((m) => ({ id: m.id, label: m.display_name ?? m.id }));
  }

  if (provider === "openai") {
    const body = (await getJson("https://api.openai.com/v1/models", {
      authorization: `Bearer ${apiKey}`,
    })) as { data?: { id: string; created?: number }[] };
    return (body.data ?? [])
      .filter((m) => /^(gpt-|o\d|chatgpt-)/.test(m.id) && !NOT_CHAT.test(m.id))
      .sort((a, b) => (b.created ?? 0) - (a.created ?? 0))
      .map((m) => ({ id: m.id, label: m.id }));
  }

  const body = (await getJson(
    "https://generativelanguage.googleapis.com/v1beta/models?pageSize=200",
    { "x-goog-api-key": apiKey }
  )) as {
    models?: { name: string; displayName?: string; supportedGenerationMethods?: string[] }[];
  };
  return (body.models ?? [])
    .filter(
      (m) =>
        m.supportedGenerationMethods?.includes("generateContent") &&
        m.name.startsWith("models/gemini") &&
        !NOT_CHAT.test(m.name)
    )
    .map((m) => ({ id: m.name.replace(/^models\//, ""), label: m.displayName ?? m.name }))
    .reverse();
}
