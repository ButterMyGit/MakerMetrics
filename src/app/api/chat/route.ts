import {
  convertToModelMessages,
  stepCountIs,
  streamText,
  type LanguageModel,
  type UIMessage,
} from "ai";
import { createOpenAI } from "@ai-sdk/openai";
import { createAnthropic } from "@ai-sdk/anthropic";
import { createGoogleGenerativeAI } from "@ai-sdk/google";
import { createClient } from "@/lib/supabase/server";
import { buildAnalystTools } from "@/lib/ai/tools";
import { buildSystemPrompt } from "@/lib/ai/prompt";
import { decryptAiSettings } from "@/lib/ai/encrypted-settings";
import { buildOrders, dataThrough, dateBounds } from "@/lib/analytics/core";
import { minIso } from "@/lib/dates";
import { prefsFromSettings } from "@/lib/prefs";
import { dbRowToSaleRow, type SaleItemDbRow } from "@/lib/types";

export const maxDuration = 300;

/** Tool rounds allowed before the model must answer in text. */
const MAX_TOOL_STEPS = 10;

function resolveModel(provider: string, apiKey: string, modelId: string): LanguageModel {
  switch (provider) {
    case "openai":
      return createOpenAI({ apiKey })(modelId);
    case "anthropic":
      return createAnthropic({ apiKey })(modelId);
    case "google":
      return createGoogleGenerativeAI({ apiKey })(modelId);
    default:
      throw new Error(`Unsupported provider: ${provider}`);
  }
}

function fail(message: string, status: number) {
  return Response.json({ error: message }, { status });
}

/** Provider errors arrive as nested objects or JSON strings; show the readable part. */
function readableError(error: unknown): string {
  const text = error instanceof Error ? error.message : String(error);
  if (/401|invalid.*api.?key|incorrect api key|authentication/i.test(text)) {
    return "The AI provider rejected your API key. Check it in Settings → AI Analyst.";
  }
  if (/404|not_found|model.*(not found|does not exist)/i.test(text)) {
    return "The selected model isn't available for your API key. Pick another one in Settings → AI Analyst.";
  }
  if (/429|rate.?limit|quota|overloaded|529/i.test(text)) {
    return "The AI provider is rate-limiting or out of credit for this key. Try again in a moment.";
  }
  return text.length > 300 ? `${text.slice(0, 300)}…` : text;
}

export async function POST(req: Request) {
  const supabase = await createClient();
  const {
    data: { user },
  } = await supabase.auth.getUser();
  if (!user) return fail("Not authenticated", 401);

  let body: { messages?: UIMessage[]; today?: string };
  try {
    body = await req.json();
  } catch {
    return fail("Invalid request.", 400);
  }
  if (!Array.isArray(body.messages) || body.messages.length === 0) {
    return fail("No message to answer.", 400);
  }

  // The saved settings are the only source of provider, model and key.
  const { data: profile, error: profileError } = await supabase
    .from("profiles")
    .select("shop_name, settings")
    .eq("user_id", user.id)
    .maybeSingle();
  if (profileError) return fail(profileError.message, 500);

  let saved;
  try {
    saved = decryptAiSettings(profile?.settings);
  } catch (error) {
    return fail(error instanceof Error ? error.message : "Failed to read saved AI settings.", 500);
  }
  if (!saved) return fail("Add your AI provider API key in Settings → AI Analyst.", 400);

  let model: LanguageModel;
  try {
    model = resolveModel(saved.provider, saved.apiKey, saved.model);
  } catch (error) {
    return fail(error instanceof Error ? error.message : "Invalid provider", 400);
  }

  // Load the user's full history once per request; tools share it.
  const dbRows: SaleItemDbRow[] = [];
  const PAGE = 1000;
  for (let from = 0; ; from += PAGE) {
    const { data, error } = await supabase
      .from("sale_items")
      .select("*")
      .order("sale_date", { ascending: true, nullsFirst: false })
      .order("transaction_id", { ascending: true })
      .range(from, from + PAGE - 1);
    if (error) return fail(error.message, 500);
    dbRows.push(...((data ?? []) as SaleItemDbRow[]));
    if (!data || data.length < PAGE) break;
  }

  const rows = dbRows.map(dbRowToSaleRow);
  const orders = buildOrders(rows);
  const prefs = prefsFromSettings(profile?.settings);
  const today =
    typeof body.today === "string" && /^\d{4}-\d{2}-\d{2}$/.test(body.today)
      ? body.today
      : new Date().toISOString().slice(0, 10);
  const latest = dataThrough(orders);
  const through = latest ? minIso(latest, today) : null;

  const tools = buildAnalystTools({
    orders,
    through,
    today,
    processingDays: prefs.processingDays,
    shareBuyerNames: prefs.shareBuyerNames === true,
  });

  let messages;
  try {
    // A stopped or failed reply leaves tool calls without results; providers
    // reject those, so they are dropped before the conversation is sent.
    messages = await convertToModelMessages(body.messages, {
      tools,
      ignoreIncompleteToolCalls: true,
    });
  } catch {
    return fail("This conversation can't be continued. Start a new chat.", 400);
  }

  const result = streamText({
    model,
    system: buildSystemPrompt({
      today,
      through,
      bounds: dateBounds(orders),
      shopName: profile?.shop_name ?? null,
      currency: rows.find((r) => r.currency)?.currency ?? "USD",
      orderCount: orders.length,
      processingDays: prefs.processingDays ?? null,
      shareBuyerNames: prefs.shareBuyerNames === true,
    }),
    messages,
    tools,
    stopWhen: stepCountIs(MAX_TOOL_STEPS + 1),
    // Guarantee a written answer: the last step may not call tools.
    prepareStep: ({ stepNumber }) =>
      stepNumber >= MAX_TOOL_STEPS ? { toolChoice: "none" as const } : {},
  });

  return result.toUIMessageStreamResponse({ onError: readableError });
}
