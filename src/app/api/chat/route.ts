import {
  convertToModelMessages,
  stepCountIs,
  streamText,
  type UIMessage,
  type LanguageModel,
} from "ai";
import { createOpenAI } from "@ai-sdk/openai";
import { createAnthropic } from "@ai-sdk/anthropic";
import { createGoogleGenerativeAI } from "@ai-sdk/google";
import { createClient } from "@/lib/supabase/server";
import { buildAnalystTools } from "@/lib/ai/tools";
import { decryptAiSettings } from "@/lib/ai/encrypted-settings";
import { dbRowToSaleRow, type SaleItemDbRow } from "@/lib/types";

export const maxDuration = 60;

const DEFAULT_MODELS: Record<string, string> = {
  openai: "gpt-5.4",
  anthropic: "claude-sonnet-4.6",
  google: "gemini-3.5-flash",
};

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

export const SYSTEM_PROMPT = `You are the MakerMetrics AI analyst, an expert e-commerce data analyst embedded in an Etsy seller's analytics dashboard.

The seller's full sales history (imported from Etsy CSV exports) is available through your tools. The tools run the exact same calculations as the dashboard, so always use them instead of guessing numbers.

Guidelines:
- Always ground answers in tool results. Call multiple tools when a question spans topics.
- "Net revenue" means what Etsy pays out after fees (before shipping label costs). Be precise about which figure you cite.
- Be concise and actionable. Sellers want to know WHAT to do and will look for proactivity: which products to restock or retire, when to launch promotions, which customers to nurture, ideas to try, etc.
- When relevant, point out caveats (small sample sizes, partial months, forecast uncertainty).
- Format answers with short paragraphs, bullet lists, and clear numbers (e.g. $1,234.56). Don't dump raw JSON.
- If there is no data, tell the user to import their Etsy CSVs on the Import page.
- Refuse requests unrelated to the seller's shop data or e-commerce analytics.

MakerMetrics pages:
- Dashboard: time range button; KPI cards for Net Revenue, Orders, Units sold, Avg order value, Repeat buyer rate, and Etsy processing fees; performance by month bar chart; top products chart; orders by day of week bar chart; recent orders preview with a "View all" button.
- Orders: time range button; searchable full order list by buyer, item, order ID, SKU, coupon, or location; each order opens a detailed view with line items, customer/location details, order totals, fees, discounts, and net.
- Products: time range button; searchable product list; product ranking by units/revenue; trend labels comparing the last 90 days against the prior 90 days; per-product monthly performance and style/product-type breakdowns.
- Customers: time range button; repeat buyer rate, lifetime value, reorder timing, new-vs-returning buyer trends, top customers with VIP tiers, geography, coupon effectiveness, and fulfillment speed.
- Forecast: full-history revenue/order forecasts, forecast accuracy/backtest summary, uncertainty ranges, and next-month product demand estimates.
- AI Analyst: chat interface that can query the same analytics functions as the dashboard; supports saved provider settings and local chat history.
- Import data: drag-and-drop Etsy CSV files from Sold Order Items and Sold Orders; re-importing files is safe because transactions are deduplicated.
- Settings: shop name, accent color, data management link, sign out, and account deletion.`;

export async function POST(req: Request) {
  const supabase = await createClient();
  const {
    data: { user },
  } = await supabase.auth.getUser();
  if (!user) {
    return Response.json({ error: "Not authenticated" }, { status: 401 });
  }

  let provider = req.headers.get("x-ai-provider") ?? "";
  let apiKey = req.headers.get("x-ai-key") ?? "";
  let modelId = req.headers.get("x-ai-model") || DEFAULT_MODELS[provider] || "";

  if (!apiKey) {
    const { data: profile, error } = await supabase
      .from("profiles")
      .select("settings")
      .eq("user_id", user.id)
      .maybeSingle();
    if (error) {
      return Response.json({ error: error.message }, { status: 500 });
    }
    try {
      const saved = decryptAiSettings(profile?.settings);
      if (saved) {
        provider = provider || saved.provider;
        modelId = modelId || saved.model || DEFAULT_MODELS[saved.provider] || "";
        apiKey = saved.apiKey;
      }
    } catch (error) {
      return Response.json(
        { error: error instanceof Error ? error.message : "Failed to read saved AI settings." },
        { status: 500 }
      );
    }
  }

  if (!provider || !apiKey) {
    return Response.json(
      { error: "Add your AI provider API key in the assistant settings." },
      { status: 400 }
    );
  }

  let model: LanguageModel;
  try {
    model = resolveModel(provider, apiKey, modelId);
  } catch (e) {
    return Response.json(
      { error: e instanceof Error ? e.message : "Invalid provider" },
      { status: 400 }
    );
  }

  // Load the user's full history once per request; tools share it.
  const rows: SaleItemDbRow[] = [];
  const PAGE = 1000;
  for (let from = 0; ; from += PAGE) {
    const { data, error } = await supabase
      .from("sale_items")
      .select("*")
      .order("sale_date", { ascending: true, nullsFirst: false })
      .order("transaction_id", { ascending: true })
      .range(from, from + PAGE - 1);
    if (error) {
      return Response.json({ error: error.message }, { status: 500 });
    }
    rows.push(...((data ?? []) as SaleItemDbRow[]));
    if (!data || data.length < PAGE) break;
  }

  const { messages }: { messages: UIMessage[] } = await req.json();

  const result = streamText({
    model,
    system: SYSTEM_PROMPT,
    messages: await convertToModelMessages(messages),
    tools: buildAnalystTools(rows.map(dbRowToSaleRow)),
    stopWhen: stepCountIs(8),
  });

  return result.toUIMessageStreamResponse({
    onError: (error) => {
      const message = error instanceof Error ? error.message : String(error);
      // Surface provider auth errors clearly (e.g. bad API key).
      return message;
    },
  });
}
