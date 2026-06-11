"use client";

import { useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";
import { useChat } from "@ai-sdk/react";
import { DefaultChatTransport } from "ai";
import {
  getAiSettings,
  getServerAiSettings,
  saveAiSettings,
  subscribeAiSettings,
  type AiSettings,
} from "@/lib/ai/settings-store";
import { PageHeader } from "@/components/page-header";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { toast } from "sonner";
import { ArrowUp, KeyRound, Loader2, Settings2, Sparkles, Square } from "lucide-react";
import { cn } from "@/lib/utils";

const PROVIDERS: {
  id: AiSettings["provider"];
  label: string;
  keyHint: string;
  models: string[];
}[] = [
  {
    id: "anthropic",
    label: "Anthropic",
    keyHint: "sk-ant-…",
    models: ["claude-sonnet-4.6", "claude-haiku-4.5", "claude-opus-4.8"],
  },
  {
    id: "openai",
    label: "OpenAI",
    keyHint: "sk-…",
    models: ["gpt-5.4", "gpt-5.4-mini", "gpt-5.5"],
  },
  {
    id: "google",
    label: "Google",
    keyHint: "AIza…",
    models: ["gemini-3.5-flash", "gemini-3.1-pro-preview"],
  },
];

const TOOL_LABELS: Record<string, string> = {
  get_overview: "Computed shop overview",
  get_monthly_performance: "Analyzed monthly performance",
  get_top_products: "Ranked products",
  get_customer_insights: "Analyzed customers",
  get_geo_breakdown: "Mapped sales geography",
  get_breakdown: "Computed breakdown",
  get_coupon_performance: "Evaluated coupons",
  get_fulfillment_stats: "Checked fulfillment speed",
  get_day_of_week_pattern: "Checked weekly patterns",
  get_forecast: "Ran the forecast model",
  search_orders: "Searched orders",
};

const SUGGESTIONS = [
  "How is my shop doing this year compared to last year?",
  "Which products should I restock before the holidays?",
  "Who are my best customers and what do they buy?",
  "Are my coupons actually making me money?",
  "What does next month look like?",
];

export default function AssistantPage() {
  const settings = useSyncExternalStore(
    subscribeAiSettings,
    getAiSettings,
    getServerAiSettings
  );
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [input, setInput] = useState("");
  const scrollRef = useRef<HTMLDivElement>(null);

  const transport = useMemo(
    () =>
      new DefaultChatTransport({
        api: "/api/chat",
        // headers() runs at request time, so it always reads fresh settings
        headers: () => {
          const s = getAiSettings();
          return {
            "x-ai-provider": s?.provider ?? "",
            "x-ai-key": s?.apiKey ?? "",
            "x-ai-model": s?.model ?? "",
          };
        },
      }),
    []
  );

  const { messages, sendMessage, status, stop, error } = useChat({
    transport,
    onError: (err) => {
      toast.error(err.message || "Something went wrong talking to the model.");
    },
  });

  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight });
  }, [messages]);

  const busy = status === "submitted" || status === "streaming";

  function submit(text: string) {
    const trimmed = text.trim();
    if (!trimmed || busy) return;
    if (!settings?.apiKey) {
      setSettingsOpen(true);
      return;
    }
    void sendMessage({ text: trimmed });
    setInput("");
  }

  return (
    <div className="flex h-[calc(100dvh-9.5rem)] flex-col lg:h-[calc(100dvh-6rem)]">
      <PageHeader
        title="AI Analyst"
        description="Ask anything about your shop — it queries your real data."
        showRange={false}
        actions={
          <Button variant="outline" size="sm" onClick={() => setSettingsOpen(true)}>
            <Settings2 className="size-4" />
            <span className="hidden sm:inline">
              {settings?.apiKey
                ? `${PROVIDERS.find((p) => p.id === settings.provider)?.label} · ${settings.model}`
                : "Set up API key"}
            </span>
          </Button>
        }
      />

      <div ref={scrollRef} className="min-h-0 flex-1 overflow-y-auto pb-4">
        {messages.length === 0 ? (
          <div className="flex h-full flex-col items-center justify-center gap-5 px-4 text-center">
            <div className="flex size-12 items-center justify-center rounded-full bg-primary/10">
              <Sparkles className="size-5 text-primary" />
            </div>
            {!settings?.apiKey ? (
              <>
                <div className="space-y-1">
                  <h2 className="text-base font-semibold">Bring your own API key</h2>
                  <p className="max-w-sm text-sm text-muted-foreground">
                    Connect an Anthropic, OpenAI, or Google API key. It stays in your
                    browser and is only used to answer your questions.
                  </p>
                </div>
                <Button onClick={() => setSettingsOpen(true)}>
                  <KeyRound className="size-4" />
                  Add API key
                </Button>
              </>
            ) : (
              <>
                <p className="max-w-sm text-sm text-muted-foreground">
                  Try one of these to get started:
                </p>
                <div className="flex max-w-lg flex-wrap justify-center gap-2">
                  {SUGGESTIONS.map((s) => (
                    <button
                      key={s}
                      onClick={() => submit(s)}
                      className="rounded-full border px-3.5 py-1.5 text-sm transition-colors hover:bg-accent"
                    >
                      {s}
                    </button>
                  ))}
                </div>
              </>
            )}
          </div>
        ) : (
          <div className="mx-auto grid max-w-3xl gap-4">
            {messages.map((message) => (
              <div
                key={message.id}
                className={cn(
                  "flex",
                  message.role === "user" ? "justify-end" : "justify-start"
                )}
              >
                <div
                  className={cn(
                    "max-w-[90%] sm:max-w-[80%]",
                    message.role === "user"
                      ? "rounded-2xl rounded-br-md bg-primary px-4 py-2.5 text-sm text-primary-foreground"
                      : "grid gap-2"
                  )}
                >
                  {message.parts.map((part, i) => {
                    if (part.type === "text") {
                      return (
                        <p
                          key={i}
                          className={cn(
                            "whitespace-pre-wrap text-sm leading-relaxed",
                            message.role === "assistant" && "text-foreground"
                          )}
                        >
                          {part.text}
                        </p>
                      );
                    }
                    if (part.type.startsWith("tool-")) {
                      const toolName = part.type.replace("tool-", "");
                      return (
                        <Badge key={i} variant="secondary" className="w-fit gap-1.5">
                          <Sparkles className="size-3" />
                          {TOOL_LABELS[toolName] ?? toolName}
                        </Badge>
                      );
                    }
                    return null;
                  })}
                </div>
              </div>
            ))}
            {status === "submitted" && (
              <div className="flex items-center gap-2 text-sm text-muted-foreground">
                <Loader2 className="size-4 animate-spin" />
                Thinking…
              </div>
            )}
            {error && (
              <Card className="border-destructive/40 p-3 text-sm text-destructive">
                {error.message}
              </Card>
            )}
          </div>
        )}
      </div>

      <form
        onSubmit={(e) => {
          e.preventDefault();
          submit(input);
        }}
        className="mx-auto flex w-full max-w-3xl items-end gap-2"
      >
        <Textarea
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault();
              submit(input);
            }
          }}
          rows={1}
          placeholder="Ask about your sales, products, customers, forecast…"
          className="max-h-32 min-h-11 flex-1 resize-none"
        />
        {busy ? (
          <Button type="button" size="icon" variant="outline" onClick={() => stop()}>
            <Square className="size-4" />
          </Button>
        ) : (
          <Button type="submit" size="icon" disabled={!input.trim()}>
            <ArrowUp className="size-4" />
          </Button>
        )}
      </form>

      <SettingsDialog
        key={settingsOpen ? "open" : "closed"}
        open={settingsOpen}
        onOpenChange={setSettingsOpen}
        settings={settings}
        onSave={(s) => {
          saveAiSettings(s);
          toast.success("AI settings saved.");
        }}
      />
    </div>
  );
}

function SettingsDialog({
  open,
  onOpenChange,
  settings,
  onSave,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  settings: AiSettings | null;
  onSave: (s: AiSettings) => void;
}) {
  // The parent remounts this dialog (via key) whenever it opens, so state
  // initializers always reflect the latest saved settings.
  const [provider, setProvider] = useState<AiSettings["provider"]>(
    settings?.provider ?? "anthropic"
  );
  const [apiKey, setApiKey] = useState(settings?.apiKey ?? "");
  const [model, setModel] = useState(settings?.model ?? "");

  const providerInfo = PROVIDERS.find((p) => p.id === provider)!;
  const effectiveModel = model || providerInfo.models[0];

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>AI provider</DialogTitle>
          <DialogDescription>
            Your API key is stored only in this browser and sent with each request — never
            saved on our servers.
          </DialogDescription>
        </DialogHeader>
        <div className="grid gap-4">
          <div className="grid gap-2">
            <Label>Provider</Label>
            <Select
              value={provider}
              onValueChange={(v) => {
                setProvider(v as AiSettings["provider"]);
                setModel("");
              }}
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {PROVIDERS.map((p) => (
                  <SelectItem key={p.id} value={p.id}>
                    {p.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="grid gap-2">
            <Label htmlFor="api-key">API key</Label>
            <Input
              id="api-key"
              type="password"
              placeholder={providerInfo.keyHint}
              value={apiKey}
              onChange={(e) => setApiKey(e.target.value)}
            />
          </div>
          <div className="grid gap-2">
            <Label>Model</Label>
            <Select value={effectiveModel} onValueChange={setModel}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {providerInfo.models.map((m) => (
                  <SelectItem key={m} value={m}>
                    {m}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>
        <DialogFooter>
          <Button
            onClick={() => {
              if (!apiKey.trim()) {
                toast.error("Enter an API key.");
                return;
              }
              onSave({ provider, apiKey: apiKey.trim(), model: effectiveModel });
              onOpenChange(false);
            }}
          >
            Save
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
