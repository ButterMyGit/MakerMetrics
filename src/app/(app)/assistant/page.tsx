"use client";

import {
  useEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
  type ReactNode,
} from "react";
import { useChat } from "@ai-sdk/react";
import { DefaultChatTransport, type UIMessage } from "ai";
import {
  deleteAiSettings,
  getAiSettings,
  getServerAiSettings,
  loadAiSettings,
  saveAiSettings,
  setAiSettingsUser,
  subscribeAiSettings,
  type AiSettings,
  type AiSettingsInput,
} from "@/lib/ai/settings-store";
import { isDemoEmail } from "@/lib/demo-account";
import { useProfile } from "@/hooks/use-profile";
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
import { Sheet, SheetContent, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { toast } from "sonner";
import {
  ArrowUp,
  KeyRound,
  Loader2,
  MessageSquare,
  MessageSquarePlus,
  PanelRightClose,
  PanelRightOpen,
  Settings2,
  Sparkles,
  Square,
  Trash2,
} from "lucide-react";
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

interface ChatThread {
  id: string;
  title: string;
  updatedAt: number;
  messages: UIMessage[];
}

function chatHistoryKey(userId: string): string {
  return `makermetrics-chat-history:${userId}`;
}

function readChatHistory(userId: string): ChatThread[] {
  if (typeof window === "undefined") return [];
  try {
    const raw = localStorage.getItem(chatHistoryKey(userId));
    const saved = raw ? (JSON.parse(raw) as ChatThread[]) : [];
    return Array.isArray(saved) ? saved : [];
  } catch {
    return [];
  }
}

function createThreadId(): string {
  return typeof crypto !== "undefined" && "randomUUID" in crypto
    ? crypto.randomUUID()
    : `thread-${Date.now()}`;
}

function threadTitle(messages: UIMessage[]): string {
  const firstUserText = messages
    .find((m) => m.role === "user")
    ?.parts.find((p) => p.type === "text")?.text;
  if (!firstUserText) return "New chat";
  return firstUserText.length > 48 ? `${firstUserText.slice(0, 45)}...` : firstUserText;
}

function renderInlineMarkdown(text: string): ReactNode[] {
  const parts = text.split(/(\*\*[^*]+\*\*|`[^`]+`)/g).filter(Boolean);
  return parts.map((part, index) => {
    if (part.startsWith("**") && part.endsWith("**")) {
      return <strong key={index}>{part.slice(2, -2)}</strong>;
    }
    if (part.startsWith("`") && part.endsWith("`")) {
      return (
        <code key={index} className="rounded bg-muted px-1 py-0.5 text-[0.85em]">
          {part.slice(1, -1)}
        </code>
      );
    }
    return part;
  });
}

function MarkdownMessage({ text }: { text: string }) {
  const lines = text.split(/\r?\n/);
  const blocks: ReactNode[] = [];
  let listItems: string[] = [];

  function flushList() {
    if (listItems.length === 0) return;
    blocks.push(
      <ul key={`list-${blocks.length}`} className="ml-5 list-disc space-y-1">
        {listItems.map((item, index) => (
          <li key={index}>{renderInlineMarkdown(item)}</li>
        ))}
      </ul>
    );
    listItems = [];
  }

  lines.forEach((line, index) => {
    const trimmed = line.trim();
    if (!trimmed) {
      flushList();
      return;
    }
    const bullet = trimmed.match(/^[-*]\s+(.+)$/);
    if (bullet) {
      listItems.push(bullet[1]);
      return;
    }
    flushList();
    if (/^-{3,}$/.test(trimmed)) {
      blocks.push(<hr key={index} className="my-2 border-border" />);
      return;
    }
    const heading = trimmed.match(/^(#{1,4})\s+(.+)$/);
    if (heading) {
      const level = heading[1].length;
      const className =
        level <= 2
          ? "mt-2 text-base font-semibold"
          : "mt-2 text-sm font-semibold";
      blocks.push(
        <p key={index} className={className}>
          {renderInlineMarkdown(heading[2])}
        </p>
      );
      return;
    }
    blocks.push(
      <p key={index} className="leading-relaxed">
        {renderInlineMarkdown(trimmed)}
      </p>
    );
  });
  flushList();

  return <div className="grid gap-2 text-sm text-foreground">{blocks}</div>;
}

function ChatHistoryPanel({
  threads,
  activeThreadId,
  onNew,
  onOpen,
  onDelete,
}: {
  threads: ChatThread[];
  activeThreadId: string;
  onNew: () => void;
  onOpen: (thread: ChatThread) => void;
  onDelete: (threadId: string) => void;
}) {
  return (
    <>
      <div className="mb-3 flex items-center justify-between gap-3">
        <h2 className="text-sm font-medium">Chat history</h2>
        <Button variant="outline" size="sm" onClick={onNew}>
          <MessageSquarePlus className="size-4" />
          New
        </Button>
      </div>
      <div className="min-h-0 flex-1 space-y-1 overflow-y-auto">
        {threads.length === 0 ? (
          <p className="px-2 py-6 text-sm text-muted-foreground">No saved chats yet.</p>
        ) : (
          threads.map((thread) => (
            <div
              key={thread.id}
              className={cn(
                "group flex items-center gap-1 rounded-md",
                thread.id === activeThreadId && "bg-accent"
              )}
            >
              <button
                type="button"
                onClick={() => onOpen(thread)}
                className="flex min-w-0 flex-1 items-center gap-2 rounded-md px-2 py-2 text-left text-sm hover:bg-accent"
              >
                <MessageSquare className="size-4 shrink-0 text-muted-foreground" />
                <span className="truncate">{thread.title}</span>
              </button>
              <Button
                type="button"
                variant="ghost"
                size="icon"
                className="size-8 opacity-100 lg:opacity-0 lg:group-hover:opacity-100"
                onClick={() => onDelete(thread.id)}
              >
                <Trash2 className="size-4" />
              </Button>
            </div>
          ))
        )}
      </div>
    </>
  );
}

export default function AssistantPage() {
  const { userId, email } = useProfile();
  const isDemo = isDemoEmail(email);
  const settings = useSyncExternalStore(
    subscribeAiSettings,
    getAiSettings,
    getServerAiSettings
  );
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [input, setInput] = useState("");
  const [historyOpen, setHistoryOpen] = useState(true);
  const [historySheetOpen, setHistorySheetOpen] = useState(false);
  const [threads, setThreads] = useState<ChatThread[]>(() => readChatHistory(userId));
  const [activeThreadId, setActiveThreadId] = useState(() => createThreadId());
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
            "x-ai-model": s?.model ?? "",
          };
        },
      }),
    []
  );

  const { messages, setMessages, sendMessage, status, stop, error } = useChat({
    transport,
    onError: (err) => {
      toast.error(err.message || "Something went wrong talking to the model.");
    },
  });

  useEffect(() => {
    setAiSettingsUser(userId);
    void loadAiSettings().catch((err) => {
      toast.error(err instanceof Error ? err.message : "Failed to load AI settings.");
    });
  }, [userId]);

  useEffect(() => {
    if (messages.length === 0) return;
    // eslint-disable-next-line react-hooks/set-state-in-effect -- Persist streamed chat state into the local per-user history list.
    setThreads((prev) => {
      const nextThread: ChatThread = {
        id: activeThreadId,
        title: threadTitle(messages),
        updatedAt: Date.now(),
        messages,
      };
      const withoutCurrent = prev.filter((t) => t.id !== activeThreadId);
      const next = [nextThread, ...withoutCurrent]
        .sort((a, b) => b.updatedAt - a.updatedAt)
        .slice(0, 25);
      localStorage.setItem(chatHistoryKey(userId), JSON.stringify(next));
      return next;
    });
  }, [activeThreadId, messages, userId]);

  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight });
  }, [messages]);

  const busy = status === "submitted" || status === "streaming";

  function submit(text: string) {
    const trimmed = text.trim();
    if (!trimmed || busy) return;
    if (!settings?.hasApiKey) {
      setSettingsOpen(true);
      return;
    }
    void sendMessage({ text: trimmed });
    setInput("");
  }

  function startNewChat() {
    if (busy) stop();
    setActiveThreadId(createThreadId());
    setMessages([]);
    setInput("");
    setHistorySheetOpen(false);
  }

  function openThread(thread: ChatThread) {
    if (busy) stop();
    setActiveThreadId(thread.id);
    setMessages(thread.messages);
    setInput("");
    setHistorySheetOpen(false);
  }

  function deleteThread(threadId: string) {
    const next = threads.filter((t) => t.id !== threadId);
    setThreads(next);
    localStorage.setItem(chatHistoryKey(userId), JSON.stringify(next));
    if (threadId === activeThreadId) {
      startNewChat();
    }
  }

  return (
    <div className="relative left-1/2 flex h-[calc(100dvh-9.5rem)] w-[calc(100vw-2rem)] -translate-x-1/2 flex-col sm:w-[calc(100vw-3rem)] lg:h-[calc(100dvh-6rem)] lg:w-[calc(100vw-19rem)] 2xl:max-w-[1600px]">
      <PageHeader
        title="AI Analyst"
        description="Ask anything about your shop"
        showRange={false}
        actions={
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => {
                if (window.matchMedia("(min-width: 1024px)").matches) {
                  setHistoryOpen((v) => !v);
                } else {
                  setHistorySheetOpen(true);
                }
              }}
            >
              {historyOpen ? (
                <PanelRightClose className="size-4" />
              ) : (
                <PanelRightOpen className="size-4" />
              )}
              <span className="hidden sm:inline">History</span>
            </Button>
            <Button variant="outline" size="sm" onClick={() => setSettingsOpen(true)}>
              <Settings2 className="size-4" />
              <span className="hidden sm:inline">
                {settings?.hasApiKey
                  ? `${PROVIDERS.find((p) => p.id === settings.provider)?.label} · ${settings.model}`
                  : "Set up API key"}
              </span>
            </Button>
          </div>
        }
      />

      <div className="mx-auto flex min-h-0 w-full max-w-7xl flex-1 gap-5">
        <div className="flex min-w-0 flex-1 flex-col">
          <div ref={scrollRef} className="min-h-0 flex-1 overflow-y-auto pb-4">
            {messages.length === 0 ? (
              <div className="flex h-full flex-col items-center justify-center gap-5 px-4 text-center">
                <div className="flex size-12 items-center justify-center rounded-full bg-primary/10">
                  <Sparkles className="size-5 text-primary" />
                </div>
                {!settings?.hasApiKey ? (
                  <>
                    <div className="space-y-1">
                      <h2 className="text-base font-semibold">Bring your own API key</h2>
                      <p className="max-w-sm text-sm text-muted-foreground">
                        Connect an Anthropic, OpenAI, or Google API key. It is encrypted and
                        saved to your account.
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
              <div className="mx-auto grid w-full max-w-4xl gap-4">
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
                          return message.role === "assistant" ? (
                            <MarkdownMessage key={i} text={part.text} />
                          ) : (
                            <p key={i} className="whitespace-pre-wrap text-sm leading-relaxed">
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
              placeholder="Ask about your sales, products, customers, forecast..."
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
        </div>

        {historyOpen && (
          <aside className="hidden w-80 shrink-0 border-l pl-5 lg:flex lg:flex-col xl:w-88">
            <ChatHistoryPanel
              threads={threads}
              activeThreadId={activeThreadId}
              onNew={startNewChat}
              onOpen={openThread}
              onDelete={deleteThread}
            />
          </aside>
        )}
      </div>

      <Sheet open={historySheetOpen} onOpenChange={setHistorySheetOpen}>
        <SheetContent side="right" className="w-[min(28rem,90vw)]">
          <SheetHeader>
            <SheetTitle>Chat history</SheetTitle>
          </SheetHeader>
          <div className="min-h-0 flex-1 px-4 pb-4">
            <ChatHistoryPanel
              threads={threads}
              activeThreadId={activeThreadId}
              onNew={startNewChat}
              onOpen={openThread}
              onDelete={deleteThread}
            />
          </div>
        </SheetContent>
      </Sheet>

      <SettingsDialog
        key={settingsOpen ? "open" : "closed"}
        open={settingsOpen}
        onOpenChange={setSettingsOpen}
        settings={settings}
        isDemo={isDemo}
        onSave={async (s) => {
          try {
            await saveAiSettings(s);
            toast.success("AI settings saved.");
          } catch (err) {
            toast.error(err instanceof Error ? err.message : "Failed to save AI settings.");
            throw err;
          }
        }}
      />
    </div>
  );
}

function SettingsDialog({
  open,
  onOpenChange,
  settings,
  isDemo,
  onSave,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  settings: AiSettings | null;
  isDemo: boolean;
  onSave: (s: AiSettingsInput) => Promise<void>;
}) {
  // The parent remounts this dialog (via key) whenever it opens, so state
  // initializers always reflect the latest saved settings.
  const [provider, setProvider] = useState<AiSettings["provider"]>(
    settings?.provider ?? "anthropic"
  );
  const [apiKey, setApiKey] = useState("");
  const [model, setModel] = useState(settings?.model ?? "");
  const [saving, setSaving] = useState(false);
  const [deleting, setDeleting] = useState(false);

  const providerInfo = PROVIDERS.find((p) => p.id === provider)!;
  const effectiveModel = model || providerInfo.models[0];

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>AI provider</DialogTitle>
          <DialogDescription>
            {isDemo
              ? "The demo account is read-only. Create your own account to save an AI provider key."
              : "Your API key is encrypted before it is saved to your account. Enter a new key here whenever you want to replace the saved key."}
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
              placeholder={settings?.hasApiKey ? "Saved API key" : providerInfo.keyHint}
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
          {settings?.hasApiKey && (
            <Button
              variant="outline"
              disabled={isDemo || saving || deleting}
              onClick={async () => {
                setDeleting(true);
                try {
                  await deleteAiSettings();
                  toast.success("AI key removed.");
                  onOpenChange(false);
                } catch (err) {
                  toast.error(err instanceof Error ? err.message : "Failed to remove AI key.");
                } finally {
                  setDeleting(false);
                }
              }}
            >
              {deleting && <Loader2 className="size-4 animate-spin" />}
              Remove key
            </Button>
          )}
          <Button
            disabled={isDemo || saving || deleting}
            onClick={async () => {
              if (!apiKey.trim()) {
                toast.error("Enter an API key.");
                return;
              }
              setSaving(true);
              try {
                await onSave({ provider, apiKey: apiKey.trim(), model: effectiveModel });
                onOpenChange(false);
              } catch {
                // The parent surfaces the toast; keep the dialog open.
              } finally {
                setSaving(false);
              }
            }}
          >
            {saving && <Loader2 className="size-4 animate-spin" />}
            Save
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
