"use client";

import { useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";
import Link from "next/link";
import { useChat } from "@ai-sdk/react";
import { DefaultChatTransport, type UIMessage } from "ai";
import {
  AlertCircle,
  ArrowUp,
  Check,
  History,
  KeyRound,
  Loader2,
  MessageSquare,
  MessageSquarePlus,
  Sparkles,
  Square,
  Trash2,
} from "lucide-react";
import {
  getAiSettings,
  getServerAiSettings,
  loadAiSettings,
  setAiSettingsUser,
  subscribeAiSettings,
} from "@/lib/ai/settings-store";
import {
  readThreads,
  sanitizeMessages,
  threadTitle,
  upsertThread,
  writeThreads,
  type ChatThread,
} from "@/lib/ai/chat-history";
import { TOOL_LABELS } from "@/lib/ai/tools";
import { buildInsights } from "@/lib/analytics/insights";
import { todayIso } from "@/lib/dates";
import { useProfile } from "@/hooks/use-profile";
import { useSalesData } from "@/hooks/use-sales-data";
import { initialParam } from "@/hooks/use-media-query";
import { Markdown } from "@/components/assistant/markdown";
import { PageHeader } from "@/components/page-header";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import { cn } from "@/lib/utils";

const FALLBACK_QUESTIONS = [
  "How is my shop doing this year compared with last year?",
  "Who are my best customers and what do they buy?",
  "What should I look at first this week?",
  "What does next month look like, and how sure is that?",
];

function createThreadId(): string {
  return typeof crypto !== "undefined" && "randomUUID" in crypto
    ? crypto.randomUUID()
    : `thread-${Date.now()}`;
}

/** The server answers errors as JSON; show the message, not the wrapper. */
function errorText(error: Error): string {
  try {
    const parsed = JSON.parse(error.message) as { error?: string };
    if (parsed && typeof parsed.error === "string") return parsed.error;
  } catch {
    // Plain text already.
  }
  return error.message || "Something went wrong talking to the model.";
}

function ToolChip({ part }: { part: { type: string; state?: string; errorText?: string } }) {
  const name = part.type.replace(/^tool-/, "") as keyof typeof TOOL_LABELS;
  const failed = part.state === "output-error";
  const done = part.state === "output-available";
  return (
    <span
      title={failed ? part.errorText : undefined}
      className={cn(
        "inline-flex w-fit items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs",
        failed ? "border-destructive/40 text-destructive" : "text-muted-foreground"
      )}
    >
      {failed ? (
        <AlertCircle className="size-3" />
      ) : done ? (
        <Check className="size-3" />
      ) : (
        <Loader2 className="size-3 animate-spin" />
      )}
      {TOOL_LABELS[name] ?? name}
      {failed && " failed"}
    </span>
  );
}

function HistoryList({
  threads,
  activeId,
  onNew,
  onOpen,
  onDelete,
}: {
  threads: ChatThread[];
  activeId: string;
  onNew: () => void;
  onOpen: (thread: ChatThread) => void;
  onDelete: (id: string) => void;
}) {
  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <Button variant="outline" size="sm" className="mb-3 w-full justify-start" onClick={onNew}>
        <MessageSquarePlus className="size-4" />
        New chat
      </Button>
      <div className="min-h-0 flex-1 space-y-0.5 overflow-y-auto">
        {threads.length === 0 ? (
          <p className="px-2 py-4 text-sm text-muted-foreground">
            Past chats are kept in this browser and listed here.
          </p>
        ) : (
          threads.map((thread) => (
            <div
              key={thread.id}
              className={cn("group flex items-center rounded-md", thread.id === activeId && "bg-accent")}
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
                aria-label="Delete chat"
                className="size-8 shrink-0 opacity-100 lg:opacity-0 lg:group-hover:opacity-100"
                onClick={() => onDelete(thread.id)}
              >
                <Trash2 className="size-4" />
              </Button>
            </div>
          ))
        )}
      </div>
    </div>
  );
}

export default function AssistantPage() {
  const { userId, prefs } = useProfile();
  const { allOrders, through, today, hasData } = useSalesData();
  const settings = useSyncExternalStore(subscribeAiSettings, getAiSettings, getServerAiSettings);
  const [input, setInput] = useState(() => initialParam("q") ?? "");
  const [historyOpen, setHistoryOpen] = useState(false);
  const [threads, setThreads] = useState<ChatThread[]>(() => readThreads(userId));
  const [activeId, setActiveId] = useState(() => createThreadId());
  const scrollRef = useRef<HTMLDivElement>(null);

  const transport = useMemo(
    () =>
      new DefaultChatTransport({
        api: "/api/chat",
        // Send only what the server needs: clean messages and the viewer's date.
        prepareSendMessagesRequest: ({ messages }) => ({
          body: { messages: sanitizeMessages(messages), today: todayIso() },
        }),
      }),
    []
  );

  const { messages, setMessages, sendMessage, status, stop, error, clearError } = useChat({
    transport,
    experimental_throttle: 60,
  });

  useEffect(() => {
    setAiSettingsUser(userId);
    void loadAiSettings().catch(() => undefined);
  }, [userId]);

  // Save a thread when a reply finishes (or stops), not on every streamed token.
  const savedRef = useRef<string>("");
  useEffect(() => {
    if (status === "submitted" || status === "streaming") return;
    const clean = sanitizeMessages(messages);
    if (clean.length === 0) return;
    const fingerprint = `${activeId}:${clean.length}:${clean[clean.length - 1].id}:${clean[clean.length - 1].parts.length}`;
    if (savedRef.current === fingerprint) return;
    savedRef.current = fingerprint;
    setThreads((prev) =>
      writeThreads(
        userId,
        upsertThread(prev, { id: activeId, title: threadTitle(clean), updatedAt: Date.now(), messages: clean })
      )
    );
  }, [status, messages, activeId, userId]);

  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight });
  }, [messages, status]);

  const suggestions = useMemo(() => {
    const fromData = buildInsights(allOrders, { through, today, processingDays: prefs.processingDays })
      .map((i) => i.question)
      .filter((q): q is string => Boolean(q));
    return [...new Set([...fromData, ...FALLBACK_QUESTIONS])].slice(0, 5);
  }, [allOrders, through, today, prefs.processingDays]);

  const busy = status === "submitted" || status === "streaming";
  const ready = settings?.hasApiKey === true;

  function submit(text: string) {
    const trimmed = text.trim();
    if (!trimmed || busy || !ready) return;
    clearError();
    // A previous reply that failed midway must not be resent as-is.
    setMessages(sanitizeMessages(messages));
    void sendMessage({ text: trimmed });
    setInput("");
  }

  function startNewChat() {
    if (busy) void stop();
    clearError();
    setActiveId(createThreadId());
    setMessages([]);
    setInput("");
    setHistoryOpen(false);
  }

  function openThread(thread: ChatThread) {
    if (busy) void stop();
    clearError();
    setActiveId(thread.id);
    savedRef.current = "";
    setMessages(thread.messages);
    setInput("");
    setHistoryOpen(false);
  }

  function deleteThread(id: string) {
    setThreads((prev) => writeThreads(userId, prev.filter((t) => t.id !== id)));
    if (id === activeId) startNewChat();
  }

  const lastMessage = messages[messages.length - 1];
  const waiting =
    status === "submitted" ||
    (status === "streaming" &&
      lastMessage?.role === "assistant" &&
      !lastMessage.parts.some((p) => p.type === "text" && p.text));

  return (
    <div className="fill-screen flex flex-col">
      <PageHeader
        title="AI Analyst"
        description="Ask about your shop. Answers come from the same numbers as the rest of the app."
        showRange={false}
        actions={
          <Button variant="outline" size="sm" className="lg:hidden" onClick={() => setHistoryOpen(true)}>
            <History className="size-4" />
            Chats
          </Button>
        }
      />

      <div className="flex min-h-0 flex-1 gap-6">
        <div className="flex min-w-0 flex-1 flex-col">
          <div ref={scrollRef} className="min-h-0 flex-1 overflow-y-auto pb-4">
            {messages.length === 0 ? (
              <div className="flex h-full flex-col items-center justify-center gap-5 px-4 text-center">
                <div className="flex size-12 items-center justify-center rounded-full bg-primary/10">
                  <Sparkles className="size-5 text-primary" />
                </div>
                {!ready ? (
                  <>
                    <div className="space-y-1">
                      <h2 className="text-base font-semibold">Connect an AI provider</h2>
                      <p className="max-w-sm text-sm text-muted-foreground">
                        The analyst runs on your own Anthropic, OpenAI or Google API key. Add one
                        in Settings and come back.
                      </p>
                    </div>
                    <Button asChild>
                      <Link href="/settings#ai">
                        <KeyRound className="size-4" />
                        Set up in Settings
                      </Link>
                    </Button>
                  </>
                ) : !hasData ? (
                  <p className="max-w-sm text-sm text-muted-foreground">
                    Import your Etsy exports first; the analyst needs your orders to answer.
                  </p>
                ) : (
                  <>
                    <p className="max-w-sm text-sm text-muted-foreground">
                      Based on your recent numbers, you might ask:
                    </p>
                    <div className="flex max-w-2xl flex-wrap justify-center gap-2">
                      {suggestions.map((s) => (
                        <button
                          key={s}
                          onClick={() => submit(s)}
                          className="rounded-2xl border px-3.5 py-1.5 text-left text-sm transition-colors hover:bg-accent"
                        >
                          {s}
                        </button>
                      ))}
                    </div>
                  </>
                )}
              </div>
            ) : (
              <div className="mx-auto grid w-full max-w-3xl gap-4">
                {messages.map((message: UIMessage) => (
                  <div
                    key={message.id}
                    className={cn("flex", message.role === "user" ? "justify-end" : "justify-start")}
                  >
                    {message.role === "user" ? (
                      <div className="max-w-[90%] rounded-2xl rounded-br-md bg-primary px-4 py-2.5 text-sm text-primary-foreground sm:max-w-[80%]">
                        {message.parts.map((part, i) =>
                          part.type === "text" ? (
                            <p key={i} className="whitespace-pre-wrap leading-relaxed">
                              {part.text}
                            </p>
                          ) : null
                        )}
                      </div>
                    ) : (
                      <div className="grid min-w-0 max-w-full gap-2">
                        <div className="flex flex-wrap gap-1.5 empty:hidden">
                          {message.parts.map((part, i) => {
                            if (!part.type.startsWith("tool-")) return null;
                            const chip = part as { type: string; state?: string; errorText?: string };
                            // A call the model retried successfully isn't worth flagging.
                            const recovered =
                              chip.state === "output-error" &&
                              message.parts.some(
                                (other, j) =>
                                  j > i &&
                                  other.type === part.type &&
                                  (other as { state?: string }).state === "output-available"
                              );
                            return recovered ? null : <ToolChip key={i} part={chip} />;
                          })}
                        </div>
                        {message.parts.map((part, i) =>
                          part.type === "text" && part.text ? <Markdown key={i} text={part.text} /> : null
                        )}
                      </div>
                    )}
                  </div>
                ))}
                {waiting && (
                  <div className="flex items-center gap-2 text-sm text-muted-foreground">
                    <Loader2 className="size-4 animate-spin" />
                    Working on it…
                  </div>
                )}
                {error && (
                  <div className="flex items-start gap-2 rounded-lg border border-destructive/40 p-3 text-sm text-destructive">
                    <AlertCircle className="mt-0.5 size-4 shrink-0" />
                    <div>
                      <p>{errorText(error)}</p>
                      <Link href="/settings#ai" className="mt-1 inline-block text-xs underline">
                        AI settings
                      </Link>
                    </div>
                  </div>
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
                if (e.key === "Enter" && !e.shiftKey && !e.nativeEvent.isComposing) {
                  e.preventDefault();
                  submit(input);
                }
              }}
              rows={1}
              disabled={!ready}
              placeholder={ready ? "Ask about sales, products, customers, the forecast…" : "Add an API key in Settings to start"}
              className="max-h-32 min-h-11 flex-1 resize-none"
            />
            {busy ? (
              <Button type="button" size="icon" variant="outline" onClick={() => void stop()} aria-label="Stop">
                <Square className="size-4" />
              </Button>
            ) : (
              <Button type="submit" size="icon" disabled={!input.trim() || !ready} aria-label="Send">
                <ArrowUp className="size-4" />
              </Button>
            )}
          </form>
          {ready && settings && (
            <p className="mx-auto mt-1.5 w-full max-w-3xl text-[11px] text-muted-foreground">
              Using {settings.model}.{" "}
              <Link href="/settings#ai" className="underline">
                Change
              </Link>
            </p>
          )}
        </div>

        <aside className="hidden w-72 shrink-0 flex-col border-l pl-5 lg:flex">
          <HistoryList
            threads={threads}
            activeId={activeId}
            onNew={startNewChat}
            onOpen={openThread}
            onDelete={deleteThread}
          />
        </aside>
      </div>

      <Sheet open={historyOpen} onOpenChange={setHistoryOpen}>
        <SheetContent side="right" className="w-[min(24rem,90vw)]">
          <SheetHeader>
            <SheetTitle>Chats</SheetTitle>
            <SheetDescription>Kept in this browser only.</SheetDescription>
          </SheetHeader>
          <div className="flex min-h-0 flex-1 flex-col px-4 pb-4">
            <HistoryList
              threads={threads}
              activeId={activeId}
              onNew={startNewChat}
              onOpen={openThread}
              onDelete={deleteThread}
            />
          </div>
        </SheetContent>
      </Sheet>
    </div>
  );
}
