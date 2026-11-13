import type { UIMessage } from "ai";

/**
 * Chat threads are kept in this browser only (localStorage), per account.
 */

export interface ChatThread {
  id: string;
  title: string;
  updatedAt: number;
  messages: UIMessage[];
}

const MAX_THREADS = 30;

function key(userId: string): string {
  return `makermetrics-chat-history:${userId}`;
}

/**
 * Make a stored or interrupted conversation safe to show and to send again:
 * drop empty messages and any tool call that never produced a result.
 */
export function sanitizeMessages(messages: UIMessage[]): UIMessage[] {
  const out: UIMessage[] = [];
  for (const m of messages) {
    if (!m || !Array.isArray(m.parts)) continue;
    const parts = m.parts.filter((p) => {
      if (p.type === "text") return p.text.trim() !== "";
      if (p.type.startsWith("tool-") || p.type === "dynamic-tool") {
        const state = (p as { state?: string }).state;
        return state === "output-available" || state === "output-error";
      }
      return true;
    });
    const hasContent = parts.some((p) => p.type === "text" || p.type.startsWith("tool-"));
    if (!hasContent) continue;
    out.push({ ...m, parts });
  }
  // A conversation can't end on a user turn that follows another user turn's
  // failed reply; keep it simple and just return what is well-formed.
  return out;
}

export function threadTitle(messages: UIMessage[]): string {
  const first = messages.find((m) => m.role === "user")?.parts.find((p) => p.type === "text");
  const text = first && first.type === "text" ? first.text.trim() : "";
  if (!text) return "New chat";
  return text.length > 60 ? `${text.slice(0, 57)}…` : text;
}

export function readThreads(userId: string): ChatThread[] {
  if (typeof window === "undefined") return [];
  try {
    const saved = JSON.parse(localStorage.getItem(key(userId)) ?? "[]") as unknown;
    if (!Array.isArray(saved)) return [];
    return (saved as ChatThread[])
      .filter((t) => t && typeof t.id === "string" && Array.isArray(t.messages))
      .map((t) => ({ ...t, messages: sanitizeMessages(t.messages) }))
      .filter((t) => t.messages.length > 0);
  } catch {
    return [];
  }
}

/** Save, dropping the oldest threads if the browser's storage is full. */
export function writeThreads(userId: string, threads: ChatThread[]): ChatThread[] {
  let list = [...threads].sort((a, b) => b.updatedAt - a.updatedAt).slice(0, MAX_THREADS);
  while (list.length > 0) {
    try {
      localStorage.setItem(key(userId), JSON.stringify(list));
      return list;
    } catch {
      list = list.slice(0, -1);
    }
  }
  try {
    localStorage.removeItem(key(userId));
  } catch {
    // Storage unavailable; history lasts for this visit only.
  }
  return list;
}

export function upsertThread(threads: ChatThread[], thread: ChatThread): ChatThread[] {
  return [thread, ...threads.filter((t) => t.id !== thread.id)];
}
