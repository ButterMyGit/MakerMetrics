import { describe, expect, it } from "vitest";
import type { UIMessage } from "ai";
import { sanitizeMessages, threadTitle, upsertThread } from "./chat-history";

const msg = (role: "user" | "assistant", parts: unknown[]): UIMessage =>
  ({ id: Math.random().toString(36), role, parts }) as UIMessage;

describe("sanitizeMessages", () => {
  it("drops tool calls that never finished, which providers reject on the next request", () => {
    const cleaned = sanitizeMessages([
      msg("user", [{ type: "text", text: "How am I doing?" }]),
      msg("assistant", [
        { type: "tool-get_overview", toolCallId: "1", state: "output-available", input: {}, output: {} },
        { type: "tool-get_products", toolCallId: "2", state: "input-available", input: {} },
        { type: "text", text: "Revenue is" },
      ]),
    ]);
    expect(cleaned[1].parts.map((p) => p.type)).toEqual(["tool-get_overview", "text"]);
  });

  it("removes an assistant message that was stopped before producing anything", () => {
    const cleaned = sanitizeMessages([
      msg("user", [{ type: "text", text: "Hi" }]),
      msg("assistant", [{ type: "tool-get_overview", toolCallId: "1", state: "input-streaming" }]),
      msg("assistant", [{ type: "text", text: "  " }]),
    ]);
    expect(cleaned).toHaveLength(1);
  });

  it("tolerates malformed stored data", () => {
    expect(sanitizeMessages([null as unknown as UIMessage, { id: "x", role: "user" } as UIMessage])).toEqual([]);
  });
});

describe("threads", () => {
  it("titles a thread from the first question", () => {
    expect(threadTitle([msg("user", [{ type: "text", text: "Why did sales drop?" }])])).toBe("Why did sales drop?");
    expect(threadTitle([])).toBe("New chat");
    expect(threadTitle([msg("user", [{ type: "text", text: "x".repeat(80) }])])).toHaveLength(58);
  });

  it("replaces an existing thread and moves it to the front", () => {
    const a = { id: "a", title: "A", updatedAt: 1, messages: [] };
    const b = { id: "b", title: "B", updatedAt: 2, messages: [] };
    expect(upsertThread([a, b], { ...a, title: "A2" }).map((t) => t.title)).toEqual(["A2", "B"]);
  });
});
