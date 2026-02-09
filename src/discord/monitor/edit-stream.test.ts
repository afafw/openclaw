import { describe, expect, it, vi } from "vitest";
import { createDiscordRollingEditStream } from "./edit-stream.js";
import { RateLimitError } from "@buape/carbon";

function createMockRateLimitError(retryAfter = 0.001): RateLimitError {
  const response = new Response(null, {
    status: 429,
    headers: {
      "X-RateLimit-Scope": "user",
      "X-RateLimit-Bucket": "test-bucket",
    },
  });
  return new RateLimitError(response, {
    message: "You are being rate limited.",
    retry_after: retryAfter,
    global: false,
  });
}

describe("createDiscordRollingEditStream", () => {
  it("throttles edits to configured interval", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);

    const sends: string[] = [];
    const edits: Array<{ id: string; content: string }> = [];
    let nextId = 1;

    const stream = createDiscordRollingEditStream({
      maxChars: 2000,
      editIntervalMs: 500,
      send: async (content) => {
        const id = `m${nextId++}`;
        sends.push(`${id}:${content}`);
        return { messageId: id };
      },
      edit: async (messageId, content) => {
        edits.push({ id: messageId, content });
      },
    });

    await stream.append("a");
    await stream.append("b");
    await stream.append("c");

    expect(sends).toHaveLength(1);
    expect(edits).toHaveLength(0);

    await vi.advanceTimersByTimeAsync(499);
    expect(edits).toHaveLength(0);

    await vi.advanceTimersByTimeAsync(1);
    expect(edits).toHaveLength(1);
    expect(edits[0]?.id).toBe("m1");

    vi.useRealTimers();
  });

  it("backs off on rate limits and applies latest buffered content", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);

    const edits: Array<{ id: string; content: string }> = [];
    let editCalls = 0;

    const stream = createDiscordRollingEditStream({
      maxChars: 2000,
      editIntervalMs: 0,
      send: async () => ({ messageId: "m1" }),
      edit: async (messageId, content) => {
        editCalls += 1;
        if (editCalls === 1) {
          throw createMockRateLimitError(1);
        }
        edits.push({ id: messageId, content });
      },
    });

    await stream.append("a");
    await vi.runOnlyPendingTimersAsync(); // run initial flush

    await stream.append("b");
    expect(edits).toHaveLength(0);

    await vi.advanceTimersByTimeAsync(1000); // wait out retry_after
    await vi.runOnlyPendingTimersAsync();

    expect(edits).toHaveLength(1);
    expect(edits[0]?.id).toBe("m1");
    expect(edits[0]?.content).toBe("a\n\nb");

    vi.useRealTimers();
  });

  it("rolls to a new message when exceeding maxChars", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);

    const sends: string[] = [];
    const edits: Array<{ id: string; content: string }> = [];
    let nextId = 1;

    const stream = createDiscordRollingEditStream({
      maxChars: 5,
      editIntervalMs: 500,
      send: async (content) => {
        const id = `m${nextId++}`;
        sends.push(`${id}:${content}`);
        return { messageId: id };
      },
      edit: async (messageId, content) => {
        edits.push({ id: messageId, content });
      },
    });

    await stream.append("hello");
    await stream.append("world");

    // Should have created at least two messages due to overflow.
    expect(sends.length).toBeGreaterThanOrEqual(2);

    vi.useRealTimers();
  });
});
