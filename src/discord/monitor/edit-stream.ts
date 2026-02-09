import { sliceUtf16Safe, truncateUtf16Safe } from "../../utils.js";
import { RateLimitError } from "@buape/carbon";

export type DiscordRollingEditStream = {
  /** Append text to the current streamed message (rolling to a new message when exceeding maxChars). */
  append: (
    text: string,
    options?: { mode?: "join" | "raw"; joiner?: string; replyToId?: string },
  ) => Promise<void>;
  /** Force an immediate edit flush of the current message (if any). */
  flush: (options?: { force?: boolean }) => Promise<void>;
  /** Stop timers; best-effort flush is still possible by calling flush() first. */
  stop: () => void;
  hasStarted: () => boolean;
};

type SendResult = { messageId: string };

export function createDiscordRollingEditStream(params: {
  maxChars: number;
  editIntervalMs: number;
  send: (content: string, options?: { replyToId?: string }) => Promise<SendResult>;
  edit: (messageId: string, content: string) => Promise<void>;
  onError?: (err: unknown) => void;
}): DiscordRollingEditStream {
  const maxChars = Math.max(1, Math.floor(params.maxChars));
  const editIntervalMs = Math.max(0, Math.floor(params.editIntervalMs));

  let stopped = false;
  let activeMessageId: string | undefined;
  let buffer = "";

  let lastEditAt = 0;
  let cooldownUntil = 0;
  let timer: NodeJS.Timeout | undefined;
  let pending = false;

  let flushRequested = false;
  let flushForced = false;
  let flushTask: Promise<void> | undefined;

  const clearTimer = () => {
    if (timer) {
      clearTimeout(timer);
      timer = undefined;
    }
  };

  const sleep = (ms: number) =>
    new Promise<void>((resolve) => {
      setTimeout(resolve, ms);
    });

  const isRateLimited = (err: unknown): err is RateLimitError => err instanceof RateLimitError;

  const noteRateLimit = (err: RateLimitError) => {
    const retryAfterMs = Math.max(0, Math.ceil(err.retryAfter * 1000));
    cooldownUntil = Math.max(cooldownUntil, Date.now() + retryAfterMs);
  };

  const safeSend = async (content: string, options?: { replyToId?: string }) => {
    try {
      return await params.send(content, options);
    } catch (err) {
      if (isRateLimited(err)) {
        noteRateLimit(err);
        throw err;
      }
      params.onError?.(err);
      throw err;
    }
  };

  const safeEdit = async (messageId: string, content: string) => {
    try {
      await params.edit(messageId, content);
    } catch (err) {
      if (isRateLimited(err)) {
        noteRateLimit(err);
        throw err;
      }
      params.onError?.(err);
      throw err;
    }
  };

  const ensureMessage = async (options?: { replyToId?: string }) => {
    if (activeMessageId) {
      return activeMessageId;
    }
    const content = truncateUtf16Safe(buffer, maxChars);
    if (!content.trim()) {
      return undefined;
    }
    const created = await safeSend(content, options);
    activeMessageId = created.messageId;
    // Treat initial send as a "write" so we don't immediately edit again.
    lastEditAt = Date.now();
    return activeMessageId;
  };

  const rollMessage = () => {
    activeMessageId = undefined;
    buffer = "";
    pending = false;
    clearTimer();
  };

  const splitIfNeeded = async (options?: { replyToId?: string }) => {
    // If a single append pushes us far beyond maxChars, roll through messages until
    // the active buffer is within limits.
    while (buffer.length > maxChars) {
      const head = sliceUtf16Safe(buffer, 0, maxChars);
      const tail = sliceUtf16Safe(buffer, head.length, buffer.length);
      buffer = head;

      // Ensure the current message exists and reflects the head before rolling.
      const msgId = await ensureMessage(options);
      if (msgId) {
        await safeEdit(msgId, buffer);
        lastEditAt = Date.now();
      } else if (buffer.trim()) {
        // No active message but we have content; send it now.
        const created = await safeSend(buffer, options);
        activeMessageId = created.messageId;
        lastEditAt = Date.now();
      }

      // Roll to a new message and continue with the remainder.
      rollMessage();
      buffer = tail;
      // Only the first message should use replyToId.
      options = undefined;
    }
  };

  const schedule = () => {
    if (stopped || pending) {
      return;
    }
    pending = true;
    clearTimer();
    const now = Date.now();
    const nextAt = Math.max(lastEditAt + editIntervalMs, cooldownUntil);
    const delay = Math.max(0, nextAt - now);
    timer = setTimeout(() => {
      void flush({ force: true }).catch(() => {});
    }, delay);
  };

  const runFlushLoop = async () => {
    try {
      while (!stopped) {
        clearTimer();
        pending = false;

        const forced = flushForced;
        flushForced = false;
        flushRequested = false;

        const now = Date.now();
        const nextAt = forced ? now : Math.max(lastEditAt + editIntervalMs, cooldownUntil);
        const delay = Math.max(0, nextAt - now);
        if (delay > 0) {
          await sleep(delay);
          if (stopped) {
            return;
          }
        }

        const content = truncateUtf16Safe(buffer, maxChars);
        if (!content.trim()) {
          return;
        }

        try {
          await splitIfNeeded();
          const msgId = await ensureMessage();
          if (!msgId) {
            return;
          }
          await safeEdit(msgId, truncateUtf16Safe(buffer, maxChars));
          lastEditAt = Date.now();
        } catch (err) {
          if (isRateLimited(err)) {
            // Drop this update; schedule a retry and let newer buffered content win.
            schedule();
            return;
          }
          throw err;
        }

        if (!flushRequested) {
          return;
        }
        // Another flush was requested while we were running; loop and apply latest buffer state.
      }
    } finally {
      flushTask = undefined;
    }
  };

  const requestFlush = (options?: { force?: boolean }) => {
    flushRequested = true;
    if (options?.force) {
      flushForced = true;
    }
    if (!flushTask) {
      flushTask = runFlushLoop();
    }
    return flushTask;
  };

  const flush = async (options?: { force?: boolean }) => {
    if (stopped) {
      return;
    }
    await requestFlush(options);
  };

  const append: DiscordRollingEditStream["append"] = async (text, options) => {
    if (stopped) {
      return;
    }
    const raw = text ?? "";
    if (!raw) {
      return;
    }
    const mode = options?.mode ?? "join";
    const joiner = options?.joiner ?? "\n\n";
    if (mode === "raw") {
      buffer += raw;
    } else if (!buffer) {
      buffer = raw;
    } else {
      buffer += `${joiner}${raw}`;
    }

    const replyToId = options?.replyToId;
    try {
      await splitIfNeeded(replyToId ? { replyToId } : undefined);
      // Ensure we have a message as soon as we have something to show.
      await ensureMessage(replyToId ? { replyToId } : undefined);
    } catch (err) {
      if (!isRateLimited(err)) {
        throw err;
      }
      // Rate limited: keep buffering and rely on scheduled flush to retry later.
    }
    schedule();
  };

  return {
    append,
    flush,
    stop: () => {
      stopped = true;
      clearTimer();
    },
    hasStarted: () => Boolean(activeMessageId),
  };
}
