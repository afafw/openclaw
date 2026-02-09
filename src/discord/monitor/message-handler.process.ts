import { ChannelType, RequestClient } from "@buape/carbon";
import type { ReplyPayload } from "../../auto-reply/types.js";
import type { DiscordMessagePreflightContext } from "./message-handler.preflight.js";
import { resolveAckReaction, resolveHumanDelayConfig } from "../../agents/identity.js";
import { resolveChunkMode } from "../../auto-reply/chunk.js";
import { dispatchInboundMessage } from "../../auto-reply/dispatch.js";
import {
  formatInboundEnvelope,
  formatThreadStarterEnvelope,
  resolveEnvelopeFormatOptions,
} from "../../auto-reply/envelope.js";
import {
  buildPendingHistoryContextFromMap,
  clearHistoryEntriesIfEnabled,
} from "../../auto-reply/reply/history.js";
import { finalizeInboundContext } from "../../auto-reply/reply/inbound-context.js";
import { createReplyDispatcherWithTyping } from "../../auto-reply/reply/reply-dispatcher.js";
import {
  removeAckReactionAfterReply,
  shouldAckReaction as shouldAckReactionGate,
} from "../../channels/ack-reactions.js";
import { logTypingFailure, logAckFailure } from "../../channels/logging.js";
import { createReplyPrefixOptions } from "../../channels/reply-prefix.js";
import { recordInboundSession } from "../../channels/session.js";
import { createTypingCallbacks } from "../../channels/typing.js";
import { resolveMarkdownTableMode } from "../../config/markdown-tables.js";
import { loadSessionStore, readSessionUpdatedAt, resolveStorePath } from "../../config/sessions.js";
import { danger, logVerbose, shouldLogVerbose } from "../../globals.js";
import { convertMarkdownTables } from "../../markdown/tables.js";
import { buildAgentSessionKey } from "../../routing/resolve-route.js";
import { resolveThreadSessionKeys } from "../../routing/session-key.js";
import { buildUntrustedChannelMetadata } from "../../security/channel-metadata.js";
import { truncateUtf16Safe } from "../../utils.js";
import { editMessageDiscord, reactMessageDiscord, removeReactionDiscord, sendMessageDiscord } from "../send.js";
import { normalizeDiscordSlug, resolveDiscordOwnerAllowFrom } from "./allow-list.js";
import { resolveTimestampMs } from "./format.js";
import {
  buildDiscordMediaPayload,
  resolveDiscordMessageText,
  resolveMediaList,
} from "./message-utils.js";
import { buildDirectLabel, buildGuildLabel, resolveReplyContext } from "./reply-context.js";
import { createDiscordRollingEditStream, type DiscordRollingEditStream } from "./edit-stream.js";
import { deliverDiscordReply } from "./reply-delivery.js";
import { resolveDiscordAutoThreadReplyPlan, resolveDiscordThreadStarter } from "./threading.js";
import { sendTyping } from "./typing.js";

export async function processDiscordMessage(ctx: DiscordMessagePreflightContext) {
  const {
    cfg,
    discordConfig,
    accountId,
    token,
    runtime,
    guildHistories,
    historyLimit,
    mediaMaxBytes,
    textLimit,
    replyToMode,
    ackReactionScope,
    message,
    author,
    sender,
    data,
    client,
    channelInfo,
    channelName,
    isGuildMessage,
    isDirectMessage,
    isGroupDm,
    baseText,
    messageText,
    shouldRequireMention,
    canDetectMention,
    effectiveWasMentioned,
    shouldBypassMention,
    threadChannel,
    threadParentId,
    threadParentName,
    threadParentType,
    threadName,
    displayChannelSlug,
    guildInfo,
    guildSlug,
    channelConfig,
    baseSessionKey,
    route,
    commandAuthorized,
  } = ctx;

  const mediaList = await resolveMediaList(message, mediaMaxBytes);
  const text = messageText;
  if (!text) {
    logVerbose(`discord: drop message ${message.id} (empty content)`);
    return;
  }
  const ackReaction = resolveAckReaction(cfg, route.agentId);
  const removeAckAfterReply = cfg.messages?.removeAckAfterReply ?? false;
  const shouldAckReaction = () =>
    Boolean(
      ackReaction &&
      shouldAckReactionGate({
        scope: ackReactionScope,
        isDirect: isDirectMessage,
        isGroup: isGuildMessage || isGroupDm,
        isMentionableGroup: isGuildMessage,
        requireMention: Boolean(shouldRequireMention),
        canDetectMention,
        effectiveWasMentioned,
        shouldBypassMention,
      }),
    );
  const ackReactionPromise = shouldAckReaction()
    ? reactMessageDiscord(message.channelId, message.id, ackReaction, {
        rest: client.rest,
      }).then(
        () => true,
        (err) => {
          logVerbose(`discord react failed for channel ${message.channelId}: ${String(err)}`);
          return false;
        },
      )
    : null;

  const fromLabel = isDirectMessage
    ? buildDirectLabel(author)
    : buildGuildLabel({
        guild: data.guild ?? undefined,
        channelName: channelName ?? message.channelId,
        channelId: message.channelId,
      });
  const senderLabel = sender.label;
  const isForumParent =
    threadParentType === ChannelType.GuildForum || threadParentType === ChannelType.GuildMedia;
  const forumParentSlug =
    isForumParent && threadParentName ? normalizeDiscordSlug(threadParentName) : "";
  const threadChannelId = threadChannel?.id;
  const isForumStarter =
    Boolean(threadChannelId && isForumParent && forumParentSlug) && message.id === threadChannelId;
  const forumContextLine = isForumStarter ? `[Forum parent: #${forumParentSlug}]` : null;
  const groupChannel = isGuildMessage && displayChannelSlug ? `#${displayChannelSlug}` : undefined;
  const groupSubject = isDirectMessage ? undefined : groupChannel;
  const untrustedChannelMetadata = isGuildMessage
    ? buildUntrustedChannelMetadata({
        source: "discord",
        label: "Discord channel topic",
        entries: [channelInfo?.topic],
      })
    : undefined;
  const senderName = sender.isPluralKit
    ? (sender.name ?? author.username)
    : (data.member?.nickname ?? author.globalName ?? author.username);
  const senderUsername = sender.isPluralKit
    ? (sender.tag ?? sender.name ?? author.username)
    : author.username;
  const senderTag = sender.tag;
  const systemPromptParts = [channelConfig?.systemPrompt?.trim() || null].filter(
    (entry): entry is string => Boolean(entry),
  );
  const groupSystemPrompt =
    systemPromptParts.length > 0 ? systemPromptParts.join("\n\n") : undefined;
  const ownerAllowFrom = resolveDiscordOwnerAllowFrom({
    channelConfig,
    guildInfo,
    sender: { id: sender.id, name: sender.name, tag: sender.tag },
  });
  const storePath = resolveStorePath(cfg.session?.store, {
    agentId: route.agentId,
  });
  const envelopeOptions = resolveEnvelopeFormatOptions(cfg);
  const previousTimestamp = readSessionUpdatedAt({
    storePath,
    sessionKey: route.sessionKey,
  });
  let combinedBody = formatInboundEnvelope({
    channel: "Discord",
    from: fromLabel,
    timestamp: resolveTimestampMs(message.timestamp),
    body: text,
    chatType: isDirectMessage ? "direct" : "channel",
    senderLabel,
    previousTimestamp,
    envelope: envelopeOptions,
  });
  const shouldIncludeChannelHistory =
    !isDirectMessage && !(isGuildMessage && channelConfig?.autoThread && !threadChannel);
  if (shouldIncludeChannelHistory) {
    combinedBody = buildPendingHistoryContextFromMap({
      historyMap: guildHistories,
      historyKey: message.channelId,
      limit: historyLimit,
      currentMessage: combinedBody,
      formatEntry: (entry) =>
        formatInboundEnvelope({
          channel: "Discord",
          from: fromLabel,
          timestamp: entry.timestamp,
          body: `${entry.body} [id:${entry.messageId ?? "unknown"} channel:${message.channelId}]`,
          chatType: "channel",
          senderLabel: entry.sender,
          envelope: envelopeOptions,
        }),
    });
  }
  const replyContext = resolveReplyContext(message, resolveDiscordMessageText, {
    envelope: envelopeOptions,
  });
  if (replyContext) {
    combinedBody = `[Replied message - for context]\n${replyContext}\n\n${combinedBody}`;
  }
  if (forumContextLine) {
    combinedBody = `${combinedBody}\n${forumContextLine}`;
  }

  let threadStarterBody: string | undefined;
  let threadLabel: string | undefined;
  let parentSessionKey: string | undefined;
  if (threadChannel) {
    const includeThreadStarter = channelConfig?.includeThreadStarter !== false;
    if (includeThreadStarter) {
      const starter = await resolveDiscordThreadStarter({
        channel: threadChannel,
        client,
        parentId: threadParentId,
        parentType: threadParentType,
        resolveTimestampMs,
      });
      if (starter?.text) {
        const starterEnvelope = formatThreadStarterEnvelope({
          channel: "Discord",
          author: starter.author,
          timestamp: starter.timestamp,
          body: starter.text,
          envelope: envelopeOptions,
        });
        threadStarterBody = starterEnvelope;
      }
    }
    const parentName = threadParentName ?? "parent";
    threadLabel = threadName
      ? `Discord thread #${normalizeDiscordSlug(parentName)} › ${threadName}`
      : `Discord thread #${normalizeDiscordSlug(parentName)}`;
    if (threadParentId) {
      parentSessionKey = buildAgentSessionKey({
        agentId: route.agentId,
        channel: route.channel,
        peer: { kind: "channel", id: threadParentId },
      });
    }
  }
  const mediaPayload = buildDiscordMediaPayload(mediaList);
  const threadKeys = resolveThreadSessionKeys({
    baseSessionKey,
    threadId: threadChannel ? message.channelId : undefined,
    parentSessionKey,
    useSuffix: false,
  });
  const replyPlan = await resolveDiscordAutoThreadReplyPlan({
    client,
    message,
    isGuildMessage,
    channelConfig,
    threadChannel,
    baseText: baseText ?? "",
    combinedBody,
    replyToMode,
    agentId: route.agentId,
    channel: route.channel,
  });
  const deliverTarget = replyPlan.deliverTarget;
  const replyTarget = replyPlan.replyTarget;
  const replyReference = replyPlan.replyReference;
  const autoThreadContext = replyPlan.autoThreadContext;

  const effectiveFrom = isDirectMessage
    ? `discord:${author.id}`
    : (autoThreadContext?.From ?? `discord:channel:${message.channelId}`);
  const effectiveTo = autoThreadContext?.To ?? replyTarget;
  if (!effectiveTo) {
    runtime.error?.(danger("discord: missing reply target"));
    return;
  }

  const ctxPayload = finalizeInboundContext({
    Body: combinedBody,
    RawBody: baseText,
    CommandBody: baseText,
    From: effectiveFrom,
    To: effectiveTo,
    SessionKey: autoThreadContext?.SessionKey ?? threadKeys.sessionKey,
    AccountId: route.accountId,
    ChatType: isDirectMessage ? "direct" : "channel",
    ConversationLabel: fromLabel,
    SenderName: senderName,
    SenderId: sender.id,
    SenderUsername: senderUsername,
    SenderTag: senderTag,
    GroupSubject: groupSubject,
    GroupChannel: groupChannel,
    UntrustedContext: untrustedChannelMetadata ? [untrustedChannelMetadata] : undefined,
    GroupSystemPrompt: isGuildMessage ? groupSystemPrompt : undefined,
    GroupSpace: isGuildMessage ? (guildInfo?.id ?? guildSlug) || undefined : undefined,
    OwnerAllowFrom: ownerAllowFrom,
    Provider: "discord" as const,
    Surface: "discord" as const,
    WasMentioned: effectiveWasMentioned,
    MessageSid: message.id,
    ParentSessionKey: autoThreadContext?.ParentSessionKey ?? threadKeys.parentSessionKey,
    ThreadStarterBody: threadStarterBody,
    ThreadLabel: threadLabel,
    Timestamp: resolveTimestampMs(message.timestamp),
    ...mediaPayload,
    CommandAuthorized: commandAuthorized,
    CommandSource: "text" as const,
    // Originating channel for reply routing.
    OriginatingChannel: "discord" as const,
    OriginatingTo: autoThreadContext?.OriginatingTo ?? replyTarget,
  });

  await recordInboundSession({
    storePath,
    sessionKey: ctxPayload.SessionKey ?? route.sessionKey,
    ctx: ctxPayload,
    updateLastRoute: isDirectMessage
      ? {
          sessionKey: route.mainSessionKey,
          channel: "discord",
          to: `user:${author.id}`,
          accountId: route.accountId,
        }
      : undefined,
    onRecordError: (err) => {
      logVerbose(`discord: failed updating session meta: ${String(err)}`);
    },
  });

  if (shouldLogVerbose()) {
    const preview = truncateUtf16Safe(combinedBody, 200).replace(/\n/g, "\\n");
    logVerbose(
      `discord inbound: channel=${message.channelId} deliver=${deliverTarget} from=${ctxPayload.From} preview="${preview}"`,
    );
  }

  const typingChannelId = deliverTarget.startsWith("channel:")
    ? deliverTarget.slice("channel:".length)
    : message.channelId;

  const streamEditsEnabled = (() => {
    const sessionKey = ctxPayload.SessionKey?.trim();
    if (!sessionKey) {
      return false;
    }
    try {
      const store = loadSessionStore(storePath);
      const entry = store[sessionKey.toLowerCase()] ?? store[sessionKey];
      return entry?.streamEdits === "on";
    } catch {
      return false;
    }
  })();

  const streamMaxChars = Math.min(textLimit, 2000);
  const editIntervalMs = 500; // 2Hz default
  const chunkMode = resolveChunkMode(cfg, "discord", accountId);
  const streamRest = new RequestClient(token, { queueRequests: false });

  const { onModelSelected, ...prefixOptions } = createReplyPrefixOptions({
    cfg,
    agentId: route.agentId,
    channel: "discord",
    accountId: route.accountId,
  });
  const tableMode = resolveMarkdownTableMode({
    cfg,
    channel: "discord",
    accountId,
  });

  let mainStreamChannelId: string | undefined;
  let mainStream: DiscordRollingEditStream | null = null;
  let mainStreamFailed = false;
  let didMarkMainStreamSent = false;

  const flushEditStream = async (stream: DiscordRollingEditStream | null) => {
    if (!stream) {
      return;
    }
    await stream.flush({ force: true });
  };

  const getMainStream = () => {
    if (!streamEditsEnabled || mainStreamFailed) {
      return null;
    }
    if (mainStream) {
      return mainStream;
    }
    const stream = createDiscordRollingEditStream({
      maxChars: streamMaxChars,
      editIntervalMs,
      send: async (content, options) => {
        const replyToId = options?.replyToId;
        const res = await sendMessageDiscord(deliverTarget, content, {
          token,
          retry: { attempts: 1 },
          rest: streamRest,
          accountId,
          replyTo: replyToId,
        });
        mainStreamChannelId = res.channelId;
        if (!didMarkMainStreamSent) {
          didMarkMainStreamSent = true;
          replyReference.markSent();
        }
        return { messageId: res.messageId };
      },
      edit: async (messageId, content) => {
        if (!mainStreamChannelId) {
          return;
        }
        await editMessageDiscord(
          mainStreamChannelId,
          messageId,
          { content },
          { rest: streamRest, token },
        );
      },
      onError: () => {
        mainStreamFailed = true;
        stream.stop();
        mainStream = null;
      },
    });
    mainStream = stream;
    return stream;
  };

  let reasoningStreamChannelId: string | undefined;
  let reasoningStream: DiscordRollingEditStream | null = null;
  let reasoningStreamFailed = false;
  let lastReasoningText = "";

  const getReasoningStream = () => {
    if (reasoningStreamFailed) {
      return null;
    }
    if (reasoningStream) {
      return reasoningStream;
    }
    const stream = createDiscordRollingEditStream({
      maxChars: streamMaxChars,
      editIntervalMs,
      send: async (content) => {
        const res = await sendMessageDiscord(deliverTarget, content, {
          token,
          rest: streamRest,
          retry: { attempts: 1 },
          accountId,
        });
        reasoningStreamChannelId = res.channelId;
        return { messageId: res.messageId };
      },
      edit: async (messageId, content) => {
        if (!reasoningStreamChannelId) {
          return;
        }
        await editMessageDiscord(
          reasoningStreamChannelId,
          messageId,
          { content },
          { rest: streamRest, token },
        );
      },
      onError: () => {
        reasoningStreamFailed = true;
        stream.stop();
        reasoningStream = null;
      },
    });
    reasoningStream = stream;
    return stream;
  };

  const { dispatcher, replyOptions, markDispatchIdle } = createReplyDispatcherWithTyping({
    ...prefixOptions,
    humanDelay: resolveHumanDelayConfig(cfg, route.agentId),
    deliver: async (payload: ReplyPayload, info) => {
      if (
        streamEditsEnabled &&
        info.kind === "block" &&
        payload.text &&
        !payload.mediaUrl &&
        (payload.mediaUrls?.length ?? 0) === 0
      ) {
        const stream = getMainStream();
        if (stream) {
          const rendered = convertMarkdownTables(payload.text, tableMode);
          const replyToId = !stream.hasStarted() ? replyReference.use() : undefined;
          try {
            await stream.append(rendered, { mode: "join", joiner: "\n\n", replyToId });
            return;
          } catch {
            // Fall back to normal delivery if edits fail mid-stream.
            mainStreamFailed = true;
            mainStream?.stop();
            mainStream = null;
          }
        }
      }

      // Flush stream buffers before sending tool/final messages so edits don't lag too far behind.
      if (info.kind !== "block") {
        await flushEditStream(mainStream);
        await flushEditStream(reasoningStream);
      }

      const replyToId = replyReference.use();
      await deliverDiscordReply({
        replies: [payload],
        target: deliverTarget,
        token,
        accountId,
        rest: client.rest,
        runtime,
        replyToId,
        textLimit,
        maxLinesPerMessage: discordConfig?.maxLinesPerMessage,
        tableMode,
        chunkMode,
      });
      replyReference.markSent();
    },
    onError: (err, info) => {
      runtime.error?.(danger(`discord ${info.kind} reply failed: ${String(err)}`));
    },
    onReplyStart: createTypingCallbacks({
      start: () => sendTyping({ client, channelId: typingChannelId }),
      onStartError: (err) => {
        logTypingFailure({
          log: logVerbose,
          channel: "discord",
          target: typingChannelId,
          error: err,
        });
      },
    }).onReplyStart,
  });

  const { queuedFinal, counts } = await dispatchInboundMessage({
    ctx: ctxPayload,
    cfg,
    dispatcher,
    replyOptions: {
      ...replyOptions,
      skillFilter: channelConfig?.skills,
      disableBlockStreaming:
        typeof discordConfig?.blockStreaming === "boolean"
          ? !discordConfig.blockStreaming
          : undefined,
      onReasoningStream: async (payload) => {
        try {
          const next = payload.text ?? "";
          if (!next.trim()) {
            return;
          }
          const stream = getReasoningStream();
          if (!stream) {
            return;
          }
          if (lastReasoningText && !next.startsWith(lastReasoningText)) {
            // Rare: reasoning rewound or changed. Restart the stream on a new message.
            reasoningStream?.stop();
            reasoningStream = null;
            reasoningStreamChannelId = undefined;
            lastReasoningText = "";
          }
          const delta = next.startsWith(lastReasoningText)
            ? next.slice(lastReasoningText.length)
            : next;
          await stream.append(delta, { mode: "raw" });
          lastReasoningText = next;
        } catch {
          // Best-effort; ignore reasoning stream failures.
        }
      },
      onModelSelected,
    },
  });
  await flushEditStream(mainStream);
  await flushEditStream(reasoningStream);
  markDispatchIdle();
  if (!queuedFinal) {
    if (isGuildMessage) {
      clearHistoryEntriesIfEnabled({
        historyMap: guildHistories,
        historyKey: message.channelId,
        limit: historyLimit,
      });
    }
    return;
  }
  if (shouldLogVerbose()) {
    const finalCount = counts.final;
    logVerbose(
      `discord: delivered ${finalCount} reply${finalCount === 1 ? "" : "ies"} to ${replyTarget}`,
    );
  }
  removeAckReactionAfterReply({
    removeAfterReply: removeAckAfterReply,
    ackReactionPromise,
    ackReactionValue: ackReaction,
    remove: async () => {
      await removeReactionDiscord(message.channelId, message.id, ackReaction, {
        rest: client.rest,
      });
    },
    onError: (err) => {
      logAckFailure({
        log: logVerbose,
        channel: "discord",
        target: `${message.channelId}/${message.id}`,
        error: err,
      });
    },
  });
  if (isGuildMessage) {
    clearHistoryEntriesIfEnabled({
      historyMap: guildHistories,
      historyKey: message.channelId,
      limit: historyLimit,
    });
  }
}
