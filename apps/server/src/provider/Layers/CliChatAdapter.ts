import {
  EventId,
  type ModelSelection,
  type ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ProviderSession,
  RuntimeItemId,
  TextGenerationError,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Random from "effect/Random";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { spawnAndCollect } from "../providerSnapshot.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import type { ProviderAdapterShape, ProviderThreadSnapshot } from "../Services/ProviderAdapter.ts";

export interface CliChatCommandInput {
  readonly cwd: string;
  readonly input: string;
  readonly model: string | undefined;
  readonly modelSelection: ModelSelection | undefined;
  readonly runtimeMode: ProviderSession["runtimeMode"];
  readonly resumeCursor: unknown;
}

export interface CliChatCommand {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly env?: NodeJS.ProcessEnv;
}

export interface CliChatAdapterOptions {
  readonly provider: ProviderDriverKind;
  readonly instanceId: ProviderInstanceId;
  readonly binaryPath: string;
  readonly environment?: NodeJS.ProcessEnv;
  readonly buildCommand: (input: CliChatCommandInput) => CliChatCommand;
  readonly extractAssistantText: (output: {
    readonly stdout: string;
    readonly stderr: string;
  }) => string;
  readonly extractResumeCursor?: (output: {
    readonly stdout: string;
    readonly stderr: string;
    readonly previous: unknown;
  }) => unknown;
}

interface CliChatSessionContext {
  session: ProviderSession;
  turns: Array<{ id: TurnId; items: Array<unknown> }>;
  stopped: boolean;
}

const stripAnsi = (value: string): string =>
  value.replace(
    // eslint-disable-next-line no-control-regex
    /[\u001b\u009b][[\]()#;?]*(?:(?:(?:(?:;[-a-zA-Z\d/#&.:=?%@~_]+)*|[a-zA-Z\d]+(?:;[-a-zA-Z\d/#&.:=?%@~_]*)*)?\u0007)|(?:(?:\d{1,4}(?:;\d{0,4})*)?[\dA-PR-TZcf-nq-uy=><~]))/g,
    "",
  );

export const stripCliAnsi = stripAnsi;

export function makeUnsupportedTextGeneration(provider: ProviderDriverKind) {
  const fail = (operation: string) =>
    Effect.fail(
      new TextGenerationError({
        operation,
        detail: `${provider} does not support T3 Code text generation helpers yet.`,
      }),
    );
  return {
    generateCommitMessage: () => fail("generateCommitMessage"),
    generatePrContent: () => fail("generatePrContent"),
    generateBranchName: () => fail("generateBranchName"),
    generateThreadTitle: () => fail("generateThreadTitle"),
  };
}

export const makeCliChatAdapter = Effect.fn("makeCliChatAdapter")(function* (
  options: CliChatAdapterOptions,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const sessions = new Map<ThreadId, CliChatSessionContext>();
  const runtimeEventQueue = yield* Queue.unbounded<ProviderRuntimeEvent>();

  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  const eventBase = Effect.fn("eventBase")(function* (input: {
    readonly threadId: ThreadId;
    readonly turnId?: TurnId;
    readonly itemId?: RuntimeItemId;
  }) {
    const eventId = EventId.make(yield* Random.nextUUIDv4);
    return {
      eventId,
      provider: options.provider,
      providerInstanceId: options.instanceId,
      threadId: input.threadId,
      createdAt: yield* nowIso,
      ...(input.turnId ? { turnId: input.turnId } : {}),
      ...(input.itemId ? { itemId: input.itemId } : {}),
    };
  });

  const offer = (event: ProviderRuntimeEvent) => Queue.offer(runtimeEventQueue, event);

  const startSession: ProviderAdapterShape<ProviderAdapterError>["startSession"] = (input) =>
    Effect.gen(function* () {
      if (input.provider !== undefined && input.provider !== options.provider) {
        return yield* new ProviderAdapterValidationError({
          provider: options.provider,
          operation: "startSession",
          issue: `Expected provider '${options.provider}' but received '${input.provider}'.`,
        });
      }

      const existing = sessions.get(input.threadId);
      if (existing && !existing.stopped) {
        sessions.delete(input.threadId);
      }

      const timestamp = yield* nowIso;
      const session: ProviderSession = {
        provider: options.provider,
        providerInstanceId: options.instanceId,
        status: "ready",
        runtimeMode: input.runtimeMode,
        cwd: input.cwd ?? process.cwd(),
        ...(input.modelSelection?.instanceId === options.instanceId
          ? { model: input.modelSelection.model }
          : {}),
        threadId: input.threadId,
        ...(input.resumeCursor !== undefined ? { resumeCursor: input.resumeCursor } : {}),
        createdAt: timestamp,
        updatedAt: timestamp,
      };
      sessions.set(input.threadId, {
        session,
        turns: [],
        stopped: false,
      });

      yield* offer({
        ...(yield* eventBase({ threadId: input.threadId })),
        type: "session.started",
        payload: input.resumeCursor !== undefined ? { resume: input.resumeCursor } : {},
      });
      yield* offer({
        ...(yield* eventBase({ threadId: input.threadId })),
        type: "session.state.changed",
        payload: { state: "ready" },
      });

      return session;
    });

  const requireSession = Effect.fn("requireSession")(function* (threadId: ThreadId) {
    const session = sessions.get(threadId);
    if (!session || session.stopped) {
      return yield* new ProviderAdapterSessionNotFoundError({
        provider: options.provider,
        threadId,
      });
    }
    return session;
  });

  const runCommand = Effect.fn("runCliChatCommand")(function* (
    command: CliChatCommand,
    cwd: string,
    threadId: ThreadId,
  ) {
    const childCommand = ChildProcess.make(command.command, [...command.args], {
      cwd,
      env: command.env ?? options.environment ?? process.env,
      shell: process.platform === "win32",
    });
    const result = yield* spawnAndCollect(command.command, childCommand).pipe(
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      Effect.mapError(
        (cause) =>
          new ProviderAdapterProcessError({
            provider: options.provider,
            threadId,
            detail: cause.message,
            cause,
          }),
      ),
    );
    if (result.code === 0) {
      return { stdout: result.stdout, stderr: result.stderr };
    }
    const detail = stripAnsi(result.stderr || result.stdout).trim();
    return yield* new ProviderAdapterProcessError({
      provider: options.provider,
      threadId,
      detail: detail || `CLI process exited with code ${result.code}.`,
    });
  });

  const sendTurn: ProviderAdapterShape<ProviderAdapterError>["sendTurn"] = (input) =>
    Effect.gen(function* () {
      const context = yield* requireSession(input.threadId);
      if (!input.input?.trim()) {
        return yield* new ProviderAdapterRequestError({
          provider: options.provider,
          method: "turn/start",
          detail: "CLI-backed providers require text input.",
        });
      }

      const turnId = TurnId.make(yield* Random.nextUUIDv4);
      const itemId = RuntimeItemId.make(yield* Random.nextUUIDv4);
      const model =
        input.modelSelection?.instanceId === options.instanceId
          ? input.modelSelection.model
          : context.session.model;
      context.session = {
        ...context.session,
        status: "running",
        activeTurnId: turnId,
        ...(model ? { model } : {}),
        updatedAt: yield* nowIso,
      };

      yield* offer({
        ...(yield* eventBase({ threadId: input.threadId, turnId })),
        type: "turn.started",
        payload: model ? { model } : {},
      });
      yield* offer({
        ...(yield* eventBase({ threadId: input.threadId, turnId, itemId })),
        type: "item.started",
        payload: {
          itemType: "assistant_message",
          status: "inProgress",
          title: `${options.provider} response`,
        },
      });

      const command = options.buildCommand({
        cwd: context.session.cwd ?? process.cwd(),
        input: input.input,
        model,
        modelSelection:
          input.modelSelection?.instanceId === options.instanceId
            ? input.modelSelection
            : undefined,
        runtimeMode: context.session.runtimeMode,
        resumeCursor: context.session.resumeCursor,
      });
      const output = yield* runCommand(
        command,
        context.session.cwd ?? process.cwd(),
        input.threadId,
      );
      const text = options.extractAssistantText(output).trim();
      const resumeCursor =
        options.extractResumeCursor?.({
          ...output,
          previous: context.session.resumeCursor,
        }) ?? context.session.resumeCursor;
      if (text) {
        yield* offer({
          ...(yield* eventBase({ threadId: input.threadId, turnId, itemId })),
          type: "content.delta",
          payload: {
            streamKind: "assistant_text",
            delta: text,
          },
        });
      }
      yield* offer({
        ...(yield* eventBase({ threadId: input.threadId, turnId, itemId })),
        type: "item.completed",
        payload: {
          itemType: "assistant_message",
          status: "completed",
          ...(text ? { data: { text } } : {}),
        },
      });
      yield* offer({
        ...(yield* eventBase({ threadId: input.threadId, turnId })),
        type: "turn.completed",
        payload: { state: "completed" },
      });

      context.turns.push({ id: turnId, items: text ? [{ type: "text", text }] : [] });
      context.session = {
        ...context.session,
        status: "ready",
        activeTurnId: undefined,
        ...(resumeCursor !== undefined ? { resumeCursor } : {}),
        updatedAt: yield* nowIso,
      };

      return {
        threadId: input.threadId,
        turnId,
        ...(resumeCursor !== undefined ? { resumeCursor } : {}),
      };
    });

  const stopSession: ProviderAdapterShape<ProviderAdapterError>["stopSession"] = (threadId) =>
    Effect.sync(() => {
      const session = sessions.get(threadId);
      if (!session) return;
      session.stopped = true;
      sessions.delete(threadId);
    });

  const readThread = (
    threadId: ThreadId,
  ): Effect.Effect<ProviderThreadSnapshot, ProviderAdapterError> =>
    requireSession(threadId).pipe(
      Effect.map((session) => ({
        threadId,
        turns: session.turns.map((turn) => ({ id: turn.id, items: [...turn.items] })),
      })),
    );

  return {
    provider: options.provider,
    capabilities: { sessionModelSwitch: "unsupported" as const },
    startSession,
    sendTurn,
    interruptTurn: (threadId: ThreadId) =>
      Effect.sync(() => {
        void sessions.get(threadId);
      }),
    respondToRequest: () => Effect.void,
    respondToUserInput: () => Effect.void,
    stopSession,
    listSessions: () =>
      Effect.succeed(
        [...sessions.values()]
          .filter((session) => !session.stopped)
          .map((session) => session.session),
      ),
    hasSession: (threadId: ThreadId) =>
      Effect.succeed(Boolean(sessions.get(threadId) && !sessions.get(threadId)?.stopped)),
    readThread,
    rollbackThread: (threadId: ThreadId) => readThread(threadId),
    stopAll: () =>
      Effect.sync(() => {
        for (const session of sessions.values()) {
          session.stopped = true;
        }
        sessions.clear();
      }),
    get streamEvents() {
      return Stream.fromQueue(runtimeEventQueue);
    },
  };
});
