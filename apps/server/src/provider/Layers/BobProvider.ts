import {
  type BobSettings,
  type ModelCapabilities,
  ProviderDriverKind,
  type ServerProviderModel,
} from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import {
  buildBooleanOptionDescriptor,
  buildSelectOptionDescriptor,
  buildServerProvider,
  DEFAULT_TIMEOUT_MS,
  detailFromResult,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";

const PROVIDER = ProviderDriverKind.make("bob");
const BOB_PRESENTATION = {
  displayName: "Bob",
  showInteractionModeToggle: true,
} as const;

const DEFAULT_BOB_MODEL_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [
    buildSelectOptionDescriptor({
      id: "chatMode",
      label: "Mode",
      options: [
        { value: "advanced", label: "Advanced", isDefault: true },
        { value: "code", label: "Code" },
        { value: "plan", label: "Plan" },
        { value: "ask", label: "Ask" },
        { value: "code-reviewer", label: "Code Reviewer" },
        { value: "simplify", label: "Simplify" },
      ],
    }),
    buildBooleanOptionDescriptor({
      id: "trustWorkspace",
      label: "Trust Workspace",
      currentValue: false,
      description: "Pass --trust to Bob Shell for the current workspace.",
    }),
    buildBooleanOptionDescriptor({
      id: "preCheckAutoApproved",
      label: "Pre-check Auto Approved",
      currentValue: false,
      description: "Ask Bob to pre-check whether auto-approved commands are safe.",
    }),
  ],
});

const BUILT_IN_MODELS: ReadonlyArray<ServerProviderModel> = [
  {
    slug: "premium",
    name: "Premium",
    isCustom: false,
    capabilities: DEFAULT_BOB_MODEL_CAPABILITIES,
  },
  {
    slug: "openai/gpt-oss-20b",
    name: "GPT OSS 20B",
    subProvider: "OpenAI",
    isCustom: false,
    capabilities: DEFAULT_BOB_MODEL_CAPABILITIES,
  },
];

export function makeBobEnvironment(
  environment: NodeJS.ProcessEnv = process.env,
): NodeJS.ProcessEnv {
  const homebrewNode = "/opt/homebrew/opt/node/bin";
  const path = environment.PATH?.includes(homebrewNode)
    ? environment.PATH
    : `${homebrewNode}:${environment.PATH ?? ""}`;
  return {
    ...environment,
    PATH: path,
    BOBSHELL_NO_RELAUNCH: "true",
  };
}

export function extractBobAssistantText(output: { readonly stdout: string }): string {
  const lines = output.stdout.split(/\r?\n/g);
  for (let index = lines.length - 1; index >= 0; index--) {
    const line = lines[index]?.trim();
    if (!line?.startsWith("{")) continue;
    try {
      const parsed = JSON.parse(line) as {
        readonly type?: string;
        readonly parameters?: { readonly result?: unknown };
        readonly output?: unknown;
      };
      if (parsed.type === "tool_use" && typeof parsed.parameters?.result === "string") {
        return parsed.parameters.result;
      }
      if (parsed.type === "tool_result" && typeof parsed.output === "string") {
        return parsed.output;
      }
    } catch {
      // Ignore non-JSON lines from the native CLI.
    }
  }
  return output.stdout;
}

export function extractBobResumeCursor(output: {
  readonly stdout: string;
  readonly previous: unknown;
}): unknown {
  for (const line of output.stdout.split(/\r?\n/g)) {
    const trimmed = line.trim();
    if (!trimmed.startsWith("{")) continue;
    try {
      const parsed = JSON.parse(trimmed) as {
        readonly type?: string;
        readonly session_id?: unknown;
      };
      if (parsed.type === "init" && typeof parsed.session_id === "string") {
        return { sessionId: parsed.session_id };
      }
    } catch {
      // Ignore non-JSON lines from the native CLI.
    }
  }
  return output.previous;
}

export const checkBobProviderStatus = Effect.fn("checkBobProviderStatus")(function* (
  bobSettings: BobSettings,
  environment: NodeJS.ProcessEnv = process.env,
): Effect.fn.Return<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const models = providerModelsFromSettings(
    BUILT_IN_MODELS,
    PROVIDER,
    bobSettings.customModels,
    DEFAULT_BOB_MODEL_CAPABILITIES,
  );

  if (!bobSettings.enabled) {
    return buildServerProvider({
      presentation: BOB_PRESENTATION,
      enabled: false,
      checkedAt,
      models,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Bob is disabled in T3 Code settings.",
      },
    });
  }

  const command = ChildProcess.make(bobSettings.binaryPath, ["--version"], {
    env: makeBobEnvironment(environment),
    shell: process.platform === "win32",
  });
  const versionProbe = yield* spawnAndCollect(bobSettings.binaryPath, command).pipe(
    Effect.timeoutOption(DEFAULT_TIMEOUT_MS),
    Effect.result,
  );

  if (Result.isFailure(versionProbe)) {
    const error = versionProbe.failure;
    return buildServerProvider({
      presentation: BOB_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: !isCommandMissingCause(error),
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: isCommandMissingCause(error)
          ? "Bob Shell CLI (`bob`) is not installed or not on PATH."
          : `Failed to execute Bob Shell health check: ${error instanceof Error ? error.message : String(error)}.`,
      },
    });
  }

  if (Option.isNone(versionProbe.success)) {
    return buildServerProvider({
      presentation: BOB_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: "Bob Shell CLI is installed but timed out while running.",
      },
    });
  }

  const result = versionProbe.success.value;
  const version = parseGenericCliVersion(`${result.stdout}\n${result.stderr}`);
  return buildServerProvider({
    presentation: BOB_PRESENTATION,
    enabled: true,
    checkedAt,
    models,
    probe: {
      installed: true,
      version,
      status: result.code === 0 ? "ready" : "warning",
      auth: { status: "unknown" },
      ...(result.code === 0
        ? {}
        : { message: detailFromResult(result) ?? "Bob Shell CLI is installed but failed to run." }),
    },
  });
});
