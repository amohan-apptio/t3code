import {
  type KiroSettings,
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

import { stripCliAnsi } from "./CliChatAdapter.ts";
import {
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

const PROVIDER = ProviderDriverKind.make("kiro");
const KIRO_PRESENTATION = {
  displayName: "Kiro",
  showInteractionModeToggle: true,
} as const;

function makeKiroModelCapabilities(agentOptions?: ReadonlyArray<{ value: string; label: string }>) {
  return createModelCapabilities({
    optionDescriptors: [
      buildSelectOptionDescriptor({
        id: "agent",
        label: "Agent",
        options:
          agentOptions && agentOptions.length > 0
            ? [{ value: "default", label: "Default", isDefault: true }, ...agentOptions]
            : [{ value: "default", label: "Default", isDefault: true }],
      }),
      buildSelectOptionDescriptor({
        id: "agentEngine",
        label: "Engine",
        options: [
          { value: "v2", label: "V2", isDefault: true },
          { value: "v1", label: "V1" },
          { value: "kas", label: "KAS" },
        ],
      }),
      buildSelectOptionDescriptor({
        id: "mode",
        label: "Mode",
        options: [
          { value: "vibe", label: "Vibe", isDefault: true },
          { value: "spec", label: "Spec" },
        ],
      }),
    ],
  });
}

const DEFAULT_KIRO_MODEL_CAPABILITIES: ModelCapabilities = makeKiroModelCapabilities();

const FALLBACK_MODELS: ReadonlyArray<ServerProviderModel> = [
  { slug: "auto", name: "Auto", isCustom: false, capabilities: DEFAULT_KIRO_MODEL_CAPABILITIES },
];

interface KiroModelListResponse {
  readonly models?: ReadonlyArray<{
    readonly model_name?: unknown;
    readonly model_id?: unknown;
    readonly context_window_tokens?: unknown;
    readonly rate_multiplier?: unknown;
    readonly rate_unit?: unknown;
  }>;
  readonly default_model?: unknown;
}

export function extractKiroAssistantText(output: { readonly stdout: string }): string {
  return stripCliAnsi(output.stdout)
    .replace(/^\s*> /gm, "")
    .replace(/Credits:.*$/gm, "")
    .trim();
}

function parseKiroAgents(stdout: string): ReadonlyArray<{ value: string; label: string }> {
  const agents: Array<{ value: string; label: string }> = [];
  for (const rawLine of stripCliAnsi(stdout).split(/\r?\n/g)) {
    const line = rawLine.trim();
    const match = line.match(/^\*?\s*(kiro_[a-zA-Z0-9_-]+)\s+(?:\(Built-in\)\s+)?(.+)$/);
    if (!match) continue;
    const value = match[1] ?? "";
    const label = match[2]?.trim() || value;
    if (value && !agents.some((agent) => agent.value === value)) {
      agents.push({ value, label });
    }
  }
  return agents;
}

function formatKiroModelName(value: string): string {
  if (value === "auto") return "Auto";
  return value
    .split(/[-_/]+/g)
    .filter(Boolean)
    .map((part) => {
      if (part.toLowerCase() === "claude") return "Claude";
      if (part.toLowerCase() === "gpt") return "GPT";
      return part.charAt(0).toUpperCase() + part.slice(1);
    })
    .join(" ");
}

function parseKiroModels(
  stdout: string,
  agentOptions: ReadonlyArray<{ value: string; label: string }>,
): ReadonlyArray<ServerProviderModel> {
  try {
    const parsed = JSON.parse(stdout) as KiroModelListResponse;
    const defaultModel = typeof parsed.default_model === "string" ? parsed.default_model : "auto";
    const capabilities = makeKiroModelCapabilities(agentOptions);
    const models = (parsed.models ?? []).flatMap((entry): ServerProviderModel[] => {
      const slug =
        typeof entry.model_id === "string"
          ? entry.model_id.trim()
          : typeof entry.model_name === "string"
            ? entry.model_name.trim()
            : "";
      if (!slug) return [];
      const name =
        typeof entry.model_name === "string" && entry.model_name.trim()
          ? formatKiroModelName(entry.model_name.trim())
          : formatKiroModelName(slug);
      return [
        {
          slug,
          name,
          isCustom: false,
          capabilities,
          ...(slug === defaultModel ? { shortName: "Auto" } : {}),
        },
      ];
    });
    return models.length > 0 ? models : FALLBACK_MODELS;
  } catch {
    return FALLBACK_MODELS;
  }
}

export const checkKiroProviderStatus = Effect.fn("checkKiroProviderStatus")(function* (
  kiroSettings: KiroSettings,
  environment: NodeJS.ProcessEnv = process.env,
): Effect.fn.Return<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const fallbackModels = providerModelsFromSettings(
    FALLBACK_MODELS,
    PROVIDER,
    kiroSettings.customModels,
    DEFAULT_KIRO_MODEL_CAPABILITIES,
  );

  if (!kiroSettings.enabled) {
    return buildServerProvider({
      presentation: KIRO_PRESENTATION,
      enabled: false,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Kiro is disabled in T3 Code settings.",
      },
    });
  }

  const command = ChildProcess.make(kiroSettings.binaryPath, ["--version"], {
    env: environment,
    shell: process.platform === "win32",
  });
  const versionProbe = yield* spawnAndCollect(kiroSettings.binaryPath, command).pipe(
    Effect.timeoutOption(DEFAULT_TIMEOUT_MS),
    Effect.result,
  );

  if (Result.isFailure(versionProbe)) {
    const error = versionProbe.failure;
    return buildServerProvider({
      presentation: KIRO_PRESENTATION,
      enabled: true,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: !isCommandMissingCause(error),
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: isCommandMissingCause(error)
          ? "Kiro CLI (`kiro-cli`) is not installed or not on PATH."
          : `Failed to execute Kiro CLI health check: ${error instanceof Error ? error.message : String(error)}.`,
      },
    });
  }

  if (Option.isNone(versionProbe.success)) {
    return buildServerProvider({
      presentation: KIRO_PRESENTATION,
      enabled: true,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: "Kiro CLI is installed but timed out while running.",
      },
    });
  }

  const result = versionProbe.success.value;
  const version = parseGenericCliVersion(`${result.stdout}\n${result.stderr}`);
  const agentListCommand = ChildProcess.make(kiroSettings.binaryPath, ["agent", "list"], {
    env: environment,
    shell: process.platform === "win32",
  });
  const agentListProbe = yield* spawnAndCollect(kiroSettings.binaryPath, agentListCommand).pipe(
    Effect.timeoutOption(DEFAULT_TIMEOUT_MS),
    Effect.result,
  );
  const agentOptions =
    Result.isSuccess(agentListProbe) &&
    Option.isSome(agentListProbe.success) &&
    agentListProbe.success.value.code === 0
      ? parseKiroAgents(agentListProbe.success.value.stdout)
      : [];
  const modelCapabilities = makeKiroModelCapabilities(agentOptions);
  const modelListCommand = ChildProcess.make(
    kiroSettings.binaryPath,
    ["chat", "--list-models", "--format", "json"],
    {
      env: environment,
      shell: process.platform === "win32",
    },
  );
  const modelListProbe = yield* spawnAndCollect(kiroSettings.binaryPath, modelListCommand).pipe(
    Effect.timeoutOption(DEFAULT_TIMEOUT_MS),
    Effect.result,
  );
  const cliModels =
    Result.isSuccess(modelListProbe) &&
    Option.isSome(modelListProbe.success) &&
    modelListProbe.success.value.code === 0
      ? parseKiroModels(modelListProbe.success.value.stdout, agentOptions)
      : [
          {
            slug: "auto",
            name: "Auto",
            isCustom: false,
            capabilities: modelCapabilities,
          },
        ];
  const models = providerModelsFromSettings(
    cliModels,
    PROVIDER,
    kiroSettings.customModels,
    modelCapabilities,
  );
  return buildServerProvider({
    presentation: KIRO_PRESENTATION,
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
        : { message: detailFromResult(result) ?? "Kiro CLI is installed but failed to run." }),
    },
  });
});
