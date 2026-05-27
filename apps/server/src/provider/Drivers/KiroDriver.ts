import { KiroSettings, ProviderDriverKind, type ServerProvider } from "@t3tools/contracts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import { ProviderDriverError } from "../Errors.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import type { ServerProviderDraft } from "../providerSnapshot.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import { makeManualOnlyProviderMaintenanceCapabilities } from "../providerMaintenance.ts";
import { checkKiroProviderStatus, extractKiroAssistantText } from "../Layers/KiroProvider.ts";
import { makeCliChatAdapter, makeUnsupportedTextGeneration } from "../Layers/CliChatAdapter.ts";

const decodeKiroSettings = Schema.decodeSync(KiroSettings);
const DRIVER_KIND = ProviderDriverKind.make("kiro");
const SNAPSHOT_REFRESH_INTERVAL = Duration.minutes(5);

export type KiroDriverEnv = ChildProcessSpawner.ChildProcessSpawner;

const withInstanceIdentity =
  (input: {
    readonly instanceId: ProviderInstance["instanceId"];
    readonly displayName: string | undefined;
    readonly accentColor: string | undefined;
    readonly continuationGroupKey: string;
  }) =>
  (snapshot: ServerProviderDraft): ServerProvider => ({
    ...snapshot,
    instanceId: input.instanceId,
    driver: DRIVER_KIND,
    ...(input.displayName ? { displayName: input.displayName } : {}),
    ...(input.accentColor ? { accentColor: input.accentColor } : {}),
    continuation: { groupKey: input.continuationGroupKey },
  });

function kiroRuntimeArgs(input: {
  readonly prompt: string;
  readonly model: string | undefined;
  readonly runtimeMode: string;
  readonly agent: string | undefined;
  readonly resumeCursor: unknown;
  readonly agentEngine: string | undefined;
  readonly mode: string | undefined;
}): ReadonlyArray<string> {
  const args = ["chat", "--no-interactive", "--wrap", "never"];
  if (input.model && input.model !== "auto") {
    args.push("--model", input.model);
  }
  if (input.agent && input.agent !== "default") {
    args.push("--agent", input.agent);
  }
  if (input.agentEngine) {
    args.push("--agent-engine", input.agentEngine);
  }
  if (input.mode) {
    args.push("--mode", input.mode);
  }
  if (input.runtimeMode === "full-access") {
    args.push("--trust-all-tools");
  } else if (input.runtimeMode === "approval-required") {
    args.push("--trust-tools=");
  }
  if (
    input.resumeCursor &&
    typeof input.resumeCursor === "object" &&
    typeof (input.resumeCursor as { readonly sessionId?: unknown }).sessionId === "string"
  ) {
    args.push("--resume-id", (input.resumeCursor as { readonly sessionId: string }).sessionId);
  } else if (input.resumeCursor) {
    args.push("--resume");
  }
  args.push(input.prompt);
  return args;
}

export const KiroDriver: ProviderDriver<KiroSettings, KiroDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "Kiro",
    supportsMultipleInstances: true,
  },
  configSchema: KiroSettings,
  defaultConfig: (): KiroSettings => decodeKiroSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const processEnv = mergeProviderInstanceEnvironment(environment);
      const effectiveConfig = { ...config, enabled } satisfies KiroSettings;
      const agent = effectiveConfig.agent.trim() || undefined;
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER_KIND,
        instanceId,
      });
      const stampIdentity = withInstanceIdentity({
        instanceId,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });

      const adapter = yield* makeCliChatAdapter({
        provider: DRIVER_KIND,
        instanceId,
        binaryPath: effectiveConfig.binaryPath,
        environment: processEnv,
        buildCommand: (input) => ({
          command: effectiveConfig.binaryPath,
          args: kiroRuntimeArgs({
            prompt: input.input,
            model: input.model,
            runtimeMode: input.runtimeMode,
            agent: getModelSelectionStringOptionValue(input.modelSelection, "agent") ?? agent,
            resumeCursor: input.resumeCursor,
            agentEngine: getModelSelectionStringOptionValue(input.modelSelection, "agentEngine"),
            mode: getModelSelectionStringOptionValue(input.modelSelection, "mode"),
          }),
          env: processEnv,
        }),
        extractAssistantText: extractKiroAssistantText,
      });

      const checkProvider = checkKiroProviderStatus(effectiveConfig, processEnv).pipe(
        Effect.map(stampIdentity),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      );
      const snapshot = yield* makeManagedServerProvider<KiroSettings>({
        maintenanceCapabilities: makeManualOnlyProviderMaintenanceCapabilities({
          provider: DRIVER_KIND,
          packageName: null,
        }),
        getSettings: Effect.succeed(effectiveConfig),
        streamSettings: Stream.never,
        haveSettingsChanged: () => false,
        initialSnapshot: (settings) =>
          checkKiroProviderStatus(settings, processEnv).pipe(
            Effect.map(stampIdentity),
            Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          ),
        checkProvider,
        refreshInterval: SNAPSHOT_REFRESH_INTERVAL,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: `Failed to build Kiro snapshot: ${cause.message ?? String(cause)}`,
              cause,
            }),
        ),
      );

      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        adapter,
        textGeneration: makeUnsupportedTextGeneration(DRIVER_KIND),
      } satisfies ProviderInstance;
    }),
};
