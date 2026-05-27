import { BobSettings, ProviderDriverKind, type ServerProvider } from "@t3tools/contracts";
import {
  getModelSelectionBooleanOptionValue,
  getModelSelectionStringOptionValue,
} from "@t3tools/shared/model";
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
import {
  checkBobProviderStatus,
  extractBobAssistantText,
  extractBobResumeCursor,
  makeBobEnvironment,
} from "../Layers/BobProvider.ts";
import { makeCliChatAdapter, makeUnsupportedTextGeneration } from "../Layers/CliChatAdapter.ts";

const decodeBobSettings = Schema.decodeSync(BobSettings);
const DRIVER_KIND = ProviderDriverKind.make("bob");
const SNAPSHOT_REFRESH_INTERVAL = Duration.minutes(5);

export type BobDriverEnv = ChildProcessSpawner.ChildProcessSpawner;

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

function bobRuntimeArgs(input: {
  readonly prompt: string;
  readonly model: string | undefined;
  readonly runtimeMode: string;
  readonly resumeCursor: unknown;
  readonly chatMode: string | undefined;
  readonly trustWorkspace: boolean | undefined;
  readonly preCheckAutoApproved: boolean | undefined;
}): ReadonlyArray<string> {
  const args = ["--output-format", "stream-json", "--hide-intermediary-output"];
  if (input.model && input.model !== "premium") {
    args.push("--model", input.model);
  }
  if (input.chatMode) {
    args.push("--chat-mode", input.chatMode);
  }
  if (input.trustWorkspace === true) {
    args.push("--trust");
  }
  if (input.preCheckAutoApproved === true) {
    args.push("--pre-check-auto-approved");
  }
  if (input.runtimeMode === "full-access") {
    args.push("--yolo");
  } else if (input.runtimeMode === "auto-accept-edits") {
    args.push("--approval-mode", "auto_edit");
  } else {
    args.push("--approval-mode", "default");
  }
  if (
    input.resumeCursor &&
    typeof input.resumeCursor === "object" &&
    typeof (input.resumeCursor as { readonly sessionId?: unknown }).sessionId === "string"
  ) {
    args.push("--resume", (input.resumeCursor as { readonly sessionId: string }).sessionId);
  }
  args.push("-p", input.prompt);
  return args;
}

export const BobDriver: ProviderDriver<BobSettings, BobDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "Bob",
    supportsMultipleInstances: true,
  },
  configSchema: BobSettings,
  defaultConfig: (): BobSettings => decodeBobSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const processEnv = makeBobEnvironment(mergeProviderInstanceEnvironment(environment));
      const effectiveConfig = { ...config, enabled } satisfies BobSettings;
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
          args: bobRuntimeArgs({
            prompt: input.input,
            model: input.model,
            runtimeMode: input.runtimeMode,
            resumeCursor: input.resumeCursor,
            chatMode: getModelSelectionStringOptionValue(input.modelSelection, "chatMode"),
            trustWorkspace: getModelSelectionBooleanOptionValue(
              input.modelSelection,
              "trustWorkspace",
            ),
            preCheckAutoApproved: getModelSelectionBooleanOptionValue(
              input.modelSelection,
              "preCheckAutoApproved",
            ),
          }),
          env: processEnv,
        }),
        extractAssistantText: extractBobAssistantText,
        extractResumeCursor: extractBobResumeCursor,
      });

      const checkProvider = checkBobProviderStatus(effectiveConfig, processEnv).pipe(
        Effect.map(stampIdentity),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      );
      const snapshot = yield* makeManagedServerProvider<BobSettings>({
        maintenanceCapabilities: makeManualOnlyProviderMaintenanceCapabilities({
          provider: DRIVER_KIND,
          packageName: null,
        }),
        getSettings: Effect.succeed(effectiveConfig),
        streamSettings: Stream.never,
        haveSettingsChanged: () => false,
        initialSnapshot: (settings) =>
          checkBobProviderStatus(settings, processEnv).pipe(
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
              detail: `Failed to build Bob snapshot: ${cause.message ?? String(cause)}`,
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
