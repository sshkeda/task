import * as BunSocket from "@effect/platform-bun/BunSocket";
import { Deferred, Duration, Effect, Schema } from "effect";
import { Rpc, RpcClient, RpcGroup, RpcSerialization } from "effect/rpc";
import type { RpcClientError } from "effect/rpc";
import { Socket } from "effect/socket";

import { ProcessIdentity } from "../../../shared/claim-processes-to-report.ts";
import {
  TerminalCommand,
  TerminalCommandEnded,
  TerminalCommandId,
} from "../../../shared/run-in-terminal.ts";
import { STOP_SIGNALS } from "../../../shared/stop-signals.ts";
import { TrackedProcessGroup } from "../../kill-leftover-processes/index.ts";

export const ProcessGroupShared = Schema.Struct({
  processGroupId: Schema.Int,
  startedGroup: Schema.NullOr(TrackedProcessGroup),
});

export const SupervisorRpcs = RpcGroup.make(
  Rpc.make("ReportReady"),
  Rpc.make("AcknowledgeStop", { payload: { signal: Schema.Literals(STOP_SIGNALS) } }),
  Rpc.make("ShareProcessGroup", { payload: ProcessGroupShared }),
  Rpc.make("RunInTerminal", { payload: TerminalCommand, success: TerminalCommandEnded }),
  Rpc.make("AwaitTerminalCommandExit", { payload: TerminalCommandId }),
  Rpc.make("ClaimUnknownOwnership", {
    payload: { processes: Schema.Array(ProcessIdentity) },
    success: Schema.Array(ProcessIdentity),
  }),
  Rpc.make("SweepBeforeExit", { success: Schema.Array(Schema.String) }),
);

export const provideSupervisorRpcSerialization = Effect.provideService(
  RpcSerialization.RpcSerialization,
  RpcSerialization.ndjson,
);

export const connectToTheSupervisor = (
  supervisorSocketPath: string,
  onDisconnect: Effect.Effect<void>,
) =>
  RpcClient.make(SupervisorRpcs, { disableTracing: true }).pipe(
    Effect.provideServiceEffect(
      RpcClient.Protocol,
      RpcClient.makeProtocolSocket({ pingInterval: Duration.infinity }),
    ),
    Effect.provideService(RpcClient.ConnectionHooks, { onConnect: Effect.void, onDisconnect }),
    Effect.provideServiceEffect(Socket.Socket, BunSocket.makeNet({ path: supervisorSocketPath })),
    provideSupervisorRpcSerialization,
  );

export const supervisorConnectionDeferred =
  Deferred.makeUnsafe<Effect.Success<ReturnType<typeof connectToTheSupervisor>>>();

export const supervisorDisconnected = Deferred.makeUnsafe<void>();

export const callTheSupervisor = (
  call: (
    supervisor: Effect.Success<ReturnType<typeof connectToTheSupervisor>>,
  ) => Effect.Effect<void, RpcClientError.RpcClientError>,
) =>
  Deferred.await(supervisorConnectionDeferred).pipe(
    Effect.flatMap(call),
    Effect.catchTag("RpcClientError", () => Effect.void),
  );

export const runInTheSupervisorTerminal = (command: typeof TerminalCommand.Type) =>
  Deferred.await(supervisorConnectionDeferred).pipe(
    Effect.flatMap((supervisor) => supervisor.RunInTerminal(command)),
    Effect.catchTag("RpcClientError", (error) => Effect.die(error)),
  );

export const awaitTheSupervisorTerminalCommandExit = (command: typeof TerminalCommandId.Type) =>
  callTheSupervisor((supervisor) => supervisor.AwaitTerminalCommandExit(command));

export const sweepBeforeExit = Effect.suspend(() => {
  const isConnected =
    Deferred.isDoneUnsafe(supervisorConnectionDeferred) &&
    !Deferred.isDoneUnsafe(supervisorDisconnected);
  return isConnected
    ? Deferred.await(supervisorConnectionDeferred).pipe(
        Effect.flatMap((supervisor) => supervisor.SweepBeforeExit()),
        Effect.catchTag("RpcClientError", () => Effect.succeed([])),
        Effect.raceFirst(Deferred.await(supervisorDisconnected).pipe(Effect.as([]))),
      )
    : Effect.succeed([]);
});
