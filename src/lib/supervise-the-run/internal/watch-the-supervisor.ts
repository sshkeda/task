import { Deferred, Effect, Exit, Option, Ref } from "effect";

import { printFailureNow } from "../../../shared/print-to-standard-error.ts";
import { takeSupervisorSocketPath } from "../../../shared/read-environment-config.ts";
import type { RunMarker } from "../../../shared/run-marker.ts";
import {
  adoptSupervisorRunMarker,
  claimUnknownOwnership,
  signalRunProcesses,
} from "../../kill-leftover-processes/index.ts";
import { claimUnknownOwnershipInTheRun } from "./claim-unknown-ownership-in-the-run.ts";
import { processGroupsSharedWithTheSupervisor } from "./share-process-groups.ts";
import {
  connectToTheSupervisor,
  supervisorConnectionDeferred,
  supervisorDisconnected,
} from "./talk-to-the-supervisor.ts";

export const WATCH_THE_SUPERVISOR_FLAG = `--preload=${Bun.fileURLToPath(import.meta.resolve("./start-watching-the-supervisor.ts"))}`;

export const supervisorProcessId = Ref.makeUnsafe<number | null>(null);

export const supervisorRunMarker = Ref.makeUnsafe<typeof RunMarker.Type | null>(null);

export const watchSupervisor = Effect.gen(function* () {
  yield* Effect.sync(() =>
    process.execArgv.splice(process.execArgv.indexOf(WATCH_THE_SUPERVISOR_FLAG), 1),
  );
  const supervisorSocketPath = yield* takeSupervisorSocketPath;
  if (Option.isNone(supervisorSocketPath)) {
    return;
  }
  yield* Ref.set(supervisorProcessId, process.ppid);
  const runMarker = yield* adoptSupervisorRunMarker.pipe(
    Effect.catchTag(["ProcRunMarkerUnreadable", "SystemLibraryNotLoaded"], (error) =>
      printFailureNow(error).pipe(Effect.as(null)),
    ),
  );
  yield* Ref.set(supervisorRunMarker, runMarker);
  const supervisor = yield* connectToTheSupervisor(
    supervisorSocketPath.value,
    Deferred.done(supervisorDisconnected, Exit.void).pipe(Effect.asVoid),
  );
  yield* Deferred.succeed(supervisorConnectionDeferred, supervisor);
  yield* Deferred.await(supervisorDisconnected).pipe(
    Effect.andThen(
      signalRunProcesses(
        {
          sessionId: process.pid,
          descendantsOfProcessId: process.pid,
          startedProcessGroups: processGroupsSharedWithTheSupervisor,
          runMarker,
          sparedProcessId: null,
        },
        "SIGKILL",
      ),
    ),
    Effect.matchEffect({
      onFailure: (error) => printFailureNow(error),
      onSuccess: (unknownOwnerships) =>
        claimUnknownOwnership(unknownOwnerships, claimUnknownOwnershipInTheRun).pipe(
          Effect.flatMap((claimed) => Effect.forEach(claimed, printFailureNow, { discard: true })),
        ),
    }),
    Effect.ensuring(Effect.sync(() => process.kill(process.pid, "SIGKILL"))),
  );
}).pipe(Effect.scoped);
