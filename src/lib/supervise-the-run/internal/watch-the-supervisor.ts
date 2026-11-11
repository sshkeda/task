import { Deferred, Effect, Exit, Option, Ref, Stdio, Stream } from "effect";

import { formatFailureLines } from "../../../shared/format-error-lines.ts";
import { takeSupervisorSocketPath } from "../../../shared/read-environment-config.ts";
import type { RunMarker } from "../../../shared/run-marker.ts";
import {
  adoptSupervisorRunMarker,
  signalRunProcesses,
} from "../../kill-leftover-processes/index.ts";
import { processGroupsSharedWithTheSupervisor } from "./share-process-groups.ts";
import { connectToTheSupervisor, supervisorConnectionDeferred } from "./talk-to-the-supervisor.ts";

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
  const stdio = yield* Stdio.Stdio;
  const printFailure = (error: Parameters<typeof formatFailureLines>[0]) =>
    Stream.make(`${formatFailureLines(error, null)}\n`).pipe(
      Stream.run(stdio.stderr()),
      Effect.ignore,
    );
  const runMarker = yield* adoptSupervisorRunMarker.pipe(
    Effect.catchTag(["ProcRunMarkerUnreadable", "SystemLibraryNotLoaded"], (error) =>
      printFailure(error).pipe(Effect.as(null)),
    ),
  );
  yield* Ref.set(supervisorRunMarker, runMarker);
  const disconnected = yield* Deferred.make<void>();
  const supervisor = yield* connectToTheSupervisor(
    supervisorSocketPath.value,
    Deferred.done(disconnected, Exit.void).pipe(Effect.asVoid),
  );
  yield* Deferred.succeed(supervisorConnectionDeferred, supervisor);
  yield* Deferred.await(disconnected).pipe(
    Effect.andThen(
      signalRunProcesses(
        {
          sessionId: process.pid,
          descendantsOfProcessId: process.pid,
          startedProcessGroups: processGroupsSharedWithTheSupervisor,
          runMarker,
        },
        "SIGKILL",
      ),
    ),
    Effect.matchEffect({
      onFailure: (error) => printFailure(error),
      onSuccess: (success) => Effect.succeed(success),
    }),
    Effect.ensuring(Effect.sync(() => process.kill(process.pid, "SIGKILL"))),
  );
}).pipe(Effect.scoped);
