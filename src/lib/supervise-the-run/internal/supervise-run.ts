import * as BunSocketServer from "@effect/platform-bun/BunSocketServer";
import {
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  FileSystem,
  Path,
  Predicate,
  Ref,
  Stream,
} from "effect";
import type { Queue } from "effect";
import { RpcServer } from "effect/rpc";
import { SocketServer } from "effect/socket";

import { TASK_TRACE_LABEL_PREFIX } from "../../../shared/names-from-package-json.ts";
import { SupervisorSocketNotOpened } from "../../../shared/process-errors.ts";
import {
  inheritedEnvironment,
  inheritedRunMarkers,
  RUN_MARKERS_VARIABLE_NAME,
  supervisedRunCommand,
} from "../../../shared/read-environment-config.ts";
import { awaitTerminalCommandExit, runInTerminal } from "../../../shared/run-in-terminal.ts";
import { saveStandardStreamFlags } from "../../../shared/save-standard-stream-flags.ts";
import { listenToSignals } from "../../../shared/stop-signals.ts";
import type { STOP_SIGNALS } from "../../../shared/stop-signals.ts";
import { createRunMarker, signalRunProcesses } from "../../kill-leftover-processes/index.ts";
import type { startedProcessGroup } from "../../kill-leftover-processes/index.ts";
import { adoptOrphans } from "./adopt-orphans-on-linux.ts";
import { provideSupervisorRpcSerialization, SupervisorRpcs } from "./talk-to-the-supervisor.ts";
import { WATCH_THE_SUPERVISOR_FLAG } from "./watch-the-supervisor.ts";

const FORCED_STOP_GRACE_SECONDS = 5;

const FORCED_STOP_GRACE = Duration.seconds(FORCED_STOP_GRACE_SECONDS);

const HOLD_STOP_UNTIL_READY_SECONDS = 5;

const HOLD_STOP_UNTIL_READY = Duration.seconds(HOLD_STOP_UNTIL_READY_SECONDS);

const SUPERVISOR_SOCKET_FILE_NAME = "supervisor.sock";

const serveTheRun = Effect.fn("serveTheRun")(
  function* (
    ready: Deferred.Deferred<void>,
    stopReceived: Deferred.Deferred<void>,
    startedProcessGroups: Map<number, ReturnType<typeof startedProcessGroup>>,
    runMarkerFileDescriptor: number | null,
  ) {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const socketPath = yield* fileSystem
      .makeTempDirectoryScoped({ prefix: `${TASK_TRACE_LABEL_PREFIX}-supervisor-` })
      .pipe(
        Effect.map((socketDirectoryPath) =>
          path.join(socketDirectoryPath, SUPERVISOR_SOCKET_FILE_NAME),
        ),
      );
    const socketServer = yield* BunSocketServer.make({ path: socketPath });
    const handlers = yield* SupervisorRpcs.toHandlers({
      ReportReady: () => Deferred.done(ready, Exit.void).pipe(Effect.asVoid),
      ShareProcessGroup: (shared) =>
        Effect.sync(() => {
          if (shared.startedGroup === null) {
            startedProcessGroups.delete(shared.processGroupId);
          } else {
            startedProcessGroups.set(shared.processGroupId, shared.startedGroup);
          }
        }),
      RunInTerminal: runInTerminal(Deferred.await(stopReceived), runMarkerFileDescriptor),
      AwaitTerminalCommandExit: awaitTerminalCommandExit,
    });
    yield* RpcServer.make(SupervisorRpcs, { disableTracing: true }).pipe(
      Effect.provideContext(handlers),
      Effect.provideServiceEffect(RpcServer.Protocol, RpcServer.makeProtocolSocketServer),
      Effect.provideService(SocketServer.SocketServer, socketServer),
      provideSupervisorRpcSerialization,
      Effect.interruptible,
      Effect.forkScoped,
    );
    return socketPath;
  },
  Effect.mapError((cause) => new SupervisorSocketNotOpened({ cause })),
);

export const superviseRun = Effect.fn("superviseRun")(function* (
  stopSignals: Queue.Dequeue<(typeof STOP_SIGNALS)[number]>,
  runProcessFilePath: string,
  runArgument: Parameters<typeof supervisedRunCommand>[3],
) {
  const reapAdoptedOrphans = yield* adoptOrphans;
  const exited = yield* Deferred.make<NodeJS.Signals | number>();
  const ready = yield* Deferred.make<void>();
  const stopReceived = yield* Deferred.make<void>();
  const startedProcessGroups = new Map<number, ReturnType<typeof startedProcessGroup>>();
  const runMarker = yield* createRunMarker(yield* inheritedRunMarkers);
  const command = yield* supervisedRunCommand(
    WATCH_THE_SUPERVISOR_FLAG,
    yield* serveTheRun(
      ready,
      stopReceived,
      startedProcessGroups,
      runMarker === null ? null : runMarker.fileDescriptor,
    ),
    runProcessFilePath,
    runArgument,
  );
  const environment = yield* inheritedEnvironment;
  const restoreStandardStreamFlags = yield* saveStandardStreamFlags;
  const child = yield* Effect.sync(() =>
    Bun.spawn(command, {
      detached: true,
      stdio: [
        "inherit",
        "inherit",
        "inherit",
        ...(runMarker === null ? [] : [runMarker.fileDescriptor]),
      ],
      env:
        runMarker === null
          ? environment
          : Object.fromEntries(
              Object.entries(environment).filter(([name]) => name !== RUN_MARKERS_VARIABLE_NAME),
            ),
      onExit: (exitedChild, exitCode) => {
        Deferred.doneUnsafe(exited, Exit.succeed(exitedChild.signalCode ?? exitCode ?? 1));
      },
    }),
  );
  yield* restoreStandardStreamFlags;
  yield* reapAdoptedOrphans(child.pid);
  const run = {
    sessionId: child.pid,
    descendantsOfProcessId: process.platform === "linux" ? process.pid : null,
    startedProcessGroups,
    runMarker,
  };
  const forced = yield* Deferred.make<void>();
  const stopCount = yield* Ref.make(0);
  yield* Stream.fromQueue(stopSignals).pipe(
    Stream.runForEach((signal) =>
      Ref.get(stopCount).pipe(
        Effect.flatMap((earlierStopCount) => {
          const isHangUpDuringStop = signal === "SIGHUP" && earlierStopCount > 0;
          if (isHangUpDuringStop) {
            return Effect.void;
          }
          const forward = Effect.sync(() => {
            child.kill(signal);
          });
          return Deferred.done(stopReceived, Exit.void).pipe(
            Effect.andThen(Ref.set(stopCount, earlierStopCount + 1)),
            Effect.andThen(
              earlierStopCount > 0
                ? forward.pipe(Effect.andThen(Deferred.done(forced, Exit.void)), Effect.asVoid)
                : Deferred.await(ready).pipe(
                    Effect.timeoutOption(HOLD_STOP_UNTIL_READY),
                    Effect.andThen(forward),
                    Effect.forkScoped,
                    Effect.asVoid,
                  ),
            ),
          );
        }),
      ),
    ),
    Effect.interruptible,
    Effect.forkScoped,
  );
  const jobControlSignals = yield* listenToSignals(["SIGTSTP", "SIGCONT"]);
  const jobControl = yield* Stream.fromQueue(jobControlSignals).pipe(
    Stream.runForEach((signal) =>
      signal === "SIGTSTP"
        ? signalRunProcesses(run, "SIGSTOP").pipe(
            Effect.andThen(Effect.sync(() => process.kill(process.pid, "SIGSTOP"))),
          )
        : signalRunProcesses(run, "SIGCONT"),
    ),
    Effect.interruptible,
    Effect.forkScoped,
  );
  yield* Deferred.await(exited).pipe(
    Effect.raceFirst(
      Deferred.await(forced).pipe(
        Effect.andThen(Deferred.await(exited).pipe(Effect.timeoutOption(FORCED_STOP_GRACE))),
      ),
    ),
    Effect.raceFirst(Fiber.join(jobControl)),
  );
  const runSkippedItsCleanup =
    (yield* Deferred.isDone(forced)) || Predicate.isString(yield* Deferred.await(exited));
  if (runSkippedItsCleanup) {
    yield* signalRunProcesses(run, "SIGKILL");
  }
  return yield* Deferred.await(exited);
});
