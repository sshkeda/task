import * as BunSocketServer from "@effect/platform-bun/BunSocketServer";
import {
  Deferred,
  Duration,
  Effect,
  Exit,
  FileSystem,
  Path,
  Predicate,
  Queue,
  Ref,
  Stream,
} from "effect";
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
import type { JOB_CONTROL_SIGNALS, STOP_SIGNALS } from "../../../shared/stop-signals.ts";
import { createRunMarker, signalRunProcesses } from "../../kill-leftover-processes/index.ts";
import type { startedProcessGroup } from "../../kill-leftover-processes/index.ts";
import { adoptOrphans } from "./adopt-orphans-on-linux.ts";
import {
  hasBeenForced,
  hasReceivedStop,
  isEnded,
  moveSupervisorStage,
  SupervisorAction,
  SupervisorEvent,
  SupervisorStage,
} from "./define-supervisor-stage.ts";
import { provideSupervisorRpcSerialization, SupervisorRpcs } from "./talk-to-the-supervisor.ts";
import { WATCH_THE_SUPERVISOR_FLAG } from "./watch-the-supervisor.ts";

const FORCED_STOP_GRACE_SECONDS = 5;

const FORCED_STOP_GRACE = Duration.seconds(FORCED_STOP_GRACE_SECONDS);

const HOLD_STOP_UNTIL_READY_SECONDS = 5;

const HOLD_STOP_UNTIL_READY = Duration.seconds(HOLD_STOP_UNTIL_READY_SECONDS);

const SUPERVISOR_SOCKET_FILE_NAME = "supervisor.sock";

const serveTheRun = Effect.fn("serveTheRun")(
  function* (
    offerEvent: (event: typeof SupervisorEvent.Type) => Effect.Effect<boolean>,
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
      ReportReady: () => offerEvent(SupervisorEvent.cases.ReportReady.make({})).pipe(Effect.asVoid),
      AcknowledgeStop: ({ signal }) =>
        offerEvent(SupervisorEvent.cases.AcknowledgeStop.make({ signal })).pipe(Effect.asVoid),
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

const toEvent = (signal: (typeof JOB_CONTROL_SIGNALS)[number] | (typeof STOP_SIGNALS)[number]) => {
  if (signal === "SIGTSTP") {
    return SupervisorEvent.cases.Suspend.make({});
  }
  if (signal === "SIGCONT") {
    return SupervisorEvent.cases.Resume.make({});
  }
  return SupervisorEvent.cases.Stop.make({ signal });
};

export const superviseRun = Effect.fn("superviseRun")(function* (
  signals: Queue.Dequeue<(typeof JOB_CONTROL_SIGNALS)[number] | (typeof STOP_SIGNALS)[number]>,
  runProcessFilePath: string,
  runArgument: Parameters<typeof supervisedRunCommand>[3],
) {
  const reapAdoptedOrphans = yield* adoptOrphans;
  const stage = yield* Ref.make<typeof SupervisorStage.Type>(
    SupervisorStage.cases.Starting.make({}),
  );
  const events = yield* Queue.unbounded<typeof SupervisorEvent.Type>();
  const exited = yield* Deferred.make<NodeJS.Signals | number>();
  const stopReceived = yield* Deferred.make<void>();
  const forced = yield* Deferred.make<void>();
  const startedProcessGroups = new Map<number, ReturnType<typeof startedProcessGroup>>();
  const runMarker = yield* createRunMarker(yield* inheritedRunMarkers);
  const command = yield* supervisedRunCommand(
    WATCH_THE_SUPERVISOR_FLAG,
    yield* serveTheRun(
      (event) => Queue.offer(events, event),
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
        const ended = exitedChild.signalCode ?? exitCode ?? 1;
        Deferred.doneUnsafe(exited, Exit.succeed(ended));
        Queue.offerUnsafe(
          events,
          SupervisorEvent.cases.Exit.make({ bySignal: Predicate.isString(ended) }),
        );
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
  const move = (event: typeof SupervisorEvent.Type) =>
    Ref.modify(stage, (current) => {
      const moved = moveSupervisorStage(current, event);
      return [moved, moved[0]];
    }).pipe(
      Effect.tap(([next]) =>
        hasReceivedStop(next) ? Deferred.done(stopReceived, Exit.void) : Effect.void,
      ),
      Effect.tap(([next]) =>
        hasBeenForced(next) ? Deferred.done(forced, Exit.void) : Effect.void,
      ),
    );
  const sweep = signalRunProcesses(run, "SIGKILL").pipe(
    Effect.andThen(move(SupervisorEvent.cases.EndSweep.make({}))),
    Effect.asVoid,
  );
  const sweepAfterJobControlFailed = move(SupervisorEvent.cases.FailJobControl.make({})).pipe(
    Effect.flatMap(([, action]) => (SupervisorAction.guards.Sweep(action) ? sweep : Effect.void)),
  );
  const perform = SupervisorAction.match({
    Forward: ({ signal }) =>
      Effect.sync(() => {
        child.kill(signal);
      }),
    Ignore: () => Effect.void,
    None: () => Effect.void,
    Resume: () =>
      signalRunProcesses(run, "SIGCONT").pipe(Effect.tapError(() => sweepAfterJobControlFailed)),
    Suspend: () =>
      signalRunProcesses(run, "SIGSTOP").pipe(
        Effect.tapError(() => sweepAfterJobControlFailed),
        Effect.andThen(Effect.sync(() => process.kill(process.pid, "SIGSTOP"))),
      ),
    Sweep: () => sweep,
  });
  yield* Stream.fromQueue(signals).pipe(
    Stream.runForEach((signal) => Queue.offer(events, toEvent(signal))),
    Effect.interruptible,
    Effect.forkScoped,
  );
  yield* Deferred.await(stopReceived).pipe(
    Effect.andThen(Deferred.await(exited).pipe(Effect.timeoutOption(HOLD_STOP_UNTIL_READY))),
    Effect.andThen(Queue.offer(events, SupervisorEvent.cases.PassHold.make({}))),
    Effect.interruptible,
    Effect.forkScoped,
  );
  yield* Deferred.await(forced).pipe(
    Effect.andThen(Deferred.await(exited).pipe(Effect.timeoutOption(FORCED_STOP_GRACE))),
    Effect.andThen(Queue.offer(events, SupervisorEvent.cases.PassGrace.make({}))),
    Effect.interruptible,
    Effect.forkScoped,
  );
  yield* Queue.take(events).pipe(
    Effect.flatMap(move),
    Effect.flatMap(([, action]) => perform(action)),
    Effect.andThen(Ref.get(stage)),
    Effect.repeat({ until: isEnded }),
  );
  return yield* Deferred.await(exited);
});
