import { Effect, Deferred, Exit, PlatformError } from "effect";
import { ChildProcessSpawner } from "effect/process";
import type { ChildProcess } from "effect/process";

import { TASK_TRACE_LABEL_PREFIX } from "../../../../../shared/names-from-package-json.ts";
import { TaskEvent } from "../../../../../shared/rpc.ts";
import type { StartTaskFiber } from "../../../../../shared/rpc.ts";
import {
  forgetEndedProcessGroups,
  killTaskProcessGroups,
  readProcessUnlessGone,
  recordLeaderExit,
  startedProcessGroup,
} from "../../../../kill-leftover-processes/index.ts";
import type { makeTaskBodyRecord } from "../../../shared/define-task-body-record.ts";
import { WorkerState, reportToMainThread } from "../../../shared/provide-worker-state.ts";
import { refuseStandardInputOutsideRootTask } from "./read-standard-input-from-main-thread.ts";
import { formatProgramNames, spawnWithBun } from "./spawn-commands-with-bun.ts";

const MICROSECONDS_PER_MILLISECOND = 1000;

export const spawnReportedProcess = Effect.fn("spawnReportedProcess")(function* (
  startMessage: typeof StartTaskFiber.Type,
  body: ReturnType<typeof makeTaskBodyRecord>,
  command: ChildProcess.Command,
) {
  const workerState = yield* WorkerState;
  const spanEndedDeferred = yield* Deferred.make<void>();
  const spawnedDeferred = yield* Deferred.make<
    ChildProcessSpawner.ChildProcessHandle,
    PlatformError.PlatformError
  >();
  const leaderExitsSeenBeforeRecord = new Map<number, string | null>();
  const onLeaderExited = (processId: number, latestMemberStartTime: string | null) =>
    Effect.sync(() => {
      leaderExitsSeenBeforeRecord.set(processId, latestMemberStartTime);
      recordLeaderExit(body.startedProcessGroups, processId, latestMemberStartTime);
    });
  const killMembersLeftByLeader = (processId: number, latestMemberStartTime: string) =>
    killTaskProcessGroups(
      new Map([
        [
          processId,
          startedProcessGroup(
            startMessage.taskName,
            body.startedProcessGroups.get(processId)?.leaderStartTime ?? null,
            latestMemberStartTime,
          ),
        ],
      ]),
      startMessage.taskName,
      (processes) => workerState.claimProcessesToReport(startMessage.taskName, processes),
    ).pipe(
      workerState.provideRuntimeServices,
      Effect.flatMap(forgetEndedProcessGroups(body.startedProcessGroups)),
    );
  yield* spawnWithBun(
    command,
    startMessage.taskName,
    refuseStandardInputOutsideRootTask(startMessage),
    startMessage.taskPackageDirectoryPath,
    startMessage.cleanup,
    {
      onLeaderExited,
      killMembersLeftByLeader,
    },
  ).pipe(
    Effect.tap((spawnedProcess) =>
      readProcessUnlessGone(startMessage.taskName, spawnedProcess.handle.pid).pipe(
        workerState.provideRuntimeServices,
        Effect.mapBoth({
          onSuccess: (leader) => leader?.startTime ?? null,
          onFailure: (readFailure) =>
            PlatformError.systemError({
              _tag: "Unknown",
              module: "ChildProcess",
              method: "spawn",
              pathOrDescriptor: formatProgramNames(command),
              description: readFailure.message,
              cause: readFailure,
            }),
        }),
        Effect.tap((leaderStartTime) =>
          Effect.sync(() => {
            const processId = spawnedProcess.handle.pid;
            body.startedProcessGroups.set(
              processId,
              startedProcessGroup(startMessage.taskName, leaderStartTime, null),
            );
            if (leaderExitsSeenBeforeRecord.has(processId)) {
              recordLeaderExit(
                body.startedProcessGroups,
                processId,
                leaderExitsSeenBeforeRecord.get(processId) ?? null,
              );
            }
          }),
        ),
        Effect.flatMap((leaderStartTime) =>
          workerState.reportStartedProcessAndAwaitRecord(
            startMessage.taskName,
            spawnedProcess.handle.pid,
            leaderStartTime,
          ),
        ),
      ),
    ),
    Effect.tap((spawnedProcess) =>
      Effect.annotateCurrentSpan("process.pid", spawnedProcess.handle.pid),
    ),
    Effect.onExit((spawnExit) =>
      Deferred.done(
        spawnedDeferred,
        Exit.map(spawnExit, (spawnedProcess) => spawnedProcess.handle),
      ),
    ),
    Effect.flatMap((spawnedProcess) =>
      spawnedProcess.exited.pipe(
        Effect.tap((processExit) =>
          reportToMainThread(
            TaskEvent.cases.TaskFiberProcessLeaderExited.make({
              taskName: startMessage.taskName,
              processId: spawnedProcess.handle.pid,
              latestMemberStartTime: processExit.latestMemberStartTime,
            }),
          ),
        ),
        Effect.tap((processExit) =>
          processExit.usage === undefined
            ? Effect.void
            : Effect.annotateCurrentSpan({
                [`${TASK_TRACE_LABEL_PREFIX}.process.cpu.milliseconds`]:
                  Number(BigInt(processExit.usage.cpuTime.total)) / MICROSECONDS_PER_MILLISECOND,
                [`${TASK_TRACE_LABEL_PREFIX}.process.memory.peak.bytes`]: processExit.usage.maxRSS,
              }),
        ),
        Effect.andThen(spawnedProcess.handle.exitCode),
      ),
    ),
    Effect.tap((exitCode) => Effect.annotateCurrentSpan("process.exit.code", exitCode)),
    Effect.withSpan("process", {
      attributes: { "process.executable.name": formatProgramNames(command) },
    }),
    Effect.exit,
    Effect.andThen(Deferred.done(spanEndedDeferred, Exit.void)),
    workerState.forkInWorkerScope,
  );
  const handle = yield* Deferred.await(spawnedDeferred);
  return ChildProcessSpawner.makeHandle({
    ...handle,
    exitCode: Deferred.await(spanEndedDeferred).pipe(Effect.andThen(handle.exitCode)),
  });
});
