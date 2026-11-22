import { Effect, Deferred, Exit, Logger, Option, PlatformError, Tracer } from "effect";
import { ChildProcessSpawner } from "effect/process";
import type { ChildProcess } from "effect/process";

import type { ProcessIdentity } from "../../../../../shared/claim-processes-to-report.ts";
import { CommandCalledAt } from "../../../../../shared/mark-task-worker-thread-contexts.ts";
import { TASK_TRACE_LABEL_PREFIX } from "../../../../../shared/names-from-package-json.ts";
import { TaskEvent } from "../../../../../shared/rpc.ts";
import type { StartTaskFiber } from "../../../../../shared/rpc.ts";
import {
  announceTaskSpanUpdate,
  endTaskSpan,
  recordTaskSpanFailure,
  startTaskSpan,
} from "../../../../../shared/task-spans.ts";
import {
  forgetEndedProcessGroups,
  killTaskProcessGroups,
  readProcessUnlessGone,
  recordLeaderExit,
  recordStartedProcessGroup,
  startedProcessGroup,
  UNREGISTERED_GENERATION,
} from "../../../../kill-leftover-processes/index.ts";
import type { makeTaskBodyRecord } from "../../../shared/define-task-body-record.ts";
import { makeTraceOnlyLogger } from "../../../shared/make-trace-log-record.ts";
import { WorkerState, reportToMainThread } from "../../../shared/provide-worker-state.ts";
import { refuseStandardInputOutsideRootTask } from "./read-standard-input-from-main-thread.ts";
import { formatProgramNames, spawnWithBun } from "./spawn-commands-with-bun.ts";

const MICROSECONDS_PER_MILLISECOND = 1000;

export const spawnReportedProcess = Effect.fn(function* (
  startMessage: typeof StartTaskFiber.Type,
  body: ReturnType<typeof makeTaskBodyRecord>,
  command: ChildProcess.Command,
) {
  const workerState = yield* WorkerState;
  const programName = formatProgramNames(command);
  const calledAt = yield* CommandCalledAt;
  const processSpan = yield* startTaskSpan("process", programName, {
    parent: Option.getOrNull(yield* Effect.serviceOption(Tracer.ParentSpan)),
    attributes: { "process.executable.name": programName },
  });
  const spanEndedDeferred = yield* Deferred.make<void>();
  const spawnedDeferred = yield* Deferred.make<
    ChildProcessSpawner.ChildProcessHandle,
    PlatformError.PlatformError
  >();
  const leaderExitsSeenBeforeRecord = new Map<number, readonly (typeof ProcessIdentity.Type)[]>();
  const onLeaderExited = (
    processId: number,
    membersAtLeaderExit: readonly (typeof ProcessIdentity.Type)[],
  ) =>
    Effect.sync(() => {
      leaderExitsSeenBeforeRecord.set(processId, membersAtLeaderExit);
      recordLeaderExit(
        body.startedProcessGroups,
        processId,
        startMessage.taskName,
        membersAtLeaderExit,
      );
    });
  const killMembersLeftByLeader = (
    processId: number,
    membersAtLeaderExit: readonly (typeof ProcessIdentity.Type)[],
  ) => {
    const trackedGroup = body.startedProcessGroups.get(processId);
    return killTaskProcessGroups(
      new Map([
        [
          processId,
          startedProcessGroup(
            trackedGroup?.generation ?? UNREGISTERED_GENERATION,
            startMessage.taskName,
            trackedGroup?.leaderStartTime ?? null,
            membersAtLeaderExit,
          ),
        ],
      ]),
      startMessage.taskName,
      (processes) => workerState.claimProcessesToReport(startMessage.taskName, processes),
      workerState.runMarker,
    ).pipe(
      workerState.provideRuntimeServices,
      Effect.provideService(
        Logger.CurrentLoggers,
        new Set([makeTraceOnlyLogger(workerState.reportToMainThreadNow)]),
      ),
      Effect.flatMap(forgetEndedProcessGroups(body.startedProcessGroups)),
    );
  };
  yield* spawnWithBun(
    command,
    startMessage.taskName,
    refuseStandardInputOutsideRootTask(startMessage),
    startMessage.runDirectoryPath,
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
              pathOrDescriptor: programName,
              description: readFailure.message,
              cause: readFailure,
            }),
        }),
        Effect.tap((leaderStartTime) =>
          Effect.sync(() => {
            const processId = spawnedProcess.handle.pid;
            recordStartedProcessGroup(
              body.startedProcessGroups,
              processId,
              startMessage.taskName,
              leaderStartTime,
            );
            const membersAtLeaderExit = leaderExitsSeenBeforeRecord.get(processId);
            if (membersAtLeaderExit !== undefined) {
              recordLeaderExit(
                body.startedProcessGroups,
                processId,
                startMessage.taskName,
                membersAtLeaderExit,
              );
            }
          }),
        ),
        Effect.flatMap((leaderStartTime) =>
          workerState.reportStartedProcessAndAwaitRecord({
            taskName: startMessage.taskName,
            processId: spawnedProcess.handle.pid,
            startTime: leaderStartTime,
            programName,
            calledAt,
          }),
        ),
      ),
    ),
    Effect.tap((spawnedProcess) =>
      Effect.sync(() => {
        processSpan.attribute("process.pid", spawnedProcess.handle.pid);
      }).pipe(Effect.andThen(announceTaskSpanUpdate(processSpan))),
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
              membersAtLeaderExit: processExit.membersAtLeaderExit,
            }),
          ),
        ),
        Effect.tap((processExit) =>
          Effect.sync(() => {
            if (processExit.usage !== undefined) {
              processSpan.attribute(
                `${TASK_TRACE_LABEL_PREFIX}.process.cpu.milliseconds`,
                Number(BigInt(processExit.usage.cpuTime.total)) / MICROSECONDS_PER_MILLISECOND,
              );
              processSpan.attribute(
                `${TASK_TRACE_LABEL_PREFIX}.process.memory.peak.bytes`,
                processExit.usage.maxRSS,
              );
            }
          }),
        ),
        Effect.andThen(spawnedProcess.handle.exitCode),
      ),
    ),
    Effect.tap((exitCode) =>
      Effect.sync(() => {
        processSpan.attribute("process.exit.code", exitCode);
        if (exitCode !== 0) {
          recordTaskSpanFailure(processSpan, {
            errorType: String(exitCode),
            message: `${programName} exited with code ${exitCode}`,
          });
        }
      }),
    ),
    Effect.withParentSpan(processSpan),
    Effect.onExit((processExit) => endTaskSpan(processSpan, processExit)),
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
