import {
  Cause,
  Clock,
  Deferred,
  Effect,
  Exit,
  Fiber,
  FiberSet,
  Match,
  Option,
  Schema,
} from "effect";

import {
  dieOnInvariantFailure,
  InternalInvariantBroken,
} from "../../../../../shared/invariant-errors.ts";
import { TASK_TRACE_LABEL_PREFIX } from "../../../../../shared/names-from-package-json.ts";
import { printLine, printTaskLines } from "../../../../../shared/print-to-standard-error.ts";
import { WorkerExited } from "../../../../../shared/process-errors.ts";
import { TaskFiberResult } from "../../../../../shared/rpc.ts";
import type { TaskMadeInBody } from "../../../../../shared/rpc.ts";
import {
  TaskCleanupFailed,
  ServedTaskFailed,
  TaskBodyFailed,
} from "../../../../../shared/task-errors.ts";
import type { AwaitedTasksFailure, TaskRequestFailure } from "../../../../../shared/task-errors.ts";
import { WorkerEnding } from "../../../shared/define-worker-stage.ts";
import { failRun, failTask } from "../../../shared/fail-run.ts";
import { WorkerTerminatedForAnotherTask } from "../../../shared/main-thread-errors.ts";
import { ProcessGroups } from "../../../shared/provide-process-groups.ts";
import { StopState } from "../../../shared/provide-stop-state.ts";
import { FromWorker, WorkerEvent, WorkerPool } from "../../../shared/provide-worker-pool.ts";
import { reportFailure } from "../../../shared/report-task-failure.ts";
import { TraceFile } from "../../../shared/write-trace-file.ts";
import { findAssignedWorkerId, isUnfinishedOnWorker } from "../shared/define-task-run-state.ts";
import type { makeTaskRunsState } from "../shared/define-task-run-state.ts";
import { isEndedByTheRun, TaskRunEvent, TaskRunOutcome } from "../shared/define-task-run-status.ts";
import { TaskStopEvent } from "../shared/define-task-stop-stage.ts";

const sendDependencyResultToWorker = Effect.fn("sendDependencyResultToWorker")(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  requestDependencyTaskResult: (
    dependencyTaskName: string,
    requestingTaskName: string,
    dependencyTaskFilePath: string | null,
    dependencyTaskMadeInBody: typeof TaskMadeInBody.Type | null,
  ) => Effect.Effect<Schema.Json | undefined, typeof TaskRequestFailure.Type>,
  messageFromWorker: typeof FromWorker.cases.TaskFiberRequestedTask.Type,
) {
  yield* Effect.annotateCurrentSpan({
    [`${TASK_TRACE_LABEL_PREFIX}.task`]: messageFromWorker.taskName,
    [`${TASK_TRACE_LABEL_PREFIX}.wanted_task`]: messageFromWorker.dependencyTaskName,
  });
  const requestingTaskRun = yield* Effect.fromNullishOr(
    taskRunsState.runsByTaskName.get(messageFromWorker.taskName),
  ).pipe(dieOnInvariantFailure("task requests", "a task that requests another task has a run"));
  const answer = yield* requestDependencyTaskResult(
    messageFromWorker.dependencyTaskName,
    messageFromWorker.taskName,
    messageFromWorker.dependencyTaskFilePath,
    messageFromWorker.dependencyTaskMadeInBody,
  ).pipe(
    Effect.exit,
    Effect.tap((dependencyExit) => {
      const dependencyWasStopped =
        Exit.isFailure(dependencyExit) && Cause.hasInterruptsOnly(dependencyExit.cause);
      return dependencyWasStopped ? requestingTaskRun.stop.awaitCleanupStarted : Effect.void;
    }),
    Effect.raceFirst(
      Deferred.await(requestingTaskRun.bodyFinishedDeferred).pipe(Effect.andThen(Effect.interrupt)),
    ),
  );
  requestingTaskRun.dependencyRequestIdsAwaitingAnswer.delete(
    messageFromWorker.dependencyRequestId,
  );
  const workerId = findAssignedWorkerId(requestingTaskRun.status);
  if (workerId === null) {
    return;
  }

  yield* (yield* WorkerPool).answerDependencyRequest(messageFromWorker.dependencyRequestId, answer);
});

const isTaskBodyFailed = Schema.is(TaskBodyFailed);

const isWorkerExited = Schema.is(WorkerExited);

const isTaskCleanupFailed = Schema.is(TaskCleanupFailed);

const isWorkerTerminatedForAnotherTask = Schema.is(WorkerTerminatedForAnotherTask);

const bodyEnding = (taskName: string, result: typeof TaskFiberResult.Type) =>
  TaskFiberResult.match(result, {
    Returned: (
      returned,
    ): Exit.Exit<
      typeof returned,
      | TaskBodyFailed
      | TaskCleanupFailed
      | WorkerExited
      | WorkerTerminatedForAnotherTask
      | typeof AwaitedTasksFailure.Type
    > => Exit.succeed(returned),
    Failed: (failed) =>
      Exit.fail(
        new TaskBodyFailed({
          taskName,
          failures: failed.failures,
          details: failed.details,
          failedCommandExitCode: failed.failedCommandExitCode,
        }),
      ),
    AwaitedTasksFailed: (awaitedTasksFailed) => Exit.fail(awaitedTasksFailed.failure),
    CleanupFailed: (cleanupFailure) =>
      Exit.fail(
        new TaskCleanupFailed({
          taskName,
          failures: cleanupFailure.failures,
          details: cleanupFailure.details,
        }),
      ),
    Interrupted: () => Exit.interrupt(),
  });

const settleBodyOutcome = Effect.fn("settleBodyOutcome")(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  taskName: string,
  ending: Exit.Exit<
    Effect.Success<ReturnType<typeof bodyEnding>>,
    Effect.Error<ReturnType<typeof bodyEnding>>
  >,
) {
  const taskRun = taskRunsState.runsByTaskName.get(taskName);
  if (taskRun === undefined) {
    return;
  }
  const failure = Exit.findErrorOption(ending);
  const failedAfterTaskEnded = TaskRunOutcome.guards.Cancelled(taskRun.outcome)
    ? Option.filter(
        failure,
        (bodyFailure) =>
          isTaskBodyFailed(bodyFailure) || isWorkerTerminatedForAnotherTask(bodyFailure),
      )
    : Option.filter(failure, (bodyFailure) =>
        isEndedByTheRun(taskRun.outcome) ? isTaskBodyFailed(bodyFailure) : false,
      );
  Deferred.doneUnsafe(taskRun.bodyOutcomeDeferred, ending);
  if (Option.isSome(failedAfterTaskEnded)) {
    yield* failTask(failedAfterTaskEnded.value, taskRunsState.markFailed);
  }
});

const reportFailureAfterOutcome = Effect.fn("reportFailureAfterOutcome")(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  taskName: string,
  ending: Exit.Exit<
    Effect.Success<ReturnType<typeof bodyEnding>>,
    Effect.Error<ReturnType<typeof bodyEnding>>
  >,
  served: boolean,
) {
  const failure = Exit.findErrorOption(ending);
  if (Option.isNone(failure)) {
    return;
  }
  const failureEndsRun =
    isTaskCleanupFailed(failure.value) ||
    isWorkerExited(failure.value) ||
    isWorkerTerminatedForAnotherTask(failure.value);
  if (failureEndsRun) {
    yield* (yield* StopState).recordFirstFailure(failure.value);
    return yield* failTask(failure.value, taskRunsState.markFailed);
  }
  if (!served) {
    return;
  }

  yield* failRun(
    new ServedTaskFailed({
      taskName,
      failures: isTaskBodyFailed(failure.value)
        ? failure.value.failures
        : reportFailure(failure.value),
      details: isTaskBodyFailed(failure.value) ? failure.value.details : undefined,
    }),
    taskRunsState.markFailed,
  );
});

const endTask = Effect.fn("endTask")(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  taskName: string,
  ending: Exit.Exit<
    Effect.Success<ReturnType<typeof bodyEnding>>,
    Effect.Error<ReturnType<typeof bodyEnding>>
  >,
) {
  const action = yield* taskRunsState.moveTaskRun(taskName, TaskRunEvent.cases.End.make({}));
  if (action === "ignored") {
    return;
  }
  taskRunsState.refreshBodyClocks(taskName, yield* Clock.currentTimeMillis);
  if (action === "close") {
    yield* printTaskLines("Info", taskName, "closed");
  }
  yield* action === "settleOutcome"
    ? settleBodyOutcome(taskRunsState, taskName, ending)
    : reportFailureAfterOutcome(taskRunsState, taskName, ending, action === "close");
  const taskRun = taskRunsState.runsByTaskName.get(taskName);
  const workerId = taskRun === undefined ? null : findAssignedWorkerId(taskRun.status);
  if (workerId !== null) {
    yield* WorkerPool.use((workerPool) => workerPool.recordTaskEnded(workerId));
  }
});

const failTasksOnWorker = Effect.fn("failTasksOnWorker")(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  taskNames: readonly string[],
  makeFailure: (taskName: string) => WorkerExited | WorkerTerminatedForAnotherTask,
) {
  const processGroups = yield* ProcessGroups;
  yield* Effect.forEach(taskNames, processGroups.killForTask, { discard: true });
  yield* Effect.forEach(
    taskNames,
    (taskName) => endTask(taskRunsState, taskName, Exit.fail(makeFailure(taskName))),
    { discard: true },
  );
});

const endTasksOfExitedWorker = Effect.fn("endTasksOfExitedWorker")(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  workerEvent: typeof WorkerEvent.cases.ExitedWithCode.Type,
) {
  const unfinishedTaskNames = [...taskRunsState.runsByTaskName]
    .filter(([, taskRun]) => isUnfinishedOnWorker(taskRun, workerEvent.workerId))
    .map(([taskName]) => taskName);
  const workerExit = {
    workerId: workerEvent.workerId,
    exitCode: workerEvent.exitCode,
    uncaughtError: workerEvent.uncaughtError,
  };
  const stopTasks = (taskNames: readonly string[]) =>
    Effect.forEach(taskNames, (taskName) => endTask(taskRunsState, taskName, Exit.interrupt()), {
      discard: true,
    });
  yield* WorkerEnding.match(workerEvent.ending, {
    Crashed: () =>
      unfinishedTaskNames.length === 0
        ? failRun(new WorkerExited({ taskName: null, ...workerExit }), taskRunsState.markFailed)
        : failTasksOnWorker(
            taskRunsState,
            unfinishedTaskNames,
            (taskName) => new WorkerExited({ taskName, ...workerExit }),
          ),
    ExitedOnRequest: () => stopTasks(unfinishedTaskNames),
    Terminated: ({ terminatingTaskName }) =>
      terminatingTaskName === null
        ? stopTasks(unfinishedTaskNames)
        : failTasksOnWorker(
            taskRunsState,
            unfinishedTaskNames.filter((taskName) => taskName !== terminatingTaskName),
            (taskName) => new WorkerTerminatedForAnotherTask({ taskName, terminatingTaskName }),
          ).pipe(
            Effect.andThen(
              stopTasks(unfinishedTaskNames.filter((taskName) => taskName === terminatingTaskName)),
            ),
          ),
  });
});

const onMessageAboutTask = Effect.fn("onMessageAboutTask")(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  requestDependencyTaskResult: Parameters<typeof sendDependencyResultToWorker>[1],
  messageFromWorker: Exclude<
    typeof FromWorker.Type,
    | typeof FromWorker.cases.StandardErrorLineRequested.Type
    | typeof FromWorker.cases.TraceLogRecordRequested.Type
  >,
) {
  const processGroups = yield* ProcessGroups;
  const clock = yield* Clock.Clock;
  const taskRun = yield* Effect.fromNullishOr(
    taskRunsState.runsByTaskName.get(messageFromWorker.taskName),
  ).pipe(dieOnInvariantFailure("worker protocol", "messages refer to a started task"));
  yield* Match.value(messageFromWorker).pipe(
    Match.tagsExhaustive({
      TaskFiberStartedProcess: (started) =>
        processGroups
          .record(started.processId, started.taskName, started.startTime)
          .pipe(
            Effect.andThen(
              WorkerPool.use((workerPool) =>
                workerPool.acknowledgeProcessRecorded(started.recordId),
              ),
            ),
          ),
      TaskFiberProcessLeaderExited: (exited) =>
        processGroups.recordLeaderExit(exited.processId, exited.latestMemberStartTime),
      TaskFiberClaimingProcessReports: (claiming) =>
        processGroups
          .claimProcessesToReport(claiming.processes)
          .pipe(
            Effect.flatMap((claimed) =>
              WorkerPool.use((workerPool) =>
                workerPool.answerReportClaim(claiming.claimId, claimed),
              ),
            ),
          ),
      TaskFiberLoading: () =>
        taskRunsState.moveTaskRun(messageFromWorker.taskName, TaskRunEvent.cases.Load.make({})),
      TaskFiberStarted: () =>
        taskRunsState.moveTaskRun(
          messageFromWorker.taskName,
          TaskRunEvent.cases.StartBody.make({}),
        ),
      TaskFiberStartedCleanUp: () => taskRun.stop.move(TaskStopEvent.cases.StartCleanup.make({})),
      TaskFiberStoppedBeforeBody: () =>
        taskRun.stop.move(TaskStopEvent.cases.StopBeforeBody.make({})),
      TaskFiberRequestedTask: (requested) => {
        taskRun.bodyRequestedTaskNames.add(requested.dependencyTaskName);
        taskRun.dependencyRequestIdsAwaitingAnswer.add(requested.dependencyRequestId);
        return FiberSet.run(
          taskRunsState.taskFibers,
          sendDependencyResultToWorker(taskRunsState, requestDependencyTaskResult, requested),
          { startImmediately: false },
        ).pipe(
          Effect.flatMap((requestFiber) =>
            Effect.sync(() => {
              taskRun.requestFibersByDependencyRequestId.set(
                requested.dependencyRequestId,
                requestFiber,
              );
              requestFiber.addObserver(() => {
                taskRun.requestFibersByDependencyRequestId.delete(requested.dependencyRequestId);
                taskRunsState.refreshBodyClocks(
                  requested.taskName,
                  clock.currentTimeMillisUnsafe(),
                );
              });
            }),
          ),
        );
      },
      TaskFiberRequestedStandardInput: (requested) =>
        taskRunsState.rootTaskStandardInput.answerNextRead(requested.taskName, (read) =>
          WorkerPool.use((workerPool) =>
            workerPool.answerStandardInputRead(requested.readId, read),
          ),
        ),
      TaskFiberReceivedAwaitedFailure: (received) =>
        Effect.sync(() => {
          taskRunsState.recordFailureReachedAwaitingTask(received.awaitedTaskName);
        }),
      TaskFiberCancelledRequest: (cancelled) => {
        taskRun.dependencyRequestIdsAwaitingAnswer.delete(cancelled.dependencyRequestId);
        const requestFiber = taskRun.requestFibersByDependencyRequestId.get(
          cancelled.dependencyRequestId,
        );
        return requestFiber === undefined ? Effect.void : Fiber.interrupt(requestFiber);
      },
      TaskFiberFailedBeforeCleanup: (failed) =>
        Effect.gen(function* () {
          yield* taskRun.stop.move(TaskStopEvent.cases.FailBeforeCleanup.make({}));
          const action = yield* taskRunsState.moveTaskRun(
            failed.taskName,
            TaskRunEvent.cases.FailBeforeCleanup.make({}),
          );
          if (action === "settleOutcome") {
            const ending = yield* Effect.exit(bodyEnding(failed.taskName, failed.failure));
            yield* settleBodyOutcome(taskRunsState, failed.taskName, ending);
          }
        }),
      TaskFiberServed: (served) =>
        Effect.gen(function* () {
          const action = yield* taskRunsState.moveTaskRun(
            served.taskName,
            TaskRunEvent.cases.Serve.make({}),
          );
          if (action === "settleOutcome") {
            yield* taskRunsState.markServed(served.taskName);
            Deferred.doneUnsafe(taskRun.bodyOutcomeDeferred, Exit.succeed(served));
          }
        }),
      TaskFiberEnded: (ended) =>
        Effect.exit(bodyEnding(ended.taskName, ended.result)).pipe(
          Effect.flatMap((ending) => endTask(taskRunsState, messageFromWorker.taskName, ending)),
        ),
    }),
  );
  taskRunsState.refreshBodyClocks(messageFromWorker.taskName, yield* Clock.currentTimeMillis);
});

const printLoggedLine = (line: typeof FromWorker.cases.StandardErrorLineRequested.Type) =>
  Effect.gen(function* () {
    const traceFile = yield* TraceFile;
    if (line.traceLogRecordJson !== undefined) {
      traceFile.writeWorkerLogRecord(line.traceLogRecordJson);
    }
    return yield* printLine(line.logLevel, line.text);
  });

const handleWorkerEventInSpan = Effect.fn("handleWorkerEvent")(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  requestDependencyTaskResult: Parameters<typeof sendDependencyResultToWorker>[1],
  workerEvent: typeof WorkerEvent.Type,
) {
  yield* Effect.annotateCurrentSpan(`${TASK_TRACE_LABEL_PREFIX}.worker`, workerEvent.workerId);
  if (WorkerEvent.guards.ExitedWithCode(workerEvent)) {
    return yield* endTasksOfExitedWorker(taskRunsState, workerEvent);
  }
  if (WorkerEvent.guards.MessageUnreadable(workerEvent)) {
    return yield* Effect.die(
      new InternalInvariantBroken({
        area: "worker protocol",
        invariant: "messages match the worker protocol",
        cause: workerEvent.reason,
      }),
    );
  }
  const messageFromWorker = workerEvent.message;
  if (FromWorker.guards.StandardErrorLineRequested(messageFromWorker)) {
    return yield* printLoggedLine(messageFromWorker);
  }
  if (FromWorker.guards.TraceLogRecordRequested(messageFromWorker)) {
    const traceFile = yield* TraceFile;
    traceFile.writeWorkerLogRecord(messageFromWorker.traceLogRecordJson);
    return;
  }
  yield* Effect.annotateCurrentSpan({
    [`${TASK_TRACE_LABEL_PREFIX}.message`]: messageFromWorker._tag,
    [`${TASK_TRACE_LABEL_PREFIX}.task`]: messageFromWorker.taskName,
  });
  yield* onMessageAboutTask(taskRunsState, requestDependencyTaskResult, messageFromWorker);
  if (FromWorker.guards.TaskFiberStarted(messageFromWorker)) {
    yield* WorkerPool.use((workerPool) => workerPool.recordBodyStartHandled(workerEvent.workerId));
  }
});

export const handleWorkerEvent = (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  requestDependencyTaskResult: Parameters<typeof sendDependencyResultToWorker>[1],
  workerEvent: typeof WorkerEvent.Type,
) => {
  const isStandardErrorLine =
    WorkerEvent.guards.MessageReceived(workerEvent) &&
    FromWorker.guards.StandardErrorLineRequested(workerEvent.message);
  return isStandardErrorLine
    ? printLoggedLine(workerEvent.message)
    : handleWorkerEventInSpan(taskRunsState, requestDependencyTaskResult, workerEvent);
};
