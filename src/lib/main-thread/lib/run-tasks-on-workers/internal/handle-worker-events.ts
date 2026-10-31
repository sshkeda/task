import { Clock, Deferred, Effect, Exit, Fiber, FiberSet, Match, Option, Schema } from "effect";

import {
  dieOnInvariantFailure,
  InternalInvariantBroken,
} from "../../../../../shared/invariant-errors.ts";
import { TASK_TRACE_LABEL_PREFIX } from "../../../../../shared/names-from-package-json.ts";
import { printLine, printTaskLines } from "../../../../../shared/print-to-standard-error.ts";
import { WorkerExited } from "../../../../../shared/process-errors.ts";
import { TaskFiberResult } from "../../../../../shared/rpc.ts";
import {
  TaskCleanupFailed,
  ServedTaskFailed,
  TaskBodyFailed,
} from "../../../../../shared/task-errors.ts";
import type { AwaitedTasksFailure, TaskRequestFailure } from "../../../../../shared/task-errors.ts";
import { TraceFile } from "../../../../../shared/write-trace-file.ts";
import { failRun, failTask } from "../../../shared/fail-run.ts";
import { ProcessGroups } from "../../../shared/provide-process-groups.ts";
import { StopState } from "../../../shared/provide-stop-state.ts";
import { FromWorker, WorkerEvent, WorkerPool } from "../../../shared/provide-worker-pool.ts";
import { reportFailure } from "../../../shared/report-task-failure.ts";
import { findAssignedWorkerId, TaskRunStatus } from "../shared/define-task-run-state.ts";
import type { makeTaskRunsState } from "../shared/define-task-run-state.ts";

const sendDependencyResultToWorker = Effect.fn("sendDependencyResultToWorker")(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  requestDependencyTaskResult: (
    dependencyTaskName: string,
    requestingTaskName: string,
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
  ).pipe(
    Effect.exit,
    Effect.raceFirst(
      Deferred.await(requestingTaskRun.bodyFinishedDeferred).pipe(Effect.andThen(Effect.interrupt)),
    ),
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

const isTaskEnded = TaskRunStatus.isAnyOf(["Failed", "Cancelled"]);

const bodyEnding = (taskName: string, result: typeof TaskFiberResult.Type) =>
  TaskFiberResult.match(result, {
    Returned: (
      returned,
    ): Exit.Exit<
      typeof returned,
      TaskBodyFailed | TaskCleanupFailed | WorkerExited | typeof AwaitedTasksFailure.Type
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
  const isOutcomeAlreadySettled =
    taskRun === undefined || Deferred.isDoneUnsafe(taskRun.bodyOutcomeDeferred);
  if (isOutcomeAlreadySettled) {
    return;
  }
  const failedAfterTaskEnded = isTaskEnded(taskRun.status)
    ? Option.filter(Exit.findErrorOption(ending), isTaskBodyFailed)
    : Option.none();
  Deferred.doneUnsafe(taskRun.bodyOutcomeDeferred, ending);
  if (Option.isSome(failedAfterTaskEnded)) {
    yield* failTask(failedAfterTaskEnded.value, taskRunsState.markFailed);
  }
});

const endTask = Effect.fn("endTask")(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  taskName: string,
  ending: Exit.Exit<
    Effect.Success<ReturnType<typeof bodyEnding>>,
    Effect.Error<ReturnType<typeof bodyEnding>>
  >,
) {
  const taskRun = taskRunsState.runsByTaskName.get(taskName);
  const isBodyAlreadyFinished =
    taskRun === undefined || Deferred.isDoneUnsafe(taskRun.bodyFinishedDeferred);
  if (isBodyAlreadyFinished) {
    return;
  }

  Deferred.doneUnsafe(taskRun.bodyFinishedDeferred, Exit.void);
  taskRunsState.refreshBodyClocks(taskName, yield* Clock.currentTimeMillis);
  const servedBeforeEnding = taskRunsState.markClosed(taskName);
  if (servedBeforeEnding) {
    yield* printTaskLines("Info", taskName, "closed");
  }
  if (!Deferred.isDoneUnsafe(taskRun.bodyOutcomeDeferred)) {
    return yield* settleBodyOutcome(taskRunsState, taskName, ending);
  }
  const failure = Exit.findErrorOption(ending);
  if (Option.isNone(failure)) {
    return;
  }
  const failureEndsRun = isTaskCleanupFailed(failure.value) || isWorkerExited(failure.value);
  if (failureEndsRun) {
    yield* (yield* StopState).recordFirstFailure(failure.value);
    return yield* failTask(failure.value, taskRunsState.markFailed);
  }
  if (!servedBeforeEnding) {
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

const endTasksOfExitedWorker = Effect.fn("endTasksOfExitedWorker")(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  workerEvent: typeof WorkerEvent.cases.ExitedWithCode.Type,
) {
  const processGroups = yield* ProcessGroups;
  const unfinished = [...taskRunsState.runsByTaskName].filter(
    ([, taskRun]) =>
      findAssignedWorkerId(taskRun.status) === workerEvent.workerId &&
      !Deferred.isDoneUnsafe(taskRun.bodyFinishedDeferred),
  );
  if (!workerEvent.terminationRequested) {
    for (const [taskName] of unfinished) {
      yield* processGroups.killForTask(taskName);
    }
  }
  const workerExit = {
    workerId: workerEvent.workerId,
    exitCode: workerEvent.exitCode,
    uncaughtError: workerEvent.uncaughtError,
  };
  const exitedUnexpectedlyWithoutTasks =
    !workerEvent.terminationRequested && unfinished.length === 0;
  if (exitedUnexpectedlyWithoutTasks) {
    return yield* failRun(
      new WorkerExited({ taskName: null, ...workerExit }),
      taskRunsState.markFailed,
    );
  }
  for (const [taskName] of unfinished) {
    yield* endTask(
      taskRunsState,
      taskName,
      workerEvent.terminationRequested
        ? Exit.interrupt()
        : Exit.fail(new WorkerExited({ taskName, ...workerExit })),
    );
  }
});

const onMessageAboutTask = Effect.fn("onMessageAboutTask")(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  requestDependencyTaskResult: Parameters<typeof sendDependencyResultToWorker>[1],
  messageFromWorker: Exclude<
    typeof FromWorker.Type,
    | typeof FromWorker.cases.StandardErrorLineRequested.Type
    | typeof FromWorker.cases.TraceWriteFailed.Type
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
      TaskFiberLoading: () => Deferred.done(taskRun.bodyLoadingDeferred, Exit.void),
      TaskFiberStarted: () => {
        taskRun.stepBeforeBody = null;
        return Deferred.complete(
          taskRun.bodyStartedAtMillisecondsDeferred,
          Clock.currentTimeMillis,
        );
      },
      TaskFiberStartedCleanUp: () => Deferred.done(taskRun.bodyCleanupStartedDeferred, Exit.void),
      TaskFiberRequestedTask: (requested) => {
        taskRun.bodyRequestedTaskNames.add(requested.dependencyTaskName);
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
        taskRunsState.rootTaskStandardInput.answerNextRead((read) =>
          WorkerPool.use((workerPool) =>
            workerPool.answerStandardInputRead(requested.readId, read),
          ),
        ),
      TaskFiberReceivedAwaitedFailure: (received) =>
        Effect.sync(() => {
          taskRunsState.recordFailureReachedAwaitingTask(received.awaitedTaskName);
        }),
      TaskFiberCancelledRequest: (cancelled) => {
        const requestFiber = taskRun.requestFibersByDependencyRequestId.get(
          cancelled.dependencyRequestId,
        );
        return requestFiber === undefined ? Effect.void : Fiber.interrupt(requestFiber);
      },
      TaskFiberFailedBeforeCleanup: (failed) =>
        Deferred.done(taskRun.bodyFailedBeforeCleanupDeferred, Exit.void).pipe(
          Effect.andThen(Effect.exit(bodyEnding(failed.taskName, failed.failure))),
          Effect.flatMap((ending) => settleBodyOutcome(taskRunsState, failed.taskName, ending)),
        ),
      TaskFiberServed: (served) =>
        Effect.sync(() => {
          taskRunsState.markServing(served.taskName);
          Deferred.doneUnsafe(taskRun.bodyOutcomeDeferred, Exit.succeed(served));
        }),
      TaskFiberEnded: (ended) => {
        taskRun.logMessages = [...ended.logs];
        return Effect.exit(bodyEnding(ended.taskName, ended.result)).pipe(
          Effect.flatMap((ending) => endTask(taskRunsState, messageFromWorker.taskName, ending)),
        );
      },
    }),
  );
  taskRunsState.refreshBodyClocks(messageFromWorker.taskName, yield* Clock.currentTimeMillis);
});

export const handleWorkerEvent = Effect.fn("handleWorkerEvent")(function* (
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
    return yield* printLine(messageFromWorker.logLevel, messageFromWorker.text);
  }
  if (FromWorker.guards.TraceWriteFailed(messageFromWorker)) {
    return yield* TraceFile.use((traceFile) =>
      traceFile.markIncomplete(
        `worker ${workerEvent.workerId} reported that ${messageFromWorker.reason}`,
      ),
    );
  }
  yield* Effect.annotateCurrentSpan({
    [`${TASK_TRACE_LABEL_PREFIX}.message`]: messageFromWorker._tag,
    [`${TASK_TRACE_LABEL_PREFIX}.task`]: messageFromWorker.taskName,
  });
  yield* onMessageAboutTask(taskRunsState, requestDependencyTaskResult, messageFromWorker);
});
