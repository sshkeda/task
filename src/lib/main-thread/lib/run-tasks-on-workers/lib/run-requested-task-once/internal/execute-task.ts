import {
  Cause,
  Clock,
  Deferred,
  Duration,
  Effect,
  Exit,
  FiberSet,
  Option,
  Ref,
  Schema,
} from "effect";

import {
  formatTaskPassed,
  formatTaskServing,
} from "../../../../../../../shared/format-task-lines.ts";
import { dieOnInvariantFailure } from "../../../../../../../shared/invariant-errors.ts";
import { TASK_TRACE_LABEL_PREFIX } from "../../../../../../../shared/names-from-package-json.ts";
import { printTaskLines } from "../../../../../../../shared/print-to-standard-error.ts";
import { WorkerExited } from "../../../../../../../shared/process-errors.ts";
import {
  TaskBodyFailed,
  TaskSetupTimedOut,
  TaskTimedOut,
} from "../../../../../../../shared/task-errors.ts";
import { failRun, failTask } from "../../../../../shared/fail-run.ts";
import { StopState } from "../../../../../shared/provide-stop-state.ts";
import { FromWorker, WorkerPool } from "../../../../../shared/provide-worker-pool.ts";
import { reportFailure, reportTaskFailure } from "../../../../../shared/report-task-failure.ts";
import {
  formatTaskRunStatusForTrace,
  findAssignedWorkerId,
  findTaskNamesOccupyingWorker,
  formatWaitingForWorker,
} from "../../../shared/define-task-run-state.ts";
import type { makeTaskRunsState } from "../../../shared/define-task-run-state.ts";
import type { makeStopwatch } from "../../../shared/make-stopwatch.ts";
import { stopTask } from "../../../shared/stop-task.ts";
import { awaitWithinCountedLimit } from "./await-within-counted-limit.ts";
import { makeTaskSetupClock } from "./make-task-setup-clock.ts";

const isTaskTimedOut = Schema.is(TaskTimedOut);

const STILL_BEFORE_BODY_NOTICE_SECONDS = 2;

const STILL_BEFORE_BODY_NOTICE_AFTER = Duration.seconds(STILL_BEFORE_BODY_NOTICE_SECONDS);

const isTaskSetupTimedOut = Schema.is(TaskSetupTimedOut);

const taskTimedOut = (taskName: string, timeoutDuration: Duration.Duration) =>
  new TaskTimedOut({ taskName, timeoutMilliseconds: Duration.toMillis(timeoutDuration) });

const awaitWithinTimeout = <Success, Failure, Services>(
  awaiting: Effect.Effect<Success, Failure, Services>,
  bodyWaitingOnRequestedTasks: ReturnType<typeof makeStopwatch>,
  bodyStartedAtMilliseconds: number,
  timeoutDuration: Duration.Duration,
) =>
  awaitWithinCountedLimit(awaiting, Duration.toMillis(timeoutDuration), (nowMilliseconds) =>
    Effect.succeed(
      nowMilliseconds -
        bodyStartedAtMilliseconds -
        bodyWaitingOnRequestedTasks.readMilliseconds(nowMilliseconds),
    ),
  );

const failRunWhenCleanupOutlastsTimeout = Effect.fn("failRunWhenCleanupOutlastsTimeout")(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  taskName: string,
  timeoutDuration: Duration.Duration,
) {
  const taskRun = yield* Effect.fromNullishOr(taskRunsState.runsByTaskName.get(taskName)).pipe(
    dieOnInvariantFailure("task timeouts", "a task whose cleanup is timed has a run"),
  );
  const bodyStartedAtMilliseconds = yield* Deferred.await(
    taskRun.bodyStartedAtMillisecondsDeferred,
  ).pipe(Effect.raceFirst(Deferred.await(taskRun.bodyFinishedDeferred).pipe(Effect.as(null))));
  if (bodyStartedAtMilliseconds === null) {
    return;
  }
  const bodyFinished = yield* awaitWithinTimeout(
    Deferred.await(taskRun.bodyFinishedDeferred),
    taskRun.bodyWaitingOnRequestedTasks,
    bodyStartedAtMilliseconds,
    timeoutDuration,
  );
  if (Option.isNone(bodyFinished)) {
    yield* failRun(taskTimedOut(taskName, timeoutDuration), taskRunsState.markFailed);
  }
});

const endsRunDirectly = Schema.is(Schema.Union([TaskSetupTimedOut, WorkerExited]));

export const executeTask = Effect.fn("executeTaskFiber")(
  function* (taskRunsState: Effect.Success<typeof makeTaskRunsState>, taskName: string) {
    yield* Effect.annotateCurrentSpan(`${TASK_TRACE_LABEL_PREFIX}.task`, taskName);
    yield* Effect.logInfo("task started").pipe(
      Effect.annotateLogs(`${TASK_TRACE_LABEL_PREFIX}.task`, taskName),
    );
    const discoveredTask = yield* Effect.fromNullishOr(
      taskRunsState.discoveredTasksByName.get(taskName),
    ).pipe(dieOnInvariantFailure("task execution", "the requested task is registered and started"));
    const taskRun = yield* Effect.fromNullishOr(taskRunsState.runsByTaskName.get(taskName)).pipe(
      dieOnInvariantFailure("task execution", "the requested task is registered and started"),
    );

    const span = yield* Effect.currentSpan.pipe(
      dieOnInvariantFailure("task execution", "the task has a tracing span"),
    );
    const setupClock = yield* makeTaskSetupClock(
      taskRunsState,
      taskName,
      discoveredTask.cleanupDuration,
    );
    const waitingForWorkerIdRef = yield* Ref.make<number | null>(null);
    const describeStepBeforeBody = () => {
      const waitingForWorkerId = Ref.getUnsafe(waitingForWorkerIdRef);
      if (waitingForWorkerId === null) {
        return taskRun.stepBeforeBody;
      }
      if (Deferred.isDoneUnsafe(taskRun.bodyStartedAtMillisecondsDeferred)) {
        return null;
      }
      const occupyingTaskNames = findTaskNamesOccupyingWorker(
        taskRunsState.runsByTaskName,
        waitingForWorkerId,
      ).filter((occupyingTaskName) => occupyingTaskName !== taskName);
      const [longestOccupyingTaskName, ...otherTaskNames] = occupyingTaskNames;
      if (longestOccupyingTaskName !== undefined) {
        return formatWaitingForWorker(waitingForWorkerId, longestOccupyingTaskName, otherTaskNames);
      }
      return Deferred.isDoneUnsafe(taskRun.bodyLoadingDeferred)
        ? `loading ${discoveredTask.filePath}`
        : `waiting for worker ${waitingForWorkerId} to take it`;
    };
    const awaitBodyStart = Effect.gen(function* () {
      yield* printTaskLines("Info", taskName, "started");

      const workerPool = yield* WorkerPool;
      taskRun.stepBeforeBody = "starting a worker";
      const workerId = yield* workerPool
        .selectWorkerIdForTask(taskRunsState.readWorkerLoad, (selectedWorkerId) =>
          taskRunsState.recordWorkerAssignment(taskName, selectedWorkerId),
        )
        .pipe(
          Effect.mapError(
            (failure) => new TaskBodyFailed({ taskName, failures: reportFailure(failure) }),
          ),
        );
      yield* setupClock.recordWorkerAssignment(workerId);
      yield* workerPool.startTaskFiber(workerId, {
        taskName,
        taskFilePath: discoveredTask.filePath,
        taskPackageDirectoryPath: discoveredTask.packageJsonDirectoryPath,
        cleanupMilliseconds: Duration.toMillis(discoveredTask.cleanupDuration),
        traceId: span.traceId,
        spanId: span.spanId,
        standardInput: taskRunsState.rootTaskStandardInput.standardInput,
      });
      yield* Ref.set(waitingForWorkerIdRef, workerId);
      const awaitBodyStarted = Deferred.await(taskRun.bodyStartedAtMillisecondsDeferred).pipe(
        Effect.raceFirst(Deferred.await(taskRun.bodyFinishedDeferred)),
      );
      const startedBeforeNotice = yield* awaitBodyStarted.pipe(
        Effect.timeoutOption(STILL_BEFORE_BODY_NOTICE_AFTER),
      );
      const stepAtNotice = describeStepBeforeBody();
      const isStillBeforeBodyAtNotice = Option.isNone(startedBeforeNotice) && stepAtNotice !== null;
      if (isStillBeforeBodyAtNotice) {
        yield* printTaskLines("Info", taskName, stepAtNotice);
      }
      yield* awaitBodyStarted;
    }).pipe((setup) =>
      setupClock.limit(setup, () => describeStepBeforeBody() ?? "waiting for its body to start"),
    );
    const timeoutDuration = discoveredTask.timeoutDuration;
    const awaitBodyOutcome = Effect.gen(function* () {
      yield* awaitBodyStart;
      const awaitOutcome = Deferred.await(taskRun.bodyOutcomeDeferred);
      const bodyStartedAtMilliseconds = yield* Deferred.poll(
        taskRun.bodyStartedAtMillisecondsDeferred,
      ).pipe(Effect.flatMap(Effect.transposeOption));
      const hasNoBodyTimeout = timeoutDuration === null || Option.isNone(bodyStartedAtMilliseconds);
      if (hasNoBodyTimeout) {
        return yield* awaitOutcome;
      }
      const outcome = yield* awaitWithinTimeout(
        awaitOutcome,
        taskRun.bodyWaitingOnRequestedTasks,
        bodyStartedAtMilliseconds.value,
        timeoutDuration,
      );
      if (Option.isNone(outcome)) {
        const timedOut = taskTimedOut(taskName, timeoutDuration);
        yield* failTask(timedOut, taskRunsState.markFailed);
        yield* FiberSet.run(
          taskRunsState.taskFibers,
          stopTask(taskRunsState, taskName, (watch) =>
            FiberSet.run(taskRunsState.taskFibers, watch).pipe(Effect.asVoid),
          ),
        );
        return yield* timedOut;
      }
      return outcome.value;
    });
    const outcome = yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const outcomeExit = yield* Effect.exit(restore(awaitBodyOutcome));
        const cleanupMayOutlastTimeout =
          Exit.isFailure(outcomeExit) &&
          timeoutDuration !== null &&
          findAssignedWorkerId(taskRun.status) !== null &&
          !Deferred.isDoneUnsafe(taskRun.bodyFinishedDeferred) &&
          !Option.exists(
            Exit.findErrorOption(outcomeExit),
            (failure) => isTaskTimedOut(failure) || isTaskSetupTimedOut(failure),
          );
        if (cleanupMayOutlastTimeout) {
          yield* FiberSet.run(
            taskRunsState.taskFibers,
            failRunWhenCleanupOutlastsTimeout(taskRunsState, taskName, timeoutDuration),
          );
        }
        return yield* outcomeExit;
      }),
    );
    const taskBodyStartedAtMilliseconds = yield* Deferred.await(
      taskRun.bodyStartedAtMillisecondsDeferred,
    );
    if (FromWorker.guards.TaskFiberServed(outcome)) {
      yield* printTaskLines(
        "Info",
        taskName,
        formatTaskServing((yield* Clock.currentTimeMillis) - taskBodyStartedAtMilliseconds),
      );
      return outcome.value;
    }

    yield* taskRunsState.markPassed(taskName);
    yield* printTaskLines("Info", taskName, formatTaskPassed(outcome.elapsedMilliseconds));
    return outcome.value;
  },
  (effect, taskRunsState, taskName) =>
    effect.pipe(
      Effect.scoped,
      Effect.onExit((taskExit) =>
        Effect.gen(function* () {
          const taskRun = taskRunsState.runsByTaskName.get(taskName);
          const isTaskRunMissing = taskRun === undefined;
          if (isTaskRunMissing) {
            return;
          }

          taskRunsState.markEndedFromExit(taskName, taskExit);
          yield* Effect.annotateCurrentSpan(
            `${TASK_TRACE_LABEL_PREFIX}.state`,
            formatTaskRunStatusForTrace(taskRun.status),
          );
          const failure = Exit.findErrorOption(taskExit);
          const failedUnreportedWithMoreThanInterrupts =
            Exit.isFailure(taskExit) &&
            !Cause.hasInterruptsOnly(taskExit.cause) &&
            !Option.exists(failure, isTaskTimedOut);
          if (failedUnreportedWithMoreThanInterrupts) {
            yield* reportTaskFailure(taskName, taskExit.cause);
          }
          const failureEndsRun = Option.isSome(failure) && endsRunDirectly(failure.value);
          if (failureEndsRun) {
            yield* (yield* StopState).recordFirstFailure(failure.value);
          }
        }),
      ),
    ),
);
