import { Array as Arrays, Clock, Deferred, Effect, Exit, Option, Duration } from "effect";

import { dieOnInvariantFailure } from "../../../../../shared/invariant-errors.ts";
import { TASK_TRACE_LABEL_PREFIX } from "../../../../../shared/names-from-package-json.ts";
import { printTaskLines } from "../../../../../shared/print-to-standard-error.ts";
import { TASK_PACKAGE } from "../../../../../shared/task-package.ts";
import { failTask } from "../../../shared/fail-run.ts";
import {
  TaskCleanupFrozeWorker,
  TaskCleanupNotStarted,
  TaskCleanupTimedOut,
} from "../../../shared/main-thread-errors.ts";
import { ProcessGroups } from "../../../shared/provide-process-groups.ts";
import { StopState } from "../../../shared/provide-stop-state.ts";
import { WorkerPool } from "../../../shared/provide-worker-pool.ts";
import { findAssignedWorkerId, isUnfinishedOnWorker } from "./define-task-run-state.ts";
import type { makeTaskRunsState } from "./define-task-run-state.ts";

const STILL_STOPPING_NOTICE_SECONDS = 2;

const STILL_STOPPING_NOTICE_AFTER = Duration.seconds(STILL_STOPPING_NOTICE_SECONDS);

const failCleanup = Effect.fn("failCleanup")(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  failure: TaskCleanupFrozeWorker | TaskCleanupNotStarted | TaskCleanupTimedOut,
) {
  yield* (yield* StopState).recordFirstFailure(failure);
  yield* failTask(failure, taskRunsState.markFailed);
});

const findCleanupDuration = (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  taskName: string,
) =>
  Effect.fromNullishOr(taskRunsState.discoveredTasksByName.get(taskName)).pipe(
    dieOnInvariantFailure("task lifecycle", "a task cleaning up on a worker was discovered"),
    Effect.map((discoveredTask) => discoveredTask.cleanupDuration),
  );

const watchWorkerOfCleanupPastItsLimit = Effect.fn("watchWorkerOfCleanupPastItsLimit")(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  taskName: string,
  workerId: number,
  cleanupDuration: Duration.Duration,
) {
  const workerPool = yield* WorkerPool;
  const taskRun = yield* Effect.fromNullishOr(taskRunsState.runsByTaskName.get(taskName)).pipe(
    dieOnInvariantFailure("task lifecycle", "a task whose cleanup is watched was started"),
  );
  const cleanupEnded = Deferred.await(taskRun.bodyFinishedDeferred).pipe(
    Effect.as("cleanup ended" as const),
  );
  const checkWorkerAnswers = Effect.gen(function* () {
    const cleanupDurationsOnWorker = yield* Effect.forEach(
      [...taskRunsState.runsByTaskName].filter(
        ([, otherTaskRun]) =>
          isUnfinishedOnWorker(otherTaskRun, workerId) &&
          (Deferred.isDoneUnsafe(otherTaskRun.bodyCleanupStartedDeferred) ||
            Deferred.isDoneUnsafe(otherTaskRun.bodyFailedBeforeCleanupDeferred)),
      ),
      ([otherTaskName]) => findCleanupDuration(taskRunsState, otherTaskName),
    );
    const answerLimit = Arrays.max([cleanupDuration, ...cleanupDurationsOnWorker], Duration.Order);
    const answer = yield* workerPool
      .awaitAnswer(workerId)
      .pipe(
        Effect.raceFirst(cleanupEnded),
        Effect.timeoutOption(answerLimit),
        Effect.map(
          Option.match({ onNone: () => "unanswered" as const, onSome: (outcome) => outcome }),
        ),
      );
    if (answer === "answered") {
      yield* cleanupEnded.pipe(Effect.timeoutOption(cleanupDuration));
    }
    return answer;
  });
  const lastAnswer = yield* checkWorkerAnswers.pipe(
    Effect.repeat({ while: (answer) => answer === "answered" }),
  );
  if (lastAnswer !== "unanswered") {
    return;
  }
  yield* workerPool.requestWorkerTermination(
    workerId,
    `because its cleanup of "${taskName}" ran past its ${Duration.format(cleanupDuration)} limit and then it stopped answering`,
  );
  yield* failCleanup(
    taskRunsState,
    new TaskCleanupFrozeWorker({
      taskName,
      cleanupMilliseconds: Duration.toMillis(cleanupDuration),
    }),
  );
});

export const stopTask = Effect.fn("stopTask")(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  taskName: string,
  watchUntilStopsEnd: <Services>(
    watch: Effect.Effect<void, never, Services>,
  ) => Effect.Effect<void, never, Services>,
) {
  yield* Effect.annotateCurrentSpan(`${TASK_TRACE_LABEL_PREFIX}.task`, taskName);
  const processGroups = yield* ProcessGroups;
  const discoveredTask = taskRunsState.discoveredTasksByName.get(taskName);
  const taskRun = taskRunsState.runsByTaskName.get(taskName);
  const isTaskNotStarted = discoveredTask === undefined || taskRun === undefined;
  if (isTaskNotStarted) {
    return;
  }
  const workerId = findAssignedWorkerId(taskRun.status);
  const hasNoBodyToStop = workerId === null || Deferred.isDoneUnsafe(taskRun.bodyFinishedDeferred);
  if (hasNoBodyToStop) {
    return;
  }

  const workerPool = yield* WorkerPool;
  const nowMilliseconds = yield* Clock.currentTimeMillis;
  const isFirstStop = yield* Deferred.succeed(
    taskRun.stopRequestedAtMillisecondsDeferred,
    nowMilliseconds,
  );
  if (isFirstStop) {
    yield* workerPool.stopTaskFiber(workerId, taskName);
  }
  const sinceStopRequested = Duration.millis(
    nowMilliseconds - (yield* Deferred.await(taskRun.stopRequestedAtMillisecondsDeferred)),
  );
  const cleanupDuration = discoveredTask.cleanupDuration;
  const awaitBodyFinished = Effect.gen(function* () {
    const isBeforeNotice = Duration.isLessThan(sinceStopRequested, STILL_STOPPING_NOTICE_AFTER);
    if (!isBeforeNotice) {
      return yield* Deferred.await(taskRun.bodyFinishedDeferred);
    }
    const finishedBeforeNotice = yield* Deferred.await(taskRun.bodyFinishedDeferred).pipe(
      Effect.timeoutOption(Duration.subtract(STILL_STOPPING_NOTICE_AFTER, sinceStopRequested)),
    );
    if (Option.isSome(finishedBeforeNotice)) {
      return;
    }
    yield* printTaskLines(
      "Warn",
      taskName,
      `still stopping after ${Duration.format(STILL_STOPPING_NOTICE_AFTER)}; ${TASK_PACKAGE.name} kills its commands and stops waiting for it when its ${Duration.format(cleanupDuration)} cleanup limit passes`,
    );
    yield* Deferred.await(taskRun.bodyFinishedDeferred);
  });
  const ended = yield* awaitBodyFinished.pipe(
    Effect.timeoutOption(
      Duration.max(Duration.subtract(cleanupDuration, sinceStopRequested), Duration.zero),
    ),
  );
  if (Option.isSome(ended)) {
    return;
  }

  yield* Effect.uninterruptible(
    Effect.gen(function* () {
      const workerAnsweredStop = Deferred.isDoneUnsafe(taskRun.bodyCleanupStartedDeferred);
      const isFirstToPassCleanupLimit = yield* Deferred.done(
        taskRun.cleanupLimitPassedDeferred,
        Exit.void,
      );
      if (isFirstToPassCleanupLimit) {
        if (!workerAnsweredStop) {
          yield* workerPool.requestWorkerTermination(
            workerId,
            `because it did not answer the stop of "${taskName}" within ${Duration.format(cleanupDuration)}`,
          );
        }
        yield* processGroups.killForTask(taskName);
        const cleanupMilliseconds = Duration.toMillis(cleanupDuration);
        const cleanupStarted =
          workerAnsweredStop || Deferred.isDoneUnsafe(taskRun.bodyFailedBeforeCleanupDeferred);
        yield* failCleanup(
          taskRunsState,
          cleanupStarted
            ? new TaskCleanupTimedOut({ taskName, cleanupMilliseconds })
            : new TaskCleanupNotStarted({ taskName, cleanupMilliseconds }),
        );
      }
      if (workerAnsweredStop) {
        yield* watchUntilStopsEnd(
          watchWorkerOfCleanupPastItsLimit(taskRunsState, taskName, workerId, cleanupDuration),
        );
      }
    }),
  );
});
