import { Array as Arrays, Clock, Deferred, Effect, Exit, Option, Duration } from "effect";

import { dieOnInvariantFailure } from "../../../../../shared/invariant-errors.ts";
import { nameTask } from "../../../../../shared/label-lines.ts";
import { TASK_TRACE_LABEL_PREFIX } from "../../../../../shared/names-from-package-json.ts";
import { failTask } from "../../../shared/fail-run.ts";
import {
  TaskCleanupFrozeWorker,
  TaskCleanupNotStarted,
  TaskCleanupTimedOut,
} from "../../../shared/main-thread-errors.ts";
import { ProcessGroups } from "../../../shared/provide-process-groups.ts";
import { StopState } from "../../../shared/provide-stop-state.ts";
import { WorkerPool } from "../../../shared/provide-worker-pool.ts";
import { isUnfinishedOnWorker } from "./define-task-run-state.ts";
import type { makeTaskRun, makeTaskRunsState } from "./define-task-run-state.ts";
import { isOnWorker } from "./define-task-run-status.ts";
import { findStopRequestedAt, hasCleanupStarted, TaskStopEvent } from "./define-task-stop-stage.ts";
import type { moveTaskStopStage } from "./define-task-stop-stage.ts";

const STOP_REQUESTED_EVENT = "stop requested";

const CLEANUP_LIMIT_PASSED_EVENT = "cleanup limit passed";

const NANOSECONDS_PER_MILLISECOND = 1_000_000n;

const failCleanup = Effect.fn(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  failure: TaskCleanupFrozeWorker | TaskCleanupNotStarted | TaskCleanupTimedOut,
) {
  yield* (yield* StopState).recordFirstFailure(failure);
  yield* taskRunsState.recordCleanupFailure(failure.taskName, failure);
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

export const interruptDependencyRequests = (taskRun: ReturnType<typeof makeTaskRun>) =>
  WorkerPool.use((workerPool) => {
    const dependencyRequestIds = [...taskRun.dependencyRequestIdsAwaitingAnswer];
    taskRun.dependencyRequestIdsAwaitingAnswer.clear();
    return Effect.forEach(
      dependencyRequestIds,
      (dependencyRequestId) =>
        workerPool.answerDependencyRequest(dependencyRequestId, Exit.interrupt()),
      { discard: true },
    );
  });

const awaitBodyFinishedWithinLimit = (
  taskRun: ReturnType<typeof makeTaskRun>,
  cleanupDuration: Duration.Duration,
  sinceStopRequested: Duration.Duration,
) =>
  Deferred.await(taskRun.bodyFinishedDeferred).pipe(
    Effect.timeoutOption(
      Duration.max(Duration.subtract(cleanupDuration, sinceStopRequested), Duration.zero),
    ),
  );

const watchWorkerOfCleanupPastItsLimit = Effect.fn(function* (
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
          hasCleanupStarted(otherTaskRun.stop.readStage()),
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
    `because its cleanup of ${nameTask(taskName)} ran past its ${Duration.format(cleanupDuration)} limit and then it stopped answering`,
    taskName,
  );
  yield* failCleanup(
    taskRunsState,
    new TaskCleanupFrozeWorker({
      taskName,
      cleanupMilliseconds: Duration.toMillis(cleanupDuration),
    }),
  );
});

export const stopTask = Effect.fn(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  taskName: string,
  watchUntilStopsEnd: <Services>(
    watch: Effect.Effect<void, never, Services>,
  ) => Effect.Effect<void, never, Services>,
) {
  const processGroups = yield* ProcessGroups;
  const discoveredTask = taskRunsState.discoveredTasksByName.get(taskName);
  const taskRun = taskRunsState.runsByTaskName.get(taskName);
  const isTaskNotStarted = discoveredTask === undefined || taskRun === undefined;
  if (isTaskNotStarted) {
    return;
  }
  const status = taskRun.status;
  if (!isOnWorker(status)) {
    return;
  }
  const workerId = status.workerId;

  const workerPool = yield* WorkerPool;
  const cleanupDuration = discoveredTask.cleanupDuration;
  const cleanupMilliseconds = Duration.toMillis(cleanupDuration);
  const notAnsweredReason = `because it did not answer the stop of ${nameTask(taskName)} within ${Duration.format(cleanupDuration)}`;
  const failPastLimit = (
    terminationReason: string,
    failure: TaskCleanupNotStarted | TaskCleanupTimedOut,
  ) =>
    workerPool
      .requestWorkerTermination(workerId, terminationReason, taskName)
      .pipe(
        Effect.andThen(processGroups.killForTask(taskName)),
        Effect.andThen(failCleanup(taskRunsState, failure)),
      );
  const watchWorker = watchUntilStopsEnd(
    watchWorkerOfCleanupPastItsLimit(taskRunsState, taskName, workerId, cleanupDuration),
  );
  const act = (action: ReturnType<typeof moveTaskStopStage>[1]) =>
    ({
      none: Effect.void,
      sendStop: workerPool.stopTaskFiber(workerId, taskName),
      failNotAnswered: failPastLimit(
        notAnsweredReason,
        new TaskCleanupNotStarted({ taskName, cleanupMilliseconds }),
      ),
      failStillLoading: failPastLimit(
        `because it did not stop loading ${nameTask(taskName)} within ${Duration.format(cleanupDuration)}`,
        new TaskCleanupNotStarted({ taskName, cleanupMilliseconds }),
      ),
      failTimedOutNotAnswered: failPastLimit(
        notAnsweredReason,
        new TaskCleanupTimedOut({ taskName, cleanupMilliseconds }),
      ),
      failTimedOutAndWatch: processGroups
        .killForTask(taskName)
        .pipe(
          Effect.andThen(
            failCleanup(taskRunsState, new TaskCleanupTimedOut({ taskName, cleanupMilliseconds })),
          ),
          Effect.andThen(watchWorker),
        ),
      watch: watchWorker,
      interruptRequests: interruptDependencyRequests(taskRun),
    })[action];
  const nowMilliseconds = yield* Clock.currentTimeMillis;
  const [stage, requestAction] = yield* taskRun.stop.move(
    TaskStopEvent.cases.RequestStop.make({ stopRequestedAtMilliseconds: nowMilliseconds }),
  );
  yield* act(requestAction);
  const stopRequestedAtMilliseconds = findStopRequestedAt(stage);
  if (stopRequestedAtMilliseconds !== null) {
    const stopRequestedAt = BigInt(stopRequestedAtMilliseconds) * NANOSECONDS_PER_MILLISECOND;
    const cleanupSpan = yield* taskRunsState.openCleanupSpan(taskName, stopRequestedAt);
    cleanupSpan?.event(STOP_REQUESTED_EVENT, stopRequestedAt, {
      [`${TASK_TRACE_LABEL_PREFIX}.cleanup.milliseconds`]: cleanupMilliseconds,
    });
    const ended = yield* awaitBodyFinishedWithinLimit(
      taskRun,
      cleanupDuration,
      Duration.millis(nowMilliseconds - stopRequestedAtMilliseconds),
    );
    if (Option.isSome(ended)) {
      return;
    }
    cleanupSpan?.event(CLEANUP_LIMIT_PASSED_EVENT, yield* Clock.currentTimeNanos, {});
  }
  yield* taskRun.stop.move(TaskStopEvent.cases.PassLimit.make({})).pipe(
    Effect.flatMap(([, limitAction]) => act(limitAction)),
    Effect.uninterruptible,
  );
});
