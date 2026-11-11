import { Context, Deferred, Effect, Exit, Layer, Schema } from "effect";
import type { Cause } from "effect";

import {
  FAILURE_REASON_NOT_RECORDED_LINE,
  formatFailureReasonLine,
} from "../../../shared/format-error-lines.ts";
import { holdTaskLogsDuring, makeTaskLogHold } from "../../../shared/hold-task-logs.ts";
import type { StandardErrorWriteFailed } from "../../../shared/print-to-standard-error.ts";
import type { WorkerExited } from "../../../shared/process-errors.ts";
import type { STOP_REASONS } from "../../../shared/stop-signals.ts";
import type {
  ServedTaskFailed,
  TaskBodyFailed,
  TaskCleanupFailed,
  TaskSetupTimedOut,
  TaskTimedOut,
} from "../../../shared/task-errors.ts";
import { TASK_PACKAGE } from "../../../shared/task-package.ts";
import type {
  AwaitedTaskFailed,
  SettledTasksFailed,
  TaskNotMadeWhileLoading,
  TaskWaitsOnItself,
} from "../../errors/index.ts";
import type { killAllProcessGroups } from "../../kill-leftover-processes/index.ts";
import { findFailureReason } from "./find-failure-reason.ts";
import { FailureReasonNotFormatted, StopRequested } from "./main-thread-errors.ts";
import type {
  BrokenTasksFound,
  ServingTaskRequestsItself,
  TaskCleanupCutOff,
  TaskCleanupFrozeWorker,
  TaskCleanupNotStarted,
  TaskCleanupTimedOut,
} from "./main-thread-errors.ts";

const isStopRequested = Schema.is(StopRequested);

const makeShutdown = Effect.gen(function* () {
  const shutdownStartedDeferred = yield* Deferred.make<void>();
  const firstFailureDeferred = yield* Deferred.make<
    | AwaitedTaskFailed
    | BrokenTasksFound
    | Effect.Error<ReturnType<typeof killAllProcessGroups>>
    | ServedTaskFailed
    | ServingTaskRequestsItself
    | SettledTasksFailed
    | StandardErrorWriteFailed
    | StopRequested
    | TaskBodyFailed
    | TaskCleanupCutOff
    | TaskCleanupFailed
    | TaskCleanupFrozeWorker
    | TaskCleanupNotStarted
    | TaskCleanupTimedOut
    | TaskNotMadeWhileLoading
    | TaskSetupTimedOut
    | TaskTimedOut
    | TaskWaitsOnItself
    | WorkerExited
  >();
  const awaitFailure = Deferred.await(firstFailureDeferred);

  const forcedStopDeferred = yield* Deferred.make<Effect.Success<typeof awaitFailure>>();
  const failedBeforeForcedStopDeferred = yield* Deferred.make<boolean>();
  const forceStopWith = (failure: Effect.Success<typeof awaitFailure>) => {
    const failedBeforeForcedStop = Deferred.isDoneUnsafe(firstFailureDeferred);
    if (Deferred.doneUnsafe(forcedStopDeferred, Exit.succeed(failure))) {
      Deferred.doneUnsafe(failedBeforeForcedStopDeferred, Exit.succeed(failedBeforeForcedStop));
    }
  };
  const printedFailures = new Set<Effect.Success<typeof awaitFailure>>();
  const failureCauseByTaskName = new Map<
    string,
    Cause.Cause<Effect.Success<typeof awaitFailure>>
  >();
  const taskLogHold = makeTaskLogHold();
  return {
    taskLogHold,
    holdTaskLogsDuring: holdTaskLogsDuring(taskLogHold),
    recordTaskFailureCause: (
      taskName: string,
      cause: Cause.Cause<Effect.Success<typeof awaitFailure>>,
    ) =>
      Effect.sync(() => {
        if (!failureCauseByTaskName.has(taskName)) {
          failureCauseByTaskName.set(taskName, cause);
        }
      }),
    formatRunFailureReasonLine: (runEndingCause: Cause.Cause<unknown>) =>
      Effect.try({
        try: () => {
          const reason = findFailureReason(runEndingCause, failureCauseByTaskName);
          return reason === null
            ? FAILURE_REASON_NOT_RECORDED_LINE
            : formatFailureReasonLine(reason);
        },
        catch: (cause) => new FailureReasonNotFormatted({ cause }),
      }).pipe(
        Effect.catchTag("FailureReasonNotFormatted", (failure) =>
          Effect.succeed(`${failure._tag}: ${failure.message}`),
        ),
      ),
    recordFirstFailure: (failure: Effect.Success<typeof awaitFailure>) =>
      Deferred.succeed(firstFailureDeferred, failure),
    recordPrintedFailure: (failure: Effect.Success<typeof awaitFailure>) =>
      Effect.sync(() => {
        printedFailures.add(failure);
      }),
    wasPrinted: (failure: Effect.Success<typeof awaitFailure>) =>
      Effect.sync(() => printedFailures.has(failure)),
    requestStop: (signal: (typeof STOP_REASONS)[number]) =>
      Effect.sync(() => {
        const stopRequested = new StopRequested({ signal });
        const stopBecameFirstFailure =
          !Deferred.isDoneUnsafe(shutdownStartedDeferred) &&
          Deferred.doneUnsafe(firstFailureDeferred, Exit.succeed(stopRequested));
        if (stopBecameFirstFailure) {
          return true;
        }
        if (signal !== "SIGHUP") {
          forceStopWith(stopRequested);
        }
        return false;
      }),
    forceStop: (failure: Effect.Success<typeof awaitFailure>) =>
      Effect.sync(() => {
        forceStopWith(failure);
        Deferred.doneUnsafe(firstFailureDeferred, Exit.succeed(failure));
      }),
    begin: Effect.sync(() => {
      Deferred.doneUnsafe(shutdownStartedDeferred, Exit.void);
      return Deferred.isDoneUnsafe(firstFailureDeferred);
    }),
    isStopping: Effect.sync(
      () =>
        Deferred.isDoneUnsafe(shutdownStartedDeferred) ||
        Deferred.isDoneUnsafe(firstFailureDeferred),
    ),
    hasFailed: Deferred.isDone(firstFailureDeferred),
    awaitFailure,
    awaitForcedStop: Deferred.await(forcedStopDeferred),
    isForced: Deferred.isDone(forcedStopDeferred),
    outcome: Effect.gen(function* () {
      const firstFailure = Deferred.isDoneUnsafe(firstFailureDeferred)
        ? yield* Deferred.await(firstFailureDeferred)
        : null;
      if (Deferred.isDoneUnsafe(forcedStopDeferred)) {
        const forcedStop = yield* Deferred.await(forcedStopDeferred);
        const isSignalAfterTaskFailure =
          isStopRequested(forcedStop) &&
          (yield* Deferred.await(failedBeforeForcedStopDeferred)) &&
          firstFailure !== null &&
          !isStopRequested(firstFailure);
        return isSignalAfterTaskFailure ? firstFailure : forcedStop;
      }
      return firstFailure;
    }),
  };
});

export class StopState extends Context.Service<StopState, Effect.Success<typeof makeShutdown>>()(
  `${TASK_PACKAGE.name}/StopState`,
) {}

export const StopStateLive = Layer.effect(StopState, makeShutdown);
