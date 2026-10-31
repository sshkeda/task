import { Cause, Clock, Duration, Effect, Exit, Fiber, Option, Ref } from "effect";

import { TaskSetupTimedOut } from "../../../../../../../shared/task-errors.ts";
import { failRun } from "../../../../../shared/fail-run.ts";
import { TaskCleanupTimedOut } from "../../../../../shared/main-thread-errors.ts";
import { RunRootTaskConfig } from "../../../../../shared/provide-run-root-task-config.ts";
import type { makeTaskRunsState } from "../../../shared/define-task-run-state.ts";
import { awaitWithinCountedLimit } from "./await-within-counted-limit.ts";

export const makeTaskSetupClock = Effect.fn("makeTaskSetupClock")(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  taskName: string,
  cleanupDuration: Duration.Duration,
) {
  const limitMilliseconds = Duration.toMillis((yield* RunRootTaskConfig).setupLimit);
  const setupStartedAtMilliseconds = yield* Clock.currentTimeMillis;
  const workerAssignment = yield* Ref.make(
    Option.none<{
      readonly workerId: number;
      readonly busyMillisecondsAtAssignment: number;
    }>(),
  );
  const countSetupMilliseconds = (nowMilliseconds: number) =>
    Effect.map(Ref.get(workerAssignment), (assignedWorker) => {
      const workerBusyMilliseconds = Option.match(assignedWorker, {
        onNone: () => 0,
        onSome: (worker) =>
          taskRunsState.readWorkerBodyBusyMilliseconds(worker.workerId, nowMilliseconds) -
          worker.busyMillisecondsAtAssignment,
      });
      return nowMilliseconds - setupStartedAtMilliseconds - workerBusyMilliseconds;
    });
  const interruptSetupKeepingFailures = <Success, Failure>(
    setupFiber: Fiber.Fiber<Success, Failure>,
  ) =>
    Effect.gen(function* () {
      const cleanup = Fiber.interrupt(setupFiber).pipe(Effect.andThen(Fiber.await(setupFiber)));
      const interruptedSetupExit = yield* cleanup.pipe(
        Effect.interruptible,
        Effect.timeoutOrElse({
          duration: cleanupDuration,
          orElse: () =>
            Effect.succeed(
              Exit.fail<Failure | TaskCleanupTimedOut>(
                new TaskCleanupTimedOut({
                  taskName,
                  cleanupMilliseconds: Duration.toMillis(cleanupDuration),
                }),
              ),
            ),
        }),
      );
      const failuresWhileInterrupted = Exit.isFailure(interruptedSetupExit)
        ? interruptedSetupExit.cause.reasons.filter((reason) => !Cause.isInterruptReason(reason))
        : [];
      const failures = Cause.fromReasons(failuresWhileInterrupted);
      if (failures.reasons.length > 0) {
        return yield* Effect.failCause(failures);
      }
    });
  const awaitSetupWithinLimit = <Success, Failure>(
    setupFiber: Fiber.Fiber<Success, Failure>,
    describeUnfinishedStep: () => string,
  ) =>
    Effect.gen(function* () {
      const setupExit = yield* awaitWithinCountedLimit(
        Fiber.await(setupFiber),
        limitMilliseconds,
        countSetupMilliseconds,
      );
      if (Option.isSome(setupExit)) {
        return setupExit.value;
      }
      yield* failRun(
        new TaskSetupTimedOut({
          taskName,
          limitMilliseconds,
          unfinishedStep: describeUnfinishedStep(),
        }),
        taskRunsState.markFailed,
      );
      return null;
    });
  return {
    recordWorkerAssignment: (workerId: number) =>
      Effect.flatMap(Clock.currentTimeMillis, (nowMilliseconds) =>
        Ref.set(
          workerAssignment,
          Option.some({
            workerId,
            busyMillisecondsAtAssignment: taskRunsState.readWorkerBodyBusyMilliseconds(
              workerId,
              nowMilliseconds,
            ),
          }),
        ),
      ),
    limit: <Success, Failure, Services>(
      setup: Effect.Effect<Success, Failure, Services>,
      describeUnfinishedStep: () => string,
    ) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const context = yield* Effect.context<Services>();
          const hasReadSetupExit = yield* Ref.make(false);
          return yield* Effect.acquireUseRelease(
            Effect.sync(() =>
              Effect.runForkWith(context)(restore(setup), { uninterruptible: true }),
            ),
            (setupFiber) =>
              Effect.interruptible(awaitSetupWithinLimit(setupFiber, describeUnfinishedStep)).pipe(
                Effect.flatMap((setupExit) => {
                  if (setupExit === null) {
                    return Effect.interrupt;
                  }
                  return Ref.set(hasReadSetupExit, true).pipe(Effect.andThen(setupExit));
                }),
              ),
            (setupFiber) =>
              Ref.get(hasReadSetupExit).pipe(
                Effect.flatMap((hasReadExit) =>
                  hasReadExit ? Effect.void : interruptSetupKeepingFailures(setupFiber),
                ),
              ),
          );
        }),
      ),
  };
});
