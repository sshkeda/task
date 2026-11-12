import { Cause, Clock, Duration, Effect, Exit, Fiber, Option, Ref } from "effect";

import { TaskSetupTimedOut } from "../../../../../../../shared/task-errors.ts";
import { failRun } from "../../../../../shared/fail-run.ts";
import { TaskCleanupTimedOut } from "../../../../../shared/main-thread-errors.ts";
import { RunRootTaskConfig } from "../../../../../shared/provide-run-root-task-config.ts";
import type { StopState } from "../../../../../shared/provide-stop-state.ts";
import { WorkerPool } from "../../../../../shared/provide-worker-pool.ts";
import { findTaskNamesOccupyingWorker } from "../../../shared/define-task-run-state.ts";
import type { makeTaskRunsState } from "../../../shared/define-task-run-state.ts";
import { isInBody } from "../../../shared/define-task-run-status.ts";
import { awaitWithinCountedLimit } from "./await-within-counted-limit.ts";

const WORKER_ANSWERS_WITHIN_MILLISECONDS = 200;

const WORKER_ANSWERS_WITHIN = Duration.millis(WORKER_ANSWERS_WITHIN_MILLISECONDS);

export const makeTaskSetupClock = Effect.fn("makeTaskSetupClock")(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  taskName: string,
  cleanupDuration: Duration.Duration,
) {
  const limitMilliseconds = Duration.toMillis((yield* RunRootTaskConfig).setupLimit);
  const workerPool = yield* WorkerPool;
  const assignedWorkerId = yield* Ref.make(Option.none<number>());
  const setupStartedAtMilliseconds = yield* Ref.make<number | null>(null);
  const workerLastAnsweredAtMilliseconds = yield* Ref.make(0);
  const workerHeldMilliseconds = yield* Ref.make(0);
  const countSetupMilliseconds = (nowMilliseconds: number) =>
    Effect.gen(function* () {
      const startedAtMilliseconds = yield* Ref.get(setupStartedAtMilliseconds);
      return startedAtMilliseconds === null
        ? 0
        : nowMilliseconds - startedAtMilliseconds - (yield* Ref.get(workerHeldMilliseconds));
    });
  const isWorkerHeldByOtherTasks = (workerId: number) =>
    Effect.gen(function* () {
      const isOccupiedByOtherTasks = findTaskNamesOccupyingWorker(
        taskRunsState.runsByTaskName,
        workerId,
      ).some((occupyingTaskName) => {
        const occupyingTaskRun = taskRunsState.runsByTaskName.get(occupyingTaskName);
        const hasStartedBody = occupyingTaskRun !== undefined && isInBody(occupyingTaskRun.status);
        return occupyingTaskName !== taskName && hasStartedBody;
      });
      if (!isOccupiedByOtherTasks) {
        return false;
      }
      const answer = yield* workerPool
        .awaitAnswer(workerId)
        .pipe(Effect.timeoutOption(WORKER_ANSWERS_WITHIN));
      return Option.isNone(answer);
    });
  const awaitWorkerHeldByOtherTasks = <Success, Failure>(
    setupFiber: Fiber.Fiber<Success, Failure>,
  ) =>
    Effect.gen(function* () {
      const workerId = yield* Ref.get(assignedWorkerId);
      if (Option.isNone(workerId)) {
        return false;
      }
      if (!(yield* isWorkerHeldByOtherTasks(workerId.value))) {
        return false;
      }
      yield* workerPool
        .awaitAnswer(workerId.value)
        .pipe(Effect.asVoid, Effect.raceFirst(Effect.asVoid(Fiber.await(setupFiber))));
      const answeredAtMilliseconds = yield* Clock.currentTimeMillis;
      const lastAnsweredAtMilliseconds = yield* Ref.getAndSet(
        workerLastAnsweredAtMilliseconds,
        answeredAtMilliseconds,
      );
      yield* Ref.update(
        workerHeldMilliseconds,
        (heldMilliseconds) =>
          heldMilliseconds + answeredAtMilliseconds - lastAnsweredAtMilliseconds,
      );
      return true;
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
  ): Effect.Effect<Exit.Exit<Success, Failure> | null, never, RunRootTaskConfig | StopState> =>
    Effect.gen(function* () {
      const setupExit = yield* awaitWithinCountedLimit(
        Fiber.await(setupFiber),
        limitMilliseconds,
        countSetupMilliseconds,
      );
      if (Option.isSome(setupExit)) {
        return setupExit.value;
      }
      if (yield* awaitWorkerHeldByOtherTasks(setupFiber)) {
        return yield* awaitSetupWithinLimit(setupFiber, describeUnfinishedStep);
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
      Ref.set(assignedWorkerId, Option.some(workerId)).pipe(
        Effect.andThen(Clock.currentTimeMillis),
        Effect.flatMap((nowMilliseconds) =>
          Effect.andThen(
            Ref.set(workerLastAnsweredAtMilliseconds, nowMilliseconds),
            Ref.set(setupStartedAtMilliseconds, nowMilliseconds),
          ),
        ),
      ),
    isWorkerHeldByOtherTasks,
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
