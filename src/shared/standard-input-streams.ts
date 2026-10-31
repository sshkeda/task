import { Cause, Deferred, Effect, Exit } from "effect";
import type { PlatformError } from "effect";

import { runInChildFiberAndJoin } from "./run-in-child-fiber-and-join.ts";

const failuresOfStandardInputClosedByCommand = new WeakSet<PlatformError.PlatformError>();

export const markStandardInputClosedByCommand = (failure: PlatformError.PlatformError) => {
  failuresOfStandardInputClosedByCommand.add(failure);
  return failure;
};

export const isStandardInputClosedByCommand = (failure: PlatformError.PlatformError) =>
  failuresOfStandardInputClosedByCommand.has(failure);

export const makeStandardInputRunner = () => {
  const interruptedItselfDeferred = Deferred.makeUnsafe<void>();
  return {
    run: <Success, Failure, Services>(
      standardInputRun: Effect.Effect<Success, Failure, Services>,
    ) =>
      Effect.withFiber((runningFiber) =>
        runInChildFiberAndJoin(
          Effect.onExit(standardInputRun, (standardInputExit) =>
            Effect.sync(() => {
              const interruptedByItsOwnFibers =
                Exit.isFailure(standardInputExit) &&
                Cause.hasInterruptsOnly(standardInputExit.cause) &&
                standardInputExit.cause.reasons.some(
                  (reason) => Cause.isInterruptReason(reason) && reason.fiberId !== runningFiber.id,
                );
              if (interruptedByItsOwnFibers) {
                Deferred.doneUnsafe(interruptedItselfDeferred, Exit.void);
              }
            }),
          ),
        ),
      ),
    interruptedItself: () => Deferred.isDoneUnsafe(interruptedItselfDeferred),
  };
};
