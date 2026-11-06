import { Effect, Fiber } from "effect";
import type { Scope } from "effect";

export const completeOnceSent =
  (scope: Scope.Scope) =>
  <Success, Failure, Services>(discardedCall: Effect.Effect<Success, Failure, Services>) =>
    discardedCall.pipe(
      Effect.forkIn(scope, { startImmediately: true }),
      Effect.flatMap((callFiber) =>
        callFiber.pollUnsafe() === undefined ? Effect.void : Fiber.join(callFiber),
      ),
    );
