import { Effect, Fiber } from "effect";

export const runInChildFiberAndJoin = <Success, Failure, Services>(
  effect: Effect.Effect<Success, Failure, Services>,
) =>
  Effect.uninterruptibleMask((restore) =>
    Effect.flatMap(
      Effect.forkChild(restore(effect), { startImmediately: true, uninterruptible: true }),
      (childFiber) =>
        Effect.interruptible(Fiber.join(childFiber)).pipe(
          Effect.onInterrupt(() =>
            Effect.andThen(Fiber.interrupt(childFiber), Fiber.join(childFiber)),
          ),
        ),
    ),
  );
