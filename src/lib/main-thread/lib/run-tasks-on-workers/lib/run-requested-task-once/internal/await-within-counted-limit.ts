import { Clock, Duration, Effect, Option } from "effect";

const SHORTEST_RECHECK_SHARE_OF_LIMIT = 0.05;

export const awaitWithinCountedLimit = <Success, Failure, Services>(
  awaiting: Effect.Effect<Success, Failure, Services>,
  limitMilliseconds: number,
  countMilliseconds: (nowMilliseconds: number) => Effect.Effect<number>,
): Effect.Effect<Option.Option<Success>, Failure, Services> =>
  Effect.gen(function* () {
    const remainingMilliseconds =
      limitMilliseconds - (yield* countMilliseconds(yield* Clock.currentTimeMillis));
    if (remainingMilliseconds <= 0) {
      return Option.none();
    }
    const exit = yield* Effect.exit(awaiting).pipe(
      Effect.timeoutOption(
        Duration.millis(
          Math.max(remainingMilliseconds, limitMilliseconds * SHORTEST_RECHECK_SHARE_OF_LIMIT),
        ),
      ),
    );
    if (Option.isNone(exit)) {
      return yield* awaitWithinCountedLimit(awaiting, limitMilliseconds, countMilliseconds);
    }
    const arrivedPastLimit =
      (yield* countMilliseconds(yield* Clock.currentTimeMillis)) > limitMilliseconds;
    return arrivedPastLimit ? Option.none() : Option.some(yield* exit.value);
  });
