import { Effect, Layer } from "effect";

import { StopState } from "../shared/provide-stop-state.ts";

export const stopsAtFirstFailure = <Services, Error, Requirements>(
  layer: Layer.Layer<Services, Error, Requirements>,
) =>
  Layer.effectContext(
    Effect.gen(function* () {
      const shutdown = yield* StopState;
      if (yield* shutdown.hasFailed) {
        const failure = yield* shutdown.awaitFailure;
        return yield* failure;
      }
      return yield* Layer.build(layer).pipe(
        Effect.raceFirst(shutdown.awaitFailure.pipe(Effect.flatMap(Effect.fail))),
        Effect.interruptible,
      );
    }),
  );
