import { Effect } from "effect";

import { TestCleanupFailed } from "./test-errors.ts";

export const failTestCleanup =
  (operation: string) =>
  <Success, Failure, Services>(effect: Effect.Effect<Success, Failure, Services>) =>
    effect.pipe(
      Effect.matchEffect({
        onFailure: (error) => Effect.die(new TestCleanupFailed({ operation, cause: error })),
        onSuccess: (success) => Effect.succeed(success),
      }),
    );
