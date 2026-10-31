import { Effect } from "effect";

import { SucceededInsteadOfFailing } from "./test-errors.ts";

export const failureAsValue = <Success, Failure, Services>(
  effect: Effect.Effect<Success, Failure, Services>,
) => Effect.flip(effect).pipe(Effect.mapError((value) => new SucceededInsteadOfFailing({ value })));

export const exitAsValue = <Success, Failure, Services>(
  effect: Effect.Effect<Success, Failure, Services>,
) => Effect.exit(effect);
