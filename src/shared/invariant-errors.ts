import { Effect, Schema } from "effect";

import { TASK_ERROR_FIELDS } from "./error-fields.ts";

export class InternalInvariantBroken extends Schema.TaggedError<InternalInvariantBroken>()(
  "InternalInvariantBroken",
  {
    ...TASK_ERROR_FIELDS,
    area: Schema.String,
    invariant: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  public override get message() {
    return `${this.area} broke the internal invariant that ${this.invariant}`;
  }
}

export const dieOnInvariantFailure =
  (area: string, invariant: string) =>
  <Success, Failure, Services>(effect: Effect.Effect<Success, Failure, Services>) =>
    effect.pipe(
      Effect.matchEffect({
        onFailure: (error) =>
          Effect.die(new InternalInvariantBroken({ area, invariant, cause: error })),
        onSuccess: (success) => Effect.succeed(success),
      }),
    );
