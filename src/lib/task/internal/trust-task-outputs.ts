import { Effect, Schema } from "effect";

import { InternalInvariantBroken } from "../../../shared/invariant-errors.ts";

export const trustTaskOutput = <Success>(decoding: Effect.Effect<unknown, Schema.SchemaError>) =>
  decoding.pipe(
    Effect.flatMap(
      Schema.decodeUnknownEffect(
        Schema.declare((candidate): candidate is Success => Schema.is(Schema.Unknown)(candidate)),
      ),
    ),
    Effect.catchTag("SchemaError", () =>
      Effect.die(
        new InternalInvariantBroken({
          area: "task results",
          invariant: "a task's result is what its output schema encoded",
        }),
      ),
    ),
  );
