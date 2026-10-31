import { Effect, Schema } from "effect";

import { InternalInvariantBroken } from "../../../shared/invariant-errors.ts";
import {
  holdsRedactedTaskResultKey,
  reviveRedactedTaskResults,
  TaskResult,
} from "../../../shared/task-results.ts";

export const trustTaskResult = <Success>(taskResult: Schema.Json | undefined) => {
  const isBranch = taskResult !== null && typeof taskResult === "object";
  const holdsRedacted = isBranch && holdsRedactedTaskResultKey(taskResult);
  return (
    holdsRedacted
      ? reviveRedactedTaskResults(taskResult)
      : Effect.succeed<typeof TaskResult.Type | undefined>(taskResult)
  ).pipe(
    Effect.flatMap((revived) =>
      Schema.decodeUnknownEffect(
        Schema.declare((candidate): candidate is Success =>
          Schema.is(Schema.UndefinedOr(Schema.Union([Schema.Json, TaskResult])))(candidate),
        ),
      )(revived),
    ),
    Effect.catchTag("SchemaError", (cause) =>
      Effect.die(
        new InternalInvariantBroken({
          area: "task results",
          invariant: "a task's result is the JSON or Redacted strings its body returned",
          cause,
        }),
      ),
    ),
  );
};
