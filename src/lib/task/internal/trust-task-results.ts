import { Array as Arrays, Effect, Schema } from "effect";

import { InternalInvariantBroken } from "../../../shared/invariant-errors.ts";
import {
  holdsRedactedTaskResultKey,
  reviveRedactedTaskResults,
  TaskResult,
} from "../../../shared/task-results.ts";

const isJsonOrUndefined = Schema.is(Schema.UndefinedOr(Schema.Json));

const [TaskResultLeaf] = TaskResult.members;

const isTaskResultLeaf = Schema.is(TaskResultLeaf);

const isTaskResultBranch = Schema.is(
  Schema.Union([Schema.Array(Schema.Unknown), Schema.Record(Schema.String, Schema.Unknown)]),
);

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
        Schema.declare((candidate): candidate is Success => {
          if (isJsonOrUndefined(candidate)) {
            return true;
          }
          const pendingParts = Array.of<unknown>(candidate);
          for (const part of pendingParts) {
            if (!isTaskResultLeaf(part)) {
              if (!isTaskResultBranch(part)) {
                return false;
              }
              for (const child of Arrays.isArray(part) ? part : Object.values(part)) {
                pendingParts.push(child);
              }
            }
          }
          return true;
        }),
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
