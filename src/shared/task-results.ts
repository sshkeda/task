import { Array as Arrays, Effect, Predicate, Redacted, Schema } from "effect";

export const REDACTED_TASK_RESULT_KEY = "~@sshkeda/task/Redacted";

export const TaskResult = Schema.Tree(
  Schema.Union([
    Schema.Null,
    Schema.Finite,
    Schema.Boolean,
    Schema.String,
    Schema.Redacted(Schema.String),
  ]),
);

export const holdsRedactedTaskResultKey = (root: Schema.JsonArray | Schema.JsonObject) => {
  const pendingBranches = [root];
  for (const branch of pendingBranches) {
    if (Object.hasOwn(branch, REDACTED_TASK_RESULT_KEY)) {
      return true;
    }
    const children = Arrays.isArray<Schema.JsonArray | Schema.JsonObject>(branch)
      ? branch
      : Object.values(branch);
    for (const child of children) {
      const isBranch = child !== null && typeof child === "object";
      if (isBranch) {
        pendingBranches.push(child);
      }
    }
  }
  return false;
};

export const reviveRedactedTaskResults = (
  value: Schema.Json,
): Effect.Effect<typeof TaskResult.Type> => {
  const isLeaf = value === null || typeof value !== "object";
  if (isLeaf) {
    return Effect.succeed(value);
  }
  return Effect.gen(function* () {
    if (Arrays.isArray<Schema.JsonArray | Schema.JsonObject>(value)) {
      return yield* Effect.forEach(value, reviveRedactedTaskResults);
    }
    const tagged = value[REDACTED_TASK_RESULT_KEY];
    if (Predicate.isString(tagged)) {
      return Redacted.make(tagged);
    }
    return Object.fromEntries(
      yield* Effect.forEach(Object.entries(value), ([key, child]) =>
        Effect.map(reviveRedactedTaskResults(child), (revived) => [key, revived] as const),
      ),
    );
  });
};
