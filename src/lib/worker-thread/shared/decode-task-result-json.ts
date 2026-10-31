import { Array as Arrays, Effect, Formatter, Option, Redacted, Schema } from "effect";

import {
  holdsRedactedTaskResultKey,
  REDACTED_TASK_RESULT_KEY,
} from "../../../shared/task-results.ts";
import { TaskResultNotJson } from "../../errors/index.ts";
import { describeIssueInput, formatPropertyAccess, REPORT_INPUT } from "./describe-issue-input.ts";
import { PlainObject } from "./plain-object.ts";

export const OptionalJson = Schema.UndefinedOr(Schema.Json);

const TaskResultPart = Schema.Union([
  Schema.Null,
  Schema.String,
  Schema.Boolean,
  Schema.Finite,
  Schema.Redacted(Schema.String),
  Schema.Array(Schema.Unknown),
  PlainObject,
]);

const isJsonLeaf = Schema.is(
  Schema.Union([Schema.Null, Schema.String, Schema.Boolean, Schema.Finite]),
);

const isRedactedString = Schema.is(Schema.Redacted(Schema.String));

const decodeTaskResultPart = Schema.decodeUnknownEffect(TaskResultPart, REPORT_INPUT);

const encodeTaskResultParts = (
  notJson: (valuePath: string, valueDescription: string) => TaskResultNotJson,
  decodingPart: ReturnType<typeof decodeTaskResultPart>,
  valuePath: string,
  onPath: WeakSet<WeakKey>,
): Effect.Effect<Schema.Json, TaskResultNotJson> =>
  Effect.gen(function* () {
    const part = yield* decodingPart;
    const isLeaf = part === null || typeof part !== "object";
    if (isLeaf) {
      return part;
    }
    if (isRedactedString(part)) {
      return { [REDACTED_TASK_RESULT_KEY]: Redacted.value(part) };
    }
    if (onPath.has(part)) {
      return yield* notJson(valuePath, "a reference back to an enclosing value");
    }
    if (Object.hasOwn(part, REDACTED_TASK_RESULT_KEY)) {
      return yield* notJson(
        valuePath,
        `an object with the reserved key ${Formatter.format(REDACTED_TASK_RESULT_KEY)}`,
      );
    }
    onPath.add(part);
    const encoded = Arrays.isArray(part)
      ? yield* Effect.forEach(part, (element, index) =>
          isJsonLeaf(element)
            ? Effect.succeed(element)
            : encodeTaskResultParts(
                notJson,
                decodeTaskResultPart(element),
                `${valuePath}${formatPropertyAccess(index)}`,
                onPath,
              ),
        )
      : Object.fromEntries(
          yield* Effect.forEach(Object.entries(part), ([key, entry]) =>
            Effect.map(
              isJsonLeaf(entry)
                ? Effect.succeed(entry)
                : encodeTaskResultParts(
                    notJson,
                    decodeTaskResultPart(entry),
                    `${valuePath}${formatPropertyAccess(key)}`,
                    onPath,
                  ),
              (encodedEntry) => [key, encodedEntry] as const,
            ),
          ),
        );
    onPath.delete(part);
    return encoded;
  }).pipe(
    Effect.catchTag("SchemaError", (cause) =>
      Effect.fail(notJson(valuePath, describeIssueInput(cause.issue))),
    ),
  );

export const decodeTaskResultJson = (
  taskName: string,
  handedOver: TaskResultNotJson["handedOver"],
  value: unknown,
) =>
  Effect.gen(function* () {
    const json = yield* Schema.decodeUnknownEffect(OptionalJson)(value).pipe(
      Effect.asSome,
      Effect.catchTag("SchemaError", () => Effect.succeedNone),
    );
    const jsonWithoutReservedKey = Option.filter(json, (candidate) => {
      const isBranch = candidate !== null && typeof candidate === "object";
      return !isBranch || !holdsRedactedTaskResultKey(candidate);
    });
    if (Option.isSome(jsonWithoutReservedKey)) {
      return jsonWithoutReservedKey.value;
    }
    return yield* encodeTaskResultParts(
      (valuePath, valueDescription) =>
        new TaskResultNotJson({ taskName, handedOver, valuePath, valueDescription }),
      Schema.decodeUnknownEffect(TaskResultPart, REPORT_INPUT)(value),
      "",
      new WeakSet(),
    );
  });
