import { Effect, Predicate, SchemaIssue } from "effect";

import { findTasksMadeWithName } from "../../../shared/record-tasks-made.ts";
import { TaskOutputInvalid } from "../../errors/index.ts";
import type { Task } from "../../task/index.ts";
import { formatPropertyAccess } from "./format-property-access.ts";

const formatIssue = SchemaIssue.makeFormatterStandardSchemaV1();

const VALUE_GOT = /, got .*$/su;

export const encodeTaskOutput = (
  taskName: string,
  handedOver: TaskOutputInvalid["handedOver"],
  value: Parameters<Extract<Task["output"], { readonly isDeclared: true }>["encode"]>[0],
) =>
  Effect.gen(function* () {
    const output = findTasksMadeWithName(taskName).at(0)?.output;
    const isDropped = output === undefined || !output.isDeclared;
    if (isDropped) {
      return;
    }
    return yield* output.encode(value).pipe(
      Effect.catchTag("SchemaError", (cause) => {
        const firstIssue = formatIssue(cause.issue).issues.at(0);
        return Effect.fail(
          new TaskOutputInvalid({
            taskName,
            handedOver,
            valuePath: (firstIssue?.path ?? [])
              .map((pathPart) =>
                formatPropertyAccess(
                  Predicate.hasProperty(pathPart, "key") ? pathPart.key : pathPart,
                ),
              )
              .join(""),
            expectation: (firstIssue?.message ?? "").replace(VALUE_GOT, ""),
          }),
        );
      }),
    );
  });
