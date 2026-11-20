import { Effect, Schema } from "effect";

import { TASK_PACKAGE } from "./task-package.ts";

export const UNATTRIBUTED_FAILURE_FIELDS = {
  taskName: Schema.Null.pipe(Schema.withConstructorDefault(Effect.succeed(null))),
};

export const TASK_ERROR_FIELDS = {
  raisedBy: Schema.Literal(TASK_PACKAGE.name).pipe(
    Schema.withConstructorDefault(Effect.succeed(TASK_PACKAGE.name)),
  ),
};

export const PRINTABLE_ERROR_FIELDS = {
  tag: Schema.String,
  raisedBy: Schema.optionalKey(Schema.Literal(TASK_PACKAGE.name)),
  location: Schema.optional(Schema.String),
  taskName: Schema.optionalKey(Schema.String),
  message: Schema.String,
  context: Schema.optionalKey(Schema.Array(Schema.String)),
  outputLines: Schema.optionalKey(Schema.Array(Schema.String)),
};
