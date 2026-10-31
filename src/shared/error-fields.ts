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
