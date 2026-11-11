import { Schema } from "effect";

import { TASK_PACKAGE } from "./task-package.ts";

export const TASK_GROUP_KEY = `~${TASK_PACKAGE.name}/TaskGroup` as const;

export const TaskGroup = Schema.Struct({
  mode: Schema.Literals(["all", "allSettled"]),
  taskNames: Schema.Array(Schema.String),
  concurrency: Schema.Union([Schema.Finite, Schema.Literal("unbounded")]),
});

export const TaskListNotRecorded = Schema.Struct({
  mode: TaskGroup.fields.mode,
  given: Schema.String,
});

export const RootTaskGroup = Schema.Struct({ name: Schema.String, ...TaskGroup.fields });
