import { Context } from "effect";
import type { Effect, Schema } from "effect";

import type { TaskResultNotJson } from "../lib/errors/index.ts";
import type { Task } from "../lib/task/index.ts";
import type { TaskRequestFailure } from "./task-errors.ts";
import { TASK_PACKAGE } from "./task-package.ts";

export class Tasks extends Context.Service<
  Tasks,
  {
    readonly requestResult: (
      task: Task,
    ) => Effect.Effect<Schema.Json | undefined, typeof TaskRequestFailure.Type>;
  }
>()(`${TASK_PACKAGE.name}/Tasks`) {}

export class Served<Success> {
  protected readonly servedValue: Success | undefined = undefined;
  private constructor() {}
}

export class Serving extends Context.Service<
  Serving,
  {
    readonly serve: <Success>(
      servedValue: Success,
    ) => Effect.Effect<Served<Success>, TaskResultNotJson>;
  }
>()(`${TASK_PACKAGE.name}/Serving`) {}
