import { Effect, Effectable } from "effect";
import type { FileSystem, Path, Scope, Stdio, Terminal } from "effect";
import type { Digest, SecureRandom } from "effect/crypto";
import type { HttpClient } from "effect/http";
import type { ChildProcessSpawner } from "effect/process";

import { recordTaskMade } from "../../../shared/record-tasks-made.ts";
import { resolveTaskOptions } from "../../../shared/resolve-task-options.ts";
import type { TaskRequestFailure } from "../../../shared/task-errors.ts";
import { TASK_PACKAGE } from "../../../shared/task-package.ts";
import type { TaskResult } from "../../../shared/task-results.ts";
import { Tasks } from "../../../shared/task-services.ts";
import type { Served, Serving } from "../../../shared/task-services.ts";
import { trustTaskResult } from "./trust-task-results.ts";

const TYPE_ID = Symbol.for(`~${TASK_PACKAGE.name}/Task`);

export class Task<
  Success = unknown,
  Services extends Effect.Services<Task["body"]> =
    | ChildProcessSpawner.ChildProcessSpawner
    | Digest.Digest
    | FileSystem.FileSystem
    | HttpClient.HttpClient
    | Path.Path
    | Scope.Scope
    | SecureRandom.SecureRandom
    | Serving
    | Stdio.Stdio
    | Tasks
    | Terminal.Terminal,
> extends Effectable.Class<Success, typeof TaskRequestFailure.Type, Tasks> {
  public readonly [TYPE_ID] = TYPE_ID;

  public readonly name: string;

  public readonly body: Effect.Effect<unknown, { readonly _tag: string }, Services>;

  public readonly options: ReturnType<typeof resolveTaskOptions>;

  public constructor(
    name: string,
    body: Effect.Effect<unknown, { readonly _tag: string }, Services>,
    options: ReturnType<typeof resolveTaskOptions>,
  ) {
    super();
    this.name = name;
    this.body = body;
    this.options = options;
    recordTaskMade(this);
  }

  public asEffect() {
    return Tasks.use((tasks) => tasks.requestResult(this)).pipe(
      Effect.flatMap((taskResult) => trustTaskResult<Success>(taskResult)),
    );
  }
}

export const make = <
  BodySuccess,
  Failure extends { readonly _tag: string },
  Services extends Effect.Services<Task["body"]>,
>(
  name: string,
  body: Effect.Effect<BodySuccess, Failure, Services> &
    NoInfer<
      Effect.Effect<
        [Serving extends Services ? Extract<BodySuccess, Served<unknown>> : true] extends [never]
          ? "serves one value and returns another"
          : BodySuccess extends Served<infer Success>
            ? Served<Success extends void ? Success : typeof TaskResult.Type>
            : BodySuccess extends void
              ? BodySuccess
              : typeof TaskResult.Type,
        Failure,
        Services
      >
    >,
  options: Parameters<typeof resolveTaskOptions>[0] = {},
): Task<BodySuccess extends Served<infer Success> ? Success : BodySuccess, Services> =>
  new Task(name, body, resolveTaskOptions(options));
