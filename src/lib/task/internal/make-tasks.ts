import { Effect, Effectable, Formatter, Predicate } from "effect";
import type { Duration, FileSystem, Path, Schema, Scope, Stdio, Terminal } from "effect";
import type { Digest, SecureRandom } from "effect/crypto";
import type { HttpClient } from "effect/http";
import type { ChildProcessSpawner } from "effect/process";

import { findCallingLocation } from "../../../shared/find-user-locations-in-stack-traces.ts";
import { InternalInvariantBroken } from "../../../shared/invariant-errors.ts";
import { recordDefinitionNotGiven, recordTaskMade } from "../../../shared/record-tasks-made.ts";
import { resolveTaskOptions } from "../../../shared/resolve-task-options.ts";
import { TASK_BODY } from "../../../shared/task-body-key.ts";
import { TaskDefinitionNotGiven } from "../../../shared/task-errors.ts";
import type { TaskRequestFailure } from "../../../shared/task-errors.ts";
import { TASK_PACKAGE } from "../../../shared/task-package.ts";
import { Tasks } from "../../../shared/task-services.ts";
import type { Served, Serving } from "../../../shared/task-services.ts";
import { declareTaskOutput, NO_TASK_OUTPUT } from "./declare-task-outputs.ts";
import { DESCRIPTION_OF_TYPE } from "./describe-given-values.ts";
import { readTaskFlags } from "./read-command-line-flags.ts";
import { startTheDefaultExport } from "./run-root-task.ts";
import { trustTaskOutput } from "./trust-task-outputs.ts";

const TASK_TYPE_ID = Symbol.for(`~${TASK_PACKAGE.name}/Task`);

const DESCRIPTION_OF_GIVEN = {
  ...DESCRIPTION_OF_TYPE,
  array: "an array",
} as const;

const RUN_VALUE_NEEDS_OUTPUT =
  "a run that returns a value needs an output schema, or Effect.asVoid";

export class Task<
  Success = unknown,
  Services extends Effect.Services<Task[typeof TASK_BODY]> =
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
  public readonly [TASK_TYPE_ID] = TASK_TYPE_ID;

  public readonly name: string;

  public readonly [TASK_BODY]: Effect.Effect<unknown, { readonly _tag: string }, Services>;

  public readonly options: ReturnType<typeof resolveTaskOptions>;

  public readonly output: ReturnType<typeof declareTaskOutput> | typeof NO_TASK_OUTPUT;

  public readonly flags:
    | (Schema.ConstraintDecoder<Schema.Struct.Type<Schema.Struct.Fields>> &
        Schema.Struct<Schema.Struct.Fields>)
    | null;

  public constructor(
    name: string,
    body: Effect.Effect<unknown, { readonly _tag: string }, Services>,
    options: ReturnType<typeof resolveTaskOptions>,
    output: ReturnType<typeof declareTaskOutput> | typeof NO_TASK_OUTPUT,
    flags:
      | (Schema.ConstraintDecoder<Schema.Struct.Type<Schema.Struct.Fields>> &
          Schema.Struct<Schema.Struct.Fields>)
      | null,
  ) {
    super();
    this.name = name;
    this[TASK_BODY] = body;
    this.options = options;
    this.output = output;
    this.flags = flags;
    recordTaskMade(this);
  }

  public asEffect() {
    const output = this.output;
    return Tasks.use((tasks) => tasks.requestResult(this)).pipe(
      Effect.flatMap((taskResult) =>
        trustTaskOutput<Success>(output.isDeclared ? output.decode(taskResult) : Effect.void),
      ),
    );
  }
}

export const make = <
  BodySuccess,
  Failure extends { readonly _tag: string },
  Services extends Effect.Services<Task[typeof TASK_BODY]>,
  Output extends Schema.Codec<unknown, unknown> | undefined = undefined,
  Flags extends
    | (Schema.ConstraintDecoder<Schema.Struct.Type<Schema.Struct.Fields>> &
        Schema.Struct<Schema.Struct.Fields>)
    | undefined = undefined,
>(definition: {
  readonly name: string;
  readonly flags?: Flags;
  readonly run: Flags extends Schema.Struct<Schema.Struct.Fields>
    ? (
        flags: Flags["Type"],
      ) => Effect.Effect<BodySuccess, Failure, Services> &
        NoInfer<
          Effect.Effect<
            Output extends Schema.Codec<unknown, unknown>
              ? [BodySuccess] extends [Output["Type"] | Served<Output["Type"]>]
                ? BodySuccess
                : Output["Type"]
              : [BodySuccess] extends [Served<void> | void]
                ? BodySuccess
                : typeof RUN_VALUE_NEEDS_OUTPUT,
            Failure,
            Services
          >
        >
    : Effect.Effect<BodySuccess, Failure, Services> &
        NoInfer<
          Effect.Effect<
            Output extends Schema.Codec<unknown, unknown>
              ? [BodySuccess] extends [Output["Type"] | Served<Output["Type"]>]
                ? BodySuccess
                : Output["Type"]
              : [BodySuccess] extends [Served<void> | void]
                ? BodySuccess
                : typeof RUN_VALUE_NEEDS_OUTPUT,
            Failure,
            Services
          >
        >;
  readonly output?: Output;
  readonly timeout?: Duration.Input | undefined;
  readonly cleanup?: Duration.Input | undefined;
  readonly isolated?: boolean | undefined;
}): Task<Output extends Schema.Codec<unknown, unknown> ? Output["Type"] : void, Services> => {
  const definitionRead: {
    readonly name: string;
    readonly flags?: Flags;
    readonly run:
      | Effect.Effect<BodySuccess, Failure, Services>
      | ((flags: NonNullable<Flags>["Type"]) => Effect.Effect<BodySuccess, Failure, Services>);
    readonly output?: Output;
    readonly timeout?: Duration.Input | undefined;
    readonly cleanup?: Duration.Input | undefined;
    readonly isolated?: boolean | undefined;
  } = definition;
  const given: unknown = definition;
  const isDefinition =
    Predicate.isObject(given) && !Array.isArray(given) && !Effect.isEffect(given);
  if (!isDefinition) {
    const describedKind = Predicate.isNull(given)
      ? "null"
      : DESCRIPTION_OF_GIVEN[Array.isArray(given) ? "array" : typeof given];
    const givenDescription = Predicate.isString(given) ? Formatter.format(given) : describedKind;
    const location = findCallingLocation();
    const taskWithoutDefinition = new Task<
      Output extends Schema.Codec<unknown, unknown> ? Output["Type"] : void,
      Services
    >(
      givenDescription,
      Effect.die(
        new TaskDefinitionNotGiven({ given: givenDescription, location: location ?? Bun.main }),
      ),
      resolveTaskOptions({}),
      NO_TASK_OUTPUT,
      null,
    );
    recordDefinitionNotGiven(taskWithoutDefinition, givenDescription, location);
    return taskWithoutDefinition;
  }
  const { name, run, output, isolated, flags, ...definedOptions } = definitionRead;
  const resolvedOptions = resolveTaskOptions(definedOptions, isolated);
  const declaredOutput = output === undefined ? NO_TASK_OUTPUT : declareTaskOutput(output);
  if (Effect.isEffect(run)) {
    return new Task(name, run, resolvedOptions, declaredOutput, flags ?? null);
  }
  return new Task(
    name,
    flags === undefined
      ? Effect.die(
          new InternalInvariantBroken({
            area: "Task.make",
            invariant: "a task whose run is a function declares flags",
          }),
        )
      : readTaskFlags(name, flags).pipe(Effect.flatMap(run)),
    resolvedOptions,
    declaredOutput,
    flags ?? null,
  );
};

startTheDefaultExport();
