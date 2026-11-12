import { Array as Arrays, Effect, Formatter, Predicate, Result, Schema } from "effect";
import type { Types } from "effect";

import { recordListMade } from "../../../shared/record-tasks-made.ts";
import { TASK_GROUP_KEY } from "../../../shared/root-task-group.ts";
import { TaskListNotGiven, TaskRequestRefusal } from "../../../shared/task-errors.ts";
import { SettledTasksFailed } from "../../errors/index.ts";
import { DESCRIPTION_OF_TYPE } from "./describe-given-values.ts";
import type { Task } from "./make-tasks.ts";

const tasksIn = (
  items:
    | readonly (Readonly<Record<string, Task>> | Task)[]
    | (Readonly<Record<string, Task>> & { readonly length?: never }),
) =>
  Object.values(items).flatMap((listedItem) =>
    Effect.isEffect(listedItem) ? [listedItem] : Object.values(listedItem),
  );

const ListedTask = Schema.Unknown.check(Schema.makeFilter((listed) => Effect.isEffect(listed)));

const TaskRecord = Schema.Record(Schema.String, ListedTask).check(
  Schema.makeFilter((record) => !Effect.isEffect(record)),
);

const isTaskList = Schema.is(
  Schema.Union([Schema.Array(Schema.Union([ListedTask, TaskRecord])), TaskRecord]),
);

const describeGivenValue = (given: Parameters<typeof tasksIn>[0]) => {
  if (Predicate.isString(given)) {
    return Formatter.format(given);
  }
  if (Predicate.isNull(given)) {
    return "null";
  }
  if (Array.isArray(given)) {
    return "an array";
  }
  if (!Effect.isEffect(given)) {
    return DESCRIPTION_OF_TYPE[typeof given];
  }
  const isNamedTask = Predicate.hasProperty(given, "name") && Predicate.isString(given.name);
  return isNamedTask ? `task ${Formatter.format(given.name)}` : "an effect";
};

const listTasks = (
  items: Parameters<typeof tasksIn>[0],
  calledFunction: ConstructorParameters<typeof TaskListNotGiven>[0]["calledFunction"],
) =>
  isTaskList(items)
    ? Effect.succeed(tasksIn(items))
    : Effect.die(new TaskListNotGiven({ calledFunction, given: describeGivenValue(items) }));

const recordedConcurrency = (concurrency: Types.Concurrency | undefined) => {
  const isUnbounded = concurrency === undefined || concurrency === Number.POSITIVE_INFINITY;
  return isUnbounded ? "unbounded" : concurrency;
};

const recordTaskGroup = <Listed extends WeakKey>(
  listed: Listed,
  mode: "all" | "allSettled",
  items: Parameters<typeof tasksIn>[0],
  options: { readonly concurrency?: Types.Concurrency | undefined },
) =>
  recordListMade(
    Object.defineProperty(listed, TASK_GROUP_KEY, {
      enumerable: true,
      value: isTaskList(items)
        ? {
            mode,
            taskNames: tasksIn(items).map((listedTask) => listedTask.name),
            concurrency: recordedConcurrency(options.concurrency),
          }
        : { mode, given: describeGivenValue(items) },
    }),
  );

export const all = (
  items: Parameters<typeof tasksIn>[0],
  options: { readonly concurrency?: Types.Concurrency | undefined } = {},
) =>
  recordTaskGroup(
    Effect.flatMap(listTasks(items, "Task.all"), (listedTasks) =>
      Effect.all(listedTasks, { concurrency: options.concurrency ?? "unbounded", discard: true }),
    ),
    "all",
    items,
    options,
  );

const isTaskRequestRefusal = Schema.is(TaskRequestRefusal);

const settleTasks = (
  items: Parameters<typeof tasksIn>[0],
  options: { readonly concurrency?: Types.Concurrency | undefined },
) =>
  Effect.flatMap(listTasks(items, "Task.allSettled"), (listedTasks) =>
    Effect.gen(function* () {
      const settledTasks = [...new Set(listedTasks)];
      const settledResults = yield* Effect.all(settledTasks, {
        concurrency: options.concurrency ?? "unbounded",
        mode: "result",
      });
      const failedSettledTasks = Arrays.zip(settledTasks, settledResults).flatMap(
        ([settledTask, settledResult]) =>
          Result.isFailure(settledResult)
            ? [{ failedTaskName: settledTask.name, failure: settledResult.failure }]
            : [],
      );
      const failures = failedSettledTasks.map((failedSettledTask) => failedSettledTask.failure);
      const refusedRequest = failures.find((failure) => isTaskRequestRefusal(failure));
      if (refusedRequest !== undefined) {
        return yield* refusedRequest;
      }
      if (failures.length > 0) {
        return yield* new SettledTasksFailed({
          settledTaskCount: settledTasks.length,
          failedTaskNames: failedSettledTasks.map(
            (failedSettledTask) => failedSettledTask.failedTaskName,
          ),
        });
      }
    }),
  ).pipe(Effect.asVoid);

export const allSettled = (
  items: Parameters<typeof tasksIn>[0],
  options: { readonly concurrency?: Types.Concurrency | undefined } = {},
) => recordTaskGroup(settleTasks(items, options), "allSettled", items, options);
