import { Array as Arrays, Effect, Result, Schema } from "effect";
import type { Types } from "effect";

import { TaskRequestRefusal } from "../../../shared/task-errors.ts";
import { SettledTasksFailed } from "../../errors/index.ts";
import { Task } from "./make-tasks.ts";

const tasksIn = (items: readonly (Readonly<Record<string, Task>> | Task)[]) =>
  items.flatMap((listedItem) =>
    listedItem instanceof Task ? [listedItem] : Object.values(listedItem),
  );

export const all = (
  items: Parameters<typeof tasksIn>[0],
  options: { readonly concurrency?: Types.Concurrency } = {},
) => Effect.all(tasksIn(items), { concurrency: options.concurrency ?? "unbounded", discard: true });

const isTaskRequestRefusal = Schema.is(TaskRequestRefusal);

export const allSettled = (
  items: Parameters<typeof tasksIn>[0],
  options: { readonly concurrency?: Types.Concurrency } = {},
) =>
  Effect.gen(function* () {
    const settledTasks = [
      ...new Map(tasksIn(items).map((settledTask) => [settledTask.name, settledTask])).values(),
    ];
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
  });
