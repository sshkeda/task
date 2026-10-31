import { workerData } from "node:worker_threads";

import { Context, Effect, Fiber, Ref, Schema } from "effect";

import { WorkerData } from "./rpc.ts";
import { TASK_PACKAGE } from "./task-package.ts";

const isTaskWorkerData = Schema.is(WorkerData);

export const isInsideTaskWorkerThread = () => isTaskWorkerData(workerData);

const taskModuleImportsInProgress = Ref.makeUnsafe(0);

export const markImportingTaskModule = <Success, Failure, Services>(
  importTaskModule: Effect.Effect<Success, Failure, Services>,
) =>
  Effect.acquireUseRelease(
    Ref.update(taskModuleImportsInProgress, (inProgress) => inProgress + 1),
    () => importTaskModule,
    () => Ref.update(taskModuleImportsInProgress, (inProgress) => inProgress - 1),
  );

export const isImportingTaskModule = () => Ref.getUnsafe(taskModuleImportsInProgress) > 0;

export const InsideTaskBody = Context.Reference<boolean>(`${TASK_PACKAGE.name}/InsideTaskBody`, {
  defaultValue: () => false,
});

export const isInsideTaskBody = () => Fiber.getCurrent()?.getRef(InsideTaskBody) === true;
