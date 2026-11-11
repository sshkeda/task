import { workerData } from "node:worker_threads";

import { Context, Fiber, Schema } from "effect";

import { WorkerData } from "./rpc.ts";
import { TASK_PACKAGE } from "./task-package.ts";

const isTaskWorkerData = Schema.is(WorkerData);

export const isInsideTaskWorkerThread = () => isTaskWorkerData(workerData);

export const InsideTaskBody = Context.Reference<boolean>(`${TASK_PACKAGE.name}/InsideTaskBody`, {
  defaultValue: () => false,
});

export const isInsideTaskBody = () => Fiber.getCurrent()?.getRef(InsideTaskBody) === true;

export const CommandLineFlags = Context.Reference<Readonly<Record<string, string>>>(
  `${TASK_PACKAGE.name}/CommandLineFlags`,
  { defaultValue: () => ({}) },
);
