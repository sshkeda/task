import type { Task } from "../lib/task/index.ts";
import { findConstructingLocation } from "./find-user-locations-in-stack-traces.ts";
import { isInsideTaskBody } from "./mark-task-worker-thread-contexts.ts";
import { readTaskOptions } from "./resolve-task-options.ts";

const tasksMadeByName = new Map<
  string,
  readonly {
    readonly task: Task;
    readonly location: string | undefined;
    readonly madeInTaskBody: boolean;
  }[]
>();

const definitionsNotGiven = new WeakMap<
  Task,
  { readonly given: string; readonly location: string | undefined }
>();

export const recordDefinitionNotGiven = (
  madeTask: Task,
  given: string,
  location: string | undefined,
) => {
  definitionsNotGiven.set(madeTask, { given, location });
};

export const findDefinitionNotGiven = (madeTask: Task) => definitionsNotGiven.get(madeTask) ?? null;

export const recordTaskMade = (madeTask: Task) => {
  tasksMadeByName.set(madeTask.name, [
    ...(tasksMadeByName.get(madeTask.name) ?? []),
    { task: madeTask, location: findConstructingLocation(), madeInTaskBody: isInsideTaskBody() },
  ]);
};

const LINE_AND_COLUMN = /(?::\d+){1,2}$/u;

export const filePathOfLocation = (location: string) => location.replace(LINE_AND_COLUMN, "");

export const listTasksMadeWithName = (taskName: string) => tasksMadeByName.get(taskName) ?? [];

export const findTasksMadeWithName = (taskName: string): readonly Task[] =>
  listTasksMadeWithName(taskName).map((madeTask) => madeTask.task);

export const findFileThatMade = (task: Task) => {
  const location = listTasksMadeWithName(task.name).find(
    (madeTask) => madeTask.task === task,
  )?.location;
  return location === undefined ? null : filePathOfLocation(location);
};

export const isMadeByThisCopy = (task: Task) => findTasksMadeWithName(task.name).includes(task);

export const listTasksMade = () => [...tasksMadeByName.entries()];

export const describeTaskMadeInBody = (task: Task) => {
  const madeTasks = listTasksMadeWithName(task.name);
  const isMadeInBody = madeTasks.some(
    (madeTask) => madeTask.task === task && madeTask.madeInTaskBody,
  );
  return isMadeInBody
    ? {
        locations: madeTasks.map((madeTask) => madeTask.location ?? null),
        options: readTaskOptions(task.options),
      }
    : null;
};
