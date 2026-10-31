import type { Task } from "../lib/task/index.ts";

const tasksMadeByName = new Map<string, Task[]>();

export const recordTaskMade = (madeTask: Task) => {
  tasksMadeByName.set(madeTask.name, [...(tasksMadeByName.get(madeTask.name) ?? []), madeTask]);
};

export const findTasksMadeWithName = (taskName: string): readonly Task[] =>
  tasksMadeByName.get(taskName) ?? [];
