import { Layer } from "effect";

import { makeTaskRuns, TaskRuns } from "./internal/make-task-runs.ts";

export { TaskRuns } from "./internal/make-task-runs.ts";

export const TaskRunsLive = Layer.effect(TaskRuns, makeTaskRuns);
