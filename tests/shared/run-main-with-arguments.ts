import type { Effect } from "effect";

import type { makeProject } from "./make-projects.ts";
import { runTaskProcess } from "./run-task-processes.ts";

export const runMainWithArguments = (
  project: Effect.Success<ReturnType<typeof makeProject>>,
  scriptArguments: readonly string[],
  environment: Readonly<Record<string, string>> = {},
) => runTaskProcess(project, "main.ts", environment, project.directory, scriptArguments);
