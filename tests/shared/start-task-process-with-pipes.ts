import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import type { makeProject } from "./make-projects.ts";
import { startTaskProcess } from "./run-task-processes.ts";
import { PipeNotReleased } from "./test-errors.ts";

const WAIT_AT_MOST = "20 seconds";

export const startTaskProcessWithPipes = (
  project: Effect.Success<ReturnType<typeof makeProject>>,
  pipeNames: readonly string[],
  environment: Readonly<Record<string, string>> = {},
) =>
  Effect.gen(function* () {
    yield* Task.exec("mkfifo", [...pipeNames], { cwd: project.directory });
    const started = yield* startTaskProcess(project, "main.ts", environment);
    const waitForLines = (lines: readonly string[], pipeName: string) =>
      started.waitForLines(lines).pipe(
        Effect.timeoutOrElse({
          duration: WAIT_AT_MOST,
          orElse: () =>
            new PipeNotReleased({
              pipeName,
              reason: `${lines.join(" and ")} did not print within ${WAIT_AT_MOST}`,
            }),
        }),
      );
    const release = (pipeName: string, after: string) =>
      Task.exec("sh", ["-c", `: > ${pipeName}`], { cwd: project.directory }).pipe(
        Effect.timeoutOrElse({
          duration: WAIT_AT_MOST,
          orElse: () =>
            new PipeNotReleased({
              pipeName,
              reason: `no task opened it to read within ${WAIT_AT_MOST} after ${after}`,
            }),
        }),
      );
    const releaseAfter = (lines: readonly string[], pipeName: string) =>
      waitForLines(lines, pipeName).pipe(
        Effect.andThen(release(pipeName, `${lines.join(" and ")} printed`)),
      );
    return {
      waitForLines,
      waitForStandardError: started.waitForStandardError,
      release,
      releaseAfter,
      waitForExit: started.waitForExit,
    };
  });
