import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import { makeProject } from "../shared/make-projects.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

export const customFormatter = Task.make({
  name: "stack capture accepts a formatter that forwards to Bun's native formatter",
  run: Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
// Babel wraps the existing formatter and forwards the captured error to it.
const nativeFormatter = Error.prepareStackTrace;
Error.prepareStackTrace = (error, frames) => nativeFormatter(error, frames);
export default Task.make({ name: "main", run: Task.exec("bun", ["--eval", "console.log('formatted command ran')"]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain("[main] formatted command ran");
      expect(ran.standardError).not.toContain("First argument must be an Error object");
    }),
  ),
  timeout: "1 minute",
  cleanup: "5 seconds",
});
