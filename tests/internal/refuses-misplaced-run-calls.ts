import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import { exists } from "../shared/access-files.ts";
import { makeProject } from "../shared/make-projects.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

const RUN_CALLED_DURING_IMPORT_LINE =
  "error RunCalledDuringImport: Task.main() ran while Task imported this file";

const testFileEndingWith = (runCall: string) => `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const check = Task.make("check", Effect.log("check ran"));
${runCall}`;

const ROOT_TASK_FILE_IMPORTING_TESTS = `import { Task } from "@sshkeda/task";
import * as tests from "./tests/check.ts";
export const ci = Task.make("ci", Task.all([tests]));
if (import.meta.main) Task.main(ci);`;

export const unguardedRunInImportedTestFile = Task.make(
  "an unguarded Task.main() in a test file imported by the root task file makes the root task file's Task.main() fail with RunCalledTwice, naming both calls",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "tests/check.ts": testFileEndingWith("Task.main(check);"),
        "main.ts": ROOT_TASK_FILE_IMPORTING_TESTS,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain(
        `main.ts:4:28: error RunCalledTwice: Task.main() was called again with "ci" while it runs "check", started at tests/check.ts:4:6`,
      );
      expect(ran.standardError).not.toContain("check ran");
      expect(ran.exitCode).toBe(1);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const guardedRunInImportedTestFile = Task.make(
  "a guarded Task.main() in a test file imported by the root task file lets the root task run it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "tests/check.ts": testFileEndingWith("if (import.meta.main) Task.main(check);"),
        "main.ts": ROOT_TASK_FILE_IMPORTING_TESTS,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain("[check] check ran");
      expect(ran.standardError).toMatch(/^passed: 2 tasks, [\d.]+s$/mu);
      expect(ran.exitCode).toBe(0);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const unguardedRunInRootTaskFile = Task.make(
  "an unguarded Task.main() in the root task file fails with RunCalledDuringImport when a worker imports it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const a = Task.make("a", Effect.log("a ran"));
const b = Task.make("b", Effect.log("b ran"));
const ci = Task.make("ci", Task.allSettled([a, b]));
Task.main(ci);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toMatch(
        new RegExp(
          String.raw`\[ci\] main\.ts:6:6: ${RegExp.escape(RUN_CALLED_DURING_IMPORT_LINE)}`,
          "u",
        ),
      );
      expect(ran.standardError).not.toContain("RootTaskNotMadeAtTopLevel");
      expect(ran.standardError).not.toContain("WorkerExited");
      expect(ran.exitCode).toBe(1);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

const RUN_CALLED_IN_TASK_BODY_LINES = `error RunCalledInTaskBody: Task.main() was called with "nested" inside a task body`;

export const runInTaskBody = Task.make(
  "Task.main() called in a task body fails that task with RunCalledInTaskBody at the call",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const nested = Task.make("nested", Effect.log("nested ran"));
export const main = Task.make("main", Effect.gen(function* () {
  Task.main(nested);
}));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain(`[main] main.ts:5:8: ${RUN_CALLED_IN_TASK_BODY_LINES}`);
      expect(ran.standardError).not.toContain("nested ran");
      expect(ran.exitCode).toBe(1);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const runInTaskBodyPromiseCallback = Task.make(
  "Task.main() called from a promise callback in a task body fails that task with RunCalledInTaskBody",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const nested = Task.make("nested", Effect.log("nested ran"));
export const main = Task.make("main", Effect.promise(async () => {
  await new Promise((resolve) => setTimeout(resolve, 10));
  Task.main(nested);
}));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain(`[main] main.ts:6:8: ${RUN_CALLED_IN_TASK_BODY_LINES}`);
      expect(ran.standardError).not.toContain("nested ran");
      expect(ran.exitCode).toBe(1);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

const expectRefusedUnderBunWatchMode = (flag: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.promise(() => Bun.write("main-started", "")));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", {}, project.directory, [], "ignore", [
        flag,
      ]);
      expect(ran.standardError).toBe(
        `error RunCalledInBunWatchMode: @sshkeda/task doesn't run under bun ${flag}`,
      );
      expect(ran.standardOutput).toBe("");
      expect(ran.exitCode).toBe(1);
      expect(yield* exists(`${project.directory}/main-started`)).toBe(false);
    }),
  );

export const runUnderBunWatch = Task.make(
  "Task.main() under bun --watch fails with RunCalledInBunWatchMode before any task starts",
  expectRefusedUnderBunWatchMode("--watch"),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const runUnderBunHot = Task.make(
  "Task.main() under bun --hot fails with RunCalledInBunWatchMode before any task starts",
  expectRefusedUnderBunWatchMode("--hot"),
  { timeout: "1 minute", cleanup: "5 seconds" },
);
