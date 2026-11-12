import { expect } from "bun:test";

import { Effect } from "effect";

import { exists } from "../shared/access-files.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

const expectRefusedUnderBunWatchMode = (flag: string, runArguments: readonly string[] = []) =>
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.promise(() => Bun.write("main-started", "")) });`,
      });
      const ran = yield* runTaskProcess(
        project,
        "main.ts",
        {},
        project.directory,
        runArguments,
        "ignore",
        [flag],
      );
      expect(ran.standardError).toBe(
        `error RunCalledInBunWatchMode: @sshkeda/task doesn't run under bun ${flag}`,
      );
      expect(ran.standardOutput).toBe("");
      expect(ran.exitCode).toBe(1);
      expect(yield* exists(`${project.directory}/main-started`)).toBe(false);
    }),
  );

export const runUnderBunWatch = makeTest(
  "a file whose default export starts a run under bun --watch fails with RunCalledInBunWatchMode before any task starts",
  expectRefusedUnderBunWatchMode("--watch"),
);

export const runUnderBunHot = makeTest(
  "a file whose default export starts a run under bun --hot fails with RunCalledInBunWatchMode before any task starts",
  expectRefusedUnderBunWatchMode("--hot"),
);

export const helpUnderBunWatch = makeTest(
  "asking for help under bun --watch fails with RunCalledInBunWatchMode instead of printing the flags",
  expectRefusedUnderBunWatchMode("--watch", ["--help"]),
);

const expectRefusedUnderBunEvaluation = (flag: string, printedFlag: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": "" });
      const evaluatedCode = `import { Effect } from "effect"; import { Task } from "@sshkeda/task"; export default Task.make({ name: "main", run: Effect.promise(() => Bun.write("main-started", "")) });`;
      const ran = yield* runTaskProcess(
        project,
        "main.ts",
        {},
        project.directory,
        [],
        "ignore",
        flag.endsWith("=") ? [`${flag}${evaluatedCode}`] : [flag, evaluatedCode],
      );
      expect(ran.standardError).toBe(
        `error RunCalledInBunEvaluation: @sshkeda/task doesn't run under bun ${printedFlag}`,
      );
      expect(ran.standardOutput).toBe("");
      expect(ran.exitCode).toBe(1);
      expect(yield* exists(`${project.directory}/main-started`)).toBe(false);
    }),
  );

export const runUnderBunEvaluate = makeTest(
  "code run by bun -e whose default export is a task fails with RunCalledInBunEvaluation before any task starts",
  expectRefusedUnderBunEvaluation("-e", "-e"),
);

export const runUnderBunEvaluateLong = makeTest(
  "code run by bun --eval or --eval= whose default export is a task fails with RunCalledInBunEvaluation before any task starts",
  Effect.all(
    [
      expectRefusedUnderBunEvaluation("--eval", "--eval"),
      expectRefusedUnderBunEvaluation("--eval=", "--eval"),
    ],
    { discard: true },
  ),
);

export const runUnderBunPrint = makeTest(
  "code run by bun -p, --print or -pe whose default export is a task fails with RunCalledInBunEvaluation before any task starts",
  Effect.all(
    [
      expectRefusedUnderBunEvaluation("-p", "-p"),
      expectRefusedUnderBunEvaluation("--print", "--print"),
      expectRefusedUnderBunEvaluation("-pe", "-pe"),
    ],
    { discard: true },
  ),
);
