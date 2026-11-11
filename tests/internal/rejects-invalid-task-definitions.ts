import { expect } from "bun:test";

import { Effect, FileSystem } from "effect";

import { findMisprintedErrorLines } from "../shared/find-misprinted-error-lines.ts";
import { REPOSITORY_DIRECTORY } from "../shared/find-repository-directory.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeScratchDirectory } from "../shared/make-temporary-directories.ts";
import { makeTest } from "../shared/make-tests.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

export const duplicate = makeTest(
  "two tasks with one name are refused",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "same", run: Effect.void });
export const b = Task.make({ name: "same", run: Effect.void });`,
      });

      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        'main.ts:3:21: error TaskMadeMoreThanOnce: makes the task "same", which 2 Task.make() calls make\n',
      );
      expect(ran.standardError).toContain(
        'main.ts:4:23: error TaskMadeMoreThanOnce: makes the task "same", which 2 Task.make() calls make\n',
      );
    }),
  ),
);

export const importCycle = makeTest(
  "task files that import each other run, with one worker or many",
  Effect.forEach(
    ["1", "4"],
    (workerCount) =>
      Effect.scoped(
        Effect.gen(function* () {
          const project = yield* makeProject({
            "build.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { lint } from "./main.ts";
export const build = Task.make({ name: "build", run: Effect.gen(function* () { yield* lint; yield* Effect.log("build ran"); }) });`,
            "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { build } from "./build.ts";
export const lint = Task.make({ name: "lint", run: Effect.log("lint ran") });
export const busy = Task.make({ name: "busy", run: Effect.sleep("1 second") });
export default Task.make({ name: "main", run: Task.all([busy, build, lint]) });`,
          });

          const ran = yield* runTaskProcess(project, "main.ts", {
            SSHKEDA_TASK_WORKERS: workerCount,
          });
          expect(ran.exitCode, `${workerCount}\n${ran.standardError}`).toBe(0);
          expect(ran.standardError, workerCount).toContain("[lint] lint ran");
          expect(ran.standardError, workerCount).toContain("[build] build ran");
        }),
      ),
    { concurrency: "unbounded", discard: true },
  ),
);

export const unexported = makeTest(
  "tasks that are not exported run, including one made in a module that is only imported transitively",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "steps.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const hiddenStep = Task.make({ name: "hidden-step", run: Effect.log("hidden step ran") });
export const steps = Task.all([hiddenStep]);`,
        "group.ts": `export { steps } from "./steps.ts";`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { steps } from "./group.ts";
const helper = Task.make({ name: "helper", run: Effect.log("helper ran") });
const hidden = Task.make({ name: "hidden", run: Effect.all([helper, steps], { discard: true }) });
export default hidden;`,
      });

      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain("[helper] helper ran");
      expect(ran.standardError).toContain("[hidden-step] hidden step ran");
      expect(ran.exitCode).toBe(0);
    }),
  ),
);

export const duplicateWithoutExports = makeTest(
  "two unexported tasks with one name in different modules are refused",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "other.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
Task.make({ name: "same", run: Effect.void });`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import "./other.ts";
const same = Task.make({ name: "same", run: Effect.log("body ran") });
export default same;`,
      });

      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        'other.ts:3:6: error TaskMadeMoreThanOnce: makes the task "same", which 2 Task.make() calls make\n',
      );
      expect(ran.standardError).toContain(
        'main.ts:4:19: error TaskMadeMoreThanOnce: makes the task "same", which 2 Task.make() calls make\n',
      );
      expect(ran.standardError).not.toContain("body ran");
    }),
  ),
);

export const madeAnywhere = makeTest(
  "tasks made in a function, a loop, a dynamically imported file or with a computed name run",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "checks.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const makeChecks = (names: readonly string[]) =>
  names.map((name) => Task.make({ name, run: Effect.log(\`\${name} ran\`) }));`,
        "loaded.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const loaded = Task.make({ name: "loaded", run: Effect.log("loaded ran") });`,
        "main.ts": `import { Effect } from "effect";
import * as taskLibrary from "@sshkeda/task";
import { makeChecks } from "./checks.ts";
const where = "./loaded.ts";
const { loaded } = await import(where);
function make() {
  return taskLibrary.Task.make({ name: "inner", run: Effect.log("inner ran") });
}
export default taskLibrary.Task.make({ name: "main", run: taskLibrary.Task.all([make(), loaded, ...makeChecks(["lint", "test"])]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      for (const line of [
        "[inner] inner ran",
        "[loaded] loaded ran",
        "[lint] lint ran",
        "[test] test ran",
      ]) {
        expect(ran.standardError).toContain(line);
      }
      expect(ran.exitCode, ran.standardError).toBe(0);
    }),
  ),
);

export const madeTwiceByPackage = makeTest(
  "a task that a package function makes twice is refused at each caller",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "node_modules/checks/package.json":
          '{ "name": "checks", "type": "module", "exports": "./index.ts" }',
        "node_modules/checks/index.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const check = (rules: readonly string[]) => Task.make({ name: "check", run: Effect.log(rules.join(", ")) });`,
        "main.ts": `import { Task } from "@sshkeda/task";
import { check } from "checks";
const first = check(["taste"]);
const second = check(["taste", "team"]);
export default first;`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        'main.ts:3:15: error TaskMadeMoreThanOnce: makes the task "check", which 2 Task.make() calls make\n',
      );
      expect(ran.standardError).toContain(
        'main.ts:4:16: error TaskMadeMoreThanOnce: makes the task "check", which 2 Task.make() calls make\n',
      );
      expect(ran.standardError).not.toContain("taste");
    }),
  ),
);

export const badNames = makeTest(
  "a task whose name is empty or not a string is refused",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "", run: Effect.log("body ran") });
export const numbered = Task.make({ name: 42, run: Effect.void });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "main.ts:3:21: error TaskNameEmpty: calls Task.make() with an empty name\n",
      );
      expect(ran.standardError).toContain(
        "main.ts:4:30: error TaskNameNotString: calls Task.make() with the name 42, which is not a string\n",
      );
      expect(ran.standardError).not.toContain("body ran");
    }),
  ),
);

export const blank = makeTest(
  "a task whose name is only whitespace is refused",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "  ", run: Effect.void });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(ran.standardError).toContain(
        "main.ts:3:21: error TaskNameBlank: calls Task.make() with a name of only whitespace\n",
      );
    }),
  ),
);

export const multiline = makeTest(
  "a task whose name has a line break is refused, since it would break every line the task prints",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "build\\ntest", run: Effect.log("ran") });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(ran.standardError).toContain(
        String.raw`main.ts:3:21: error TaskNameMultiline: calls Task.make() with the name "build\ntest", which has a line break`,
      );
      expect(ran.standardError).not.toContain("] ran");
    }),
  ),
);

export const madeInBody = makeTest(
  "a task made inside another task's body runs when that body awaits it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Task.make({ name: "late", run: Effect.log("late ran") });
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain("[late] late ran");
      expect(ran.standardError).toMatch(/^passed: 2 tasks, [\d.]+s$/mu);
      expect(ran.exitCode).toBe(0);
    }),
  ),
);

const projectLoadingLateFile = (lateTaskOptions: string) =>
  makeProject({
    "late.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const late = Task.make({ name: "late", run: Effect.log("late ran")${lateTaskOptions} });`,
    "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const { late } = yield* Effect.promise(() => import("./late.ts"));
  yield* late;
}) });`,
  });

export const madeByLateImport = makeTest(
  "a task in a file that a task body loads with import() runs when that body awaits it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* projectLoadingLateFile("");
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain("[late] late ran");
      expect(ran.standardError).toMatch(/^passed: 2 tasks, [\d.]+s$/mu);
      expect(ran.exitCode).toBe(0);
    }),
  ),
);

export const brokenByLateImport = makeTest(
  "a task with an invalid timeout in a file that a task body loads with import() is refused when that body awaits it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* projectLoadingLateFile(", timeout: -1");
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "late: error TaskTimeoutInvalid: has the timeout -1, which is not a duration above 0\n",
      );
      expect(ran.standardError).not.toContain("late ran");
    }),
  ),
);

export const sameNameInTwoLateFiles = makeTest(
  "a second task with a name another file's task already ran under is refused when a body loads it with import() and awaits it, instead of being skipped",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "first.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const check = Task.make({ name: "check", run: Effect.log("first ran") });`,
        "second.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const check = Task.make({ name: "check", run: Effect.log("second ran") });`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const first = yield* Effect.promise(() => import("./first.ts"));
  yield* first.check;
  const second = yield* Effect.promise(() => import("./second.ts"));
  yield* second.check;
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(ran.standardError).toContain("[check] first ran\n");
      expect(ran.standardError).toContain(
        'first.ts:3:27: error TaskMadeMoreThanOnce: makes the task "check", which 2 Task.make() calls make\n',
      );
      expect(ran.standardError).toContain(
        'second.ts:3:27: error TaskMadeMoreThanOnce: makes the task "check", which 2 Task.make() calls make\n',
      );
      expect(ran.standardError).not.toContain("second ran");
      expect(ran.standardError).not.toMatch(/^passed: /mu);
    }),
  ),
);

export const sameNameInOneSettledList = makeTest(
  "two tasks with one name from two files that an isolated task loads with import() and passes to Task.allSettled() are refused, instead of the list keeping only the last",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "first.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const check = Task.make({ name: "check", run: Effect.fail({ _tag: "FirstFailed" }) });`,
        "second.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const check = Task.make({ name: "check", run: Effect.log("second ran") });`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", isolated: true, run: Effect.gen(function* () {
  const first = yield* Effect.promise(() => import("./first.ts"));
  const second = yield* Effect.promise(() => import("./second.ts"));
  yield* Task.allSettled([first.check, second.check]);
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(ran.standardError).toContain(
        'error TaskMadeMoreThanOnce: makes the task "check", which 2 Task.make() calls make\n',
      );
      expect(ran.standardError).not.toMatch(/^passed: /mu);
    }),
  ),
);

export const definitionNotGiven = makeTest(
  "Task.make() given a string where its definition belongs is refused with TaskDefinitionNotGiven at that call, naming what it was given, whether that task is the default export or one it uses",
  Effect.scoped(
    Effect.gen(function* () {
      for (const defaultExport of [
        'Task.make("check")',
        'Task.make({ name: "main", run: Task.make("check") })',
      ]) {
        const project = yield* makeProject({
          "main.ts": `import { Task } from "@sshkeda/task";
export default ${defaultExport};`,
        });
        const ran = yield* runTaskProcess(project, "main.ts");
        expect(ran.exitCode, ran.standardError).toBe(1);
        expect(ran.standardError).toMatch(
          /^main\.ts:2:\d+: error TaskDefinitionNotGiven: calls Task\.make\(\) with "check", which is not a task definition$/mu,
        );
        expect(ran.standardError).not.toContain("InternalInvariantBroken");
        expect(ran.standardError).not.toMatch(/\] started$/mu);
      }
    }),
  ),
);

export const defaultTaskNameNotString = makeTest(
  "a default export whose task name is not a string is refused with TaskNameNotString at its Task.make() call, and no task starts",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: 42, run: Effect.log("body ran") });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toBe(
        "main.ts:3:21: error TaskNameNotString: calls Task.make() with the name 42, which is not a string",
      );
      expect(ran.exitCode).toBe(1);
    }),
  ),
);

export const misspelledSharedOptions = makeTest(
  "misspelled options in shared objects are refused instead of dropped",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const shared = { timeout: "1 minute", cleanup: "5 seconds", cach: true } as const;
const misspelledTwice = { timout: "1 minute", cleenup: "5 seconds" } as const;
export const built = Task.make({ name: "built", run: Effect.log("built ran"), ...misspelledTwice });
export default Task.make({ name: "main", run: Effect.log("body ran"), ...shared });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        'main.ts:6:21: error TaskOptionsUnknown: task "main" has the option cach outside the task options (timeout, cleanup)\n',
      );
      expect(ran.standardError).toContain(
        'main.ts:5:27: error TaskOptionsUnknown: task "built" has the options timout, cleenup outside the task options (timeout, cleanup)\n',
      );
      expect(ran.standardError).not.toContain("body ran");
    }),
  ),
);

export const secondTaskPackageCopy = makeTest(
  "a task made with a second copy of the task package than the one that starts the run is refused",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const repository = yield* REPOSITORY_DIRECTORY;
      const secondCopy = yield* makeScratchDirectory;
      yield* fileSystem.copy(`${repository}/src`, `${secondCopy}/src`);
      yield* fileSystem.copyFile(`${repository}/package.json`, `${secondCopy}/package.json`);
      yield* fileSystem.symlink(`${repository}/node_modules`, `${secondCopy}/node_modules`);
      const project = yield* makeProject({
        "library/tasks.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const other = Task.make({ name: "other", run: Effect.log("other ran") });`,
        "main.ts": `import { Task } from "@sshkeda/task";
import { other } from "./library/tasks.ts";
export default Task.make({ name: "main", run: other });`,
      });
      yield* fileSystem.makeDirectory(`${project.directory}/library/node_modules/@sshkeda`, {
        recursive: true,
      });
      yield* fileSystem.symlink(
        secondCopy,
        `${project.directory}/library/node_modules/@sshkeda/task`,
      );
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        '[main] error TaskMadeByAnotherCopy: the task "other" was made by another copy of @sshkeda/task, so this copy can\'t run it\n',
      );
      expect(ran.standardError).not.toContain("other ran");
    }),
  ),
);

export const piped = makeTest(
  "piping a task makes an ordinary Effect that awaits the task, while the task keeps its options",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const main = Task.make({ name: "main", run: Effect.log("main ran"), timeout: "1 minute", cleanup: "1 second" });
const afterMain = main.pipe(Effect.andThen(Effect.log("after main")));
export default Task.make({ name: "root", run: afterMain });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain("[main] main ran");
      expect(ran.standardError).toContain("[root] after main");
      expect(ran.exitCode).toBe(0);
    }),
  ),
);

export const grouped = makeTest(
  "an exported Effect that is not a task runs as part of the task that uses it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const a = Task.make({ name: "a", run: Effect.log("a ran") });
export const tests = Effect.all([a], { discard: true });
export default Task.make({ name: "main", run: tests });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain("[a] a ran");
      expect(ran.exitCode).toBe(0);
    }),
  ),
);

export const invalidRootDurations = makeTest(
  "invalid root task durations are collected as definition violations instead of throwing during import",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.log("body ran"), timeout: "5 secnds", cleanup: "soon" });
console.error("definitions loaded");`,
      });
      const failedRun = yield* runTaskProcess(project, "main.ts");
      expect(failedRun.exitCode).toBe(1);
      expect(failedRun.standardError).toContain("definitions loaded");
      expect(failedRun.standardError).toContain(
        `main: error TaskTimeoutInvalid: has the timeout '5 secnds', which is not a duration above 0`,
      );
      expect(failedRun.standardError).toContain(
        `main: error TaskCleanupInvalid: has the cleanup 'soon', which is not a duration of 0 or more`,
      );
      expect(findMisprintedErrorLines(failedRun.standardError)).toEqual([]);
      expect(failedRun.standardError).toMatch(/^failed: 0 of 0 tasks, [\d.]+s$/mu);
      expect(failedRun.standardError).not.toContain("body ran");
    }),
  ),
);

export const invalidImportedDuration = makeTest(
  "an imported task with an invalid duration is refused before the root task starts",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "dependency.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const dependency = Task.make({ name: "dependency", run: Effect.void, timeout: "invalid", cleanup: "1 second" });`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { dependency } from "./dependency.ts";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.log("body ran");
  yield* dependency;
}) });`,
      });
      const failedRun = yield* runTaskProcess(project, "main.ts");
      expect(failedRun.exitCode).toBe(1);
      expect(failedRun.standardError).toContain(
        `dependency: error TaskTimeoutInvalid: has the timeout 'invalid', which is not a duration above 0`,
      );
      expect(failedRun.standardError).toMatch(/^failed: 0 of 0 tasks, [\d.]+s$/mu);
      expect(failedRun.standardError).not.toContain("body ran");
      expect(failedRun.standardError).not.toContain("[main] started");
    }),
  ),
);

export const durationsOutOfRange = makeTest(
  "a zero or negative timeout and a negative cleanup are refused as definition violations before any task starts",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const zero = Task.make({ name: "zero", run: Effect.log("zero ran"), timeout: 0, cleanup: 0 });
export const negative = Task.make({ name: "negative", run: Effect.log("negative ran"), timeout: "-1 seconds", cleanup: -5 });
export default Task.make({ name: "main", run: Task.all([zero, negative]) });`,
      });
      const failedRun = yield* runTaskProcess(project, "main.ts");
      expect(failedRun.exitCode).toBe(1);
      for (const line of [
        "zero: error TaskTimeoutInvalid: has the timeout 0, which is not a duration above 0",
        "negative: error TaskTimeoutInvalid: has the timeout '-1 seconds', which is not a duration above 0",
        "negative: error TaskCleanupInvalid: has the cleanup -5, which is not a duration of 0 or more",
      ]) {
        expect(failedRun.standardError).toContain(line);
      }
      expect(failedRun.standardError).not.toContain("zero: error TaskCleanupInvalid");
      expect(findMisprintedErrorLines(failedRun.standardError)).toEqual([]);
      expect(failedRun.standardError).toMatch(/^failed: 0 of 0 tasks, [\d.]+s$/mu);
      expect(failedRun.standardError).not.toContain(" ran\n");
    }),
  ),
);

export const supportedDurationForms = makeTest(
  "discovery accepts all supported duration forms, cleanup zero and an infinite cleanup without changing task identity",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Duration, Effect } from "effect";
import { Task } from "@sshkeda/task";
export const milliseconds = Task.make({ name: "milliseconds", run: Effect.void, timeout: 60000, cleanup: 0 });
export const nanoseconds = Task.make({ name: "nanoseconds", run: Effect.void, timeout: 60000000000n, cleanup: "0 millis" });
export const tuple = Task.make({ name: "tuple", run: Effect.void, timeout: [60, 0], cleanup: [0, 0] });
export const object = Task.make({ name: "object", run: Effect.void, timeout: { minutes: 1 }, cleanup: { seconds: 0 } });
export const parsed = Task.make({ name: "parsed", run: Effect.void, timeout: Duration.minutes(1), cleanup: Duration.zero });
export const infinite = Task.make({ name: "infinite", run: Task.exec("true", []), timeout: Duration.infinity, cleanup: Duration.infinity });
export default Task.make({ name: "main", run: Task.all([milliseconds, nanoseconds, tuple, object, parsed, infinite]), timeout: "1 minute", cleanup: "5 seconds" });`,
      });
      const successfulRun = yield* runTaskProcess(project, "main.ts");
      expect(successfulRun.exitCode).toBe(0);
      expect(successfulRun.standardError).toMatch(/^passed: 7 tasks, [\d.]+s$/mu);
      expect(successfulRun.standardError).not.toMatch(/ error \w+: /u);
    }),
  ),
);
