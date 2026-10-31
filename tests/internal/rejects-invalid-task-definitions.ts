import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect, FileSystem } from "effect";

import { findMisprintedErrorLines } from "../shared/find-misprinted-error-lines.ts";
import { REPOSITORY_DIRECTORY } from "../shared/find-repository-directory.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeScratchDirectory } from "../shared/make-temporary-directories.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

export const duplicate = Task.make(
  "two tasks with one name are refused",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const a = Task.make("same", Effect.void);
export const b = Task.make("same", Effect.void);
if (import.meta.main) Task.main(a);`,
      });

      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        'main.ts:3: error TaskMadeMoreThanOnce: makes the task "same", which 2 Task.make() calls make\n',
      );
      expect(ran.standardError).toContain(
        'main.ts:4: error TaskMadeMoreThanOnce: makes the task "same", which 2 Task.make() calls make\n',
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const importCycle = Task.make(
  "task files that import each other are refused before anything runs, with one worker or many",
  Effect.forEach(
    ["1", "4"],
    (workerCount) =>
      Effect.scoped(
        Effect.gen(function* () {
          const project = yield* makeProject({
            "build.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { lint } from "./main.ts";
export const build = Task.make("build", Effect.gen(function* () { yield* lint; yield* Effect.log("build ran"); }));`,
            "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { build } from "./build.ts";
export const lint = Task.make("lint", Effect.log("lint ran"));
export const busy = Task.make("busy", Effect.sleep("1 second"));
export const main = Task.make("main", Task.all([busy, build, lint]));
if (import.meta.main) Task.main(main);`,
          });

          const ran = yield* runTaskProcess(project, "main.ts", {
            SSHKEDA_TASK_WORKERS: workerCount,
          });
          expect(ran.exitCode, `${workerCount}\n${ran.standardError}`).toBe(1);
          expect(ran.standardError, workerCount).toContain(
            "main.ts: error TaskFileImportsItself: imports build.ts, which imports main.ts; a worker imports only the file of the task it runs, so a task file in an import cycle would load in a different order there than when Task.main() loaded it\n",
          );
          expect(ran.standardError, workerCount).not.toContain("] started");
        }),
      ),
    { concurrency: "unbounded", discard: true },
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const unexported = Task.make(
  "tasks that are not exported run, including one made in a module that is only imported transitively",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "steps.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const hiddenStep = Task.make("hidden-step", Effect.log("hidden step ran"));
export const steps = Task.all([hiddenStep]);`,
        "group.ts": `export { steps } from "./steps.ts";`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { steps } from "./group.ts";
const helper = Task.make("helper", Effect.log("helper ran"));
const hidden = Task.make("hidden", Effect.all([helper, steps], { discard: true }));
if (import.meta.main) Task.main(hidden);`,
      });

      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain("[helper] helper ran");
      expect(ran.standardError).toContain("[hidden-step] hidden step ran");
      expect(ran.exitCode).toBe(0);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const duplicateWithoutExports = Task.make(
  "two unexported tasks with one name in different modules are refused",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "other.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
Task.make("same", Effect.void);`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import "./other.ts";
const same = Task.make("same", Effect.log("body ran"));
if (import.meta.main) Task.main(same);`,
      });

      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        'other.ts:3: error TaskMadeMoreThanOnce: makes the task "same", which 2 Task.make() calls make\n',
      );
      expect(ran.standardError).toContain(
        'main.ts:4: error TaskMadeMoreThanOnce: makes the task "same", which 2 Task.make() calls make\n',
      );
      expect(ran.standardError).not.toContain("body ran");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const nestedWithoutExports = Task.make(
  "an unexported task made inside a function is refused",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
function make() {
  return Task.make("inner", Effect.log("inner ran"));
}
const inner = make();
const main = Task.make("main", inner);
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(
        /^main\.ts:4: error TaskCalledBelowTopLevel: calls Task\.make\(\) below the top level of its module$/mu,
      );
      expect(ran.standardError).not.toContain("inner ran");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const nested = Task.make(
  "a task made below the top level of its module is refused",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const make = () => Task.make("inner", Effect.void);
export const inner = make();
export const main = Task.make("main", inner);
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(
        /^main\.ts:3: error TaskCalledBelowTopLevel: calls Task\.make\(\) below the top level of its module\nfailed: 0 tasks, [\d.]+s$/mu,
      );
      expect(ran.standardError).not.toContain("task refused to run");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const computed = Task.make(
  "a task whose name is not a literal is refused",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const name = "main";
export const main = Task.make(name, Effect.void);
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "main.ts:4: error TaskNameComputed: calls Task.make() with the name name, which is computed",
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const empty = Task.make(
  "a task whose name is empty is refused",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("", Effect.void);
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "main.ts:3: error TaskNameEmpty: calls Task.make() with an empty name",
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const dynamic = Task.make(
  "an import of a computed path is refused, since the code key could not follow it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const where = "./other.ts";
export const main = Task.make("main", Effect.promise(() => import(where)));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain(
        "error ModuleLoadedByComputedPath: calls import(where), which has no literal path",
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const typeOnlyEmptyImports = Task.make(
  "type-only imports and exports with empty specifier lists are not followed as side-effect imports",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import type {} from "package-that-is-not-installed";
export type {} from "another-package-that-is-not-installed";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.log("body ran"));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).not.toContain("cannot resolve import");
      expect(ran.standardError).toContain("[main] body ran");
      expect(ran.exitCode).toBe(0);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const unfound = Task.make(
  "missing dependencies name the requesting task and identify the missing task",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "make.ts": 'import { Task } from "@sshkeda/task";\nexport const make = Task.make;\n',
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { make } from "./make.ts";
export const hidden = make("hidden", Effect.void);
export const main = Task.make("main", hidden);
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        '[main] error TaskNotMadeAtTopLevel: the module scan found no top-level Task.make("hidden") call',
      );
      expect(findMisprintedErrorLines(ran.standardError)).toEqual([]);
      expect(ran.standardError).not.toContain("[hidden] error:");
      expect(ran.standardError).not.toContain("TypeError");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const copied = Task.make(
  "a root task made again below the top level is refused along with the other refusals",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const where = "./other.ts";
export const load = Task.make("load", Effect.promise(() => import(where)));
export const main = Task.make("main", Effect.log("body ran"));
if (import.meta.main) Task.main(Task.make("main", Effect.log("body ran"), { cleanup: "1 second" }));`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "error ModuleLoadedByComputedPath: calls import(where), which has no literal path",
      );
      expect(ran.standardError).toContain(
        "main.ts:6: error TaskCalledBelowTopLevel: calls Task.make() below the top level of its module\n",
      );
      expect(ran.standardError).toContain(
        'main.ts: error TaskMadeAgainInThread: Task.make() made "main" 2 times in this thread, but this file has its only top-level Task.make() call\n',
      );
      expect(findMisprintedErrorLines(ran.standardError)).toEqual([]);
      expect(ran.standardError).not.toContain("body ran");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const misspelledSharedOptions = Task.make(
  "a misspelled option in a shared object and the removed cache options are refused instead of dropped",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const shared = { timeout: "1 minute", cleanup: "5 seconds", cach: true } as const;
const sharedCache = { cache: true, inputs: ["src/**"], outputs: ["dist/**"], env: ["FOO"] } as const;
export const built = Task.make("built", Effect.log("built ran"), sharedCache);
export const main = Task.make("main", Effect.log("body ran"), shared);
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        'main.ts:6: error TaskOptionsUnknown: task "main" has the option cach outside the task options (timeout, cleanup)\n',
      );
      expect(ran.standardError).toContain(
        'main.ts:5: error TaskOptionsUnknown: task "built" has the options cache, inputs, outputs, env outside the task options (timeout, cleanup)\n',
      );
      expect(ran.standardError).not.toContain("body ran");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const defaultExported = Task.make(
  "a task made by a top-level export default runs",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "dependency.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make("dependency", Effect.log("dependency ran"));`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import dependency from "./dependency.ts";
const main = Task.make("main", dependency);
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain("[dependency] dependency ran");
      expect(ran.exitCode).toBe(0);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const typedTopLevelTasks = Task.make(
  "TypeScript assertions around a top-level task keep its declaration visible",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "definition.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const satisfied = Task.make("satisfied", Effect.log("typed task ran")) satisfies Task.Task<void>;
export const asserted = Task.make("asserted", Effect.log("typed task ran")) as Task.Task<void>;
export const angled = <Task.Task<void>>Task.make("angled", Effect.log("typed task ran"));
export const nonnull = Task.make("nonnull", Effect.log("typed task ran"))!;
export const piped = (Task.make("piped", Effect.log("typed task ran")) satisfies Task.Task<void>).pipe(Effect.asVoid) satisfies Effect.Effect<void>;
export default Task.make("default", Effect.log("typed task ran")) satisfies Task.Task<void>;`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import defaultTask, { satisfied, asserted, angled, nonnull, piped } from "./definition.ts";
export const main = Task.make("root", Effect.all([satisfied, asserted, angled, nonnull, piped, defaultTask], { discard: true })) satisfies Task.Task<void>;
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      for (const name of ["satisfied", "asserted", "angled", "nonnull", "piped", "default"]) {
        expect(ran.standardError).toContain(`[${name}] typed task ran`);
      }
      expect(ran.exitCode, ran.standardError).toBe(0);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const secondTaskPackageCopy = Task.make(
  "a task made with a second copy of the task package than the one calling Task.main() is refused",
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
export const other = Task.make("other", Effect.log("other ran"));`,
        "main.ts": `import { Task } from "@sshkeda/task";
import { other } from "./library/tasks.ts";
const main = Task.make("main", other);
if (import.meta.main) Task.main(main);`,
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
        'library/tasks.ts: error TaskNotMadeByImport: importing this file did not make task "other" in the copy of @sshkeda/task that calls Task.main()\n',
      );
      expect(ran.standardError).not.toContain("other ran");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const piped = Task.make(
  "piping a task makes an ordinary Effect that awaits the task, while the task keeps its options",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const main = Task.make("main", Effect.log("main ran"), { timeout: "1 minute", cleanup: "1 second" });
const afterMain = main.pipe(Effect.andThen(Effect.log("after main")));
const root = Task.make("root", afterMain);
if (import.meta.main) Task.main(root);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain("[main] main ran");
      expect(ran.standardError).toContain("[root] after main");
      expect(ran.exitCode).toBe(0);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const grouped = Task.make(
  "an exported Effect that is not a task runs as part of the task that uses it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const a = Task.make("a", Effect.log("a ran"));
export const tests = Effect.all([a], { discard: true });
export const main = Task.make("main", tests);
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain("[a] a ran");
      expect(ran.exitCode).toBe(0);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const namespaced = Task.make(
  "a task made through a namespace import below the top level of its module is refused",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import * as taskLibrary from "@sshkeda/task";
const make = () => taskLibrary.Task.make("inner", Effect.void);
export const inner = make();
export const main = taskLibrary.Task.make("main", inner);
if (import.meta.main) taskLibrary.Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "main.ts:3: error TaskCalledBelowTopLevel: calls Task.make() below the top level of its module",
      );
      expect(ran.standardError).not.toContain("not made by a top-level Task.make() call");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const invalidRootDurations = Task.make(
  "invalid root task durations are collected as definition violations instead of throwing during import",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.log("body ran"), { timeout: "5 secnds", cleanup: "soon" });
if (import.meta.main) {
  console.error("definitions loaded");
  Task.main(main);
}`,
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
      expect(failedRun.standardError).toMatch(/^failed: 0 tasks, [\d.]+s$/mu);
      expect(failedRun.standardError).not.toContain("not made by a top-level Task.make() call");
      expect(failedRun.standardError).not.toContain("body ran");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const invalidImportedDuration = Task.make(
  "an imported task with an invalid duration is refused before the root task starts",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "dependency.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const dependency = Task.make("dependency", Effect.void, { timeout: "invalid", cleanup: "1 second" });`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { dependency } from "./dependency.ts";
export const main = Task.make("main", Effect.gen(function* () {
  yield* Effect.log("body ran");
  yield* dependency;
}));
if (import.meta.main) Task.main(main);`,
      });
      const failedRun = yield* runTaskProcess(project, "main.ts");
      expect(failedRun.exitCode).toBe(1);
      expect(failedRun.standardError).toContain(
        `dependency: error TaskTimeoutInvalid: has the timeout 'invalid', which is not a duration above 0`,
      );
      expect(failedRun.standardError).toMatch(/^failed: 0 tasks, [\d.]+s$/mu);
      expect(failedRun.standardError).not.toContain("body ran");
      expect(failedRun.standardError).not.toContain("[main] started");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const durationsOutOfRange = Task.make(
  "a zero or negative timeout and a negative cleanup are refused as definition violations before any task starts",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const zero = Task.make("zero", Effect.log("zero ran"), { timeout: 0, cleanup: 0 });
export const negative = Task.make("negative", Effect.log("negative ran"), { timeout: "-1 seconds", cleanup: -5 });
export const main = Task.make("main", Task.all([zero, negative]));
if (import.meta.main) Task.main(main);`,
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
      expect(failedRun.standardError).toMatch(/^failed: 0 tasks, [\d.]+s$/mu);
      expect(failedRun.standardError).not.toContain(" ran\n");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const supportedDurationForms = Task.make(
  "discovery accepts all supported duration forms and cleanup zero without changing task identity",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Duration, Effect } from "effect";
import { Task } from "@sshkeda/task";
export const milliseconds = Task.make("milliseconds", Effect.void, { timeout: 60000, cleanup: 0 });
export const nanoseconds = Task.make("nanoseconds", Effect.void, { timeout: 60000000000n, cleanup: "0 millis" });
export const tuple = Task.make("tuple", Effect.void, { timeout: [60, 0], cleanup: [0, 0] });
export const object = Task.make("object", Effect.void, { timeout: { minutes: 1 }, cleanup: { seconds: 0 } });
export const parsed = Task.make("parsed", Effect.void, { timeout: Duration.minutes(1), cleanup: Duration.zero });
export const main = Task.make("main", Task.all([milliseconds, nanoseconds, tuple, object, parsed]), { timeout: "1 minute", cleanup: "5 seconds" });
if (import.meta.main) Task.main(main);`,
      });
      const successfulRun = yield* runTaskProcess(project, "main.ts");
      expect(successfulRun.exitCode).toBe(0);
      expect(successfulRun.standardError).toMatch(/^passed: 6 tasks, [\d.]+s$/mu);
      expect(successfulRun.standardError).not.toContain("invalid");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

const RUN_CALLED_TWICE_LINES =
  'error RunCalledTwice: Task.main() was called again with "second" while it runs "first"';

export const runCalledTwice = Task.make(
  "a second Task.main() call in the same process is refused before either task starts, and exits 1",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const first = Task.make("first", Effect.log("first ran"));
export const second = Task.make("second", Effect.log("second ran"));
if (import.meta.main) {
  Task.main(first);
  Task.main(second);
}`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        `main.ts:7:8: ${RUN_CALLED_TWICE_LINES}, started at main.ts:6:8`,
      );
      expect(ran.standardError).not.toContain("first ran");
      expect(ran.standardError).not.toContain("second ran");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const runCalledAgainWhileRunning = Task.make(
  "a Task.main() call while a run is going stops that run, runs its finalizers and exits 1",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const first = Task.make("first", Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("first cleaned up"));
  yield* Effect.sync(() => new BroadcastChannel("running").postMessage("running"));
  yield* Effect.never;
}));
export const second = Task.make("second", Effect.log("second ran"));
if (import.meta.main) {
  Task.main(first);
  new BroadcastChannel("running").onmessage = () => Task.main(second);
}`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain("[first] first cleaned up");
      expect(ran.standardError).toMatch(/^failed: 1 task, 1 stopped, [\d.]+s$/mu);
      expect(ran.standardError).toContain(RUN_CALLED_TWICE_LINES);
      expect(ran.standardError).not.toContain("second ran");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);
