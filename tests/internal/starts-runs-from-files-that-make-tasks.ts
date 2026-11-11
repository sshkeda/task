import { expect } from "bun:test";

import { Effect } from "effect";

import { exists } from "../shared/access-files.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

const TASKS_FILE = `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const build = Task.make({ name: "build", run: Effect.promise(() => Bun.write("build-ran", "")) });`;

const entryFileExporting = (defaultExport: string) => `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { build } from "./tasks.ts";
export const lint = Task.make({ name: "lint", run: Effect.promise(() => Bun.write("lint-ran", "")) });
export const test = Task.make({ name: "test", run: Effect.gen(function* () {
  yield* build;
  yield* Effect.promise(() => Bun.write("test-ran", ""));
}) });
export const unused = Task.make({ name: "unused", run: Effect.promise(() => Bun.write("unused-ran", "")) });
${defaultExport}`;

const expectMarkers = (directory: string, ranMarkers: readonly string[]) =>
  Effect.gen(function* () {
    for (const marker of ["build", "lint", "test", "unused"]) {
      const markerExists = yield* exists(`${directory}/${marker}-ran`);
      expect([marker, markerExists]).toEqual([marker, ranMarkers.includes(marker)]);
    }
  });

export const defaultGroup = makeTest(
  "a file Bun starts whose default export is Task.allSettled() of a record, with options, runs that group as a task named after the file",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "tasks.ts": TASKS_FILE,
        "main.ts": entryFileExporting(
          "export default Task.allSettled({ lint, test }, { concurrency: 1 });",
        ),
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toMatch(/^\[main\.ts\] started$/mu);
      expect(ran.standardError).toMatch(/^\[main\.ts\] passed in [\d.]+s$/mu);
      expect(ran.standardError).toMatch(/^passed: 4 tasks, [\d.]+s$/mu);
      expect(ran.exitCode).toBe(0);
      yield* expectMarkers(project.directory, ["build", "lint", "test"]);
    }),
  ),
);

export const unboundedDefaultGroup = makeTest(
  "a file Bun starts whose default export is Task.all() with concurrency Infinity runs that group",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "tasks.ts": TASKS_FILE,
        "main.ts": `import { Task } from "@sshkeda/task";
import { build } from "./tasks.ts";
export default Task.all([build], { concurrency: Infinity });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toMatch(/^\[build\] started$/mu);
      expect(ran.standardError).toMatch(/^passed: 2 tasks, [\d.]+s$/mu);
      expect(ran.exitCode).toBe(0);
      yield* expectMarkers(project.directory, ["build"]);
    }),
  ),
);

export const unreadableDefaultGroup = makeTest(
  "a file Bun starts whose default export is Task.all() with concurrency NaN fails with DefaultExportNotRunnable and starts no task",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "tasks.ts": TASKS_FILE,
        "main.ts": `import { Task } from "@sshkeda/task";
import { build } from "./tasks.ts";
export default Task.all([build], { concurrency: Number.NaN });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toBe(
        "main.ts: error DefaultExportNotRunnable: its default export is a Task.all() or Task.allSettled() list whose options can't be read",
      );
      expect(ran.standardOutput).toBe("");
      expect(ran.exitCode).toBe(1);
      yield* expectMarkers(project.directory, []);
    }),
  ),
);

export const defaultListWithAnExportThatIsNotATask = makeTest(
  "a file Bun starts whose default export is Task.all() of a module with an export that is not a task fails with TaskListNotGiven and starts no task",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "tasks.ts": TASKS_FILE,
        "listed.ts": `export { build } from "./tasks.ts";
export const describe = () => "not a task";`,
        "main.ts": `import { Task } from "@sshkeda/task";
import * as listed from "./listed.ts";
export default Task.all(listed);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toBe(
        "main.ts: error TaskListNotGiven: Task.all() was called with an object, which is not a list or record of tasks",
      );
      expect(ran.standardOutput).toBe("");
      expect(ran.exitCode).toBe(1);
      yield* expectMarkers(project.directory, []);
    }),
  ),
);

export const startupDefect = makeTest(
  "a flag default that throws while the file Bun starts is being read fails the run with that error and exit 1, and starts no task",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({
  name: "main",
  flags: Schema.Struct({ stage: Schema.String.pipe(Schema.withDecodingDefaultKey(Effect.sync(() => { throw new Error("no stage file"); }))) }),
  run: ({ stage }) => Effect.log(\`stage \${stage}\`),
});`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain("no stage file");
      expect(ran.standardError).not.toContain("[main] started");
      expect(ran.standardOutput).toBe("");
      expect(ran.exitCode).toBe(1);
    }),
  ),
);

export const defaultTask = makeTest(
  "a file Bun starts whose default export is a task runs that task as the root, on one worker or many",
  Effect.scoped(
    Effect.gen(function* () {
      for (const workerCount of ["1", "4"]) {
        const project = yield* makeProject({
          "tasks.ts": TASKS_FILE,
          "main.ts": entryFileExporting("export default test;"),
        });
        const ran = yield* runTaskProcess(project, "main.ts", {
          SSHKEDA_TASK_WORKERS: workerCount,
        });
        expect(ran.standardError).toMatch(/^\[test\] started$/mu);
        expect(ran.standardError).toMatch(/^passed: 2 tasks, [\d.]+s$/mu);
        expect(ran.exitCode).toBe(0);
        yield* expectMarkers(project.directory, ["build", "test"]);
      }
    }),
  ),
);

export const failingDefaultGroup = makeTest(
  "a default-exported Task.allSettled() lets every task finish and exits 1 when one fails",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const ok = Task.make({ name: "ok", run: Effect.promise(() => Bun.write("ok-ran", "")) });
export const bad = Task.make({ name: "bad", run: Task.exec("sh", ["-c", "exit 3"]) });
export default Task.allSettled([ok, bad]);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toMatch(/^\[bad\] error CommandFailed: sh exited with code 3$/mu);
      expect(ran.standardError).toMatch(
        /^\[main\.ts\] error SettledTasksFailed: 1 of 2 tasks failed: bad$/mu,
      );
      expect(ran.standardError).toMatch(/^failed: bad \(1 of 3 tasks\), [\d.]+s$/mu);
      expect(ran.exitCode).toBe(1);
      expect(yield* exists(`${project.directory}/ok-ran`)).toBe(true);
    }),
  ),
);

export const namedTaskInDefaultGroup = makeTest(
  "a task name after the file picks one task when a file's default export starts the run, and the others never start",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "tasks.ts": TASKS_FILE,
        "main.ts": entryFileExporting("export default Task.all([lint, test]);"),
      });
      const ran = yield* runTaskProcess(project, "main.ts", {}, project.directory, ["lint"]);
      expect(ran.standardError).toMatch(/^passed: 1 task, [\d.]+s$/mu);
      expect(ran.standardError).not.toContain("[main.ts]");
      expect(ran.exitCode).toBe(0);
      yield* expectMarkers(project.directory, ["lint"]);
    }),
  ),
);

export const defaultExportNotRunnable = makeTest(
  "a file Bun starts that makes tasks but has no task or task list as its default export fails with DefaultExportNotRunnable and starts no task",
  Effect.scoped(
    Effect.gen(function* () {
      for (const [defaultExport, refusal] of [
        ["", "makes tasks, but has no default export to run"],
        [
          'export default Effect.log("not a task");',
          "makes tasks, but its default export is neither a task nor a Task.all() or Task.allSettled() list",
        ],
      ] as const) {
        const project = yield* makeProject({
          "tasks.ts": TASKS_FILE,
          "main.ts": entryFileExporting(defaultExport),
        });
        const ran = yield* runTaskProcess(project, "main.ts");
        expect(ran.standardError).toBe(`main.ts: error DefaultExportNotRunnable: ${refusal}`);
        expect(ran.standardOutput).toBe("");
        expect(ran.exitCode).toBe(1);
        yield* expectMarkers(project.directory, []);
      }
    }),
  ),
);

export const reExportNotRunnable = makeTest(
  "a file Bun starts that only re-exports a task fails with DefaultExportNotRunnable and starts no task, with or without that task's name",
  Effect.scoped(
    Effect.gen(function* () {
      for (const scriptArguments of [[], ["build"]]) {
        const project = yield* makeProject({
          "tasks.ts": TASKS_FILE,
          "main.ts": 'export { build } from "./tasks.ts";',
        });
        const ran = yield* runTaskProcess(
          project,
          "main.ts",
          {},
          project.directory,
          scriptArguments,
        );
        expect(ran.standardError).toBe(
          "main.ts: error DefaultExportNotRunnable: exports tasks, but has no default export to run",
        );
        expect(ran.standardOutput).toBe("");
        expect(ran.exitCode).toBe(1);
        yield* expectMarkers(project.directory, []);
      }
    }),
  ),
);

const FORGETFUL_FILE = `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const mark = (name: string) => Effect.promise(() => Bun.write(\`\${name}-ran\`, "")).pipe(Effect.asVoid);
const printsOutput = Task.make({ name: "prints output", run: mark("prints") });
const failsOnExit = Task.make({ name: "fails on exit 1", run: mark("fails") });
const killsOnTimeout = Task.make({ name: "kills on timeout", run: mark("kills") });
const retries = Task.make({ name: "retries", run: mark("retries") });
export const shared = Task.make({ name: "shared", run: mark("shared") });
export default Task.all([printsOutput, failsOnExit]);
`;

export const forgottenTasks = makeTest(
  "a run started from a file's default export fails with TasksNeverRan when the file makes a task it does not export and nothing ran it, unless a task was picked by name",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "tests/commands.ts": FORGETFUL_FILE, "main.ts": "" });
      const ran = yield* runTaskProcess(project, "tests/commands.ts");
      expect(ran.standardError).toMatch(/^failed: 0 of 3 tasks, [\d.]+s$/mu);
      expect(ran.standardError).toMatch(
        /^tests\/commands\.ts: error TasksNeverRan: makes "kills on timeout", "retries" but nothing ran them$/mu,
      );
      expect(ran.exitCode).toBe(1);
      expect(yield* exists(`${project.directory}/prints-ran`)).toBe(true);
      expect(yield* exists(`${project.directory}/kills-ran`)).toBe(false);
      expect(yield* exists(`${project.directory}/shared-ran`)).toBe(false);
      const picked = yield* runTaskProcess(project, "tests/commands.ts", {}, project.directory, [
        "retries",
      ]);
      expect(picked.standardError).toMatch(/^passed: 1 task, [\d.]+s$/mu);
      expect(picked.exitCode).toBe(0);
    }),
  ),
);

export const logLevelSetting = makeTest(
  "SSHKEDA_TASK_LOG=error makes a passing run started from a default export print nothing, and any other value but info fails before a task starts",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({
  name: "main",
  run: Effect.gen(function* () {
    yield* Effect.log("main ran");
    yield* Effect.promise(() => Bun.write("main-ran", ""));
  }),
});
`,
      });
      const quiet = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_LOG: "error" });
      expect(quiet.standardError).toBe("");
      expect(quiet.exitCode).toBe(0);
      expect(yield* exists(`${project.directory}/main-ran`)).toBe(true);
      const told = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_LOG: "info" });
      expect(told.standardError).toContain("[main] main ran\n");
      for (const invalid of ["verbose", ""]) {
        const refusedProject = yield* makeProject({
          "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.promise(() => Bun.write("main-ran", "")).pipe(Effect.asVoid) });
`,
        });
        const refused = yield* runTaskProcess(refusedProject, "main.ts", {
          SSHKEDA_TASK_LOG: invalid,
        });
        expect(refused.standardError).toBe(
          `error LogLevelInvalid: SSHKEDA_TASK_LOG is "${invalid}", not info or error`,
        );
        expect(refused.exitCode).toBe(1);
        expect(yield* exists(`${refusedProject.directory}/main-ran`)).toBe(false);
      }
    }),
  ),
);

export const topLevelAwait = makeTest(
  "a file that awaits at the top level before and after making its default-exported task runs that task once it has loaded",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
await Bun.write("before-made", "");
export const late = Task.make({ name: "late", run: Effect.promise(() => Bun.write("late-ran", "")) });
export default late;
await Bun.write("after-made", "");`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toMatch(/^\[late\] started$/mu);
      expect(ran.standardError).toMatch(/^passed: 1 task, [\d.]+s$/mu);
      expect(ran.exitCode).toBe(0);
      expect(yield* exists(`${project.directory}/late-ran`)).toBe(true);
    }),
  ),
);

export const programImportingTasks = makeTest(
  "a program that only imports a file that makes tasks starts no run",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "tasks.ts": TASKS_FILE,
        "main.ts": `import { build } from "./tasks.ts";
console.log(\`imported \${build.name}\`);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardOutput).toBe("imported build\n");
      expect(ran.standardError).toBe("");
      expect(ran.exitCode).toBe(0);
      expect(yield* exists(`${project.directory}/build-ran`)).toBe(false);
    }),
  ),
);
