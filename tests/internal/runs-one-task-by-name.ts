import { expect } from "bun:test";

import { Effect } from "effect";

import { exists, read, write } from "../shared/access-files.ts";
import { FAILURE_EXIT_CODE, USAGE_ERROR_EXIT_CODE } from "../shared/exit-codes-of-failures.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { withSecondsNamed } from "../shared/name-output-files.ts";
import { runMainWithArguments } from "../shared/run-main-with-arguments.ts";
import { startTaskProcess } from "../shared/run-task-processes.ts";

const CI_FILE = `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const mark = (name: string) => Effect.promise(() => Bun.write(\`\${name}-ran\`, "")).pipe(Effect.asVoid);
const build = Task.make({ name: "build", run: mark("build") });
const lint = Task.make({ name: "lint", run: mark("lint") });
const testA = Task.make({ name: "test A", run: mark("test-a") });
const testB = Task.make({
  name: "test B",
  run: Effect.gen(function* () {
    yield* build;
    yield* mark("test-b");
  }),
});
export default Task.make({
  name: "ci",
  run: Effect.gen(function* () {
    yield* Task.all([lint, testA, testB]);
    yield* mark("ci");
  }),
});
`;

const expectMarkers = (directory: string, ranMarkers: readonly string[]) =>
  Effect.gen(function* () {
    for (const marker of ["build", "lint", "test-a", "test-b", "ci"]) {
      const markerExists = yield* exists(`${directory}/${marker}-ran`);
      expect([marker, markerExists]).toEqual([marker, ranMarkers.includes(marker)]);
    }
  });

const FAILED_COMMAND_EXIT_CODE = 3;

export const namedTask = makeTest(
  "a task name after the file starts the run at that task, so only it and the tasks it asks for run, and tasks left out are not reported",
  Effect.scoped(
    Effect.gen(function* () {
      const whole = yield* makeProject({ "main.ts": CI_FILE });
      const ranWhole = yield* runMainWithArguments(whole, []);
      expect(ranWhole.standardError).toMatch(/^passed: 5 tasks, [\d.]+s$/mu);
      expect(ranWhole.exitCode).toBe(0);
      yield* expectMarkers(whole.directory, ["build", "lint", "test-a", "test-b", "ci"]);
      const picked = yield* makeProject({ "main.ts": CI_FILE });
      const ranTestB = yield* runMainWithArguments(picked, ["test B"]);
      expect(ranTestB.standardError).toMatch(/^passed: 2 tasks, [\d.]+s$/mu);
      expect(ranTestB.exitCode).toBe(0);
      yield* expectMarkers(picked.directory, ["build", "test-b"]);
      const leaf = yield* makeProject({ "main.ts": CI_FILE });
      const ranBuild = yield* runMainWithArguments(leaf, ["build"]);
      expect(ranBuild.standardError).toMatch(/^passed: 1 task, [\d.]+s$/mu);
      expect(ranBuild.exitCode).toBe(0);
      yield* expectMarkers(leaf.directory, ["build"]);
    }),
  ),
);

export const namedTasks = makeTest(
  "several task names after the file run only those tasks and the tasks they use, each once, under a root named after the file",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": CI_FILE });
      const ran = yield* runMainWithArguments(project, ["test B", "lint", "test B"], {
        SSHKEDA_TASK_LOG: "debug",
      });
      expect(ran.exitCode, ran.standardError).toBe(0);
      const lines = withSecondsNamed(ran.standardError).split("\n");
      const started = lines.filter((line) => line.startsWith("started: "));
      expect(started[0]).toBe("started: [main.ts], <seconds>, 1.output");
      expect(
        started
          .map((line) => line.slice(0, line.indexOf(", ")))
          .toSorted((left, right) => left.localeCompare(right)),
      ).toEqual(["started: [build]", "started: [lint]", "started: [main.ts]", "started: [test B]"]);
      expect(lines.filter((line) => line.startsWith("passed: "))).toEqual([
        "passed: 4 tasks, <seconds>",
      ]);
      yield* expectMarkers(project.directory, ["build", "lint", "test-b"]);
    }),
  ),
);

export const repeatedTaskName = makeTest(
  "giving the same task name twice runs it once as the root",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": CI_FILE });
      const ran = yield* runMainWithArguments(project, ["build", "build"], {
        SSHKEDA_TASK_LOG: "debug",
      });
      expect(ran.exitCode, ran.standardError).toBe(0);
      const lines = withSecondsNamed(ran.standardError).split("\n");
      expect(lines.filter((line) => line.startsWith("started: "))).toEqual([
        "started: [build], <seconds>, 1.output",
      ]);
      expect(lines.filter((line) => line.startsWith("passed: "))).toEqual([
        "passed: 1 task, <seconds>",
      ]);
      yield* expectMarkers(project.directory, ["build"]);
    }),
  ),
);

export const namedTaskExitCode = makeTest(
  "the task named after the file is the root, so its failed command's exit code is the run's",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const broken = Task.make({
  name: "broken",
  run: Task.exec("sh", ["-c", "exit ${FAILED_COMMAND_EXIT_CODE}"]).pipe(Effect.asVoid),
});
export default Task.make({ name: "main", run: Effect.void });
`,
      });
      const ran = yield* runMainWithArguments(project, ["broken"]);
      expect(ran.standardError).toMatch(
        /^error: \[broken\] main\.ts:5:13: CommandFailed: sh exited with code 3$/mu,
      );
      expect(ran.exitCode).toBe(FAILED_COMMAND_EXIT_CODE);
    }),
  ),
);

export const namedTasksExitCode = makeTest(
  "when one of several named tasks fails, the others finish and the run exits 1 like a default-exported Task.allSettled() list",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const broken = Task.make({ name: "broken", run: Task.exec("sh", ["-c", "exit ${FAILED_COMMAND_EXIT_CODE}"]).pipe(Effect.asVoid) });
const ok = Task.make({ name: "ok", run: Effect.gen(function* () {
  while (!(yield* Effect.promise(() => Bun.file("release-ok").exists()))) {
    yield* Effect.sleep("10 millis");
  }
  yield* Effect.promise(() => Bun.write("ok-ran", ""));
}) });
export default Task.make({ name: "ci", run: Task.all([broken, ok]) });
`,
      });
      const command = yield* startTaskProcess(project, "main.ts", {}, project.directory, [
        "broken",
        "ok",
      ]);
      expect(yield* command.waitForLine("error: [broken]")).toEndWith(
        `CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}`,
      );
      yield* write(`${project.directory}/release-ok`, "");
      const ran = yield* command.waitForExit;
      expect(ran.exitCode, ran.standardError).toBe(FAILURE_EXIT_CODE);
      expect(ran.standardError).toMatch(/^failed: \[broken\] \(1 of 3 tasks\), [\d.]+s$/mu);
      expect(yield* exists(`${project.directory}/ok-ran`)).toBe(true);
    }),
  ),
);

export const argumentsRefused = makeTest(
  "invalid arguments and unknown task names after the file fail the run with one line before any task starts and exit 2",
  Effect.scoped(
    Effect.gen(function* () {
      for (const [scriptArguments, refusal] of [
        [[""], 'RunArgumentsNotAccepted: the argument "" is not the name of a task to start'],
        [["  "], 'RunArgumentsNotAccepted: the argument "  " is not the name of a task to start'],
        [["--stage"], "FlagNotRead: the flag --stage is not read by any task"],
        [["-s"], 'RunArgumentsNotAccepted: the argument "-s" is not the name of a task to start'],
        [
          ["lint", "", "bild", "-s"],
          'RunArgumentsNotAccepted: the arguments "" "-s" are not names of tasks to start',
        ],
        [
          ["bild"],
          'StartTaskNotMade: the argument "bild" is not the name of a task made while the task files loaded',
        ],
        [
          ["bild", "lint", "lnt", "bild"],
          'StartTaskNotMade: the arguments "bild" "lnt" are not the names of tasks made while the task files loaded',
        ],
      ] as const) {
        const project = yield* makeProject({ "main.ts": CI_FILE });
        const ran = yield* runMainWithArguments(project, scriptArguments);
        expect(ran.standardError).toBe(`error: ${refusal}`);
        expect(ran.standardOutput).toBe("");
        expect(ran.exitCode).toBe(USAGE_ERROR_EXIT_CODE);
        yield* expectMarkers(project.directory, []);
      }
    }),
  ),
);

export const namedTaskStaysWithTheRun = makeTest(
  "a task name after the file does not reach Task runs that a task starts through Task.exec or Bun.spawn",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "inner.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({
  name: "inner",
  run: Effect.promise(() => Bun.write(\`\${process.env.MARK}-inner-ran\`, process.argv.slice(2).join(","))).pipe(Effect.asVoid),
});
`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const outer = Task.make({
  name: "outer",
  run: Effect.gen(function* () {
    yield* Task.exec(process.execPath, ["inner.ts"], { env: { MARK: "exec" } });
    yield* Effect.promise(() => Bun.spawn([process.execPath, "inner.ts"], { env: { ...process.env, MARK: "spawn" }, stdout: "ignore", stderr: "ignore" }).exited);
  }),
});
export default Task.make({
  name: "main",
  run: Effect.promise(() => Bun.write("main-ran", "")).pipe(Effect.asVoid),
});
`,
      });
      const ran = yield* runMainWithArguments(project, ["outer"]);
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(yield* read(`${project.directory}/exec-inner-ran`)).toBe("");
      expect(yield* read(`${project.directory}/spawn-inner-ran`)).toBe("");
      expect(yield* exists(`${project.directory}/main-ran`)).toBe(false);
    }),
  ),
);
