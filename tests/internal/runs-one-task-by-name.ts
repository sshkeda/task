import { expect } from "bun:test";

import { Effect } from "effect";

import { exists, read } from "../shared/access-files.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

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

const runWithArguments = (
  project: Effect.Success<ReturnType<typeof makeProject>>,
  scriptArguments: readonly string[],
) => runTaskProcess(project, "main.ts", {}, project.directory, scriptArguments);

const FAILED_COMMAND_EXIT_CODE = 3;

export const namedTask = makeTest(
  "a task name after the file starts the run at that task, so only it and the tasks it asks for run, and tasks left out are not reported",
  Effect.scoped(
    Effect.gen(function* () {
      const whole = yield* makeProject({ "main.ts": CI_FILE });
      const ranWhole = yield* runWithArguments(whole, []);
      expect(ranWhole.standardError).toMatch(/^passed: 5 tasks, [\d.]+s$/mu);
      expect(ranWhole.exitCode).toBe(0);
      yield* expectMarkers(whole.directory, ["build", "lint", "test-a", "test-b", "ci"]);
      const picked = yield* makeProject({ "main.ts": CI_FILE });
      const ranTestB = yield* runWithArguments(picked, ["test B"]);
      expect(ranTestB.standardError).toMatch(/^\[test B\] started$/mu);
      expect(ranTestB.standardError).toMatch(/^passed: 2 tasks, [\d.]+s$/mu);
      expect(ranTestB.exitCode).toBe(0);
      yield* expectMarkers(picked.directory, ["build", "test-b"]);
      const leaf = yield* makeProject({ "main.ts": CI_FILE });
      const ranBuild = yield* runWithArguments(leaf, ["build"]);
      expect(ranBuild.standardError).toMatch(/^passed: 1 task, [\d.]+s$/mu);
      expect(ranBuild.exitCode).toBe(0);
      yield* expectMarkers(leaf.directory, ["build"]);
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
      const ran = yield* runWithArguments(project, ["broken"]);
      expect(ran.standardError).toMatch(
        /^\[broken\] error CommandFailed: sh exited with code 3$/mu,
      );
      expect(ran.exitCode).toBe(FAILED_COMMAND_EXIT_CODE);
    }),
  ),
);

export const argumentsRefused = makeTest(
  "arguments after the file that are not the name of one task made while loading fail the run with one line before any task starts",
  Effect.scoped(
    Effect.gen(function* () {
      for (const [scriptArguments, refusal] of [
        [[""], 'RunArgumentsNotAccepted: the argument "" is not the name of one task to start'],
        [["  "], 'RunArgumentsNotAccepted: the argument "  " is not the name of one task to start'],
        [["--stage"], "FlagNotRead: the flag --stage is not read by any task"],
        [["-s"], 'RunArgumentsNotAccepted: the argument "-s" is not the name of one task to start'],
        [
          ["lint", "build"],
          'RunArgumentsNotAccepted: the arguments "lint" "build" are not the name of one task to start',
        ],
        [
          ["bild"],
          'StartTaskNotMade: the argument "bild" is not the name of a task made while the task files loaded',
        ],
      ] as const) {
        const project = yield* makeProject({ "main.ts": CI_FILE });
        const ran = yield* runWithArguments(project, scriptArguments);
        expect(ran.standardError).toBe(`error ${refusal}`);
        expect(ran.standardOutput).toBe("");
        expect(ran.exitCode).toBe(1);
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
      const ran = yield* runWithArguments(project, ["outer"]);
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(yield* read(`${project.directory}/exec-inner-ran`)).toBe("");
      expect(yield* read(`${project.directory}/spawn-inner-ran`)).toBe("");
      expect(yield* exists(`${project.directory}/main-ran`)).toBe(false);
    }),
  ),
);
