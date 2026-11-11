import { expect } from "bun:test";

import { Effect, Exit, FileSystem, Scope } from "effect";

import { failTestCleanup } from "../shared/fail-test-cleanup.ts";
import { exitAsValue } from "../shared/failure-as-value.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";
import { ChildStillRunning } from "../shared/test-errors.ts";

const RUN_COUNT = 2;

const CHILD_TASK_FILE_SOURCE = `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make({ name: "child", run: Effect.gen(function* () {
  yield* Effect.log("child trace marker");
  yield* Effect.sync(() => { process.stdout.write("partial stdout"); process.stderr.write("partial stderr"); });
}) });`;

const CHILD_ENTRY_SOURCE = `${CHILD_TASK_FILE_SOURCE}
export default main;`;

export const assertionFailure = makeTest(
  "an assertion failure after child exit retains each child's output and traces",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const projectScope = yield* Scope.make();
      const project = yield* makeProject({ "main.ts": CHILD_ENTRY_SOURCE }).pipe(
        Scope.provide(projectScope),
      );
      yield* Effect.addFinalizer(() =>
        Effect.all([
          fileSystem.remove(project.directory, { recursive: true, force: true }),
          fileSystem.remove(project.home, { recursive: true, force: true }),
        ]).pipe(Effect.asVoid, failTestCleanup("remove preserved failure fixture")),
      );
      for (const attempt of Array.from({ length: RUN_COUNT }, (_unused, index) => index + 1)) {
        const result = yield* runTaskProcess(project, "main.ts");
        expect(result.exitCode, `attempt ${attempt}: ${result.standardError}`).toBe(0);
      }
      yield* Scope.close(projectScope, Exit.die(new Error("assertion failed")));
      const outputs = yield* fileSystem.glob("child-*-output.txt", { root: project.directory });
      expect(outputs.length).toBe(RUN_COUNT);
      for (const outputFilePath of outputs) {
        const contents = yield* fileSystem.readFileString(`${project.directory}/${outputFilePath}`);
        expect(contents).toContain("partial stdout");
        expect(contents).toContain("partial stderr");
      }
      const traces = yield* fileSystem.glob("**/*.jsonl", {
        root: `${project.home}/.sshkeda-task/traces`,
      });
      expect(traces.length).toBeGreaterThan(0);
      const contents = yield* fileSystem.readFileString(
        `${project.home}/.sshkeda-task/traces/${traces[0]}`,
      );
      expect(contents).toContain("child trace marker");
    }),
  ),
);

const interruptedChild = (interrupt: boolean) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const projectScope = yield* Scope.make();
      const project = yield* makeProject({
        "main.ts": `${CHILD_TASK_FILE_SOURCE}
export const hanging = Task.make({ name: "hanging", run: Effect.gen(function* () {
  yield* main;
  yield* Effect.log("ready to interrupt");
  yield* Effect.never;
}) });
export default hanging;`,
      }).pipe(Scope.provide(projectScope));
      yield* Effect.addFinalizer(() =>
        Effect.all([
          fileSystem.remove(project.directory, { recursive: true, force: true }),
          fileSystem.remove(project.home, { recursive: true, force: true }),
        ]).pipe(Effect.asVoid, failTestCleanup("remove preserved failure fixture")),
      );
      const childExit = yield* Effect.scoped(
        Effect.gen(function* () {
          const child = yield* startTaskProcess(project, "main.ts");
          yield* child.waitForLine("ready to interrupt");
          return yield* interrupt
            ? Effect.interrupt
            : Effect.fail(new ChildStillRunning({ processId: child.processId }));
        }),
      ).pipe(exitAsValue);
      expect(Exit.isFailure(childExit)).toBe(true);
      yield* Scope.close(projectScope, childExit);
      expect(yield* fileSystem.exists(project.directory)).toBe(true);
      expect(yield* fileSystem.exists(project.home)).toBe(true);
      const outputs = yield* fileSystem.glob("child-*-output.txt", { root: project.directory });
      expect(outputs.length).toBe(1);
      const contents = yield* fileSystem.readFileString(`${project.directory}/${outputs[0]}`);
      expect(contents).toContain("partial stdout");
      expect(contents).toContain("partial stderr");
      expect(
        (yield* fileSystem.glob("**/*.jsonl", { root: `${project.home}/.sshkeda-task/traces` }))
          .length,
      ).toBeGreaterThan(0);
    }),
  );

export const failedWhileChildRuns = makeTest(
  "a test that fails while its child runs preserves partial output and traces",
  interruptedChild(false),
);
export const interrupted = makeTest(
  "an interrupted child preserves partial output and traces",
  interruptedChild(true),
);

export const successful = makeTest(
  "a successful test removes its project and child trace directories",
  Effect.gen(function* () {
    const project = yield* Effect.scoped(
      Effect.gen(function* () {
        const childProject = yield* makeProject({ "main.ts": CHILD_ENTRY_SOURCE });
        const result = yield* runTaskProcess(childProject, "main.ts");
        expect(result.exitCode, result.standardError).toBe(0);
        return childProject;
      }),
    );
    const fileSystem = yield* FileSystem.FileSystem;
    expect(yield* fileSystem.exists(project.directory)).toBe(false);
    expect(yield* fileSystem.exists(project.home)).toBe(false);
  }),
);
