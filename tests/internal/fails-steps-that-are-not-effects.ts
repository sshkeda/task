import { expect } from "bun:test";

import { Effect } from "effect";

import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

const NOT_AN_EFFECT = 'Effect.flatMap(Effect.void, () => "x" as never)';

const failsInsteadOfHanging = (taskSource: string, expectedError: RegExp) =>
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Stream } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export default ${taskSource};`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(expectedError);
      expect(ran.standardError).toMatch(/^failed: main \(1 of 1 task\), [\d.]+s$/mu);
    }),
  );

export const bodyStep = makeTest(
  "a task body step that is not an effect, as when types are bypassed, fails the task instead of hanging the run",
  failsInsteadOfHanging(
    `Task.make({ name: "main", run: ${NOT_AN_EFFECT} })`,
    /^\[main\] error TaskBodyFailed: its body failed with Fiber.runLoop: Not a valid effect: x$/mu,
  ),
);

export const bodyFinalizerStep = makeTest(
  "a task body finalizer step that is not an effect fails the task instead of hanging the run",
  failsInsteadOfHanging(
    `Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => ${NOT_AN_EFFECT});
  yield* Effect.log("body done");
}) })`,
    /^\[main\] error TaskCleanupFailed: its cleanup failed with Fiber.runLoop: Not a valid effect: x$/mu,
  ),
);

export const execStandardInputStep = makeTest(
  "a Task.exec standard input stream step that is not an effect fails the task instead of hanging the run",
  failsInsteadOfHanging(
    `Task.make({ name: "main", run: Task.exec("cat", [], { stdin: Stream.fromEffect(${NOT_AN_EFFECT}) }) })`,
    /^\[main\] error CommandStandardInputFailed: the standard input stream of cat failed with Fiber.runLoop: Not a valid effect: x$/mu,
  ),
);

export const childProcessStandardInputStep = makeTest(
  "a ChildProcess.make stdin stream step that is not an effect fails its command's exitCode instead of hanging the run",
  failsInsteadOfHanging(
    `Task.make({ name: "main", run: Effect.scoped(Effect.gen(function* () {
  const child = yield* ChildProcess.make("cat", [], { stdin: Stream.fromEffect(${NOT_AN_EFFECT}) });
  return yield* child.exitCode;
})) })`,
    /^\[main\] error TaskBodyFailed: its body failed with Fiber.runLoop: Not a valid effect: x$/mu,
  ),
);
