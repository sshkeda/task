import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import { makeProject } from "../shared/make-projects.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

const NOT_AN_EFFECT = 'Effect.flatMap(Effect.void, () => "x" as never)';

const failsInsteadOfHanging = (taskSource: string, expectedError: RegExp) =>
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Stream } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export const main = ${taskSource};
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(expectedError);
      expect(ran.standardError).toMatch(/^failed: main \(1 of 1 task\), [\d.]+s$/mu);
    }),
  );

export const bodyStep = Task.make(
  "a task body step that is not an effect, as when types are bypassed, fails the task instead of hanging the run",
  failsInsteadOfHanging(
    `Task.make("main", ${NOT_AN_EFFECT})`,
    /^\[main\] error TaskBodyFailed: its body failed with Fiber.runLoop: Not a valid effect: x$/mu,
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const bodyFinalizerStep = Task.make(
  "a task body finalizer step that is not an effect fails the task instead of hanging the run",
  failsInsteadOfHanging(
    `Task.make("main", Effect.gen(function* () {
  yield* Effect.addFinalizer(() => ${NOT_AN_EFFECT});
  yield* Effect.log("body done");
}))`,
    /^\[main\] error TaskBodyFailed: its body failed with Fiber.runLoop: Not a valid effect: x$/mu,
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const execStandardInputStep = Task.make(
  "a Task.exec standard input stream step that is not an effect fails the task instead of hanging the run",
  failsInsteadOfHanging(
    `Task.make("main", Task.exec("cat", [], { stdin: Stream.fromEffect(${NOT_AN_EFFECT}) }))`,
    /^\[main\] error CommandStandardInputFailed: the standard input stream of cat failed with Fiber.runLoop: Not a valid effect: x$/mu,
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const childProcessStandardInputStep = Task.make(
  "a ChildProcess.make stdin stream step that is not an effect fails its command's exitCode instead of hanging the run",
  failsInsteadOfHanging(
    `Task.make("main", Effect.scoped(Effect.gen(function* () {
  const child = yield* ChildProcess.make("cat", [], { stdin: Stream.fromEffect(${NOT_AN_EFFECT}) });
  return yield* child.exitCode;
})))`,
    /^\[main\] error TaskBodyFailed: its body failed with Fiber.runLoop: Not a valid effect: x$/mu,
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);
