import { expect } from "bun:test";

import { Effect } from "effect";

import { countOccurrences } from "../shared/count-occurrences.ts";
import { EXIT_CODE_AFTER_SIGINT } from "../shared/exit-codes-after-stop-signals.ts";
import { findMisprintedErrorLines } from "../shared/find-misprinted-error-lines.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";

const projectWithMain = (main: string) =>
  makeProject({
    "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
${main}`,
  });

export const nested = makeTest(
  "a task made in a body awaits tasks made while loading and tasks made in other bodies, and each runs once",
  Effect.scoped(
    Effect.gen(function* () {
      const project =
        yield* projectWithMain(`export const lint = Task.make({ name: "lint", run: Effect.log("lint ran") });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const inner = Task.make({ name: "inner", output: Schema.Finite, run: Effect.gen(function* () { yield* lint; yield* Effect.log("inner ran"); return 2; }) });
  const outer = Task.make({ name: "outer", output: Schema.Finite, run: Effect.gen(function* () { yield* lint; return (yield* inner) + 1; }) });
  const [outerResult, innerResult] = yield* Effect.all([outer, inner], { concurrency: "unbounded" });
  yield* Effect.log(\`main got \${outerResult} and \${innerResult}\`);
}) });`);
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain("[main] main got 3 and 2");
      expect(countOccurrences(ran.standardError, "[lint] lint ran")).toBe(1);
      expect(countOccurrences(ran.standardError, "[inner] inner ran")).toBe(1);
      expect(ran.standardError).toMatch(/^passed: 4 tasks, [\d.]+s$/mu);
      expect(ran.exitCode).toBe(0);
    }),
  ),
);

export const oneWorker = makeTest(
  "tasks made in a body share the worker that made them, so they run with one worker",
  Effect.scoped(
    Effect.gen(function* () {
      const project =
        yield* projectWithMain(`const other = Task.make({ name: "other", run: Effect.log("other ran") });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const made = ["first", "second", "third"].map((name) => Task.make({ name, run: Effect.sleep("100 millis").pipe(Effect.andThen(Effect.log(\`\${name} ran\`))) }));
  yield* Task.all([other, ...made]);
}) });`);
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "1" });
      for (const name of ["other", "first", "second", "third"]) {
        expect(ran.standardError).toContain(`[${name}] ${name} ran`);
      }
      expect(ran.standardError).toMatch(/^passed: 5 tasks, [\d.]+s$/mu);
      expect(ran.exitCode).toBe(0);
    }),
  ),
);

export const madeTwiceInBodies = makeTest(
  "two bodies that make tasks with one name are refused at each Task.make() call, on one worker or two",
  Effect.scoped(
    Effect.gen(function* () {
      for (const workerCount of ["1", "2"]) {
        const project =
          yield* projectWithMain(`const first = Task.make({ name: "first", run: Effect.gen(function* () { yield* Task.make({ name: "made", run: Effect.log("made by first") }); }) });
const second = Task.make({ name: "second", run: Effect.gen(function* () { yield* Effect.sleep("300 millis"); yield* Task.make({ name: "made", run: Effect.log("made by second") }); }) });
export default Task.make({ name: "main", run: Task.all([first, second]) });`);
        const ran = yield* runTaskProcess(project, "main.ts", {
          SSHKEDA_TASK_WORKERS: workerCount,
        });
        expect(ran.exitCode).toBe(1);
        expect(ran.standardError).toContain(
          'main.ts:3:85: error TaskMadeMoreThanOnce: makes the task "made", which 2 Task.make() calls make\n',
        );
        expect(ran.standardError).toContain(
          'main.ts:4:122: error TaskMadeMoreThanOnce: makes the task "made", which 2 Task.make() calls make\n',
        );
        expect(ran.standardError).not.toContain("made by second");
        expect(findMisprintedErrorLines(ran.standardError)).toEqual([]);
      }
    }),
  ),
);

export const madeAgainInBody = makeTest(
  "a task made in a body with the name of a task made while loading is refused at both Task.make() calls",
  Effect.scoped(
    Effect.gen(function* () {
      const project =
        yield* projectWithMain(`export const lint = Task.make({ name: "lint", run: Effect.log("loaded lint ran") });
export default Task.make({ name: "main", run: Effect.gen(function* () { yield* Task.make({ name: "lint", run: Effect.log("body lint ran") }); }) });`);
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        'main.ts:3:26: error TaskMadeMoreThanOnce: makes the task "lint", which 2 Task.make() calls make\n',
      );
      expect(ran.standardError).toContain(
        'main.ts:4:85: error TaskMadeMoreThanOnce: makes the task "lint", which 2 Task.make() calls make\n',
      );
      expect(ran.standardError).not.toContain("lint ran");
    }),
  ),
);

export const brokenInBody = makeTest(
  "a task made in a body with invalid options is refused when that body awaits it",
  Effect.scoped(
    Effect.gen(function* () {
      const project =
        yield* projectWithMain(`export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Task.make({ name: "made", run: Effect.log("made ran"), timeout: -1 });
}) });`);
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "made: error TaskTimeoutInvalid: has the timeout -1, which is not a duration above 0\n",
      );
      expect(ran.standardError).not.toContain("made ran");
    }),
  ),
);

export const timedOutInBody = makeTest(
  "a task made in a body keeps its timeout",
  Effect.scoped(
    Effect.gen(function* () {
      const project =
        yield* projectWithMain(`export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Task.make({ name: "made", run: Effect.never, timeout: "1 second" });
}) });`);
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain("[made] error TaskTimedOut: timed out after 1s\n");
      expect(ran.standardError).toContain("[main] error AwaitedTaskFailed: made failed\n");
    }),
  ),
);

export const stoppedInBody = makeTest(
  "Ctrl-C stops a task made in a body and runs its finalizers",
  Effect.scoped(
    Effect.gen(function* () {
      const project =
        yield* projectWithMain(`export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Task.make({ name: "made", run: Effect.gen(function* () {
    yield* Effect.addFinalizer(() => Effect.log("made cleaned up"));
    yield* Effect.log("made sleeping");
    yield* Effect.never;
  }) });
}) });`);
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[made] made sleeping");
      yield* command.sendSignal("INT");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(exited.standardError).toContain("[made] made cleaned up");
      expect(exited.standardError).toMatch(
        /^failed: 0 of 2 tasks, 2 stopped, [\d.]+s\nerror StopRequested: received SIGINT$/mu,
      );
    }),
  ),
);

export const madeOutsideTheBodyFiber = makeTest(
  "a task made in a promise callback inside a body is still refused, since no task body was running when it was made",
  Effect.scoped(
    Effect.gen(function* () {
      const project =
        yield* projectWithMain(`export default Task.make({ name: "main", run: Effect.gen(function* () {
  const made = yield* Effect.promise(async () => {
    await Promise.resolve();
    return Task.make({ name: "made", run: Effect.log("made ran") });
  });
  yield* made;
}) });`);
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        '[main] error TaskNotMadeWhileLoading: the task "made" was not made while the task files loaded, so no worker thread can run it\n',
      );
      expect(ran.standardError).not.toContain("made ran");
    }),
  ),
);

export const importedByTwoBodies = makeTest(
  "a task in a file that bodies on two workers load with import() runs once rather than counting as made in a body",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "late.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const late = Task.make({ name: "late", run: Effect.sleep("300 millis").pipe(Effect.andThen(Effect.log("late ran"))) });`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const awaitLate = Effect.gen(function* () {
  const { late } = yield* Effect.promise(() => import("./late.ts"));
  yield* late;
});
const first = Task.make({ name: "first", run: awaitLate });
const second = Task.make({ name: "second", run: Effect.sleep("100 millis").pipe(Effect.andThen(awaitLate)) });
export default Task.make({ name: "main", run: Task.all([first, second]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "2" });
      expect(countOccurrences(ran.standardError, "[late] late ran")).toBe(1);
      expect(ran.standardError).toMatch(/^passed: 4 tasks, [\d.]+s$/mu);
      expect(ran.exitCode).toBe(0);
    }),
  ),
);
