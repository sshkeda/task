import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import { exists } from "../shared/access-files.ts";
import { countOccurrences } from "../shared/count-occurrences.ts";
import { EXIT_CODE_AFTER_SIGINT } from "../shared/exit-codes-after-stop-signals.ts";
import { failureAsValue } from "../shared/failure-as-value.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { readAttribute, readTraceDirectory, readTraces } from "../shared/read-trace-files.ts";
import {
  TASK_ENVIRONMENT_VARIABLE_PREFIX,
  runTaskProcess,
  startTaskProcess,
} from "../shared/run-task-processes.ts";
import { typeCheckSource } from "../shared/type-check-source.ts";

const SHELL_WAITING_FOR_RELEASE = `import { ChildProcess } from "effect/process";
const shell = (script: string) => ChildProcess.make("sh", ["-c", script]).pipe(Effect.flatMap((child) => child.exitCode), Effect.scoped);
const waitForRelease = (fifoName: string) => shell(\`cat \${fifoName}\`);
const blockUntilStopped = (taskName: string) => Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log(\`\${taskName} cleaned up\`));
  yield* Effect.log(\`\${taskName} waiting\`);
  yield* shell(\`echo > \${taskName}-waiting\`);
  yield* Effect.never;
});`;

const makeFifos = (
  project: Effect.Success<ReturnType<typeof makeProject>>,
  fifoNames: readonly string[],
) => Task.exec("mkfifo", [...fifoNames], { cwd: project.directory });

const release = (project: Effect.Success<ReturnType<typeof makeProject>>, fifoName: string) =>
  Task.exec("sh", ["-c", `echo > ${fifoName}`], { cwd: project.directory });

const STOPPED_LINE = "stopped: the run ended before it finished";

const fourSiblingsProject = (groupFunctionName: "all" | "allSettled") =>
  makeProject({
    "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
${SHELL_WAITING_FOR_RELEASE}
export const one = Task.make({ name: "one", run: Effect.gen(function* () {
  yield* waitForRelease("release-one");
  yield* Effect.log("one finished after the failures");
}) });
export const two = Task.make({ name: "two", run: Effect.fail(new TaskError({ message: "two broke" })) });
export const three = Task.make({ name: "three", run: Effect.gen(function* () {
  yield* waitForRelease("release-three");
  yield* Effect.log("three finished after the failures");
}) });
export const four = Task.make({ name: "four", run: Effect.fail(new TaskError({ message: "four broke" })) });
export default Task.make({ name: "main", run: Task.${groupFunctionName}([one, { two, three }, four]) });`,
  });

export const everySiblingReports = makeTest(
  "allSettled runs every sibling to completion, prints each failure and fails with a count",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* fourSiblingsProject("allSettled");
      yield* makeFifos(project, ["release-one", "release-three"]);
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("error TaskError: ");
      yield* command.waitForLine("error TaskError: ");
      yield* release(project, "release-one");
      yield* release(project, "release-three");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(1);
      expect(exited.standardError).toContain("[one] one finished after the failures");
      expect(exited.standardError).toContain("[three] three finished after the failures");
      expect(exited.standardError).toContain("[one] passed in");
      expect(exited.standardError).toContain("[three] passed in");
      expect(exited.standardError).toMatch(/^\[two\] \S+: error TaskError: two broke$/mu);
      expect(exited.standardError).toMatch(/^\[four\] \S+: error TaskError: four broke$/mu);
      expect(exited.standardError).toContain(
        "[main] error SettledTasksFailed: 2 of 4 tasks failed: two, four",
      );
      expect(exited.standardError).toMatch(/^failed: two, four \(2 of 5 tasks\), [\d.]+s$/mu);
    }),
  ),
);

export const passingGroup = makeTest(
  "allSettled whose tasks all pass passes",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const one = Task.make({ name: "one", run: Effect.succeed(1) });
export const two = Task.make({ name: "two", run: Effect.log("two ran") });
export default Task.make({ name: "main", run: Task.allSettled([{ one, two }, one]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain("[two] two ran");
      expect(ran.standardError).toMatch(/^passed: 3 tasks, [\d.]+s$/mu);
    }),
  ),
);

export const records = makeTest(
  "all and allSettled run every task in a record of tasks",
  Effect.scoped(
    Effect.gen(function* () {
      const source = `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const one = Task.make({ name: "one", run: Effect.log("one ran") });
export const two = Task.make({ name: "two", run: Effect.log("two ran") });
export const together = Task.make({ name: "together", run: Task.all({ one, two }) });
export const settled = Task.make({ name: "settled", run: Task.allSettled({ one, two }) });
const byName: Readonly<Record<string, Task.Task>> = { one, two };
export const named = Task.make({ name: "named", run: Task.all(byName) });
export default Task.make({ name: "main", run: Task.all([together, settled, named]) });`;
      yield* typeCheckSource(source);
      const project = yield* makeProject({ "main.ts": source });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain("[one] one ran");
      expect(ran.standardError).toContain("[two] two ran");
      expect(ran.standardError).toMatch(/^passed: 6 tasks, [\d.]+s$/mu);
    }),
  ),
);

export const notTaskLists = makeTest(
  "all and allSettled given something other than a list or record of tasks fail the calling task with TaskListNotGiven at the call, and no listed task starts",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const a = Task.make({ name: "a", run: Effect.promise(() => Bun.write("a-started", "")) });
export const together = Task.make({ name: "together", run: Task.all("name", [a]) });
export const settled = Task.make({ name: "settled", run: Task.allSettled(a) });
export const holes = Task.make({ name: "holes", run: Task.all([a, 5]) });
export const nothing = Task.make({ name: "nothing", run: Task.allSettled(undefined) });
export default Task.make({ name: "main", run: Task.allSettled([together, settled, holes, nothing]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain(
        '[together] main.ts:4:65: error TaskListNotGiven: Task.all() was called with "name", which is not a list or record of tasks\n',
      );
      expect(ran.standardError).toContain(
        '[settled] main.ts:5:63: error TaskListNotGiven: Task.allSettled() was called with task "a", which is not a list or record of tasks\n',
      );
      expect(ran.standardError).toContain(
        "[holes] main.ts:6:59: error TaskListNotGiven: Task.all() was called with an array, which is not a list or record of tasks\n",
      );
      expect(ran.standardError).toContain(
        "[nothing] main.ts:7:63: error TaskListNotGiven: Task.allSettled() was called with undefined, which is not a list or record of tasks\n",
      );
      expect(ran.standardError).toContain(
        "[main] error SettledTasksFailed: 4 of 4 tasks failed: together, settled, holes, nothing\n",
      );
      expect(ran.standardError).not.toContain("[a] started");
      expect(ran.standardOutput).toBe("");
      expect(ran.exitCode).toBe(1);
      expect(yield* exists(`${project.directory}/a-started`)).toBe(false);
    }),
  ),
);

export const failFastAll = makeTest(
  "all still ends the run at the first failure and reports the tasks it stopped",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* fourSiblingsProject("all");
      yield* makeFifos(project, ["release-one", "release-three"]);
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(
        /^\[(?:two|four)\] main\.ts:\d+:\d+: error TaskError: (?:two|four) broke$/mu,
      );
      expect(ran.standardError).toMatch(
        /^\[main\] error AwaitedTaskFailed: (?:two|four) failed$/mu,
      );
      expect(ran.standardError).not.toContain("finished after the failures");
      expect(ran.standardError).not.toContain("tasks failed");
      expect(ran.standardError).toContain(`[one] ${STOPPED_LINE}`);
      expect(ran.standardError).toContain(`[three] ${STOPPED_LINE}`);
      expect(ran.standardError).toMatch(
        /^failed: (?:two, four \(2 of 5 tasks\), 2 stopped|(?:two|four) \(1 of 5 tasks\), 3 stopped), [\d.]+s$/mu,
      );
    }),
  ),
);

export const interrupt = makeTest(
  "Ctrl-C during allSettled stops every task and exits 130",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
${SHELL_WAITING_FOR_RELEASE}
export const broken = Task.make({ name: "broken", run: Effect.gen(function* () {
  yield* waitForRelease("left-waiting");
  yield* waitForRelease("right-waiting");
  return yield* Effect.fail(new TaskError({ message: "broken broke" }));
}) });
export const left = Task.make({ name: "left", run: blockUntilStopped("left") });
export const right = Task.make({ name: "right", run: blockUntilStopped("right") });
export default Task.make({ name: "main", run: Task.allSettled([broken, left, right]) });`,
      });
      yield* makeFifos(project, ["left-waiting", "right-waiting"]);
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine(": error TaskError: broken broke");
      yield* command.sendSignal("INT");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
      for (const blockedTaskName of ["left", "right"]) {
        expect(exited.standardError).toContain(
          `[${blockedTaskName}] ${blockedTaskName} cleaned up`,
        );
        expect(exited.standardError).toContain(`[${blockedTaskName}] ${STOPPED_LINE}`);
      }
      expect(exited.standardError).toContain(`[main] ${STOPPED_LINE}`);
      expect(exited.standardError).not.toContain("tasks failed");
      expect(exited.standardError).toMatch(
        /^failed: broken \(1 of 4 tasks\), 3 stopped, [\d.]+s\nerror StopRequested: received SIGINT$/mu,
      );
    }),
  ),
);

export const allSettledInsideAll = makeTest(
  "allSettled inside all settles its own group, then all fails fast and stops the rest",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
${SHELL_WAITING_FOR_RELEASE}
export const build = Task.make({ name: "build", run: Effect.gen(function* () {
  yield* waitForRelease("blocked-waiting");
  return yield* Effect.fail(new TaskError({ message: "build broke" }));
}) });
export const lint = Task.make({ name: "lint", run: waitForRelease("release-lint") });
export const blocked = Task.make({ name: "blocked", run: blockUntilStopped("blocked") });
export const checks = Task.make({ name: "checks", run: Task.allSettled([build, lint]) });
export default Task.make({ name: "main", run: Task.all([checks, blocked]) });`,
      });
      yield* makeFifos(project, ["release-lint", "blocked-waiting"]);
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("error TaskError: build broke");
      yield* release(project, "release-lint");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(1);
      expect(exited.standardError).toContain("[lint] passed in");
      expect(exited.standardError).toContain(
        "[checks] error SettledTasksFailed: 1 of 2 tasks failed: build",
      );
      expect(exited.standardError).toContain("[main] error AwaitedTaskFailed: checks failed\n");
      expect(exited.standardError).toContain(
        `[blocked] blocked cleaned up\n[blocked] ${STOPPED_LINE}`,
      );
      expect(exited.standardError).toMatch(
        /^failed: build \(1 of 5 tasks\), 1 stopped, [\d.]+s$/mu,
      );
    }),
  ),
);

export const allInsideAllSettled = makeTest(
  "all inside allSettled fails its own task at the first failure while the other siblings run to completion, and the trace records each state",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
${SHELL_WAITING_FOR_RELEASE}
export const build = Task.make({ name: "build", run: Effect.gen(function* () {
  yield* waitForRelease("blocked-waiting");
  return yield* Effect.fail(new TaskError({ message: "build broke" }));
}) });
export const blocked = Task.make({ name: "blocked", run: blockUntilStopped("blocked") });
export const pipeline = Task.make({ name: "pipeline", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("pipeline stopped"));
  yield* Task.all([build, blocked]);
}) });
export const lint = Task.make({ name: "lint", run: waitForRelease("release-lint") });
export default Task.make({ name: "main", run: Task.allSettled([pipeline, lint]) });`,
      });
      yield* makeFifos(project, ["release-lint", "blocked-waiting"]);
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("error TaskError: build broke");
      yield* command.waitForLine("[pipeline] pipeline stopped");
      yield* release(project, "release-lint");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(1);
      expect(exited.standardError).toContain("[pipeline] error AwaitedTaskFailed: build failed\n");
      expect(exited.standardError).toContain("[lint] passed in");
      expect(exited.standardError).toContain(
        "[main] error SettledTasksFailed: 1 of 2 tasks failed: pipeline",
      );
      expect(exited.standardError).toContain(
        `[blocked] blocked cleaned up\n[blocked] ${STOPPED_LINE}`,
      );
      expect(exited.standardError).toMatch(
        /^failed: build \(1 of 5 tasks\), 1 stopped, [\d.]+s$/mu,
      );
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, exited));
      const recordedStates = Object.fromEntries(
        ["build", "pipeline", "blocked", "lint", "main"].map((taskName) => [
          taskName,
          readAttribute(
            trace?.findOnlySpan(`executeTaskFiber ${taskName}`)?.attributes ?? [],
            "sshkeda_task.state",
          ),
        ]),
      );
      expect(recordedStates).toEqual({
        build: "failed",
        pipeline: "failed",
        blocked: "cancelled",
        lint: "passed",
        main: "failed",
      });
    }),
  ),
);

export const awaitedFailFastElsewhere = makeTest(
  "a failed task that a task outside allSettled also yields fails that task, and the failure reaches the root",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
${SHELL_WAITING_FOR_RELEASE}
export const flaky = Task.make({ name: "flaky", run: Effect.gen(function* () {
  yield* waitForRelease("blocked-waiting");
  yield* waitForRelease("release-flaky");
  return yield* Effect.fail(new TaskError({ message: "flaky broke" }));
}) });
export const blocked = Task.make({ name: "blocked", run: blockUntilStopped("blocked") });
export const consumer = Task.make({ name: "consumer", run: Effect.gen(function* () {
  yield* Effect.log("consumer waiting");
  yield* flaky;
}) });
export const checks = Task.make({ name: "checks", run: Task.allSettled([flaky, blocked]) });
export default Task.make({ name: "main", run: Task.all([checks, consumer]) });`,
      });
      yield* makeFifos(project, ["blocked-waiting", "release-flaky"]);
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[consumer] consumer waiting");
      yield* release(project, "release-flaky");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(1);
      expect(exited.standardError).toMatch(/^\[flaky\] \S+: error TaskError: flaky broke$/mu);
      expect(exited.standardError).toContain("[consumer] error AwaitedTaskFailed: flaky failed");
      expect(exited.standardError).toContain("[main] error AwaitedTaskFailed: consumer failed");
      expect(exited.standardError).toContain(`[blocked] blocked cleaned up`);
      expect(exited.standardError).toContain(`[checks] ${STOPPED_LINE}`);
      expect(exited.standardError).not.toContain("tasks failed");
      expect(exited.standardError).toMatch(
        /^failed: flaky \(1 of 5 tasks\), 2 stopped, [\d.]+s$/mu,
      );
    }),
  ),
);

export const awaitedFailFastInsideGroup = makeTest(
  "a sibling that yields a failed sibling fails with a reason naming it, and the group counts both",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
${SHELL_WAITING_FOR_RELEASE}
export const flaky = Task.make({ name: "flaky", run: Effect.fail(new TaskError({ message: "flaky broke" })) });
export const consumer = Task.make({ name: "consumer", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("consumer stopped"));
  yield* flaky;
  yield* Effect.log("consumer continued");
}) });
export const lint = Task.make({ name: "lint", run: waitForRelease("release-lint") });
export default Task.make({ name: "main", run: Task.allSettled([flaky, consumer, lint]) });`,
      });
      yield* makeFifos(project, ["release-lint"]);
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("error TaskError: flaky broke");
      yield* command.waitForLine("[consumer] consumer stopped");
      yield* release(project, "release-lint");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(1);
      expect(exited.standardError).not.toContain("consumer continued");
      expect(exited.standardError).toContain("[lint] passed in");
      expect(exited.standardError).toContain("[consumer] error AwaitedTaskFailed: flaky failed\n");
      expect(exited.standardError).toContain(
        "[main] error SettledTasksFailed: 2 of 3 tasks failed: flaky, consumer",
      );
      expect(countOccurrences(exited.standardError, ": error TaskError: flaky broke")).toBe(1);
      expect(exited.standardError).toMatch(/^failed: flaky \(1 of 4 tasks\), [\d.]+s$/mu);
    }),
  ),
);

export const timedOut = makeTest(
  "a task that times out inside allSettled is stopped, its siblings finish, and allSettled fails with it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
${SHELL_WAITING_FOR_RELEASE}
export const broken = Task.make({ name: "broken", run: Effect.fail(new TaskError({ message: "broken broke" })) });
export const slow = Task.make({ name: "slow", run: blockUntilStopped("slow"), timeout: "300 millis", cleanup: "5 seconds" });
export const later = Task.make({ name: "later", run: Effect.gen(function* () {
  yield* waitForRelease("release-later");
  yield* Effect.log("later finished after the timeout");
}) });
export default Task.make({ name: "main", run: Task.allSettled([broken, slow, later]) });`,
      });
      yield* makeFifos(project, ["release-later"]);
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[slow] error TaskTimedOut: timed out after 300ms");
      yield* command.waitForLine("[slow] slow cleaned up");
      yield* release(project, "release-later");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(1);
      expect(exited.standardError).toContain("[later] later finished after the timeout");
      expect(exited.standardError).toContain("[later] passed in");
      expect(exited.standardError).toContain(
        "[main] error SettledTasksFailed: 2 of 3 tasks failed: broken, slow",
      );
      expect(exited.standardError).not.toContain(STOPPED_LINE);
      expect(exited.standardError).toMatch(/^failed: broken, slow \(2 of 4 tasks\), [\d.]+s$/mu);
    }),
  ),
);

export const timeoutReachesAllSettledBeforeCleanup = makeTest(
  "a task that times out inside allSettled fails it before the task's cleanup ends, so code that catches that failure runs even when the cleanup outlasts its limit",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const slow = Task.make({ name: "slow", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("cleanup started").pipe(Effect.andThen(Effect.never)));
  return yield* Effect.never;
}), timeout: "300 millis", cleanup: "2 seconds" });
export default Task.make({ name: "main", run: Task.allSettled([slow]).pipe(
  Effect.catchTag("SettledTasksFailed", (failure) => Effect.log(\`main caught \${failure.failedTaskNames.join(", ")}\`)),
) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      const lines = ran.standardError.split("\n");
      expect(lines).toContain("[slow] error TaskTimedOut: timed out after 300ms");
      expect(lines.indexOf("[main] main caught slow")).toBeGreaterThan(-1);
      expect(lines.indexOf("[main] main caught slow")).toBeLessThan(
        lines.indexOf(
          "[slow] error TaskCleanupTimedOut: its cleanup did not finish within 2s, so the run stopped waiting for it",
        ),
      );
      expect(ran.standardError).not.toContain(STOPPED_LINE);
    }),
  ),
);

export const inferredTypes = makeTest(
  "allSettled infers its result and error types",
  Effect.scoped(
    Effect.gen(function* () {
      yield* typeCheckSource(`import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const a = Task.make({ name: "a", run: Effect.void });
export const b = Task.make({ name: "b", run: Effect.void });
export default Task.make({ name: "main", run: Task.allSettled([a, { b }]).pipe(
  Effect.catchTag("SettledTasksFailed", (failure) => Effect.log(failure.settledTaskCount, failure.failedTaskNames)),
  Effect.catchTag("TaskWaitsOnItself", (failure) => Effect.log(failure.message)),
  Effect.flatMap((result: void) => Effect.succeed(result)),
) });
export const sameValueAsAll: Effect.Success<ReturnType<typeof Task.allSettled>> = undefined as unknown as Effect.Success<ReturnType<typeof Task.all>>;`);
      const failed = yield* failureAsValue(
        typeCheckSource(`import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const a = Task.make({ name: "a", run: Effect.void });
export default Task.make({ name: "main", run: Task.allSettled([a]).pipe(Effect.catchTag("CommandFailed", () => Effect.void)) });`),
      );
      expect(failed.message).toContain("main.ts(4,");
      expect(failed.message).toContain('"CommandFailed"');
    }),
  ),
);

export const caughtDependencyFailure = makeTest(
  "a body that catches a failed dependency handles it, and the run passes with the failure counted",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export const broken = Task.make({ name: "broken", run: Effect.fail(new TaskError({ message: "broken broke" })) });
export default Task.make({ name: "main", run: broken.pipe(
  Effect.catchTag("AwaitedTaskFailed", (failure) => Effect.log(\`recovered from \${failure.awaitedTaskName}\`)),
) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toMatch(/^\[broken\] \S+: error TaskError: broken broke$/mu);
      expect(ran.standardError).toContain("[main] recovered from broken");
      expect(ran.standardError).toContain("[main] passed in");
      expect(ran.standardError).toMatch(/^passed: 2 tasks, 1 caught failure, [\d.]+s$/mu);
    }),
  ),
);

export const caughtSettledTasksFailed = makeTest(
  "a caught SettledTasksFailed lets the run pass and reports every task still running as stopped",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
${SHELL_WAITING_FOR_RELEASE}
export const broken = Task.make({ name: "broken", run: Effect.fail(new TaskError({ message: "broken broke" })) });
export const blocked = Task.make({ name: "blocked", run: blockUntilStopped("blocked") });
export const starter = Task.make({ name: "starter", run: Effect.gen(function* () {
  yield* Effect.forkChild(blocked);
  yield* waitForRelease("blocked-waiting");
}) });
export default Task.make({ name: "main", run: Task.allSettled([broken, starter]).pipe(
  Effect.catchTag("SettledTasksFailed", (failure) => Effect.log(\`handled \${failure.message}\`)),
) });`,
      });
      yield* makeFifos(project, ["blocked-waiting"]);
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toMatch(/^\[broken\] \S+: error TaskError: broken broke$/mu);
      expect(ran.standardError).toContain("[main] handled 1 of 2 tasks failed");
      expect(ran.standardError).toContain(
        `[blocked] blocked cleaned up\n[blocked] ${STOPPED_LINE}`,
      );
      expect(ran.standardError).toMatch(
        /^passed: 4 tasks, 1 caught failure, 1 stopped, [\d.]+s$/mu,
      );
    }),
  ),
);

export const unawaitedFailure = makeTest(
  "a failure that no task awaited fails the run even when the root passes",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
${SHELL_WAITING_FOR_RELEASE}
export const build = Task.make({ name: "build", run: Effect.fail(new TaskError({ message: "build broke" })) });
export const late = Task.make({ name: "late", run: Effect.gen(function* () {
  yield* waitForRelease("release-late");
  return yield* Effect.fail(new TaskError({ message: "late broke" }));
}) });
export const pipeline = Task.make({ name: "pipeline", run: Task.all([build, late]) });
export const lint = Task.make({ name: "lint", run: waitForRelease("release-lint") });
export default Task.make({ name: "main", run: Task.allSettled([pipeline, lint]).pipe(
  Effect.catchTag("SettledTasksFailed", (failure) => Effect.log(\`handled \${failure.message}\`)),
) });`,
      });
      yield* makeFifos(project, ["release-late", "release-lint"]);
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[pipeline] error AwaitedTaskFailed: build failed");
      yield* release(project, "release-late");
      yield* command.waitForLine("error TaskError: late broke");
      yield* release(project, "release-lint");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(1);
      expect(exited.standardError).toContain("[main] handled 1 of 2 tasks failed");
      expect(exited.standardError).toContain("[main] passed in");
      expect(exited.standardError).toMatch(
        /^failed: build, late \(2 of 5 tasks\), [\d.]+s\nerror UnawaitedTasksFailed: no task awaited the failure of late$/mu,
      );
    }),
  ),
);

export const settledInBody = makeTest(
  "allSettled in a body settles every task and fails the body with the count",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export const ok = Task.make({ name: "ok", run: Effect.log("ok ran") });
export const broken = Task.make({ name: "broken", run: Effect.fail(new TaskError({ message: "broken broke" })) });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Task.allSettled([ok, broken]);
  yield* Effect.log("main body ran");
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain("[ok] passed in");
      expect(ran.standardError).toMatch(/^\[broken\] \S+: error TaskError: broken broke$/mu);
      expect(ran.standardError).toContain(
        "[main] error SettledTasksFailed: 1 of 2 tasks failed: broken",
      );
      expect(ran.standardError).not.toContain("main body ran");
      expect(ran.standardError).toMatch(/^failed: broken \(1 of 3 tasks\), [\d.]+s$/mu);
    }),
  ),
);

export const servedMember = makeTest(
  "a serving task inside allSettled settles once it serves and stays up until the run ends",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const server = Task.make({ name: "server", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("server stopped"));
  return yield* Task.serve("up");
}), cleanup: "5 seconds" });
export const ok = Task.make({ name: "ok", run: Effect.log("ok ran") });
export default Task.make({ name: "main", run: Task.allSettled([server, ok]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain("[server] serving after");
      expect(ran.standardError).toContain("[main] passed in");
      expect(ran.standardError).toContain("[server] server stopped");
      expect(ran.standardError).not.toContain(STOPPED_LINE);
      expect(ran.standardError).toMatch(/^passed: 3 tasks, [\d.]+s$/mu);
    }),
  ),
);

export const abandonedRequest = makeTest(
  "a dependency the awaiting body stopped waiting for fails the run when it fails later",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
${SHELL_WAITING_FOR_RELEASE}
export const dependency = Task.make({ name: "dependency", run: Effect.gen(function* () {
  yield* shell("echo > dependency-started");
  yield* waitForRelease("release-dependency");
  return yield* Effect.fail(new TaskError({ message: "dependency broke" }));
}) });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* dependency.pipe(Effect.raceFirst(waitForRelease("dependency-started")));
  yield* Effect.log("main stopped waiting");
  yield* waitForRelease("release-main");
}) });`,
      });
      yield* makeFifos(project, ["dependency-started", "release-dependency", "release-main"]);
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[main] main stopped waiting");
      yield* release(project, "release-dependency");
      yield* command.waitForLine(": error TaskError: dependency broke");
      yield* release(project, "release-main");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(1);
      expect(exited.standardError).toContain("[main] passed in");
      expect(exited.standardError).toMatch(
        /^failed: dependency \(1 of 2 tasks\), [\d.]+s\nerror UnawaitedTasksFailed: no task awaited the failure of dependency$/mu,
      );
    }),
  ),
);

export const promptFailFast = makeTest(
  "a failure reaches the root before the failing body's finalizers finish, so all stops the siblings at once",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
${SHELL_WAITING_FOR_RELEASE}
export const broken = Task.make({ name: "broken", run: Effect.gen(function* () {
  yield* waitForRelease("sibling-waiting");
  return yield* Effect.fail(new TaskError({ message: "broken broke" }));
}) });
export const middle = Task.make({ name: "middle", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => waitForRelease("release-middle-finalizer").pipe(Effect.andThen(Effect.log("middle finalizer finished"))));
  yield* broken;
}) });
export const sibling = Task.make({ name: "sibling", run: blockUntilStopped("sibling") });
export default Task.make({ name: "main", run: Task.all([middle, sibling]) });`,
      });
      yield* makeFifos(project, ["sibling-waiting", "release-middle-finalizer"]);
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[sibling] sibling cleaned up");
      yield* release(project, "release-middle-finalizer");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(1);
      expect(exited.standardError).toContain("[middle] error AwaitedTaskFailed: broken failed");
      expect(exited.standardError).toContain("[main] error AwaitedTaskFailed: middle failed");
      expect(exited.standardError).toContain("[middle] middle finalizer finished");
      expect(exited.standardError.indexOf("[sibling] sibling cleaned up")).toBeLessThan(
        exited.standardError.indexOf("[middle] middle finalizer finished"),
      );
      expect(exited.standardError).toMatch(
        /^failed: broken \(1 of 4 tasks\), 1 stopped, [\d.]+s$/mu,
      );
    }),
  ),
);

const INTERRUPT_OFFSET_COUNT = 16;

const EARLIEST_INTERRUPT_OFFSET_IN_MILLISECONDS = -5;

const INTERRUPT_OFFSETS_FROM_THE_FAILURE_IN_MILLISECONDS = Array.from(
  { length: INTERRUPT_OFFSET_COUNT },
  (_unused, index) => index + EARLIEST_INTERRUPT_OFFSET_IN_MILLISECONDS,
);

const describeInterruptOffset = (interruptOffsetMilliseconds: number) =>
  interruptOffsetMilliseconds < 0
    ? `before-${-interruptOffsetMilliseconds}`
    : `after-${interruptOffsetMilliseconds}`;

export const abandonedRequestSweep = makeTest(
  "a dependency failure that races the awaiting body giving up on it never lets the run pass",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject(
        Object.fromEntries(
          INTERRUPT_OFFSETS_FROM_THE_FAILURE_IN_MILLISECONDS.map((interruptOffsetMilliseconds) => {
            const offsetName = describeInterruptOffset(interruptOffsetMilliseconds);
            const failGivingUp =
              interruptOffsetMilliseconds < 0
                ? `yield* Fiber.interrupt(requestFiber);
  yield* Effect.sleep("${-interruptOffsetMilliseconds} millis");
  raceChannel.postMessage("fail now");`
                : `raceChannel.postMessage("fail now");
  yield* Effect.sleep("${interruptOffsetMilliseconds} millis");
  yield* Fiber.interrupt(requestFiber);`;
            return [
              `main-${offsetName}.ts`,
              `import { Effect, Exit, Fiber } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
${SHELL_WAITING_FOR_RELEASE}
export const dependency = Task.make({ name: "dependency", run: Effect.callback<void>((resume) => {
  const raceChannel = new BroadcastChannel("race-${offsetName}");
  raceChannel.onmessage = (event) => {
    if (event.data === "main joined") raceChannel.postMessage("dependency joined");
    if (event.data === "fail now") {
      raceChannel.close();
      resume(Effect.void);
    }
  };
  raceChannel.postMessage("dependency joined");
}).pipe(Effect.andThen(Effect.fail(new TaskError({ message: "dependency broke" })))) });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const raceChannel = new BroadcastChannel("race-${offsetName}");
  const dependencyJoined = new Promise((resolve) => {
    raceChannel.onmessage = (event) => {
      if (event.data === "dependency joined") resolve(undefined);
    };
  });
  raceChannel.postMessage("main joined");
  const requestFiber = yield* Effect.forkChild(Effect.exit(dependency));
  yield* Effect.promise(() => dependencyJoined);
  ${failGivingUp}
  raceChannel.close();
  const requestExit = yield* Fiber.await(requestFiber);
  yield* waitForRelease("release-main-${offsetName}");
  if (Exit.isSuccess(requestExit)) {
    return yield* requestExit.value;
  }
}) });`,
            ];
          }),
        ),
      );
      yield* makeFifos(
        project,
        INTERRUPT_OFFSETS_FROM_THE_FAILURE_IN_MILLISECONDS.map(
          (interruptOffsetMilliseconds) =>
            `release-main-${describeInterruptOffset(interruptOffsetMilliseconds)}`,
        ),
      );
      const exitCodes = yield* Effect.forEach(
        INTERRUPT_OFFSETS_FROM_THE_FAILURE_IN_MILLISECONDS,
        (interruptOffsetMilliseconds) =>
          Effect.scoped(
            Effect.gen(function* () {
              const offsetName = describeInterruptOffset(interruptOffsetMilliseconds);
              const command = yield* startTaskProcess(project, `main-${offsetName}.ts`);
              yield* command.waitForLine(": error TaskError: dependency broke");
              yield* release(project, `release-main-${offsetName}`);
              const exited = yield* command.waitForExit;
              return { offsetName, exitCode: exited.exitCode };
            }),
          ),
        { concurrency: "unbounded" },
      );
      expect(exitCodes).toEqual(
        INTERRUPT_OFFSETS_FROM_THE_FAILURE_IN_MILLISECONDS.map((interruptOffsetMilliseconds) => ({
          offsetName: describeInterruptOffset(interruptOffsetMilliseconds),
          exitCode: 1,
        })),
      );
    }),
  ),
  "2 minutes",
);

const brokenTaskWithFailingCleanup = (cleanupEffect: string, mainBody: string) =>
  makeProject({
    "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export const broken = Task.make({ name: "broken", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => ${cleanupEffect});
  return yield* Effect.fail(new TaskError({ message: "broken broke" }));
}) });
export default Task.make({ name: "main", run: ${mainBody} });`,
  });

const CATCHING_MAIN_BODY = `broken.pipe(Effect.catchTag("AwaitedTaskFailed", () => Effect.log("main caught it")))`;

export const cleanupDefectAfterCaughtFailure = makeTest(
  "a cleanup that dies after the body failed fails the run even when the failure was caught",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* brokenTaskWithFailingCleanup(
        `Effect.die(new Error("cleanup broke"))`,
        CATCHING_MAIN_BODY,
      );
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(/^\[broken\] \S+: error TaskError: broken broke$/mu);
      expect(ran.standardError).toMatch(
        /^\[broken\] \S+: error TaskCleanupFailed: its cleanup failed with cleanup broke$/mu,
      );
      expect(ran.standardError).toMatch(
        /^failed: broken \(1 of 2 tasks\)(?:, 1 stopped)?, [\d.]+s$/mu,
      );
    }),
  ),
);

export const cleanupDefectAfterReturn = makeTest(
  "a cleanup that dies after the body returned fails the task as a cleanup failure, not a body failure",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.die(new Error("cleanup broke")));
  return 1;
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(
        /^\[main\] \S+: error TaskCleanupFailed: its cleanup failed with cleanup broke$/mu,
      );
      expect(ran.standardError).not.toContain("TaskBodyFailed");
    }),
  ),
);

export const workerExitDuringCleanupAfterCaughtFailure = makeTest(
  "a worker that exits during cleanup after the body failed fails the run even when the failure was caught",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* brokenTaskWithFailingCleanup(
        "Effect.sync(() => process.exit(3))",
        CATCHING_MAIN_BODY,
      );
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(/^\[broken\] \S+: error TaskError: broken broke$/mu);
      expect(ran.standardError).toContain(
        "[broken] error WorkerExited: its worker exited on its own with code 3",
      );
    }),
  ),
);

export const cleanupDefectAfterUncaughtFailure = makeTest(
  "a cleanup that dies after an uncaught body failure prints both errors",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* brokenTaskWithFailingCleanup(
        `Effect.die(new Error("cleanup broke"))`,
        "broken",
      );
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(/^\[broken\] \S+: error TaskError: broken broke$/mu);
      expect(ran.standardError).toMatch(
        /^\[broken\] \S+: error TaskCleanupFailed: its cleanup failed with cleanup broke$/mu,
      );
      expect(ran.standardError).toMatch(
        /^\[main\] (?:error AwaitedTaskFailed: broken failed|stopped: the run ended before it finished)$/mu,
      );
    }),
  ),
);

export const timeoutCoversCleanupAfterFailure = makeTest(
  "a timeout that runs out while a failed task's finalizer hangs reports that task as timed out, and the task that caught its failure is stopped",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
${SHELL_WAITING_FOR_RELEASE}
export const broken = Task.make({ name: "broken", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("cleanup started").pipe(Effect.andThen(Effect.never)));
  return yield* Effect.fail(new TaskError({ message: "broken broke" }));
}), timeout: "300 millis", cleanup: "300 millis" });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* broken.pipe(Effect.catchTag("AwaitedTaskFailed", () => Effect.log("main caught it")));
  yield* waitForRelease("release-main");
}) });`,
      });
      yield* makeFifos(project, ["release-main"]);
      const ran = yield* runTaskProcess(project, "main.ts", {
        [`${TASK_ENVIRONMENT_VARIABLE_PREFIX}_WORKERS`]: "1",
      });
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain("[broken] cleanup started");
      expect(ran.standardError).toContain("[main] main caught it");
      expect(ran.standardError).toContain("[broken] error TaskTimedOut: timed out after 300ms");
      expect(ran.standardError).toContain(`[main] ${STOPPED_LINE}`);
    }),
  ),
);

export const workerExitDuringCleanupAfterUncaughtFailure = makeTest(
  "a worker that exits during cleanup after an uncaught body failure prints the worker exit",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* brokenTaskWithFailingCleanup(
        "Effect.sync(() => process.exit(3))",
        "broken",
      );
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(/^\[broken\] \S+: error TaskError: broken broke$/mu);
      expect(ran.standardError).toContain(
        "[broken] error WorkerExited: its worker exited on its own with code 3",
      );
    }),
  ),
);
