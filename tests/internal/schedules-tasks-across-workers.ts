import { expect } from "bun:test";

import { Effect, FileSystem } from "effect";

import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { readAttribute, readTraceDirectory, readTraces } from "../shared/read-trace-files.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

const MISSING_TIMESTAMP = 0n;

const WORKER_CAP = 4;

const REPEATED_RUN_COUNT = 3;

const IDLE_WORKER_RUN_STARTED_WORKER_COUNT = 2;

const MAIN_THREAD_BUSY_MILLISECONDS = 3000;

const readBodies = (trace: Effect.Success<ReturnType<typeof readTraces>>[number] | undefined) =>
  (trace?.spans ?? []).flatMap((span) => {
    const worker = readAttribute(span.attributes, "sshkeda_task.worker");
    const name = readAttribute(span.attributes, "sshkeda_task.task");
    const isTaskBodyOnAWorker =
      span.thread === "worker" && typeof worker === "number" && typeof name === "string";
    return isTaskBodyOnAWorker
      ? [
          {
            name,
            worker,
            start: BigInt(span.startTimeUnixNano),
            end: BigInt(span.endTimeUnixNano),
          },
        ]
      : [];
  });

const WAIT_FOR_EVERY_TASK_STARTED = `const waitForEveryTaskStarted = (taskName: string, taskCount: number) => Effect.callback<void>((resume) => {
  const startedChannel = new BroadcastChannel("started");
  const startedTaskNames = new Set([taskName]);
  const resumeWhenEveryTaskStarted = () => {
    if (startedTaskNames.size < taskCount) return;
    startedChannel.close();
    resume(Effect.void);
  };
  startedChannel.onmessage = (event) => {
    if (startedTaskNames.has(event.data)) return;
    startedTaskNames.add(event.data);
    startedChannel.postMessage(taskName);
    resumeWhenEveryTaskStarted();
  };
  startedChannel.postMessage(taskName);
  resumeWhenEveryTaskStarted();
});`;

export const sameWorker = makeTest(
  "with one worker, a body that yields another task lets that task run on the same worker",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const dependency = Task.make({ name: "dep", output: Schema.Finite, run: Effect.succeed(1) });
export default Task.make({ name: "main", run: Effect.gen(function* () { yield* Effect.log(\`got \${yield* dependency}\`); }) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "1" });
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain("[main] got 1");
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, ran));
      expect(
        readBodies(trace)
          .map((body) => `${body.name} on ${body.worker}`)
          .toSorted(),
      ).toEqual(["dep on 0", "main on 0"]);
    }),
  ),
);

const readStartedWorkers = (
  trace: Effect.Success<ReturnType<typeof readTraces>>[number] | undefined,
) =>
  readAttribute(
    trace?.findOnlySpan("runRootTaskFiber")?.attributes ?? [],
    "sshkeda_task.workers.started",
  );

export const fibers = makeTest(
  "with one worker, tasks run as fibers side by side on it, and a task waiting on another lets that task run on the same worker",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Deferred, Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
const slowStarted = Deferred.makeUnsafe<void>();
const otherStarted = Deferred.makeUnsafe<void>();
export const slow = Task.make({ name: "slow", output: Schema.String, run: Effect.gen(function* () {
  yield* Deferred.succeed(slowStarted, undefined);
  yield* Deferred.await(otherStarted);
  return "slow done";
}) });
export const other = Task.make({ name: "other", run: Effect.gen(function* () {
  yield* Deferred.succeed(otherStarted, undefined);
  yield* Deferred.await(slowStarted);
}) });
export const waiting = Task.make({ name: "waiting", run: Effect.gen(function* () { yield* Effect.log(\`got \${yield* slow}\`); }) });
export default Task.make({ name: "main", run: Task.all([waiting, other]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "1" });
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain("[waiting] got slow done");
      expect(ran.standardError).toMatch(/^passed: 4 tasks, [\d.]+s$/mu);
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, ran));
      expect(readStartedWorkers(trace)).toBe(1);
      const bodies = readBodies(trace);
      expect(bodies.map((body) => `${body.name} on ${body.worker}`).toSorted()).toEqual([
        "main on 0",
        "other on 0",
        "slow on 0",
        "waiting on 0",
      ]);
      const slowBody = bodies.find((body) => body.name === "slow");
      const otherBody = bodies.find((body) => body.name === "other");
      expect((slowBody?.start ?? MISSING_TIMESTAMP) < (otherBody?.end ?? MISSING_TIMESTAMP)).toBe(
        true,
      );
      expect((otherBody?.start ?? MISSING_TIMESTAMP) < (slowBody?.end ?? MISSING_TIMESTAMP)).toBe(
        true,
      );
    }),
  ),
);

export const started = makeTest(
  "workers start on first assignment and parallel tasks fill the configured worker limit, sharing the worker of a task that only waits on them",
  Effect.scoped(
    Effect.gen(function* () {
      const single = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.void });`,
      });
      const alone = yield* runTaskProcess(single, "main.ts");
      const [aloneTrace] = yield* readTraces(yield* readTraceDirectory(single, alone));
      expect(readStartedWorkers(aloneTrace)).toBe(1);
      expect(readBodies(aloneTrace).map((body) => body.worker)).toEqual([0]);

      const names = Array.from({ length: 20 }, (_unused, index) => `t${index}`);
      const files = {
        "main.ts": [
          'import { Effect } from "effect";',
          'import { Task } from "@sshkeda/task";',
          WAIT_FOR_EVERY_TASK_STARTED,
          ...names.map(
            (name) =>
              `export const ${name} = Task.make({ name: "${name}", run: waitForEveryTaskStarted("${name}", ${names.length}) });`,
          ),
          `export default Task.make({ name: "main", run: Task.all([${names.join(", ")}]) });`,
        ].join("\n"),
      };
      const capped = yield* makeProject(files);
      const cappedRun = yield* runTaskProcess(capped, "main.ts", {
        SSHKEDA_TASK_WORKERS: String(WORKER_CAP),
      });
      expect(cappedRun.exitCode).toBe(0);
      const [cappedTrace] = yield* readTraces(yield* readTraceDirectory(capped, cappedRun));
      expect(readStartedWorkers(cappedTrace)).toBe(WORKER_CAP);
      expect(readBodies(cappedTrace).filter((body) => body.worker >= WORKER_CAP)).toEqual([]);

      const roomy = yield* makeProject(files);
      const roomyRun = yield* runTaskProcess(roomy, "main.ts", { SSHKEDA_TASK_WORKERS: "50" });
      expect(roomyRun.exitCode).toBe(0);
      const [roomyTrace] = yield* readTraces(yield* readTraceDirectory(roomy, roomyRun));
      expect(readStartedWorkers(roomyTrace)).toBe(names.length);
    }),
  ),
);

export const readyWhileMainThreadBusy = makeTest(
  "a worker that reports ready while the main thread is still busy starting it runs its task",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Worker, isMainThread } from "node:worker_threads";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.log("ran") });
if (isMainThread) {
  const listen = Worker.prototype.on;
  const busyWorkers = new WeakSet();
  Worker.prototype.on = function (...listening) {
    const worker = listen.apply(this, listening);
    if (!busyWorkers.has(this)) {
      busyWorkers.add(this);
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ${MAIN_THREAD_BUSY_MILLISECONDS});
    }
    return worker;
  };
}`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", {
        SSHKEDA_TASK_SETUP_LIMIT: "20 seconds",
      });
      expect(ran.standardError).toContain("[main] ran");
      expect(ran.exitCode).toBe(0);
    }),
  ),
);

export const unusedDefinitions = makeTest(
  "unused task definitions do not start workers",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const unusedFirst = Task.make({ name: "unused-first", run: Effect.die("must not run") });
export const unusedSecond = Task.make({ name: "unused-second", run: Effect.die("must not run") });
export const unusedThird = Task.make({ name: "unused-third", run: Effect.die("must not run") });
export default Task.make({ name: "main", run: Effect.void });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "4" });
      expect(ran.exitCode).toBe(0);
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, ran));
      expect(readStartedWorkers(trace)).toBe(1);
      expect(readBodies(trace).map((body) => body.name)).toEqual(["main"]);
    }),
  ),
);

export const nestedDependencies = makeTest(
  "parallel and nested dependencies complete on one worker on every run",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
${WAIT_FOR_EVERY_TASK_STARTED}
export const first = Task.make({ name: "first", run: waitForEveryTaskStarted("first", 3) });
export const second = Task.make({ name: "second", run: waitForEveryTaskStarted("second", 3) });
export const third = Task.make({ name: "third", run: waitForEveryTaskStarted("third", 3) });
export const inner = Task.make({ name: "inner", run: Task.all([first, second, third]).pipe(Effect.andThen(Effect.log("inner finished after its dependencies"))) });
export default Task.make({ name: "main", run: inner });`,
      });
      yield* Effect.forEach(
        Array.from({ length: REPEATED_RUN_COUNT }),
        () =>
          Effect.gen(function* () {
            const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "1" });
            expect(ran.exitCode).toBe(0);
            expect(ran.standardError).toMatch(/^passed: 5 tasks, [\d.]+s$/mu);
            expect(ran.standardError).toContain("[inner] inner finished after its dependencies");
          }),
        { discard: true },
      );
    }),
  ),
);

export const oneWorker = makeTest(
  "trace files live under ~/.sshkeda-task, and SSHKEDA_TASK_WORKERS=1 runs every body on worker 0",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const names = ["a", "b", "c", "d"];
      const project = yield* makeProject({
        "main.ts": [
          'import { Effect } from "effect";',
          'import { Task } from "@sshkeda/task";',
          WAIT_FOR_EVERY_TASK_STARTED,
          ...names.map(
            (name) =>
              `export const ${name} = Task.make({ name: "${name}", run: waitForEveryTaskStarted("${name}", ${names.length}) });`,
          ),
          `export default Task.make({ name: "main", run: Task.all([${names.join(", ")}]) });`,
        ].join("\n"),
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "1" });
      expect(ran.exitCode).toBe(0);
      expect(yield* fileSystem.readDirectory(`${project.home}/.sshkeda-task`)).toEqual(["traces"]);
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, ran));
      expect(trace?.file.startsWith(`${project.home}/.sshkeda-task/traces/project/`)).toBe(true);
      const bodies = readBodies(trace);
      expect(bodies.map((body) => body.name).toSorted()).toEqual([...names, "main"]);
      expect(bodies.filter((body) => body.worker !== 0)).toEqual([]);
    }),
  ),
);

export const workerArguments = makeTest(
  "workers see the one task name a run was started with as their only command-line argument, so a task module that reads process.argv at top level loads in its worker",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { isMainThread } from "node:worker_threads";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const startedWith = JSON.stringify(process.argv.slice(2));
export const deploy = Task.make({ name: "deploy", run: Effect.log(\`deploying from \${isMainThread ? "main thread" : "worker"} with \${startedWith}\`) });
export const verify = Task.make({ name: "verify", run: Effect.log(\`verifying with \${startedWith}\`) });
export default Task.all([deploy, verify]);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", {}, project.directory, ["deploy"]);
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain('[deploy] deploying from worker with ["deploy"]\n');
      expect(ran.standardError).not.toContain("verifying");
      expect(ran.standardError).toMatch(/^passed: 1 task, [\d.]+s$/mu);
    }),
  ),
);

export const idleWorkerFirst = makeTest(
  "a task goes to a started worker no task occupies before a new worker starts",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
${WAIT_FOR_EVERY_TASK_STARTED}
export const first = Task.make({ name: "first", run: waitForEveryTaskStarted("first", 2) });
export const second = Task.make({ name: "second", run: waitForEveryTaskStarted("second", 2) });
export const third = Task.make({ name: "third", run: Effect.void });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Task.all([first, second]);
  yield* third;
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "4" });
      expect(ran.exitCode).toBe(0);
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, ran));
      expect(readStartedWorkers(trace)).toBe(IDLE_WORKER_RUN_STARTED_WORKER_COUNT);
      const workersByTaskName = new Map(readBodies(trace).map((body) => [body.name, body.worker]));
      expect(
        [workersByTaskName.get("first"), workersByTaskName.get("second")].toSorted(
          (left = -1, right = -1) => left - right,
        ),
      ).toEqual([0, 1]);
      expect(workersByTaskName.get("third")).toBe(0);
      expect(workersByTaskName.get("main")).toBe(0);
    }),
  ),
);

const SETUP_LIMIT_LONGER_THAN_A_WORKER_START = "5 seconds";

const BUSY_MILLISECONDS_LONGER_THAN_THE_SETUP_LIMIT = 7000;

export const waitingForBusyWorkerCountsTowardNoLimit = makeTest(
  "a task queued on a worker that other tasks occupy says so after 2s, naming the first task and a count of the rest, and that wait counts toward neither its timeout nor the setup limit",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const blocker = Task.make({ name: "blocker", run: Effect.sync(() => {
  const end = Date.now() + ${BUSY_MILLISECONDS_LONGER_THAN_THE_SETUP_LIMIT};
  while (Date.now() < end) {}
}) });
export const quick = Task.make({ name: "quick", run: Effect.log("quick ran"), timeout: "1 second", cleanup: "1 second" });
export const queued = Task.make({ name: "queued", run: Effect.log("queued ran"), timeout: "1 second", cleanup: "1 second" });
export default Task.make({ name: "main", run: Task.all([blocker, quick, queued]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", {
        SSHKEDA_TASK_WORKERS: "1",
        SSHKEDA_TASK_SETUP_LIMIT: SETUP_LIMIT_LONGER_THAN_A_WORKER_START,
      });
      expect(ran.standardError).toContain(
        '[quick] waiting for worker 0, which is running "blocker" and 1 other task',
      );
      expect(ran.standardError).not.toContain("TimedOut");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain("[quick] quick ran");
      expect(ran.standardError).toContain("[queued] queued ran");
    }),
  ),
);

const SETUP_LIMIT_SHORTER_THAN_THE_COMMANDS = "2 seconds";

const MAXIMUM_SECONDS_BEFORE_THE_COMMANDS_END = 15;

export const commandWaitsCountTowardSetupLimit = makeTest(
  "a task whose module hangs while loading on a worker where other tasks wait on commands fails at the setup limit, since that wait leaves the worker free",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "sleepers.ts": `import { Task } from "@sshkeda/task";
export const first = Task.make({ name: "first", run: Task.exec("sleep", ["20"]) });
export const second = Task.make({ name: "second", run: Task.exec("sleep", ["20"]) });
export const third = Task.make({ name: "third", run: Task.exec("sleep", ["20"]) });`,
        "hang.ts": `import { existsSync, writeFileSync } from "node:fs";
import { isMainThread } from "node:worker_threads";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
if (!isMainThread) {
  if (existsSync("loaded-in-a-worker")) {
    await new Promise(() => {});
  }
  writeFileSync("loaded-in-a-worker", "");
}
export const hang = Task.make({ name: "hang", run: Effect.void });`,
        "main.ts": `import { Effect, Fiber } from "effect";
import { Task } from "@sshkeda/task";
import { first, second, third } from "./sleepers.ts";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const sleepers = yield* Effect.forkChild(Task.all([first, second, third]));
  yield* Effect.sleep("1 second");
  const { hang } = yield* Effect.promise(() => import("./hang.ts"));
  yield* hang;
  yield* Fiber.join(sleepers);
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", {
        SSHKEDA_TASK_WORKERS: "2",
        SSHKEDA_TASK_SETUP_LIMIT: SETUP_LIMIT_SHORTER_THAN_THE_COMMANDS,
      });
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).not.toContain("[hang] waiting for worker");
      expect(ran.standardError).toContain(
        `[hang] error TaskSetupTimedOut: setup timed out after 2s while loading ${project.directory}/main.ts`,
      );
      const { seconds } =
        /^failed: hang \(1 of 5 tasks\), 4 stopped, (?<seconds>[\d.]+)s$/mu.exec(ran.standardError)
          ?.groups ?? {};
      expect(Number(seconds)).toBeLessThan(MAXIMUM_SECONDS_BEFORE_THE_COMMANDS_END);
    }),
  ),
);

export const loopWhileLoadingFailsAtSetupLimit = makeTest(
  "a task whose module loops forever while loading on a worker where another task waits to start fails at the setup limit, since no started body holds that worker",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "sleepers.ts": `import { Task } from "@sshkeda/task";
export const first = Task.make({ name: "first", run: Task.exec("sleep", ["20"]) });
export const second = Task.make({ name: "second", run: Task.exec("sleep", ["20"]) });
export const third = Task.make({ name: "third", run: Task.exec("sleep", ["20"]) });`,
        "loops.ts": `import { existsSync, writeFileSync } from "node:fs";
import { isMainThread } from "node:worker_threads";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
if (!isMainThread) {
  if (existsSync("loaded-in-a-worker")) {
    while (true) {}
  }
  writeFileSync("loaded-in-a-worker", "");
}
export const loops = Task.make({ name: "loops", run: Effect.void });`,
        "main.ts": `import { Task } from "@sshkeda/task";
import { first, second, third } from "./sleepers.ts";
import { loops } from "./loops.ts";
export default Task.make({ name: "main", run: Task.all([first, second, third, loops]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", {
        SSHKEDA_TASK_WORKERS: "2",
        SSHKEDA_TASK_SETUP_LIMIT: SETUP_LIMIT_SHORTER_THAN_THE_COMMANDS,
      });
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain("error TaskSetupTimedOut: setup timed out after 2s");
      const { seconds } =
        /^failed: .+, (?<seconds>[\d.]+)s$/mu.exec(ran.standardError)?.groups ?? {};
      expect(Number(seconds)).toBeLessThan(MAXIMUM_SECONDS_BEFORE_THE_COMMANDS_END);
    }),
  ),
);

export const blockedWhileLoadingLeavesSetupLimit = makeTest(
  "a task whose module awaits while loading on a worker that another task's body then blocks says so, and that block counts toward neither its setup limit nor its notice",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "heavy.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const heavy = Task.make({ name: "heavy", run: Effect.promise(() => new Promise((resolve) => setTimeout(resolve, 500))).pipe(
  Effect.andThen(Effect.sync(() => {
    const end = Date.now() + ${BUSY_MILLISECONDS_LONGER_THAN_THE_SETUP_LIMIT};
    while (Date.now() < end) {}
  })),
) });`,
        "awaits.ts": `import { isMainThread } from "node:worker_threads";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
if (!isMainThread) {
  await new Promise((resolve) => setTimeout(resolve, 1000));
}
export const awaits = Task.make({ name: "awaits", run: Effect.log("awaits ran") });`,
        "main.ts": `import { Effect, Fiber } from "effect";
import { Task } from "@sshkeda/task";
import { heavy } from "./heavy.ts";
export const first = Task.make({ name: "first", run: Task.exec("sleep", ["3"]) });
export const second = Task.make({ name: "second", run: Task.exec("sleep", ["3"]) });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const others = yield* Effect.forkChild(Task.all([first, heavy, second]));
  const { awaits } = yield* Effect.promise(() => import("./awaits.ts"));
  yield* awaits;
  yield* Fiber.join(others);
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", {
        SSHKEDA_TASK_WORKERS: "2",
        SSHKEDA_TASK_SETUP_LIMIT: SETUP_LIMIT_LONGER_THAN_A_WORKER_START,
      });
      expect(ran.standardError).toContain(
        '[awaits] waiting for worker 1, which is running "heavy"',
      );
      expect(ran.standardError).not.toContain("TimedOut");
      expect(ran.standardError).toContain("[awaits] awaits ran");
      expect(ran.exitCode).toBe(0);
    }),
  ),
);

export const dependencyWaitersLeaveWorkerFree = makeTest(
  "a worker whose tasks only wait on other tasks counts as free, so a new task goes there instead of to a blocked worker",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const blocker = Task.make({ name: "blocker", run: Effect.sync(() => {
  const end = Date.now() + 4000;
  while (Date.now() < end) {}
}) });
export const quick = Task.make({ name: "quick", run: Effect.log("quick ran"), timeout: "2 seconds", cleanup: "1 second" });
export const middle = Task.make({ name: "middle", run: Effect.gen(function* () {
  yield* Effect.sleep("300 millis");
  return yield* quick;
}) });
export default Task.make({ name: "main", run: Task.all([blocker, middle]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "2" });
      expect(ran.standardError).not.toContain("TaskTimedOut");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain("[quick] quick ran");
    }),
  ),
);

export const chainReusesWorker = makeTest(
  "in a chain where each task only waits on the next, every task runs on the first task's worker",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const third = Task.make({ name: "third", run: Effect.void });
export const second = Task.make({ name: "second", run: Effect.asVoid(third) });
export default Task.make({ name: "main", run: Effect.asVoid(second) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "4" });
      expect(ran.exitCode).toBe(0);
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, ran));
      expect(readStartedWorkers(trace)).toBe(1);
      expect(
        readBodies(trace)
          .map((body) => `${body.name} on ${body.worker}`)
          .toSorted(),
      ).toEqual(["main on 0", "second on 0", "third on 0"]);
    }),
  ),
);

export const servingWorkerSharedLast = makeTest(
  "a new task starts an unused worker before sharing the worker of a serving task",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const server = Task.make({ name: "server", run: Task.serve("up") });
export const after = Task.make({ name: "after", run: Effect.void });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* server;
  yield* after;
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "4" });
      expect(ran.exitCode).toBe(0);
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, ran));
      expect(
        readBodies(trace)
          .map((body) => `${body.name} on ${body.worker}`)
          .toSorted(),
      ).toEqual(["after on 1", "main on 0", "server on 0"]);
    }),
  ),
);

export const scale = makeTest(
  "2,000 tasks run cold on two workers",
  Effect.scoped(
    Effect.gen(function* () {
      const names = Array.from({ length: 2000 }, (_unused, index) => `t${index}`);
      const project = yield* makeProject({
        "main.ts": [
          'import { Effect } from "effect";',
          'import { Task } from "@sshkeda/task";',
          ...names.map(
            (name, index) =>
              `export const ${name} = Task.make({ name: "${name}", run: Effect.succeed(${index}) });`,
          ),
          `export default Task.make({ name: "all", run: Effect.all([${names.join(", ")}], { concurrency: "unbounded" }) });`,
        ].join("\n"),
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "2" });
      expect(ran.standardError).toMatch(/^passed: 2001 tasks, [\d.]+s$/mu);
    }),
  ),
  "2 minutes",
);
