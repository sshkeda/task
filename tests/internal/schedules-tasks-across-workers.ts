import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect, FileSystem, Schema } from "effect";

import { read } from "../shared/access-files.ts";
import { makeProject } from "../shared/make-projects.ts";
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

export const sameWorker = Task.make(
  "with one worker, a body that yields another task lets that task run on the same worker",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const dependency = Task.make("dep", Effect.succeed(1));
export const main = Task.make("main", Effect.gen(function* () { yield* Effect.log(\`got \${yield* dependency}\`); }));
if (import.meta.main) Task.main(main);`,
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

const readStartedWorkers = (
  trace: Effect.Success<ReturnType<typeof readTraces>>[number] | undefined,
) =>
  readAttribute(
    trace?.findOnlySpan("runRootTaskFiber")?.attributes ?? [],
    "sshkeda_task.workers.started",
  );

export const fibers = Task.make(
  "with one worker, tasks run as fibers side by side on it, and a task waiting on another lets that task run on the same worker",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Deferred, Effect } from "effect";
import { Task } from "@sshkeda/task";
const slowStarted = Deferred.makeUnsafe<void>();
const otherStarted = Deferred.makeUnsafe<void>();
export const slow = Task.make("slow", Effect.gen(function* () {
  yield* Deferred.succeed(slowStarted, undefined);
  yield* Deferred.await(otherStarted);
  return "slow done";
}));
export const other = Task.make("other", Effect.gen(function* () {
  yield* Deferred.succeed(otherStarted, undefined);
  yield* Deferred.await(slowStarted);
}));
export const waiting = Task.make("waiting", Effect.gen(function* () { yield* Effect.log(\`got \${yield* slow}\`); }));
export const main = Task.make("main", Task.all([waiting, other]));
if (import.meta.main) Task.main(main);`,
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const started = Task.make(
  "workers start on first assignment and parallel tasks fill the configured worker limit, sharing the worker of a task that only waits on them",
  Effect.scoped(
    Effect.gen(function* () {
      const single = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.void);
if (import.meta.main) Task.main(main);`,
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
              `export const ${name} = Task.make("${name}", waitForEveryTaskStarted("${name}", ${names.length}));`,
          ),
          `export const main = Task.make("main", Task.all([${names.join(", ")}]));`,
          "if (import.meta.main) Task.main(main);",
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const readyWhileMainThreadBusy = Task.make(
  "a worker that reports ready while the main thread is still busy starting it runs its task",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Worker } from "node:worker_threads";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.log("ran"));
if (import.meta.main) {
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
  Task.main(main);
}`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", {
        SSHKEDA_TASK_SETUP_LIMIT: "20 seconds",
      });
      expect(ran.standardError).toContain("[main] ran");
      expect(ran.exitCode).toBe(0);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const unusedDefinitions = Task.make(
  "unused task definitions do not start workers",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const unusedFirst = Task.make("unused-first", Effect.die("must not run"));
export const unusedSecond = Task.make("unused-second", Effect.die("must not run"));
export const unusedThird = Task.make("unused-third", Effect.die("must not run"));
export const main = Task.make("main", Effect.void);
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "4" });
      expect(ran.exitCode).toBe(0);
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, ran));
      expect(readStartedWorkers(trace)).toBe(1);
      expect(readBodies(trace).map((body) => body.name)).toEqual(["main"]);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const nestedDependencies = Task.make(
  "parallel and nested dependencies complete on one worker on every run",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
${WAIT_FOR_EVERY_TASK_STARTED}
export const first = Task.make("first", waitForEveryTaskStarted("first", 3));
export const second = Task.make("second", waitForEveryTaskStarted("second", 3));
export const third = Task.make("third", waitForEveryTaskStarted("third", 3));
export const inner = Task.make("inner", Task.all([first, second, third]).pipe(Effect.andThen(Effect.log("inner finished after its dependencies"))));
export const main = Task.make("main", inner);
if (import.meta.main) Task.main(main);`,
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

const MARK_IMPORT = (file: string) => `import { appendFileSync } from "node:fs";
import { isMainThread, threadId } from "node:worker_threads";
if (!isMainThread) appendFileSync("imports", \`\${threadId} ${file}\\n\`);`;

const MARK_RAN = (name: string) =>
  `Effect.sync(() => appendFileSync("ran", \`\${threadId} ${name}\\n\`))`;

export const ownFile = Task.make(
  "a worker imports only the files of the tasks it runs and what those files import, not the other task files in the run",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "server.ts": `${MARK_IMPORT("server.ts")}
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const server = Task.make("server", ${MARK_RAN("server")}.pipe(Effect.andThen(Task.serve("up"))));`,
        "leaf.ts": `${MARK_IMPORT("leaf.ts")}
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const leaf = Task.make("leaf", ${MARK_RAN("leaf")});`,
        "main.ts": `${MARK_IMPORT("main.ts")}
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { server } from "./server.ts";
import { leaf } from "./leaf.ts";
export const main = Task.make("main", ${MARK_RAN("main")}.pipe(Effect.andThen(server), Effect.andThen(leaf)));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "2" });
      expect(ran.exitCode).toBe(0);
      const readLines = (file: string) =>
        read(`${project.directory}/${file}`).pipe(
          Effect.map((text) =>
            text
              .trim()
              .split("\n")
              .map((line) => line.split(" ")),
          ),
          Effect.flatMap(
            Schema.decodeUnknownEffect(Schema.Array(Schema.Tuple([Schema.String, Schema.String]))),
          ),
        );
      const imports = yield* readLines("imports");
      const ranOn = yield* readLines("ran");
      const filesImportedByTask = new Map([
        ["main", ["leaf.ts", "main.ts", "server.ts"]],
        ["server", ["server.ts"]],
        ["leaf", ["leaf.ts"]],
      ]);
      for (const thread of new Set(ranOn.map(([ranThread]) => ranThread))) {
        const expected = new Set(
          ranOn.flatMap(([ranThread, name]) =>
            ranThread === thread ? (filesImportedByTask.get(name) ?? []) : [],
          ),
        );
        expect(
          imports
            .filter(([importThread]) => importThread === thread)
            .map(([, file]) => file)
            .toSorted((left, right) => left.localeCompare(right)),
        ).toEqual([...expected].toSorted((left, right) => left.localeCompare(right)));
      }
      expect(
        imports.filter(([thread]) => !ranOn.some(([ranThread]) => ranThread === thread)),
      ).toEqual([]);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const oneWorker = Task.make(
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
              `export const ${name} = Task.make("${name}", waitForEveryTaskStarted("${name}", ${names.length}));`,
          ),
          `export const main = Task.make("main", Task.all([${names.join(", ")}]));`,
          "if (import.meta.main) Task.main(main);",
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const workerArguments = Task.make(
  "workers see the run's command-line arguments, so a task module that reads process.argv at top level loads in its worker",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { isMainThread } from "node:worker_threads";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const target = process.argv[2];
if (target === undefined) throw new Error("usage: bun main.ts <target>");
export const deploy = Task.make("deploy", Effect.log(\`deploying \${target} from \${isMainThread ? "main thread" : "worker"} with \${JSON.stringify(process.argv.slice(2))}\`));
if (import.meta.main) Task.main(deploy);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", {}, project.directory, [
        "staging",
        "--dry-run",
      ]);
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain(
        '[deploy] deploying staging from worker with ["staging","--dry-run"]',
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const idleWorkerFirst = Task.make(
  "a task goes to a started worker no task occupies before a new worker starts",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
${WAIT_FOR_EVERY_TASK_STARTED}
export const first = Task.make("first", waitForEveryTaskStarted("first", 2));
export const second = Task.make("second", waitForEveryTaskStarted("second", 2));
export const third = Task.make("third", Effect.void);
export const main = Task.make("main", Effect.gen(function* () {
  yield* Task.all([first, second]);
  yield* third;
}));
if (import.meta.main) Task.main(main);`,
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

const SETUP_LIMIT_LONGER_THAN_A_WORKER_START = "5 seconds";

const BUSY_MILLISECONDS_LONGER_THAN_THE_SETUP_LIMIT = 7000;

export const waitingForBusyWorkerCountsTowardNoLimit = Task.make(
  "a task queued on a worker that other tasks occupy says so after 2s, naming the first task and a count of the rest, and that wait counts toward neither its timeout nor the setup limit",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const blocker = Task.make("blocker", Effect.sync(() => {
  const end = Date.now() + ${BUSY_MILLISECONDS_LONGER_THAN_THE_SETUP_LIMIT};
  while (Date.now() < end) {}
}));
export const quick = Task.make("quick", Effect.log("quick ran"), { timeout: "1 second", cleanup: "1 second" });
export const queued = Task.make("queued", Effect.log("queued ran"), { timeout: "1 second", cleanup: "1 second" });
export const main = Task.make("main", Task.all([blocker, quick, queued]));
if (import.meta.main) Task.main(main);`,
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const dependencyWaitersLeaveWorkerFree = Task.make(
  "a worker whose tasks only wait on other tasks counts as free, so a new task goes there instead of to a blocked worker",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const blocker = Task.make("blocker", Effect.sync(() => {
  const end = Date.now() + 4000;
  while (Date.now() < end) {}
}));
export const quick = Task.make("quick", Effect.log("quick ran"), { timeout: "2 seconds", cleanup: "1 second" });
export const middle = Task.make("middle", Effect.gen(function* () {
  yield* Effect.sleep("300 millis");
  return yield* quick;
}));
export const main = Task.make("main", Task.all([blocker, middle]));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "2" });
      expect(ran.standardError).not.toContain("TaskTimedOut");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain("[quick] quick ran");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const chainReusesWorker = Task.make(
  "in a chain where each task only waits on the next, every task runs on the first task's worker",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const third = Task.make("third", Effect.void);
export const second = Task.make("second", Effect.asVoid(third));
export const main = Task.make("main", Effect.asVoid(second));
if (import.meta.main) Task.main(main);`,
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const servingWorkerSharedLast = Task.make(
  "a new task starts an unused worker before sharing the worker of a serving task",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const server = Task.make("server", Task.serve("up"));
export const after = Task.make("after", Effect.void);
export const main = Task.make("main", Effect.gen(function* () {
  yield* server;
  yield* after;
}));
if (import.meta.main) Task.main(main);`,
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const scale = Task.make(
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
              `export const ${name} = Task.make("${name}", Effect.succeed(${index}));`,
          ),
          `export const all = Task.make("all", Effect.all([${names.join(", ")}], { concurrency: "unbounded" }));`,
          "if (import.meta.main) Task.main(all);",
        ].join("\n"),
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "2" });
      expect(ran.standardError).toMatch(/^passed: 2001 tasks, [\d.]+s$/mu);
    }),
  ),
  { timeout: "2 minutes", cleanup: "5 seconds" },
);
