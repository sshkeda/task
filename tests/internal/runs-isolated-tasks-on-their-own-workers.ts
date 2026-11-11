import { expect } from "bun:test";

import { Effect } from "effect";

import { EXIT_CODE_AFTER_SIGINT } from "../shared/exit-codes-after-stop-signals.ts";
import { failureAsValue } from "../shared/failure-as-value.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { readAttribute, readTraceDirectory, readTraces } from "../shared/read-trace-files.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";
import { typeCheckSource } from "../shared/type-check-source.ts";

const ONE_WORKER = { SSHKEDA_TASK_WORKERS: "1" } as const;

const ISOLATED_WORKER_COUNT_WITH_TWO_ISOLATED_TASKS = 3;

const MISSING_TIMESTAMP = 0n;

const listTaskBodiesOnWorkers = (
  trace: Effect.Success<ReturnType<typeof readTraces>>[number] | undefined,
) =>
  (trace?.spans ?? []).flatMap((span) => {
    const worker = readAttribute(span.attributes, "sshkeda_task.worker");
    const name = readAttribute(span.attributes, "sshkeda_task.task");
    const isTaskBodyOnAWorker =
      span.thread === "worker" && typeof worker === "number" && typeof name === "string";
    return isTaskBodyOnAWorker
      ? [
          {
            label: `${name} on ${worker}`,
            start: BigInt(span.startTimeUnixNano),
            end: BigInt(span.endTimeUnixNano),
          },
        ]
      : [];
  });

const makeChangingProject = (isolated: boolean) =>
  makeProject({
    "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const changer = Task.make({
  name: "changer",
  isolated: ${String(isolated)},
  run: Effect.sync(() => {
    const original = Error.prepareStackTrace;
    Error.prepareStackTrace = function replaced(error, frames) {
      return original === undefined ? \`\${String(error)}\\n    at \${frames.join("\\n    at ")}\` : original(error, frames);
    };
    globalThis.changedByTask = "changer";
  }),
});
const reader = Task.make({
  name: "reader",
  run: Effect.suspend(() =>
    Effect.log(\`prepareStackTrace replaced: \${Error.prepareStackTrace?.name === "replaced"}, global: \${String(globalThis.changedByTask)}\`),
  ),
});
export default Task.make({
  name: "main",
  run: Effect.gen(function* () {
    yield* changer;
    yield* reader;
  }),
});
`,
  });

export const sharedWorkerKeepsChanges = makeTest(
  "with one worker, a task that replaces Error.prepareStackTrace and sets a global leaves both for the next task on the shared worker",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runTaskProcess(yield* makeChangingProject(false), "main.ts", ONE_WORKER);
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain(
        "[reader] prepareStackTrace replaced: true, global: changer\n",
      );
    }),
  ),
);

export const isolatedWorkerDropsChanges = makeTest(
  "with one worker, the same task made with isolated: true runs on its own worker, and the next task sees neither change",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeChangingProject(true);
      const ran = yield* runTaskProcess(project, "main.ts", ONE_WORKER);
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain(
        "[reader] prepareStackTrace replaced: false, global: undefined\n",
      );
      expect(ran.standardError).toMatch(/^passed: 3 tasks, [\d.]+s$/mu);
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, ran));
      expect(
        listTaskBodiesOnWorkers(trace)
          .map((body) => body.label)
          .toSorted((left, right) => left.localeCompare(right)),
      ).toEqual(["changer on 1", "main on 0", "reader on 0"]);
    }),
  ),
);

export const isolatedWorkerEnds = makeTest(
  "an isolated task's worker exits after the task ends while the run goes on, and the output it printed last is kept",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { existsSync, watch, writeFileSync } from "node:fs";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const awaitFile = (path: string) => Effect.callback<void>((resume) => {
  const watcher = watch(".");
  const resumeIfWritten = () => {
    if (!existsSync(path)) return;
    watcher.close();
    resume(Effect.void);
  };
  watcher.on("change", resumeIfWritten);
  resumeIfWritten();
});
const compile = Task.make({
  name: "compile",
  isolated: true,
  run: Effect.sync(() => {
    process.on("exit", () => writeFileSync("isolated-worker-exited", ""));
    console.log("compile printed this last");
  }),
});
const after = Task.make({
  name: "after",
  run: Effect.gen(function* () {
    yield* awaitFile("isolated-worker-exited");
    yield* Effect.log("the isolated worker exited");
  }),
});
export default Task.make({
  name: "main",
  run: Effect.gen(function* () {
    yield* compile;
    yield* after;
  }),
});
`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardOutput).toBe("compile printed this last\n");
      expect(ran.standardError).toContain("[after] the isolated worker exited\n");
      expect(ran.standardError).toMatch(/^passed: 3 tasks, [\d.]+s$/mu);
    }),
  ),
);

export const isolatedTasksWaitForTheLimit = makeTest(
  "with one worker, two isolated tasks run one after the other on their own workers while the task awaiting them only waits",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const first = Task.make({ name: "first", isolated: true, run: Effect.log("first ran") });
const second = Task.make({ name: "second", isolated: true, run: Effect.log("second ran") });
export default Task.make({ name: "main", run: Task.all([first, second]) });
`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", ONE_WORKER);
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toMatch(/^passed: 3 tasks, [\d.]+s$/mu);
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, ran));
      expect(
        readAttribute(
          trace?.findOnlySpan("runRootTaskFiber")?.attributes ?? [],
          "sshkeda_task.workers.started",
        ),
      ).toBe(ISOLATED_WORKER_COUNT_WITH_TWO_ISOLATED_TASKS);
      const [earlier, later] = listTaskBodiesOnWorkers(trace)
        .filter((body) => body.label !== "main on 0")
        .toSorted((left, right) => Number(left.start - right.start));
      expect([earlier?.label.at(-1), later?.label.at(-1)]).toEqual(["1", "2"]);
      expect((earlier?.end ?? MISSING_TIMESTAMP) <= (later?.start ?? MISSING_TIMESTAMP)).toBe(true);
    }),
  ),
);

export const noDeadlockWithOneWorker = makeTest(
  "with one worker, a shared task that awaits an isolated task and an isolated task that awaits a shared task and a task made in its body both pass",
  Effect.scoped(
    Effect.gen(function* () {
      const sharedAwaitsIsolated = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const isolated = Task.make({ name: "isolated", isolated: true, run: Effect.log("isolated ran") });
export default Task.make({ name: "main", run: Effect.gen(function* () { yield* isolated; }) });
`,
      });
      const first = yield* runTaskProcess(sharedAwaitsIsolated, "main.ts", ONE_WORKER);
      expect(first.exitCode, first.standardError).toBe(0);
      expect(first.standardError).toContain("[isolated] isolated ran\n");
      expect(first.standardError).toMatch(/^passed: 2 tasks, [\d.]+s$/mu);
      const isolatedAwaitsShared = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const shared = Task.make({ name: "shared", run: Effect.log("shared ran") });
const isolated = Task.make({
  name: "isolated",
  isolated: true,
  run: Effect.gen(function* () {
    yield* shared;
    yield* Task.make({ name: "made", run: Effect.log("made ran") });
  }),
});
export default Task.make({ name: "main", run: Effect.gen(function* () { yield* isolated; }) });
`,
      });
      const second = yield* runTaskProcess(isolatedAwaitsShared, "main.ts", ONE_WORKER);
      expect(second.exitCode, second.standardError).toBe(0);
      expect(second.standardError).toContain("[shared] shared ran\n");
      expect(second.standardError).toContain("[made] made ran\n");
      expect(second.standardError).toMatch(/^passed: 4 tasks, [\d.]+s$/mu);
      const [trace] = yield* readTraces(yield* readTraceDirectory(isolatedAwaitsShared, second));
      expect(
        listTaskBodiesOnWorkers(trace)
          .map((body) => body.label)
          .toSorted((left, right) => left.localeCompare(right)),
      ).toEqual(["isolated on 1", "made on 1", "main on 0", "shared on 0"]);
    }),
  ),
);

export const keptWhileAwaitingInParallel = makeTest(
  "an isolated task that awaits a task made in its body and a shared task together keeps its worker after the first of them ends, and passes",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const slow = Task.make({ name: "slow", run: Effect.sleep("1 second") });
const isolated = Task.make({ name: "isolated", isolated: true, run: Effect.gen(function* () {
  const local = Task.make({ name: "local", run: Effect.log("local ran") });
  yield* Task.all([local, slow]);
  yield* Effect.log("after both");
}) });
export default Task.make({ name: "main", run: isolated });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("[local] local ran\n");
      expect(ran.standardError).toContain("[isolated] after both\n");
      expect(ran.standardError).toMatch(/^passed: 4 tasks, [\d.]+s$/mu);
    }),
  ),
);

export const isolatedServerKeepsItsWorker = makeTest(
  "an isolated task that serves keeps its worker until the run stops it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { existsSync, writeFileSync } from "node:fs";
import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
const server = Task.make({
  name: "server",
  isolated: true,
  output: Schema.String,
  run: Effect.gen(function* () {
    process.on("exit", () => writeFileSync("server-worker-exited", ""));
    yield* Effect.addFinalizer(() => Effect.log("server stopped"));
    return yield* Task.serve("up");
  }),
});
const after = Task.make({
  name: "after",
  run: Effect.suspend(() => Effect.log(\`server worker exited: \${existsSync("server-worker-exited")}\`)),
});
export default Task.make({
  name: "main",
  run: Effect.gen(function* () {
    yield* Effect.log(\`server is \${yield* server}\`);
    yield* after;
  }),
});
`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("[main] server is up\n");
      expect(ran.standardError).toContain("[after] server worker exited: false\n");
      expect(ran.standardError).toContain("[server] server stopped\n");
      expect(ran.standardError).toContain("[server] closed\n");
    }),
  ),
);

export const forcedStopCutsOffIsolatedCleanup = makeTest(
  "a second Ctrl-C while an isolated task's finalizer is still running names that task as cut off",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const deploy = Task.make({
  name: "deploy",
  isolated: true,
  run: Effect.gen(function* () {
    yield* Effect.addFinalizer(() => Effect.gen(function* () {
      yield* Effect.log("releasing lock");
      yield* Effect.never;
    }));
    yield* Effect.log("deploying");
    yield* Effect.never;
  }),
});
export default Task.make({ name: "main", run: Effect.gen(function* () { yield* deploy; }) });
`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[deploy] deploying");
      yield* command.sendSignal("INT");
      yield* command.waitForLine("[deploy] releasing lock");
      yield* command.sendSignal("INT");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode, exited.standardError).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(exited.standardError).toContain(
        "[deploy] error TaskCleanupCutOff: its cleanup was cut off because the stop was forced\n",
      );
      expect(exited.standardError).toMatch(/^error StopRequested: received SIGINT$/mu);
    }),
  ),
);

export const invalidIsolated = makeTest(
  "an isolated that is not true or false is refused before any task runs",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.log("main ran"), isolated: "yes" });
`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "main: error TaskIsolatedInvalid: sets isolated to 'yes', which is not true or false\n",
      );
      expect(ran.standardError).not.toContain("main ran");
    }),
  ),
);

export const isolatedInBody = makeTest(
  "a task made inside another task's body with isolated: true is refused when that body awaits it, and the run fails",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({
  name: "main",
  run: Effect.gen(function* () {
    yield* Task.make({ name: "made", isolated: true, run: Effect.log("made ran") });
    yield* Effect.log("main went on");
  }),
});
`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        'made: error TaskIsolatedInBody: is made inside the body of "main", so it cannot run on a worker of its own\n',
      );
      expect(ran.standardError).not.toContain("made ran");
      expect(ran.standardError).not.toContain("main went on");
    }),
  ),
);

export const mistypedIsolated = makeTest(
  "an isolated that is not a boolean does not compile",
  Effect.scoped(
    Effect.gen(function* () {
      const failed = yield* failureAsValue(
        typeCheckSource(`import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.void, isolated: "yes" });`),
      );
      expect(failed.message).toContain(
        "main.ts(3,60): error TS2322: Type 'string' is not assignable to type 'boolean | undefined'.",
      );
    }),
  ),
);
