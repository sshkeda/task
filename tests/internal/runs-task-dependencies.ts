import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import { countOccurrences } from "../shared/count-occurrences.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";

export const values = makeTest(
  "a task's value reaches the task that yields it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const answer = Task.make({ name: "answer", output: Schema.Struct({ answer: Schema.Finite }), run: Effect.succeed({ answer: 41 }) });
export default Task.make({ name: "main", run: Effect.gen(function* () { yield* Effect.log(\`got \${(yield* answer).answer + 1}\`); }) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain("[main] got 42");
      expect(ran.standardError).toContain("[answer] passed in");
      expect(ran.standardError).toMatch(/^passed: 2 tasks, [\d.]+s$/mu);
    }),
  ),
);

export const once = makeTest(
  "a task yielded by several tasks runs once per run",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const shared = Task.make({ name: "shared", run: Effect.log("shared ran") });
export const left = Task.make({ name: "left", run: shared });
export const right = Task.make({ name: "right", run: shared });
export default Task.make({ name: "main", run: Effect.all([left, right, shared], { concurrency: "unbounded", discard: true }) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      expect(countOccurrences(ran.standardError, "[shared] shared ran")).toBe(1);
    }),
  ),
);

export const parallel = makeTest(
  "tasks yielded with unbounded concurrency run at the same time",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { ChildProcess } from "effect/process";
const shell = (script: string) => ChildProcess.make("sh", ["-c", script]).pipe(Effect.flatMap((child) => child.exitCode), Effect.scoped);
export const make = Task.make({ name: "make", run: shell("mkfifo ping pong") });
export const ping = Task.make({ name: "ping", run: make.pipe(Effect.andThen(shell("echo ping > ping; cat pong"))) });
export const pong = Task.make({ name: "pong", run: make.pipe(Effect.andThen(shell("cat ping; echo pong > pong"))) });
export default Task.make({ name: "main", run: Effect.all([ping, pong], { concurrency: "unbounded", discard: true }) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
    }),
  ),
);

export const finalizers = makeTest(
  "a cancelled task's finalizers run before the command exits",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
import { ChildProcess } from "effect/process";
const shell = (script: string) => ChildProcess.make("sh", ["-c", script]).pipe(Effect.flatMap((child) => child.exitCode), Effect.scoped);
export const make = Task.make({ name: "make", run: shell("mkfifo ready") });
export const slow = Task.make({ name: "slow", run: Effect.gen(function* () {
  yield* make;
  yield* Effect.addFinalizer(() => Effect.log("slow cleaned up"));
  yield* shell("echo > ready");
  yield* Effect.never;
}) });
export const bad = Task.make({ name: "bad", run: Effect.gen(function* () {
  yield* make;
  yield* shell("cat ready");
  return yield* Effect.fail(new TaskError({ message: "bad broke" }));
}) });
export default Task.make({ name: "main", run: Effect.all([slow, bad], { concurrency: "unbounded", discard: true }) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).not.toContain("[slow] passed");
      expect(ran.standardError).toContain("[slow] slow cleaned up");
    }),
  ),
);

export const deepDeclaredOutput = makeTest(
  "a value thousands of levels deep that matches its output schema reaches the task that yields it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const history = Task.make({ name: "history", output: Schema.Json, run: Effect.sync(() => {
  let release: Schema.Json = "first release";
  for (let depth = 0; depth < 5000; depth++) release = [release];
  return release;
}) });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  let release = yield* history;
  let depth = 0;
  while (Array.isArray(release)) { release = release[0] ?? null; depth++; }
  yield* Effect.log(\`found \${release} at depth \${depth}\`);
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("[main] found first release at depth 5000\n");
    }),
  ),
);

export const longChain = makeTest(
  "a run whose tasks each yield the one before, 600 deep, stops in seconds after the root task passes",
  Effect.scoped(
    Effect.gen(function* () {
      const chainLength = 600;
      const links = Array.from(
        { length: chainLength - 1 },
        (_unused, index) =>
          `export const t${index + 1} = Task.make({ name: "t${index + 1}", output: Schema.Finite, run: Effect.map(t${index}, (depth) => depth + 1) });`,
      );
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const t0 = Task.make({ name: "t0", output: Schema.Finite, run: Effect.succeed(0) });
${links.join("\n")}
export default Task.make({ name: "main", run: Effect.flatMap(t${chainLength - 1}, (depth) => Effect.log(\`depth \${depth}\`)) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain(`[main] depth ${chainLength - 1}\n`);
      expect(ran.standardError).toMatch(/^passed: 601 tasks, [\d.]+s$/mu);
    }),
  ),
  "30 seconds",
);

export const chainDeeperThanTheStack = makeTest(
  "a run whose tasks each yield the one before, 1,000 deep on a small stack, stops without overflowing it",
  Effect.scoped(
    Effect.gen(function* () {
      const chainLength = 1000;
      const links = Array.from(
        { length: chainLength - 1 },
        (_unused, index) =>
          `export const t${index + 1} = Task.make({ name: "t${index + 1}", output: Schema.Finite, run: Effect.map(t${index}, (depth) => depth + 1) });`,
      );
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const t0 = Task.make({ name: "t0", output: Schema.Finite, run: Effect.succeed(0) });
${links.join("\n")}
export default Task.make({ name: "main", run: Effect.flatMap(t${chainLength - 1}, (depth) => Effect.log(\`depth \${depth}\`)) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", {
        BUN_JSC_maxPerThreadStackUsage: "500000",
      });
      expect(ran.standardError).not.toContain("Maximum call stack size exceeded");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toMatch(/^passed: 1001 tasks, [\d.]+s$/mu);
    }),
  ),
  "30 seconds",
);

export const deepResult = makeTest(
  "a body whose result is too deep or too broken to hand to the main thread fails and names the task",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const deep = Task.make({ name: "deep", output: Schema.Json, run: Effect.sync(() => {
  let value: Schema.Json = 1;
  for (let depth = 0; depth < 200000; depth++) value = [value];
  return value;
}) });
export default Task.make({ name: "main", run: Effect.gen(function* () { yield* deep; }) });`,
        "unreadable.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "unreadable", output: Schema.Struct({ value: Schema.Finite }), run: Effect.sync(() => {
  const thrown = { get message(): string { throw thrown; } };
  return { get value(): number { throw thrown; } };
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        '[deep] error TaskResultNotHandedOver: could not hand the result of "deep" to the main thread because Maximum call stack size exceeded.\n',
      );
      const unreadable = yield* runTaskProcess(project, "unreadable.ts");
      expect(unreadable.exitCode).toBe(1);
      expect(unreadable.standardError).toContain(
        '[unreadable] error TaskResultNotHandedOver: could not hand the result of "unreadable" to the main thread because a thrown value that has no message\n',
      );
    }),
  ),
);

export const deepServed = makeTest(
  "a served value too deep or too broken to hand to the main thread fails and names the task",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const deep = Task.make({ name: "deep", output: Schema.Json, run: Effect.gen(function* () {
  let value: Schema.Json = 1;
  for (let depth = 0; depth < 50000; depth++) value = [value];
  return yield* Task.serve(value);
}) });
export default Task.make({ name: "main", run: Effect.gen(function* () { yield* deep; }) });`,
        "unreadable.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const unreadable = Task.make({ name: "unreadable", output: Schema.Struct({ value: Schema.Finite }), run: Effect.gen(function* () {
  const thrown = { get message(): string { throw thrown; } };
  return yield* Task.serve({ get value(): number { throw thrown; } });
}) });
export default Task.make({ name: "main", run: Effect.gen(function* () { yield* unreadable; }) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "[deep] error RangeError: Maximum call stack size exceeded.\n",
      );
      expect(ran.standardError).toContain("[main] error AwaitedTaskFailed: deep failed\n");
      const unreadable = yield* runTaskProcess(project, "unreadable.ts");
      expect(unreadable.exitCode).toBe(1);
      expect(unreadable.standardError).toContain(
        '[unreadable] error TaskResultNotHandedOver: could not hand the result of "unreadable" to the main thread because a thrown value that has no message\n',
      );
      expect(unreadable.standardError).toContain(
        "[main] error AwaitedTaskFailed: unreadable failed\n",
      );
    }),
  ),
);

export const cycle = makeTest(
  "a task that waits on itself through another fails and names the cycle",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const a: Task.Task<number> = Task.make({ name: "a", output: Schema.Finite, run: Effect.gen(function* () { return (yield* b) + 1; }) });
export const b: Task.Task<number> = Task.make({ name: "b", output: Schema.Finite, run: Effect.gen(function* () { return (yield* a) + 1; }) });
export default a;`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "[b] error TaskWaitsOnItself: waits on itself through b -> a -> b",
      );
    }),
  ),
);

export const startedTogetherCycle = makeTest(
  "tasks started together that await each other fail and name the cycle",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const a: Task.Task<void> = Task.make({ name: "a", run: Effect.suspend(() => b).pipe(Effect.andThen(Effect.log("a body finished"))) });
export const b: Task.Task<void> = Task.make({ name: "b", run: Effect.suspend(() => a).pipe(Effect.andThen(Effect.log("b body finished"))) });
export default Task.make({ name: "main", run: Task.all([a, b]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(
        /\[(?:a\] error TaskWaitsOnItself: waits on itself through a -> b -> a|b\] error TaskWaitsOnItself: waits on itself through b -> a -> b)/u,
      );
      expect(ran.standardError).not.toContain("body finished");
    }),
  ),
);

export const ownCommands = makeTest(
  "with one worker, a task that starts a long-running command and then a second one at the same time completes",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const server = yield* ChildProcess.make("sh", ["-c", "read line < gate"]);
  yield* ChildProcess.make("sh", ["-c", "echo > gate"]).pipe(Effect.flatMap((client) => client.exitCode), Effect.scoped);
  yield* server.exitCode;
}).pipe(Effect.scoped) });`,
      });
      yield* Task.exec("mkfifo", [`${project.directory}/gate`]);
      const ran = yield* runTaskProcess(project, "main.ts", {
        SSHKEDA_TASK_WORKERS: "1",
      });
      expect(ran.exitCode).toBe(0);
    }),
  ),
);

export const yieldWhileRunningCommand = makeTest(
  "with one worker, a task whose command is still running yields another task that runs a command, and both complete",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export const client = Task.make({ name: "client", run: ChildProcess.make("sh", ["-c", "echo > gate"]).pipe(Effect.flatMap((child) => child.exitCode), Effect.scoped) });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const server = yield* ChildProcess.make("sh", ["-c", "read line < gate"]);
  yield* client;
  yield* server.exitCode;
}).pipe(Effect.scoped) });`,
      });
      yield* Task.exec("mkfifo", [`${project.directory}/gate`]);
      const ran = yield* runTaskProcess(project, "main.ts", {
        SSHKEDA_TASK_WORKERS: "1",
      });
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toMatch(/^passed: 2 tasks, [\d.]+s$/mu);
    }),
  ),
);

export const nothing = makeTest(
  "a task that returns nothing gives undefined to the task that yields it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const empty = Task.make({ name: "empty", run: Effect.void });
export const blank = Task.make({ name: "blank", output: Schema.Null, run: Effect.succeed(null) });
export default Task.make({ name: "main", run: Effect.gen(function* () { yield* Effect.log(\`got \${String(yield* empty)} and \${String(yield* blank)}\`); }) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("[main] got undefined and null");
    }),
  ),
);

export const commonJsTaskFiles = makeTest(
  "tasks made in a CommonJS file that requires @sshkeda/task are found, by name or through the package object",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "named.cjs": `const { Effect, Schema } = require("effect");
const { Task } = require("@sshkeda/task");
const named = Task.make({ name: "named", output: Schema.String, run: Effect.succeed("named") });
module.exports = { named };`,
        "whole.cjs": `const { Effect, Schema } = require("effect");
const sshkedaTask = require("@sshkeda/task");
const whole = sshkedaTask.Task.make({ name: "whole", output: Schema.String, run: Effect.succeed("whole") });
module.exports = { whole };`,
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
import { named } from "./named.cjs";
import { whole } from "./whole.cjs";
export default Task.make({ name: "main", run: Effect.gen(function* () { yield* Effect.log(\`got \${yield* named} and \${yield* whole}\`); }) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain("[main] got named and whole");
      expect(ran.exitCode).toBe(0);
    }),
  ),
);

export const configuredScriptExtensions = makeTest(
  "tasks in a file whose extension bunfig.toml or --loader maps to a script loader are found, and its imports are followed",
  Effect.scoped(
    Effect.gen(function* () {
      for (const [bunfig, bunArguments] of [
        ['[loader]\n".task" = "ts"\n', []],
        ["", ["--loader", ".task:ts"]],
      ] as const) {
        const project = yield* makeProject({
          "bunfig.toml": bunfig,
          "lib.ts": 'export const base: string = "one";\n',
          "build.task": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
import { base } from "./lib.ts";
export const build = Task.make({ name: "build", output: Schema.String, run: Effect.succeed(base as string) });
`,
          "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { build } from "./build.task";
export default Task.make({ name: "main", run: Effect.gen(function* () { yield* Effect.log(\`got \${yield* build}\`); }) });`,
        });
        const runMain = startTaskProcess(
          project,
          "main.ts",
          {},
          project.directory,
          [],
          "ignore",
          bunArguments,
        ).pipe(
          Effect.flatMap((command) => command.waitForExit),
          Effect.scoped,
        );
        const ran = yield* runMain;
        expect(ran.standardError).toContain("[main] got one");
      }
    }),
  ),
);

const ONE_SECOND_SETUP_LIMIT = { SSHKEDA_TASK_SETUP_LIMIT: "1 second" } as const;

const SETUP_LIMIT_LONGER_THAN_A_WORKER_START = { SSHKEDA_TASK_SETUP_LIMIT: "10 seconds" } as const;

export const setupLimitBoundsDiscoveryImport = makeTest(
  "the setup limit bounds loading the root task file during discovery before a worker starts",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { isMainThread } from "node:worker_threads";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const isDiscoveryImport = isMainThread && !import.meta.main;
if (isDiscoveryImport) {
  process.stderr.write("discovery import waiting\\n");
  await new Promise(() => {});
}
export default Task.make({ name: "main", run: Effect.void });`,
      });
      const command = yield* startTaskProcess(project, "main.ts", ONE_SECOND_SETUP_LIMIT);
      const timedOut = yield* command.waitForExit.pipe(
        Effect.onInterrupt(() => command.sendSignal("KILL")),
      );
      expect(timedOut.standardError).toContain("discovery import waiting");
      expect(timedOut.exitCode).toBe(1);
      expect(timedOut.standardError).toContain(
        `error TaskSetupTimedOut: setup timed out after 1s while loading ${project.directory}/main.ts`,
      );
      expect(timedOut.standardError).not.toContain("[main] started");
    }),
  ),
  "30 seconds",
);

export const setupLimitBoundsWorkerImport = makeTest(
  "the setup limit bounds loading a task's module in the worker, so a top-level await that never settles fails a task that sets no timeout",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { isMainThread } from "node:worker_threads";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
if (!isMainThread) {
  process.stderr.write("worker import waiting\\n");
  await new Promise(() => {});
}
export default Task.make({ name: "main", run: Effect.log("body ran"), cleanup: "5 seconds" });`,
      });
      const timedOut = yield* runTaskProcess(
        project,
        "main.ts",
        SETUP_LIMIT_LONGER_THAN_A_WORKER_START,
      );
      expect(timedOut.standardError).toContain("worker import waiting");
      expect(timedOut.exitCode).toBe(1);
      expect(timedOut.standardError).toContain(
        `[main] error TaskSetupTimedOut: setup timed out after 10s while loading ${project.directory}/main.ts`,
      );
      expect(timedOut.standardError).not.toContain("body ran");
    }),
  ),
  "30 seconds",
);

export const setupLimitStopsBodyAfterBlockingWorkerImport = makeTest(
  "a task whose module blocks its worker past the setup limit does not start its body after TaskSetupTimedOut",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { isMainThread } from "node:worker_threads";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
if (!isMainThread) {
  const until = Date.now() + 12500;
  while (Date.now() < until) {}
}
export default Task.make({ name: "main", run: Effect.log("body ran") });`,
      });
      const timedOut = yield* runTaskProcess(
        project,
        "main.ts",
        SETUP_LIMIT_LONGER_THAN_A_WORKER_START,
      );
      expect(timedOut.exitCode).toBe(1);
      expect(timedOut.standardError).toContain(
        `[main] error TaskSetupTimedOut: setup timed out after 10s while loading ${project.directory}/main.ts`,
      );
      expect(timedOut.standardError).not.toContain("body ran");
    }),
  ),
  "30 seconds",
);

export const timeoutSkipsSlowWorkerImport = makeTest(
  "a task's timeout starts when its body starts, so a module that loads slower than the timeout does not fail the task",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { isMainThread } from "node:worker_threads";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
if (!isMainThread) {
  await Bun.sleep(2500);
}
export default Task.make({ name: "main", run: Effect.log("body ran"), timeout: "1 second", cleanup: "1 second" });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).not.toContain("TaskTimedOut");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain("[main] body ran");
    }),
  ),
  "30 seconds",
);

export const timeoutSkipsWaitingForRequestedTask = makeTest(
  "a task's timeout does not count time its body spends waiting for a task it yields",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const slow = Task.make({ name: "slow", output: Schema.String, run: Effect.sleep("3 seconds").pipe(Effect.as("slow value")) });
export default Task.make({ name: "main", run: Effect.gen(function* () { yield* Effect.log(\`got \${yield* slow}\`); }), timeout: "1 second", cleanup: "1 second" });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).not.toContain("TaskTimedOut");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain("[main] got slow value");
    }),
  ),
  "30 seconds",
);

export const timeoutSkipsWaitingForParallelRequestedTasks = makeTest(
  "a task's timeout does not count time its body spends waiting for tasks it yields in parallel, nested, or through all and allSettled, until the last one ends",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const short = Task.make({ name: "short", output: Schema.String, run: Effect.sleep("500 millis").pipe(Effect.as("short")) });
export const long = Task.make({ name: "long", output: Schema.String, run: Effect.sleep("3 seconds").pipe(Effect.as("long")) });
export const inner = Task.make({ name: "inner", output: Schema.String, run: Effect.sleep("3 seconds").pipe(Effect.as("inner")) });
export const outer = Task.make({ name: "outer", output: Schema.String, run: inner });
export const viaEffectAll = Task.make({ name: "via-effect-all", run: Effect.all([short, long], { concurrency: "unbounded" }).pipe(Effect.flatMap((values) => Effect.log(\`got \${values.join(" ")}\`))), timeout: "1 second", cleanup: "1 second" });
export const viaAll = Task.make({ name: "via-all", run: Task.all([short, long]).pipe(Effect.andThen(Effect.log("both ended"))), timeout: "1 second", cleanup: "1 second" });
export const viaAllSettled = Task.make({ name: "via-all-settled", run: Task.allSettled([short, long]).pipe(Effect.andThen(Effect.log("both settled"))), timeout: "1 second", cleanup: "1 second" });
export const nested = Task.make({ name: "nested", run: outer.pipe(Effect.flatMap((value) => Effect.log(\`got \${value}\`))), timeout: "1 second", cleanup: "1 second" });
export default Task.make({ name: "main", run: Effect.all([viaEffectAll, viaAll, viaAllSettled, nested], { concurrency: "unbounded", discard: true }) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).not.toContain("TaskTimedOut");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain("[via-effect-all] got short long");
      expect(ran.standardError).toContain("[via-all] both ended");
      expect(ran.standardError).toContain("[via-all-settled] both settled");
      expect(ran.standardError).toContain("[nested] got inner");
    }),
  ),
  "30 seconds",
);

export const timeoutCountsOwnWork = makeTest(
  "a task's timeout still counts its body's own work",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.sleep("2 seconds").pipe(Effect.andThen(Effect.log("own work done"))), timeout: "1 second", cleanup: "1 second" });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain("[main] error TaskTimedOut: timed out after 1s");
      expect(ran.standardError).not.toContain("own work done");
    }),
  ),
  "30 seconds",
);

export const timeoutResumesAfterWaitingForRequestedTask = makeTest(
  "a task's timeout resumes once the task it yields ends, so own work after the wait still times out",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const slow = Task.make({ name: "slow", output: Schema.String, run: Effect.sleep("3 seconds").pipe(Effect.as("slow value")) });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.log(\`got \${yield* slow}\`);
  yield* Effect.sleep("2 seconds");
  yield* Effect.log("own work done");
}), timeout: "1 second", cleanup: "1 second" });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain("[main] got slow value");
      expect(ran.standardError).toContain("[main] error TaskTimedOut: timed out after 1s");
      expect(ran.standardError).not.toContain("own work done");
    }),
  ),
  "30 seconds",
);

export const timeoutAddsOwnWorkAcrossWaiting = makeTest(
  "a task's timeout adds up its body's own work before and after waiting for a task it yields",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const slow = Task.make({ name: "slow", output: Schema.String, run: Effect.sleep("3 seconds").pipe(Effect.as("slow value")) });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.sleep("700 millis");
  yield* Effect.log(\`got \${yield* slow}\`);
  yield* Effect.sleep("700 millis");
  yield* Effect.log("own work done");
}), timeout: "1 second", cleanup: "1 second" });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain("[main] got slow value");
      expect(ran.standardError).toContain("[main] error TaskTimedOut: timed out after 1s");
      expect(ran.standardError).not.toContain("own work done");
    }),
  ),
  "30 seconds",
);

export const requestersGetTheirOwnResult = makeTest(
  "a body that changes a requested task's result in place does not change another requester's value",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const names = Task.make({ name: "names", output: Schema.Array(Schema.String), run: Effect.succeed(["b", "c", "a"]) });
export const sorted = Task.make({ name: "sorted", run: Effect.gen(function* () {
  (yield* names).sort();
}) });
export const asGiven = Task.make({ name: "as-given", run: Effect.gen(function* () {
  yield* Effect.log(\`as-given sees \${(yield* names).join(",")}\`);
}) });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* sorted;
  yield* asGiven;
  yield* Effect.log(\`body sees \${(yield* names).join(",")}\`);
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("[as-given] as-given sees b,c,a");
      expect(ran.standardError).toContain("[main] body sees b,c,a");
    }),
  ),
);
