import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import { countOccurrences } from "../shared/count-occurrences.ts";
import { makeProject } from "../shared/make-projects.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";

export const values = Task.make(
  "a task's value reaches the task that yields it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const answer = Task.make("answer", Effect.succeed({ answer: 41 }));
export const main = Task.make("main", Effect.gen(function* () { yield* Effect.log(\`got \${(yield* answer).answer + 1}\`); }));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain("[main] got 42");
      expect(ran.standardError).toContain("[answer] passed in");
      expect(ran.standardError).toMatch(/^passed: 2 tasks, [\d.]+s$/mu);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const moduleExports = Task.make(
  "a value or served value made of a module's exports reaches the task that yields it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "settings.ts": `export const host = "localhost";
export const port = 4000;`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import * as settings from "./settings.ts";
export const config = Task.make("config", Effect.succeed(settings));
export const server = Task.make("server", Effect.gen(function* () {
  return yield* Task.serve({ url: \`http://\${settings.host}:\${settings.port}\`, settings });
}));
export const main = Task.make("main", Effect.gen(function* () {
  const { host, port } = yield* config;
  const { url, settings: served } = yield* server;
  yield* Effect.log(\`config \${host}:\${port}, served \${url} on \${served.port}\`);
}));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain(
        "[main] config localhost:4000, served http://localhost:4000 on 4000\n",
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const once = Task.make(
  "a task yielded by several tasks runs once per run",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const shared = Task.make("shared", Effect.log("shared ran"));
export const left = Task.make("left", shared);
export const right = Task.make("right", shared);
export const main = Task.make("main", Effect.all([left, right, shared], { concurrency: "unbounded", discard: true }));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      expect(countOccurrences(ran.standardError, "[shared] shared ran")).toBe(1);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const parallel = Task.make(
  "tasks yielded with unbounded concurrency run at the same time",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { ChildProcess } from "effect/process";
const shell = (script: string) => ChildProcess.make("sh", ["-c", script]).pipe(Effect.flatMap((child) => child.exitCode), Effect.scoped);
export const make = Task.make("make", shell("mkfifo ping pong"));
export const ping = Task.make("ping", make.pipe(Effect.andThen(shell("echo ping > ping; cat pong"))));
export const pong = Task.make("pong", make.pipe(Effect.andThen(shell("cat ping; echo pong > pong"))));
export const main = Task.make("main", Effect.all([ping, pong], { concurrency: "unbounded", discard: true }));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const finalizers = Task.make(
  "a cancelled task's finalizers run before the command exits",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
import { ChildProcess } from "effect/process";
const shell = (script: string) => ChildProcess.make("sh", ["-c", script]).pipe(Effect.flatMap((child) => child.exitCode), Effect.scoped);
export const make = Task.make("make", shell("mkfifo ready"));
export const slow = Task.make("slow", Effect.gen(function* () {
  yield* make;
  yield* Effect.addFinalizer(() => Effect.log("slow cleaned up"));
  yield* shell("echo > ready");
  yield* Effect.never;
}));
export const bad = Task.make("bad", Effect.gen(function* () {
  yield* make;
  yield* shell("cat ready");
  return yield* Effect.fail(new TaskError({ message: "bad broke" }));
}));
export const main = Task.make("main", Effect.all([slow, bad], { concurrency: "unbounded", discard: true }));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).not.toContain("[slow] passed");
      expect(ran.standardError).toContain("[slow] slow cleaned up");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const json = Task.make(
  "a body that returns something that is neither JSON nor a Redacted string fails and names that part",
  Effect.forEach(
    [
      ["Effect.succeed(10n)", "returned a bigint, which is not JSON"],
      [
        "Effect.succeed({ entry: 'dist/server.js', builtAt: new Date() })",
        "returned an instance of Date at .builtAt, which is not JSON",
      ],
      [
        "Effect.succeed({ steps: [1, { 'run at': () => 1 }] })",
        'returned a function at .steps[1]["run at"], which is not JSON',
      ],
      ["Effect.succeed({ count: Number.NaN })", "returned NaN at .count, which is not JSON"],
      [
        "Effect.succeed({ child: new (class ChildProcess {})(), other: Object.create({ a: 1 }) })",
        "returned an instance of ChildProcess at .child, which is not JSON",
      ],
      [
        "Effect.succeed([Object.create({ a: 1 })])",
        "returned an object whose prototype isn't Object.prototype at [0], which is not JSON",
      ],
      [
        "Effect.sync(() => { const value: Record<string, unknown> = { name: 'loop' }; value.self = value; return value; })",
        "returned a reference back to an enclosing value at .self, which is not JSON",
      ],
      [
        "Effect.sync(() => { const shared = { kept: 1 }; return { first: shared, second: [shared, { when: new Date(0) }] }; })",
        "returned an instance of Date at .second[1].when, which is not JSON",
      ],
      [
        "Effect.succeed({ count: Redacted.make(1) })",
        "returned an object whose prototype isn't Object.prototype at .count, which is not JSON",
      ],
      [
        "Effect.succeed({ nested: { '~@sshkeda/task/Redacted': 'not a secret' } })",
        'returned an object with the reserved key "~@sshkeda/task/Redacted" at .nested, which is not JSON',
      ],
    ],
    ([body, message]) =>
      Effect.scoped(
        Effect.gen(function* () {
          const project = yield* makeProject({
            "main.ts": `import { Effect, Redacted } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", ${body});
if (import.meta.main) Task.main(main);`,
          });
          const ran = yield* runTaskProcess(project, "main.ts");
          expect(ran.exitCode).toBe(1);
          expect(ran.standardError).toContain(`[main] error TaskResultNotJson: ${message}\n`);
        }),
      ),
    { concurrency: "unbounded", discard: true },
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const deepRedactedResult = Task.make(
  "a value holding a Redacted string thousands of levels deep reaches the task that yields it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Redacted } from "effect";
import { Task } from "@sshkeda/task";
type Release = { readonly token: Redacted.Redacted<string> } | { readonly previous: Release };
export const history = Task.make("history", Effect.sync(() => {
  let release: Release = { token: Redacted.make("fake-deep-token") };
  for (let depth = 0; depth < 5000; depth++) release = { previous: release };
  return release;
}));
export const main = Task.make("main", Effect.gen(function* () {
  let release: Release = yield* history;
  let depth = 0;
  while ("previous" in release) { release = release.previous; depth++; }
  yield* Effect.log(\`found \${release.token} at depth \${depth}\`);
}));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("[main] found <redacted> at depth 5000\n");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const deepResult = Task.make(
  "a body whose result is too deep or too broken to hand to the main thread fails and names the task",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const deep = Task.make("deep", Effect.sync(() => {
  let value: unknown = 1;
  for (let depth = 0; depth < 200000; depth++) value = [value];
  return value;
}));
export const main = Task.make("main", Effect.gen(function* () { yield* deep; }));
if (import.meta.main) Task.main(main);`,
        "unreadable.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const unreadable = Task.make("unreadable", Effect.sync(() => {
  const thrown = { get message(): string { throw thrown; } };
  return { get value(): number { throw thrown; } };
}));
if (import.meta.main) Task.main(unreadable);`,
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const deepServed = Task.make(
  "a served value too deep or too broken to hand to the main thread fails and names the task",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const deep = Task.make("deep", Effect.gen(function* () {
  let value: unknown = 1;
  for (let depth = 0; depth < 50000; depth++) value = [value];
  return yield* Task.serve(value);
}));
export const main = Task.make("main", Effect.gen(function* () { yield* deep; }));
if (import.meta.main) Task.main(main);`,
        "unreadable.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const unreadable = Task.make("unreadable", Effect.gen(function* () {
  const thrown = { get message(): string { throw thrown; } };
  return yield* Task.serve({ get value(): number { throw thrown; } });
}));
export const main = Task.make("main", Effect.gen(function* () { yield* unreadable; }));
if (import.meta.main) Task.main(main);`,
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const cycle = Task.make(
  "a task that waits on itself through another fails and names the cycle",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const a: Task.Task<number> = Task.make("a", Effect.gen(function* () { return (yield* b) + 1; }));
export const b: Task.Task<number> = Task.make("b", Effect.gen(function* () { return (yield* a) + 1; }));
if (import.meta.main) Task.main(a);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "[b] error TaskWaitsOnItself: waits on itself through b -> a -> b",
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const startedTogetherCycle = Task.make(
  "tasks started together that await each other fail and name the cycle",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const a: Task.Task<void> = Task.make("a", Effect.suspend(() => b).pipe(Effect.andThen(Effect.log("a body finished"))));
export const b: Task.Task<void> = Task.make("b", Effect.suspend(() => a).pipe(Effect.andThen(Effect.log("b body finished"))));
export const main = Task.make("main", Task.all([a, b]));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(
        /\[(?:a\] error TaskWaitsOnItself: waits on itself through a -> b -> a|b\] error TaskWaitsOnItself: waits on itself through b -> a -> b)/u,
      );
      expect(ran.standardError).not.toContain("body finished");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const ownCommands = Task.make(
  "with one worker, a task that starts a long-running command and then a second one at the same time completes",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.gen(function* () {
  const server = yield* ChildProcess.make("sh", ["-c", "read line < gate"]);
  yield* ChildProcess.make("sh", ["-c", "echo > gate"]).pipe(Effect.flatMap((client) => client.exitCode), Effect.scoped);
  yield* server.exitCode;
}).pipe(Effect.scoped));
if (import.meta.main) Task.main(main);`,
      });
      yield* Task.exec("mkfifo", [`${project.directory}/gate`]);
      const ran = yield* runTaskProcess(project, "main.ts", {
        SSHKEDA_TASK_WORKERS: "1",
      });
      expect(ran.exitCode).toBe(0);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const yieldWhileRunningCommand = Task.make(
  "with one worker, a task whose command is still running yields another task that runs a command, and both complete",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export const client = Task.make("client", ChildProcess.make("sh", ["-c", "echo > gate"]).pipe(Effect.flatMap((child) => child.exitCode), Effect.scoped));
export const main = Task.make("main", Effect.gen(function* () {
  const server = yield* ChildProcess.make("sh", ["-c", "read line < gate"]);
  yield* client;
  yield* server.exitCode;
}).pipe(Effect.scoped));
if (import.meta.main) Task.main(main);`,
      });
      yield* Task.exec("mkfifo", [`${project.directory}/gate`]);
      const ran = yield* runTaskProcess(project, "main.ts", {
        SSHKEDA_TASK_WORKERS: "1",
      });
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toMatch(/^passed: 2 tasks, [\d.]+s$/mu);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const nothing = Task.make(
  "a task that returns nothing gives undefined to the task that yields it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const empty = Task.make("empty", Effect.void);
export const blank = Task.make("blank", Effect.succeed(null));
export const main = Task.make("main", Effect.gen(function* () { yield* Effect.log(\`got \${String(yield* empty)} and \${String(yield* blank)}\`); }));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("[main] got undefined and null");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const commonJsTaskFiles = Task.make(
  "tasks made in a CommonJS file that requires @sshkeda/task are found, by name or through the package object",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "named.cjs": `const { Effect } = require("effect");
const { Task } = require("@sshkeda/task");
const named = Task.make("named", Effect.succeed("named"));
module.exports = { named };`,
        "whole.cjs": `const { Effect } = require("effect");
const sshkedaTask = require("@sshkeda/task");
const whole = sshkedaTask.Task.make("whole", Effect.succeed("whole"));
module.exports = { whole };`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { named } from "./named.cjs";
import { whole } from "./whole.cjs";
export const main = Task.make("main", Effect.gen(function* () { yield* Effect.log(\`got \${yield* named} and \${yield* whole}\`); }));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain("[main] got named and whole");
      expect(ran.exitCode).toBe(0);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const configuredScriptExtensions = Task.make(
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
          "build.task": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { base } from "./lib.ts";
export const build = Task.make("build", Effect.succeed(base as string));
`,
          "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { build } from "./build.task";
export const main = Task.make("main", Effect.gen(function* () { yield* Effect.log(\`got \${yield* build}\`); }));
if (import.meta.main) Task.main(main);`,
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

const ONE_SECOND_SETUP_LIMIT = { SSHKEDA_TASK_SETUP_LIMIT: "1 second" } as const;

const SETUP_LIMIT_LONGER_THAN_A_WORKER_START = { SSHKEDA_TASK_SETUP_LIMIT: "10 seconds" } as const;

export const setupLimitBoundsDiscoveryImport = Task.make(
  "the setup limit bounds importing task modules during discovery before a worker starts",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "pending.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
process.stderr.write("discovery import waiting\\n");
await new Promise(() => {});
export const pending = Task.make("pending", Effect.void);`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.promise(() => import("./pending.ts")));
if (import.meta.main) Task.main(main);`,
      });
      const command = yield* startTaskProcess(project, "main.ts", ONE_SECOND_SETUP_LIMIT);
      const timedOut = yield* command.waitForExit.pipe(
        Effect.onInterrupt(() => command.sendSignal("KILL")),
      );
      expect(timedOut.standardError).toContain("discovery import waiting");
      expect(timedOut.exitCode).toBe(1);
      expect(timedOut.standardError).toContain(
        `error TaskSetupTimedOut: setup timed out after 1s while loading ${project.directory}/pending.ts`,
      );
      expect(timedOut.standardError).not.toContain("[main] started");
    }),
  ),
  { timeout: "30 seconds", cleanup: "5 seconds" },
);

export const setupLimitBoundsWorkerImport = Task.make(
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
export const main = Task.make("main", Effect.log("body ran"), { cleanup: "5 seconds" });
if (import.meta.main) Task.main(main);`,
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
  { timeout: "30 seconds", cleanup: "5 seconds" },
);

export const timeoutSkipsSlowWorkerImport = Task.make(
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
export const main = Task.make("main", Effect.log("body ran"), { timeout: "1 second", cleanup: "1 second" });
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).not.toContain("TaskTimedOut");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain("[main] body ran");
    }),
  ),
  { timeout: "30 seconds", cleanup: "5 seconds" },
);

export const timeoutSkipsWaitingForRequestedTask = Task.make(
  "a task's timeout does not count time its body spends waiting for a task it yields",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const slow = Task.make("slow", Effect.sleep("3 seconds").pipe(Effect.as("slow value")));
export const main = Task.make("main", Effect.gen(function* () { yield* Effect.log(\`got \${yield* slow}\`); }), { timeout: "1 second", cleanup: "1 second" });
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).not.toContain("TaskTimedOut");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain("[main] got slow value");
    }),
  ),
  { timeout: "30 seconds", cleanup: "5 seconds" },
);

export const timeoutSkipsWaitingForParallelRequestedTasks = Task.make(
  "a task's timeout does not count time its body spends waiting for tasks it yields in parallel, nested, or through all and allSettled, until the last one ends",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const short = Task.make("short", Effect.sleep("500 millis").pipe(Effect.as("short")));
export const long = Task.make("long", Effect.sleep("3 seconds").pipe(Effect.as("long")));
export const inner = Task.make("inner", Effect.sleep("3 seconds").pipe(Effect.as("inner")));
export const outer = Task.make("outer", inner);
export const viaEffectAll = Task.make("via-effect-all", Effect.all([short, long], { concurrency: "unbounded" }).pipe(Effect.flatMap((values) => Effect.log(\`got \${values.join(" ")}\`))), { timeout: "1 second", cleanup: "1 second" });
export const viaAll = Task.make("via-all", Task.all([short, long]).pipe(Effect.andThen(Effect.log("both ended"))), { timeout: "1 second", cleanup: "1 second" });
export const viaAllSettled = Task.make("via-all-settled", Task.allSettled([short, long]).pipe(Effect.andThen(Effect.log("both settled"))), { timeout: "1 second", cleanup: "1 second" });
export const nested = Task.make("nested", outer.pipe(Effect.flatMap((value) => Effect.log(\`got \${value}\`))), { timeout: "1 second", cleanup: "1 second" });
export const main = Task.make("main", Effect.all([viaEffectAll, viaAll, viaAllSettled, nested], { concurrency: "unbounded", discard: true }));
if (import.meta.main) Task.main(main);`,
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
  { timeout: "30 seconds", cleanup: "5 seconds" },
);

export const timeoutCountsOwnWork = Task.make(
  "a task's timeout still counts its body's own work",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.sleep("2 seconds").pipe(Effect.andThen(Effect.log("own work done"))), { timeout: "1 second", cleanup: "1 second" });
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain("[main] error TaskTimedOut: timed out after 1s");
      expect(ran.standardError).not.toContain("own work done");
    }),
  ),
  { timeout: "30 seconds", cleanup: "5 seconds" },
);

export const timeoutResumesAfterWaitingForRequestedTask = Task.make(
  "a task's timeout resumes once the task it yields ends, so own work after the wait still times out",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const slow = Task.make("slow", Effect.sleep("3 seconds").pipe(Effect.as("slow value")));
export const main = Task.make("main", Effect.gen(function* () {
  yield* Effect.log(\`got \${yield* slow}\`);
  yield* Effect.sleep("2 seconds");
  yield* Effect.log("own work done");
}), { timeout: "1 second", cleanup: "1 second" });
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain("[main] got slow value");
      expect(ran.standardError).toContain("[main] error TaskTimedOut: timed out after 1s");
      expect(ran.standardError).not.toContain("own work done");
    }),
  ),
  { timeout: "30 seconds", cleanup: "5 seconds" },
);

export const timeoutAddsOwnWorkAcrossWaiting = Task.make(
  "a task's timeout adds up its body's own work before and after waiting for a task it yields",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const slow = Task.make("slow", Effect.sleep("3 seconds").pipe(Effect.as("slow value")));
export const main = Task.make("main", Effect.gen(function* () {
  yield* Effect.sleep("700 millis");
  yield* Effect.log(\`got \${yield* slow}\`);
  yield* Effect.sleep("700 millis");
  yield* Effect.log("own work done");
}), { timeout: "1 second", cleanup: "1 second" });
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain("[main] got slow value");
      expect(ran.standardError).toContain("[main] error TaskTimedOut: timed out after 1s");
      expect(ran.standardError).not.toContain("own work done");
    }),
  ),
  { timeout: "30 seconds", cleanup: "5 seconds" },
);

export const requestersGetTheirOwnResult = Task.make(
  "a body that changes a requested task's result in place does not change another requester's value",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const names = Task.make("names", Effect.succeed(["b", "c", "a"]));
export const sorted = Task.make("sorted", Effect.gen(function* () {
  (yield* names).sort();
}));
export const asGiven = Task.make("as-given", Effect.gen(function* () {
  yield* Effect.log(\`as-given sees \${(yield* names).join(",")}\`);
}));
export const main = Task.make("main", Effect.gen(function* () {
  yield* sorted;
  yield* asGiven;
  yield* Effect.log(\`body sees \${(yield* names).join(",")}\`);
}));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("[as-given] as-given sees b,c,a");
      expect(ran.standardError).toContain("[main] body sees b,c,a");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);
