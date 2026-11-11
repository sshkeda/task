import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect, FileSystem } from "effect";

import { exists } from "../shared/access-files.ts";
import { EXIT_CODE_AFTER_SIGINT } from "../shared/exit-codes-after-stop-signals.ts";
import { findKilledProcesses } from "../shared/find-killed-processes.ts";
import { isProcessAlive } from "../shared/is-process-alive.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";

export const users = makeTest(
  "a serving task stays up for the tasks that use it and stops when the run ends",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const server = Task.make({ name: "server", output: Schema.Struct({ port: Schema.Finite }), run: Effect.gen(function* () {
  yield* Effect.log("server up");
  yield* Effect.addFinalizer(() => Effect.log("server down"));
  return yield* Task.serve({ port: 1234 });
}) });
export const first = Task.make({ name: "first", run: Effect.gen(function* () { yield* Effect.log(\`first used \${(yield* server).port}\`); }) });
export const second = Task.make({ name: "second", run: Effect.gen(function* () { yield* first; yield* Effect.log(\`second used \${(yield* server).port}\`); }) });
export default Task.make({ name: "main", run: Effect.all([first, second], { concurrency: "unbounded", discard: true }) });`,
      });

      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);

      const told = ran.standardError
        .split("\n")
        .filter((line) => / (?:up|used \d+|down|closed)$/u.test(line));
      expect(told).toEqual([
        "[server] server up",
        "[first] first used 1234",
        "[second] second used 1234",
        "[server] server down",
        "[server] closed",
      ]);
    }),
  ),
);

const SERVES_AFTER_USING_DATABASE = `Effect.gen(function* () {
  const url = yield* database;
  yield* Effect.addFinalizer(() => Effect.sleep("300 millis").pipe(Effect.andThen(Effect.log(\`server down, used \${url}\`))));
  return yield* Task.serve(url);
})`;

const USES_DATABASE_AFTER_SERVING = `Effect.gen(function* () {
  const serving = yield* Effect.forkScoped(Task.serve("up"));
  yield* Effect.sleep("200 millis");
  const url = yield* database;
  yield* Effect.addFinalizer(() => Effect.sleep("300 millis").pipe(Effect.andThen(Effect.log(\`server down, used \${url}\`))));
  writeFileSync(SERVER_HAS_DATABASE, "");
  return yield* Fiber.join(serving);
})`;

const WAITS_UNTIL_SERVER_HAS_DATABASE = `Effect.andThen(server, Effect.gen(function* () {
  while (!existsSync(SERVER_HAS_DATABASE)) yield* Effect.sleep("10 millis");
}))`;

export const stopOrder = makeTest(
  "a serving task stops only after the tasks that requested it, whether the run passes or fails and whether they requested it before or after serving",
  Effect.forEach(
    [
      ["passes", SERVES_AFTER_USING_DATABASE, "Effect.asVoid(server)"],
      [
        "fails",
        SERVES_AFTER_USING_DATABASE,
        'Effect.andThen(server, Effect.fail(new TaskError({ message: "boom" })))',
      ],
      ["uses it after serving", USES_DATABASE_AFTER_SERVING, WAITS_UNTIL_SERVER_HAS_DATABASE],
    ],
    ([variant, serverBody, mainBody]) =>
      Effect.scoped(
        Effect.gen(function* () {
          const project = yield* makeProject({
            "main.ts": `import { existsSync, writeFileSync } from "node:fs";
import { Effect, Fiber, Schema } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
const SERVER_HAS_DATABASE = new URL("server-has-database", import.meta.url);
export const database = Task.make({ name: "database", output: Schema.String, run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("database down"));
  return yield* Task.serve("db-url");
}) });
export const server = Task.make({ name: "server", run: ${serverBody} });
export default Task.make({ name: "main", run: ${mainBody} });`,
          });

          const ran = yield* runTaskProcess(project, "main.ts");
          expect(ran.exitCode, variant).toBe(variant === "fails" ? 1 : 0);
          const told = ran.standardError
            .split("\n")
            .filter((line) => / (?:down.*|closed)$/u.test(line));
          expect(told, `${variant}\n${ran.standardError}`).toEqual([
            "[server] server down, used db-url",
            "[server] closed",
            "[database] database down",
            "[database] closed",
          ]);
        }),
      ),
    { concurrency: "unbounded", discard: true },
  ),
);

export const independentStops = makeTest(
  "serving tasks that do not request each other stop at the same time when the run ends",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const serve = (name: string) => Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log(\`\${name} stopping\`).pipe(Effect.andThen(Effect.sleep("2 seconds")), Effect.andThen(Effect.log(\`\${name} stopped\`))));
  return yield* Task.serve(name);
});
export const first = Task.make({ name: "first", run: serve("first") });
export const second = Task.make({ name: "second", run: serve("second") });
export const third = Task.make({ name: "third", run: serve("third") });
export const fourth = Task.make({ name: "fourth", run: serve("fourth") });
export default Task.make({ name: "main", run: Effect.all([first, second, third, fourth], { concurrency: "unbounded", discard: true }) });`,
      });

      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const told = ran.standardError
        .split("\n")
        .filter((line) => / (?:stopping|stopped)$/u.test(line))
        .map((line) => line.replace(/^\[\w+\] \w+ /u, ""));
      expect(told, ran.standardError).toEqual([
        "stopping",
        "stopping",
        "stopping",
        "stopping",
        "stopped",
        "stopped",
        "stopped",
        "stopped",
      ]);
    }),
  ),
);

export const stopOrderThroughCompletedTasks = makeTest(
  "a serving task keeps its transitive services alive through completed tasks during cleanup on success and failure",
  Effect.forEach(
    [false, true],
    (fails) =>
      Effect.scoped(
        Effect.gen(function* () {
          const project = yield* makeProject({
            "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export const database = Task.make({ name: "database", output: Schema.String, run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("database down"));
  return yield* Task.serve("db-url");
}) });
export const connection = Task.make({ name: "connection", output: Schema.String, run: database });
export const adapter = Task.make({ name: "adapter", output: Schema.String, run: connection });
export const server = Task.make({ name: "server", run: Effect.gen(function* () {
  const url = yield* adapter;
  yield* Effect.addFinalizer(() => Effect.sleep("300 millis").pipe(Effect.andThen(Effect.log(\`server down, used \${url}\`))));
  return yield* Task.serve(url);
}) });
export default Task.make({ name: "main", run: server.pipe(Effect.andThen(${fails ? 'Effect.fail(new TaskError({ message: "boom" }))' : "Effect.void"})) });`,
          });
          const ran = yield* runTaskProcess(project, "main.ts");
          expect(ran.exitCode, ran.standardError).toBe(fails ? 1 : 0);
          expect(
            ran.standardError.split("\n").filter((line) => / (?:down.*|closed)$/u.test(line)),
          ).toEqual([
            "[server] server down, used db-url",
            "[server] closed",
            "[database] database down",
            "[database] closed",
          ]);
        }),
      ),
    { discard: true },
  ),
);

export const servingCycle = makeTest(
  "two serving tasks that requested each other fail the run with an error naming both",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { existsSync, writeFileSync } from "node:fs";
import { Effect, Fiber } from "effect";
import { Task } from "@sshkeda/task";
const DATABASE_HAS_SERVER = new URL("database-has-server", import.meta.url);
export const database = Task.make({ name: "database", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("database down"));
  const serving = yield* Effect.forkScoped(Task.serve("db-url"));
  yield* Effect.sleep("300 millis");
  yield* server;
  writeFileSync(DATABASE_HAS_SERVER, "");
  return yield* Fiber.join(serving);
}) });
export const server = Task.make({ name: "server", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("server down"));
  yield* database;
  return yield* Task.serve("up");
}) });
export default Task.make({ name: "main", run: Effect.andThen(server, Effect.gen(function* () {
  while (!existsSync(DATABASE_HAS_SERVER)) yield* Effect.sleep("10 millis");
})) });`,
      });

      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(ran.standardError).toContain(
        "[database] error ServingTaskRequestsItself: requests itself while serving through database -> server -> database, so task cannot stop each of them after the tasks that requested it\n",
      );
      expect(ran.standardError).toContain("[server] server down\n");
      expect(ran.standardError).toContain("[database] database down\n");
    }),
  ),
);

export const servingSelfRequest = makeTest(
  "a serving task that seeds itself through a task that requests it passes and closes after the tasks that requested it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { existsSync, writeFileSync } from "node:fs";
import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
const API_SEEDED = new URL("api-seeded", import.meta.url);
export const api: Task.Task<string> = Task.make({ name: "api", output: Schema.String, run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("api down"));
  yield* Effect.forkScoped(Effect.gen(function* () {
    yield* Effect.log(yield* seed);
    writeFileSync(API_SEEDED, "");
  }));
  return yield* Task.serve("http://api");
}) });
export const seed = Task.make({ name: "seed", output: Schema.String, run: Effect.gen(function* () { return \`seeded \${yield* api}\`; }) });
export default Task.make({ name: "main", run: Effect.andThen(api, Effect.gen(function* () {
  while (!existsSync(API_SEEDED)) yield* Effect.sleep("10 millis");
})) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("[api] seeded http://api\n");
      expect(ran.standardError).toContain("[api] api down\n[api] closed\n");
      expect(ran.standardError).not.toContain("ServingTaskRequestsItself");
      expect(ran.standardError).toMatch(/^passed: 3 tasks, [\d.]+s$/mu);
    }),
  ),
);

export const returns = makeTest(
  "a task that returns closes its scope at once, stopping what it started",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("cleaned up"));
  yield* ChildProcess.make("sleep", ["1000"]);
  return "done";
}) });`,
      });

      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain("[main] cleaned up\n[main] passed in");
      expect(ran.standardError).not.toContain("killed ");
    }),
  ),
);

export const ends = makeTest(
  "a serving root that ends on its own ends the run, and its timeout covered only start-up",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const reader = yield* ChildProcess.make("sh", ["-c", "read line < gate"]);
  return yield* Effect.raceFirst(Task.serve("up"), reader.exitCode.pipe(Effect.as("down")));
}), timeout: "5 seconds", cleanup: "5 seconds" });`,
      });
      yield* Task.exec("mkfifo", [`${project.directory}/gate`]);

      const command = yield* startTaskProcess(project, "main.ts");
      const gate = yield* fileSystem.open(`${project.directory}/gate`, { flag: "r+" });
      yield* command.waitForLine("main is serving");
      const timeoutWitness = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const witness = Task.make({ name: "witness", run: Effect.never, timeout: "5 seconds", cleanup: "5 seconds" });
export default witness;`,
      });
      const witnessRun = yield* runTaskProcess(timeoutWitness, "main.ts");
      expect(witnessRun.standardError).toContain(
        "[witness] error TaskTimedOut: timed out after 5s",
      );
      yield* gate.write(new TextEncoder().encode("\n"));

      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(0);
      expect(exited.standardError).toContain("[main] closed");
      expect(exited.standardError).toMatch(/^passed: 1 task, [\d.]+s$/mu);
    }),
  ),
);

export const servedOutputMismatch = makeTest(
  "serving a value that does not match the task's output fails with TaskOutputInvalid, naming where and what was expected without printing the value",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", output: Schema.Struct({ port: Schema.Finite }), run: Effect.gen(function* () {
  return yield* Task.serve({ port: "4010" });
}) });`,
      });

      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "[main] error TaskOutputInvalid: served a value that does not match the task's output at .port: Expected number\n",
      );
      expect(ran.standardError).not.toContain("4010");
    }),
  ),
);

const ROOTS = {
  "server.ts": `import { Effect } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export const server = Task.make({ name: "server", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.scoped(Effect.gen(function* () {
    const child = yield* ChildProcess.make("sh", ["-c", "read line < cleanup-gate && touch marker"]);
    yield* Effect.log(\`cleaning up in \${child.pid}\`);
    yield* child.exitCode;
  })));
  return yield* Task.serve("up");
}) });`,
  "pass.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { server } from "./server.ts";
export const pass = Task.make({ name: "pass", run: Effect.asVoid(server) });
export default pass;`,
  "fail.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
import { server } from "./server.ts";
export const fail = Task.make({ name: "fail", run: Effect.andThen(server, Effect.fail(new TaskError({ message: "boom" }))) });
export default fail;`,
  "held.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { server } from "./server.ts";
export const held = Task.make({ name: "held", run: Effect.gen(function* () {
  yield* server;
  yield* Effect.log("ready");
  yield* Effect.never;
}) });
export default held;`,
} as const;

const cleaned = (directory: string, standardError: string) =>
  Effect.gen(function* () {
    const processId = Number(
      /\[server\] cleaning up in (?<processId>\d+)/u.exec(standardError)?.[1],
    );
    expect(standardError).toContain("[server] closed");
    expect(yield* exists(`${directory}/marker`)).toBe(true);
    expect(findKilledProcesses(standardError)).toEqual([]);
    expect(yield* isProcessAlive(processId)).toBe(false);
  });

const startRootWithCleanupGate = (rootFile: string) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const project = yield* makeProject(ROOTS);
    yield* Task.exec("mkfifo", [`${project.directory}/cleanup-gate`]);
    const cleanupGate = yield* fileSystem.open(`${project.directory}/cleanup-gate`, { flag: "r+" });
    const command = yield* startTaskProcess(project, rootFile);
    return {
      project,
      command,
      finishWhenCleanupStarted: Effect.gen(function* () {
        yield* command.waitForLine("[server] cleaning up in");
        yield* cleanupGate.write(new TextEncoder().encode("\n"));
        return yield* command.waitForExit;
      }),
    };
  });

export const finalizers = makeTest(
  "without a cleanup option, a serving task's finalizer commands finish before the run ends, whether it passes, fails or is interrupted",
  Effect.all(
    [
      Effect.scoped(
        Effect.gen(function* () {
          const passing = yield* startRootWithCleanupGate("pass.ts");
          const ran = yield* passing.finishWhenCleanupStarted;
          expect(ran.exitCode).toBe(0);
          yield* cleaned(passing.project.directory, ran.standardError);
        }),
      ),
      Effect.scoped(
        Effect.gen(function* () {
          const failing = yield* startRootWithCleanupGate("fail.ts");
          const ran = yield* failing.finishWhenCleanupStarted;
          expect(ran.standardError).toMatch(/^\[fail\] \S+: error TaskError: boom$/mu);
          yield* cleaned(failing.project.directory, ran.standardError);
        }),
      ),
      Effect.scoped(
        Effect.gen(function* () {
          const held = yield* startRootWithCleanupGate("held.ts");
          yield* held.command.waitForLine("[held] ready");
          yield* held.command.sendSignal("INT");
          const exited = yield* held.finishWhenCleanupStarted;
          expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
          yield* cleaned(held.project.directory, exited.standardError);
        }),
      ),
    ],
    { concurrency: "unbounded", discard: true },
  ),
);

export const failedTaskFinalizerAwaitsTask = makeTest(
  "a failed task's finalizer that awaits a task as the run ends gets that task's interrupt instead of waiting out its cleanup limit",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export const notify = Task.make({ name: "notify", run: Effect.log("notifying") });
export const build = Task.make({ name: "build", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => notify.pipe(Effect.catchCause(() => Effect.log("notify ended"))));
  return yield* Effect.fail(new TaskError({ message: "build broke" }));
}) });
export default Task.make({ name: "main", run: build.pipe(Effect.catchTag("AwaitedTaskFailed", () => Effect.log("main caught it"))) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain("[main] main caught it");
      expect(ran.standardError).toContain("[build] notify ended");
      expect(ran.standardError).not.toContain("TaskCleanupTimedOut");
      expect(ran.standardError).not.toContain("still stopping");
    }),
  ),
);

export const finalizerAwaitsTask = makeTest(
  "a serving task's finalizer that awaits a task as the run ends gets that task's interrupt instead of waiting out its cleanup limit",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const teardown = Task.make({ name: "teardown", run: Effect.log("tearing down") });
export const server = Task.make({ name: "server", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => teardown.pipe(Effect.catchCause(() => Effect.log("teardown ended"))));
  return yield* Task.serve("up");
}) });
export default Task.make({ name: "main", run: Effect.asVoid(server) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain("[server] teardown ended");
      expect(ran.standardError).not.toContain("TaskCleanupTimedOut");
      expect(ran.standardError).not.toContain("still stopping");
    }),
  ),
);

export const finalizerAwaitsStoppedTask = makeTest(
  "a finalizer that awaits a task the run already stopped gets that task's interrupt once its own task is stopped",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { existsSync, writeFileSync } from "node:fs";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const SLOW_IS_UP = new URL("slow-is-up", import.meta.url);
const SLOW_IS_CLEANING_UP = new URL("slow-is-cleaning-up", import.meta.url);
export const dependency = Task.make({ name: "dependency", run: Effect.never });
export const late = Task.make({ name: "late", run: Effect.addFinalizer(() => Effect.gen(function* () {
  while (!existsSync(SLOW_IS_CLEANING_UP)) yield* Effect.sleep("10 millis");
  yield* dependency.pipe(Effect.catchCause(() => Effect.log("dependency ended")));
})), cleanup: "5 seconds" });
export const slow = Task.make({ name: "slow", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.sync(() => writeFileSync(SLOW_IS_CLEANING_UP, "")).pipe(Effect.andThen(Effect.sleep("1 second"))));
  writeFileSync(SLOW_IS_UP, "");
  return yield* Effect.never;
}) });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.forkDetach(late);
  yield* Effect.sleep("50 millis");
  yield* Effect.forkDetach(dependency);
  yield* Effect.sleep("50 millis");
  yield* Effect.forkDetach(slow);
  while (!existsSync(SLOW_IS_UP)) yield* Effect.sleep("10 millis");
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain("[late] dependency ended");
      expect(ran.standardError).not.toContain("TaskCleanupTimedOut");
    }),
  ),
);

export const exceeded = makeTest(
  "when a task's cleanup outlasts its duration, the processes the task started are killed and reported with that task",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Stream } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export const server = Task.make({ name: "server", run: Effect.gen(function* () {
  const child = yield* ChildProcess.make("sh", ["-c", "sleep 1000 & sleep 1001 & echo started; wait"]);
  yield* child.stdout.pipe(Stream.decodeText, Stream.splitLines, Stream.runHead);
  yield* Effect.log("processes started");
  yield* Effect.addFinalizer(() => Effect.never);
  return yield* Task.serve("up");
}), cleanup: "500 millis" });
export default Task.make({ name: "main", run: Effect.asVoid(server) });`,
      });

      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[server] processes started");
      const ran = yield* command.waitForExit;
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "[server] error TaskCleanupTimedOut: its cleanup did not finish within 500ms, so the run stopped waiting for it",
      );
      const killed = findKilledProcesses(ran.standardError);
      expect(killed.map((each) => `${each.command} by ${each.by}`).toSorted()).toEqual([
        'sh by "server"',
        'sleep by "server"',
        'sleep by "server"',
      ]);
      for (const each of killed) {
        expect(yield* isProcessAlive(each.processId)).toBe(false);
      }
    }),
  ),
);

export const cleanupTimedOut = makeTest(
  "a cleanup that outlasts its duration fails the run and exits 1, even when every task passed",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const server = Task.make({ name: "server", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.never);
  return yield* Task.serve("up");
}), cleanup: "200 millis" });
export default Task.make({ name: "main", run: Effect.asVoid(server) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain("[main] passed in");
      expect(ran.standardError).toContain(
        "[server] error TaskCleanupTimedOut: its cleanup did not finish within 200ms, so the run stopped waiting for it",
      );
      expect(ran.standardError).toMatch(/^failed: server \(1 of 2 tasks\), [\d.]+s$/mu);
    }),
  ),
);

export const cleanupFailed = makeTest(
  "a serving task whose cleanup fails with a typed error when the run ends fails the run and names the error",
  Effect.forEach(
    [
      [
        "onInterrupt",
        'Task.serve("up").pipe(Effect.onInterrupt(() => Effect.fail(new CloseFailed())))',
      ],
      ["ensuring", 'Task.serve("up").pipe(Effect.ensuring(Effect.fail(new CloseFailed())))'],
    ],
    ([closedBy, serverBody]) =>
      Effect.scoped(
        Effect.gen(function* () {
          const project = yield* makeProject({
            "main.ts": `import { Data, Effect } from "effect";
import { Task } from "@sshkeda/task";
class CloseFailed extends Data.TaggedError("CloseFailed") {}
export const server = Task.make({ name: "server", run: ${serverBody} });
export default Task.make({ name: "main", run: Effect.asVoid(server) });`,
          });
          const ran = yield* runTaskProcess(project, "main.ts");
          expect(ran.exitCode, `${closedBy}\n${ran.standardError}`).toBe(1);
          expect(ran.standardError, closedBy).toMatch(/^\[server\] .*error CloseFailed/mu);
        }),
      ),
    { concurrency: "unbounded", discard: true },
  ),
);
