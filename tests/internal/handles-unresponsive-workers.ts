import { expect } from "bun:test";

import { Effect } from "effect";

import { exists } from "../shared/access-files.ts";
import { EXIT_CODE_AFTER_SIGINT } from "../shared/exit-codes-after-stop-signals.ts";
import { findKilledProcesses } from "../shared/find-killed-processes.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { readShellExecutableName } from "../shared/read-shell-executable-name.ts";
import { readProjectRunsDirectory, readTraces } from "../shared/read-trace-files.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";

const SPAN_STATUS_UNSET = 0;

export const loop = makeTest(
  "a stop waits for a worker stuck in a synchronous loop, whatever its task's cleanup duration, until a second Ctrl-C terminates it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const spin = Task.make({ name: "spin", run: Effect.gen(function* () {
  yield* Effect.log("spinning");
  yield* Effect.sync(() => { while (true) {} });
}), cleanup: "5 minutes" });
export default Task.make({ name: "main", run: spin });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[spin] spinning");
      yield* command.sendSignal("INT");
      yield* command.waitForLine("stopping: press ctrl-c again to force");
      yield* command.sendSignal("INT");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(exited.standardError).toContain(
        "\nerror: [spin] main.ts:3:26: TaskCleanupCutOff: its cleanup was cut off because the stop was forced\n",
      );
      expect(exited.standardError).not.toContain("[spin] stopped:");
      expect(exited.standardError).toMatch(
        /\nfailed: \[spin\] \[main\] \(2 of 2 tasks\), [\d.]+s\nrun: \S+\/$/u,
      );
    }),
  ),
);

export const sharedWorker = makeTest(
  "a task whose cleanup runs out after its worker started that cleanup does not get the worker it shares with another task terminated, so both tasks' finalizers run",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { writeFileSync } from "node:fs";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
const serveUntilStopped = (name: string, afterCleanup: Effect.Effect<void>) => Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.sync(() => writeFileSync(\`\${name}-cleaned-up\`, "")).pipe(Effect.andThen(afterCleanup)));
  return yield* Task.serve("up");
});
export const quick = Task.make({ name: "quick", run: serveUntilStopped("quick", Effect.never), cleanup: "1 second" });
export const slow = Task.make({ name: "slow", run: serveUntilStopped("slow", Effect.void), cleanup: "10 minutes" });
export default Task.make({ name: "main", run: Effect.andThen(Effect.andThen(slow, quick), Effect.fail(new TaskError({ message: "boom" }))) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "1" });
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "\nerror: [quick] main.ts:9:27: TaskCleanupTimedOut: its cleanup did not finish within 1s, so the run stopped waiting for it\n",
      );
      expect(ran.standardError).not.toContain("terminated that worker");
      expect(yield* exists(`${project.directory}/quick-cleaned-up`)).toBe(true);
      expect(yield* exists(`${project.directory}/slow-cleaned-up`)).toBe(true);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      expect(trace?.findTaskSpan("cleanup", "slow")?.status).toEqual({ code: SPAN_STATUS_UNSET });
      expect(trace?.findTaskSpan("cleanup", "quick")?.attributes).toContainEqual({
        key: "error.type",
        value: { stringValue: "TaskCleanupTimedOut" },
      });
    }),
  ),
);

export const frozenFibers = makeTest(
  "a worker frozen by one task's synchronous loop is terminated when that task times out and its cleanup duration runs out, the tasks still on it fail naming that task, and the run fails",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const waiter = Task.make({ name: "waiter", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("waiter cleaned up"));
  yield* Effect.log("waiting");
  yield* Effect.sync(() => { while (true) {} });
}), timeout: "1 second", cleanup: "1 second" });
export const other = Task.make({ name: "other", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("other cleaned up"));
  yield* Effect.log("waiting");
  yield* waiter;
}), cleanup: "1 second" });
export default Task.make({ name: "main", run: other });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "1" });
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain("[other] waiting");
      expect(ran.standardError).toContain(
        "\nerror: [waiter] main.ts:3:28: TaskTimedOut: timed out after 1s\nerror: [waiter] no command was running\nerror: [waiter] main.ts:3:28: TaskCleanupNotStarted: its worker did not start its cleanup within 1s, so task terminated that worker\n",
      );
      for (const taskPrefix of ["error: [other] main.ts:8:27", "error: [main] main.ts:13:21"]) {
        expect(ran.standardError).toContain(
          `\n${taskPrefix}: WorkerTerminatedForAnotherTask: its worker was terminated because [waiter] ran past its cleanup limit on it\n`,
        );
      }
      expect(ran.standardError).not.toContain("cleaned up");
      expect(ran.standardError).toMatch(
        /^failed: \[waiter\] \[other\] \[main\] \(3 of 3 tasks\), [\d.]+s$/mu,
      );
    }),
  ),
);

export const frozenCleanupAfterSelfInterrupt = makeTest(
  "a body that interrupts itself and whose cleanup then freezes its worker fails with TaskCleanupTimedOut, since that cleanup started, and the worker is terminated when the cleanup limit runs out, so a task sharing it still ends",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const server = Task.make({ name: "server", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("server cleaned up"));
  return yield* Task.serve("up");
}) });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* server;
  yield* Effect.addFinalizer(() => Effect.sync(() => { while (true) {} }));
  yield* Effect.interrupt;
}), cleanup: "1 second" });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "1" });
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(
        /\nerror: \[main\] main\.ts:7:21: CauseWithoutFailure: was interrupted without a failure\nerror: \[main\] main\.ts:7:21: TaskCleanupTimedOut: its cleanup did not finish within 1s, so the run stopped waiting for it\n[\s\S]*\nerror: \[main\] its output is in \S+\/1\.output\nended: \[main\]/u,
      );
      expect(ran.standardError).toMatch(
        /\nfailed: \[server\] \[main\] \(2 of 2 tasks\), [\d.]+s\nrun: \S+\/$/u,
      );
      expect(ran.standardError).not.toContain("server cleaned up");
    }),
  ),
);

export const frozenAfterStartingCleanup = makeTest(
  "a worker that starts a task's cleanup and then freezes does not hang the run, since each task on it ends at its cleanup limit, 5s by default",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Latch } from "effect";
import { Task } from "@sshkeda/task";
const waiterCleanupStarted = Latch.makeUnsafe();
export const waiter = Task.make({ name: "waiter", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => waiterCleanupStarted.open.pipe(Effect.andThen(Effect.sleep("20 seconds")), Effect.andThen(Effect.log("waiter cleaned up"))));
  return yield* Task.serve("up");
}) });
export const freezer = Task.make({ name: "freezer", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => waiterCleanupStarted.await.pipe(Effect.andThen(Effect.log("freezer cleanup started")), Effect.andThen(Effect.sync(() => { while (true) {} }))));
  return yield* Task.serve("up");
}), cleanup: "1 second" });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* waiter;
  yield* freezer;
  yield* Effect.log("ready to stop");
  yield* Effect.never;
}) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "1" });
      yield* command.waitForLine("[main] ready to stop");
      yield* command.sendSignal("INT");
      const ran = yield* command.waitForExit;
      expect(ran.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(ran.standardError).toContain("[freezer] freezer cleanup started");
      expect(ran.standardError).toContain(
        "\nerror: [freezer] main.ts:8:29: TaskCleanupTimedOut: its cleanup did not finish within 1s, so the run stopped waiting for it\n",
      );
      expect(ran.standardError).toContain(
        "\nerror: [waiter] main.ts:4:28: TaskCleanupTimedOut: its cleanup did not finish within 5s, so the run stopped waiting for it\n",
      );
      expect(ran.standardError).not.toContain("waiter cleaned up");
    }),
  ),
);

export const frozenAfterAnsweringPastCleanupLimit = makeTest(
  "a worker whose task's cleanup answers past its limit and freezes only later does not hang the run, since a cleanup waiting on that worker ends at its default 5s cleanup limit",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export const waiter = Task.make({ name: "waiter", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.sleep("20 seconds").pipe(Effect.andThen(Effect.log("waiter cleaned up"))));
  return yield* Task.serve("up");
}) });
export const freezer = Task.make({ name: "freezer", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.sleep("1500 millis").pipe(Effect.andThen(Effect.log("freezing now")), Effect.andThen(Effect.sync(() => { while (true) {} }))));
  return yield* Task.serve("up");
}), cleanup: "1 second" });
export default Task.make({ name: "main", run: Effect.andThen(Effect.andThen(waiter, freezer), Effect.fail(new TaskError({ message: "boom" }))) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "1" });
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "\nerror: [freezer] main.ts:8:29: TaskCleanupTimedOut: its cleanup did not finish within 1s, so the run stopped waiting for it\n",
      );
      expect(ran.standardError).toContain("[freezer] freezing now");
      expect(ran.standardError).toContain(
        "\nerror: [waiter] main.ts:4:28: TaskCleanupTimedOut: its cleanup did not finish within 5s, so the run stopped waiting for it\n",
      );
      expect(ran.standardError).not.toContain("waiter cleaned up");
    }),
  ),
);

export const bystanderOnTerminatedWorker = makeTest(
  "a task still running a command on a shared worker that is terminated because another task hung past its timeout fails, naming that task, and has its commands killed before it fails",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { existsSync } from "node:fs";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const runner = Task.make({ name: "runner", run: Task.exec("sh", ["-c", "touch runner-started; sleep 1000"]) });
export const hanger = Task.make({ name: "hanger", run: Effect.gen(function* () {
  yield* Effect.sleep("50 millis").pipe(Effect.repeat({ until: () => existsSync("runner-started") }));
  yield* Effect.log("hanging");
  yield* Effect.sync(() => { while (true) {} });
}), timeout: "1 second", cleanup: "1 second" });
export default Task.make({ name: "main", run: Task.allSettled([runner, hanger]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "1" });
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain("[hanger] hanging");
      expect(ran.standardError).toContain(
        "\nerror: [hanger] main.ts:5:28: TaskCleanupNotStarted: its worker did not start its cleanup within 1s, so task terminated that worker\n",
      );
      const runnerFailure =
        "\nerror: [runner] main.ts:4:28: WorkerTerminatedForAnotherTask: its worker was terminated because [hanger] ran past its cleanup limit on it\n";
      expect(ran.standardError).toContain(runnerFailure);
      const runnerKills = findKilledProcesses(ran.standardError).filter(
        (killed) => killed.by === "[runner]",
      );
      expect(runnerKills.map((killed) => killed.command).toSorted()).toEqual(
        [yield* readShellExecutableName, "sleep"].toSorted(),
      );
      const lastRunnerKill = ran.standardError.lastIndexOf("killed: [runner] ");
      expect(lastRunnerKill).toBeLessThan(ran.standardError.indexOf(runnerFailure));
      expect(ran.standardError).toMatch(
        /^failed: \[runner\] \[hanger\] \[main\] \(3 of 3 tasks\), [\d.]+s$/mu,
      );
    }),
  ),
);

export const longSynchronousCleanupWithinItsLimit = makeTest(
  "a task's synchronous cleanup that blocks its worker within its own cleanup limit finishes even when another task on that worker runs past a shorter limit",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { writeFileSync } from "node:fs";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export const quick = Task.make({ name: "quick", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.never);
  return yield* Task.serve("up");
}), cleanup: "1 second" });
export const slow = Task.make({ name: "slow", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.sleep("200 millis").pipe(Effect.andThen(Effect.sync(() => {
    const end = Date.now() + 4000;
    while (Date.now() < end) {}
    writeFileSync("slow-cleaned-up", "");
  }))));
  return yield* Task.serve("up");
}), cleanup: "10 seconds" });
export default Task.make({ name: "main", run: Effect.andThen(Effect.andThen(slow, quick), Effect.fail(new TaskError({ message: "boom" }))) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "1" });
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "\nerror: [quick] main.ts:5:27: TaskCleanupTimedOut: its cleanup did not finish within 1s, so the run stopped waiting for it\n",
      );
      expect(ran.standardError).not.toContain("TaskCleanupFrozeWorker");
      expect(yield* exists(`${project.directory}/slow-cleaned-up`)).toBe(true);
    }),
  ),
);

export const stuckLoopTimesOutThenCleansUp = makeTest(
  "a task with timeout and cleanup whose body is stuck in a synchronous loop ends the run within those durations without any signal, and the trace says why its worker was terminated, though the worker exits before the run waits for its workers",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const spin = Task.make({ name: "spin", run: Effect.gen(function* () {
  yield* Effect.log("spinning");
  yield* Effect.sync(() => { while (true) {} });
}), timeout: "1 second", cleanup: "1 second" });
export default Task.make({ name: "main", run: spin });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain("[spin] spinning");
      expect(ran.standardError).toContain(
        "\nerror: [spin] main.ts:3:26: TaskTimedOut: timed out after 1s\nerror: [spin] no command was running\nerror: [spin] main.ts:3:26: TaskCleanupNotStarted: its worker did not start its cleanup within 1s, so task terminated that worker\n",
      );
      expect(ran.standardError).toMatch(
        /\nfailed: \[spin\] \[main\] \(2 of 2 tasks\), [\d.]+s\nrun: \S+\/$/u,
      );
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      expect(
        (trace?.logs ?? [])
          .map((log) => String(log.body.stringValue))
          .filter((body) => body.startsWith("worker terminated")),
      ).toEqual(["worker terminated because it did not answer the stop of [spin] within 1s"]);
    }),
  ),
);

export const frozenAtShutdown = makeTest(
  "a worker frozen after its task passed is shut down at the end without waiting on it, so the run passes and exits",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const freezer = Task.make({ name: "freezer", run: Effect.sync(() => {
  process.once("exit", () => {
    while (true) {}
  });
}) });
export default Task.make({ name: "main", run: freezer });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toMatch(/^passed: 2 tasks, [\d.]+s$/mu);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      expect(
        (trace?.logs ?? []).some(
          (log) =>
            log.body.stringValue ===
            "worker terminated because it did not exit within 2s of being asked to",
        ),
      ).toBe(true);
    }),
  ),
);

const makeServerWithEndlessCleanup = (isolated: boolean) =>
  makeProject({
    "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
const server = Task.make({
  name: "server",
  isolated: ${String(isolated)},
  run: Effect.gen(function* () {
    yield* Effect.addFinalizer(() => Effect.never);
    return yield* Task.serve("up");
  }),
  cleanup: "1 second",
});
export default Task.make({
  name: "main",
  run: Effect.andThen(server, Effect.fail(new TaskError({ message: "boom" }))),
});
`,
  });

export const terminatedAtShutdownStillEndsItsTasks = makeTest(
  "a shared or isolated worker terminated at the end of the run because it did not exit in time still ends the serving task left on it, so that task prints closed",
  Effect.scoped(
    Effect.gen(function* () {
      for (const isolated of [false, true]) {
        const project = yield* makeServerWithEndlessCleanup(isolated);
        const ran = yield* runTaskProcess(project, "main.ts");
        expect(ran.exitCode).toBe(1);
        expect(ran.standardError).toContain(
          "\nerror: [server] main.ts:4:21: TaskCleanupTimedOut: its cleanup did not finish within 1s, so the run stopped waiting for it\n",
        );
        expect(ran.standardError, `isolated: ${String(isolated)}`).toContain("closed: [server]\n");
        const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
        expect(
          (trace?.logs ?? []).some(
            (log) =>
              log.body.stringValue ===
              "worker terminated because it did not exit within 2s of being asked to",
          ),
        ).toBe(true);
      }
    }),
  ),
);

const WAITER_CLEANUP_SECONDS = 30;

const MAXIMUM_ELAPSED_SECONDS = 15;

export const frozenBeforeAnotherTaskStartsCleanup = makeTest(
  "a worker that freezes in a task's cleanup before another task on it starts its own is terminated once it misses an answer for that cleanup limit, so the other task ends without waiting out its longer limit",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const waiter = Task.make({ name: "waiter", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("waiter cleaned up"));
  return yield* Task.serve("up");
}), cleanup: "${WAITER_CLEANUP_SECONDS} seconds" });
export const freezer = Task.make({ name: "freezer", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("freezer cleanup started").pipe(Effect.andThen(Effect.sync(() => { while (true) {} }))));
  return yield* Effect.never;
}), timeout: "1 second", cleanup: "1 second" });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* waiter;
  yield* freezer;
}), cleanup: "1 second" });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "1" });
      expect(ran.exitCode).toBe(1);
      const lines = ran.standardError.split("\n");
      expect(lines).toContain("error: [freezer] main.ts:7:29: TaskTimedOut: timed out after 1s");
      expect(ran.standardError).toContain("[freezer] freezer cleanup started");
      expect(lines).toContain(
        "error: [freezer] main.ts:7:29: TaskCleanupTimedOut: its cleanup did not finish within 1s, so the run stopped waiting for it",
      );
      const mainCleanupNotStarted =
        "error: [main] main.ts:11:21: TaskCleanupNotStarted: its worker did not start its cleanup within 1s, so task terminated that worker";
      const workerTerminations = [
        "error: [freezer] main.ts:7:29: TaskCleanupFrozeWorker: its cleanup ran past its 1s limit and then stopped its worker from answering, so task terminated that worker",
        mainCleanupNotStarted,
      ].filter((line) => lines.includes(line));
      expect(workerTerminations, ran.standardError).not.toEqual([]);
      expect(ran.standardError).not.toContain("waiter cleaned up");
      expect(ran.standardError).toMatch(
        /^error: \[waiter\] main\.ts:3:28: WorkerTerminatedForAnotherTask: its worker was terminated because \[(?:freezer|main)\] ran past its cleanup limit on it$/mu,
      );
      const summary = lines.includes(mainCleanupNotStarted)
        ? /^failed: \[waiter\] \[freezer\] \[main\] \(3 of 3 tasks\), (?<seconds>[\d.]+)s$/mu
        : /^failed: \[waiter\] \[freezer\] \(2 of 3 tasks\), (?<seconds>[\d.]+)s$/mu;
      const { seconds } = summary.exec(ran.standardError)?.groups ?? {};
      const elapsedSeconds = Number(seconds);
      expect(elapsedSeconds).toBeLessThan(MAXIMUM_ELAPSED_SECONDS);
    }),
  ),
);

export const frozenWhileLoading = makeTest(
  "a task stopped while still loading whose worker then freezes fails with TaskCleanupNotStarted, since no body started, and its worker is terminated when the cleanup limit runs out",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "loading.ts": `import { isMainThread, parentPort } from "node:worker_threads";
if (!isMainThread) {
  parentPort?.on("message", () => {
    while (true) {}
  });
  console.error("worker loading");
  await new Promise(() => {});
}
export const greeting = "hello";`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { greeting } from "./loading.ts";
export default Task.make({ name: "main", run: Effect.log(greeting), cleanup: "1 second" });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("worker loading");
      yield* command.sendSignal("INT");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(exited.standardError).not.toContain("TaskCleanupTimedOut");
      expect(exited.standardError).not.toContain("TaskCleanupFrozeWorker");
      expect(exited.standardError).toMatch(
        /\nerror: \[main\] main\.ts:4:21: TaskCleanupNotStarted: its worker did not start its cleanup within 1s, so task terminated that worker\n(?:ended: \[main\], [\d.]+s, (?:stopped|failed)\n)?failed: \[main\] \(1 of 1 task\), [\d.]+s\nrun: \S+\/$/u,
      );
      expect(
        exited.standardError.split("\n").filter((line) => line.startsWith("ended: [main], ")),
        exited.standardError,
      ).toHaveLength(1);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, exited));
      expect(
        (trace?.logs ?? [])
          .map((log) => String(log.body.stringValue))
          .filter((body) => body.startsWith("worker terminated")),
      ).toEqual(["worker terminated because it did not stop loading [main] within 1s"]);
    }),
  ),
);

export const terminationRefused = makeTest(
  "a worker whose termination fails does not keep the run from ending with its summary and a failing exit code",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Worker, isMainThread } from "node:worker_threads";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
if (isMainThread) {
  const unreadable = Object.defineProperty(new Error(), "message", { get() { throw new Error("unreadable"); } });
  Worker.prototype.terminate = () => Promise.reject(process.env.UNREADABLE_REFUSAL === "1" ? unreadable : new Error("terminate refused"));
}
export const waiter = Task.make({ name: "waiter", run: Effect.sync(() => { while (true) {} }), timeout: "1 second", cleanup: "1 second", isolated: true });
export default Task.make({ name: "main", run: Effect.asVoid(Task.allSettled([waiter])) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "\nerror: WorkerTerminationFailed: could not terminate worker 1: terminate refused\n",
      );
      expect(ran.standardError).toMatch(/^failed: \[waiter\] \(1 of 2 tasks\), [\d.]+s$/mu);
      const unreadable = yield* runTaskProcess(project, "main.ts", { UNREADABLE_REFUSAL: "1" });
      expect(unreadable.exitCode, unreadable.standardError).toBe(1);
      expect(unreadable.standardError).toContain("\nerror: WorkerTerminationFailed: ");
      expect(unreadable.standardError).toMatch(/^failed: \[waiter\] \(1 of 2 tasks\), [\d.]+s$/mu);
    }),
  ),
  "30 seconds",
);

export const slowTerminationRefusal = makeTest(
  "a worker that exits slowly fails the run when its termination is refused later than the exit grace, and passes it when its termination succeeds",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Worker, isMainThread } from "node:worker_threads";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
if (isMainThread && process.env.REJECT_TERMINATION === "1") {
  Worker.prototype.terminate = () => Bun.sleep(2500).then(() => { throw new Error("terminate refused late"); });
}
export default Task.make({ name: "main", isolated: true, run: Effect.sync(() => {
  process.once("exit", () => { Bun.sleepSync(6000); });
}) });`,
      });
      const refused = yield* runTaskProcess(project, "main.ts", { REJECT_TERMINATION: "1" });
      expect(refused.exitCode, refused.standardError).toBe(1);
      expect(refused.standardError).toContain(
        "\nerror: WorkerTerminationFailed: could not terminate worker 0: terminate refused late\n",
      );
      expect(refused.standardError).toMatch(/^failed: 0 of 1 task, [\d.]+s$/mu);
      const slow = yield* runTaskProcess(project, "main.ts");
      expect(slow.exitCode, slow.standardError).toBe(0);
      expect(slow.standardError).toMatch(/^passed: 1 task, [\d.]+s$/mu);
    }),
  ),
  "30 seconds",
);
