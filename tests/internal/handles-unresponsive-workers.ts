import { expect } from "bun:test";

import { Effect } from "effect";

import { exists } from "../shared/access-files.ts";
import { EXIT_CODE_AFTER_SIGINT } from "../shared/exit-codes-after-stop-signals.ts";
import { findKilledProcesses } from "../shared/find-killed-processes.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { readAttribute, readTraceDirectory, readTraces } from "../shared/read-trace-files.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";

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
      yield* command.waitForLine("stopping, press ctrl-c again to force");
      yield* command.sendSignal("INT");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(exited.standardError).toContain("[spin] stopped: the run ended before it finished");
      expect(exited.standardError).toMatch(/^error StopRequested: received SIGINT$/mu);
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
        "[quick] error TaskCleanupTimedOut: its cleanup did not finish within 1s, so the run stopped waiting for it",
      );
      expect(ran.standardError).not.toContain("terminated that worker");
      expect(yield* exists(`${project.directory}/quick-cleaned-up`)).toBe(true);
      expect(yield* exists(`${project.directory}/slow-cleaned-up`)).toBe(true);
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, ran));
      const endedTasks = trace?.spans
        .filter(
          (span) => readAttribute(span.attributes, "sshkeda_task.message") === "TaskFiberEnded",
        )
        .map((span) => readAttribute(span.attributes, "sshkeda_task.task"));
      expect(endedTasks).toContain("slow");
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
      expect(ran.standardError).toContain("[waiter] error TaskTimedOut: timed out after 1s");
      expect(ran.standardError).toContain(
        "[waiter] error TaskCleanupNotStarted: its worker did not start its cleanup within 1s, so task terminated that worker",
      );
      for (const taskName of ["other", "main"]) {
        expect(ran.standardError).toContain(
          `[${taskName}] error WorkerTerminatedForAnotherTask: its worker was terminated because "waiter" ran past its cleanup limit on it\n`,
        );
      }
      expect(ran.standardError).not.toContain("cleaned up");
      expect(ran.standardError).toMatch(
        /^failed: waiter, other, main \(3 of 3 tasks\), [\d.]+s$/mu,
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
      expect(ran.standardError).toContain(
        "[main] error TaskCleanupTimedOut: its cleanup did not finish within 1s, so the run stopped waiting for it",
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
        "[freezer] error TaskCleanupTimedOut: its cleanup did not finish within 1s, so the run stopped waiting for it",
      );
      expect(ran.standardError).toContain(
        "[waiter] error TaskCleanupTimedOut: its cleanup did not finish within 5s, so the run stopped waiting for it",
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
        "[freezer] error TaskCleanupTimedOut: its cleanup did not finish within 1s, so the run stopped waiting for it",
      );
      expect(ran.standardError).toContain("[freezer] freezing now");
      expect(ran.standardError).toContain(
        "[waiter] error TaskCleanupTimedOut: its cleanup did not finish within 5s, so the run stopped waiting for it",
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
        "[hanger] error TaskCleanupNotStarted: its worker did not start its cleanup within 1s, so task terminated that worker",
      );
      const runnerFailure =
        '[runner] error WorkerTerminatedForAnotherTask: its worker was terminated because "hanger" ran past its cleanup limit on it\n';
      expect(ran.standardError).toContain(runnerFailure);
      const runnerKills = findKilledProcesses(ran.standardError).filter(
        (killed) => killed.by === '"runner"',
      );
      expect(runnerKills.map((killed) => killed.command).toSorted()).toEqual(["sh", "sleep"]);
      const lastRunnerKill = ran.standardError.lastIndexOf('started by "runner"');
      expect(lastRunnerKill).toBeLessThan(ran.standardError.indexOf(runnerFailure));
      expect(ran.standardError).toMatch(
        /^failed: runner, hanger, main \(3 of 3 tasks\), [\d.]+s$/mu,
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
        "[quick] error TaskCleanupTimedOut: its cleanup did not finish within 1s, so the run stopped waiting for it",
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
      expect(ran.standardError).toContain("[spin] error TaskTimedOut: timed out after 1s");
      expect(ran.standardError).toContain(
        "[spin] error TaskCleanupNotStarted: its worker did not start its cleanup within 1s, so task terminated that worker",
      );
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, ran));
      expect(
        (trace?.logs ?? [])
          .map((log) => String(log.body.stringValue))
          .filter((body) => body.startsWith("worker terminated")),
      ).toEqual(['worker terminated because it did not answer the stop of "spin" within 1s']);
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
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, ran));
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
          "[server] error TaskCleanupTimedOut: its cleanup did not finish within 1s, so the run stopped waiting for it\n",
        );
        expect(ran.standardError, `isolated: ${String(isolated)}`).toContain("[server] closed\n");
        const [trace] = yield* readTraces(yield* readTraceDirectory(project, ran));
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
      expect(ran.standardError).toContain("[freezer] error TaskTimedOut: timed out after 1s");
      expect(ran.standardError).toContain("[freezer] freezer cleanup started");
      expect(ran.standardError).toContain(
        "[freezer] error TaskCleanupTimedOut: its cleanup did not finish within 1s, so the run stopped waiting for it",
      );
      const mainCleanupNotStarted =
        "[main] error TaskCleanupNotStarted: its worker did not start its cleanup within 1s, so task terminated that worker\n";
      const workerTerminations = [
        "[freezer] error TaskCleanupFrozeWorker: its cleanup ran past its 1s limit and then stopped its worker from answering, so task terminated that worker\n",
        mainCleanupNotStarted,
      ].filter((line) => ran.standardError.includes(line));
      expect(workerTerminations, ran.standardError).not.toEqual([]);
      expect(ran.standardError).not.toContain("waiter cleaned up");
      expect(ran.standardError).toMatch(
        /^\[waiter\] error WorkerTerminatedForAnotherTask: its worker was terminated because "(?:freezer|main)" ran past its cleanup limit on it$/mu,
      );
      const summary = ran.standardError.includes(mainCleanupNotStarted)
        ? /^failed: waiter, freezer, main \(3 of 3 tasks\), (?<seconds>[\d.]+)s$/mu
        : /^failed: waiter, freezer \(2 of 3 tasks\), (?<seconds>[\d.]+)s$/mu;
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
      expect(exited.standardError).toContain(
        "[main] error TaskCleanupNotStarted: its worker did not start its cleanup within 1s, so task terminated that worker",
      );
      expect(exited.standardError).not.toContain("TaskCleanupTimedOut");
      expect(exited.standardError).not.toContain("TaskCleanupFrozeWorker");
      expect(exited.standardError).toMatch(
        /^failed: main \(1 of 1 task\), [\d.]+s\nerror StopRequested: received SIGINT$/mu,
      );
      const [trace] = yield* readTraces(yield* readTraceDirectory(project, exited));
      expect(
        (trace?.logs ?? [])
          .map((log) => String(log.body.stringValue))
          .filter((body) => body.startsWith("worker terminated")),
      ).toEqual(['worker terminated because it did not stop loading "main" within 1s']);
    }),
  ),
);
