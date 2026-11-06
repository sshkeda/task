import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import { exists } from "../shared/access-files.ts";
import { EXIT_CODE_AFTER_SIGINT } from "../shared/exit-codes-after-stop-signals.ts";
import { makeProject } from "../shared/make-projects.ts";
import { readAttribute, readTraceDirectory, readTraces } from "../shared/read-trace-files.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";

export const loop = Task.make(
  "a stop waits for a worker stuck in a synchronous loop, whatever its task's cleanup duration, until a second Ctrl-C terminates it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const spin = Task.make("spin", Effect.gen(function* () {
  yield* Effect.log("spinning");
  yield* Effect.sync(() => { while (true) {} });
}), { cleanup: "5 minutes" });
export const main = Task.make("main", spin);
if (import.meta.main) Task.main(main);`,
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const sharedWorker = Task.make(
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
export const quick = Task.make("quick", serveUntilStopped("quick", Effect.never), { cleanup: "1 second" });
export const slow = Task.make("slow", serveUntilStopped("slow", Effect.void), { cleanup: "10 minutes" });
export const main = Task.make("main", Effect.andThen(Effect.andThen(slow, quick), Effect.fail(new TaskError({ message: "boom" }))));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "1" });
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "[quick] error TaskCleanupTimedOut: its cleanup did not finish within 1s",
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const frozenFibers = Task.make(
  "a worker frozen by one task's synchronous loop is terminated when that task times out and its cleanup duration runs out, its other fibers are stopped with it, and the run fails",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const waiter = Task.make("waiter", Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("waiter cleaned up"));
  yield* Effect.log("waiting");
  yield* Effect.sync(() => { while (true) {} });
}), { timeout: "1 second", cleanup: "1 second" });
export const other = Task.make("other", Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("other cleaned up"));
  yield* Effect.log("waiting");
  yield* waiter;
}), { cleanup: "1 second" });
export const main = Task.make("main", other);
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "1" });
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain("[other] waiting");
      expect(ran.standardError).toContain("[waiter] error TaskTimedOut: timed out after 1s");
      expect(ran.standardError).toContain(
        "[waiter] error TaskCleanupNotStarted: its worker did not start its cleanup within 1s, so task terminated that worker",
      );
      expect(ran.standardError).not.toContain("[other] error");
      expect(ran.standardError).not.toContain("cleaned up");
      expect(ran.standardError).toMatch(/^failed: waiter \(1 of 3 tasks\), 2 stopped, [\d.]+s$/mu);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const frozenCleanupAfterSelfInterrupt = Task.make(
  "a body that interrupts itself and whose cleanup then freezes its worker fails with TaskCleanupTimedOut, since that cleanup started, and the worker is terminated when the cleanup limit runs out, so a task sharing it still ends",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const server = Task.make("server", Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("server cleaned up"));
  return yield* Task.serve("up");
}));
export const main = Task.make("main", Effect.gen(function* () {
  yield* server;
  yield* Effect.addFinalizer(() => Effect.sync(() => { while (true) {} }));
  yield* Effect.interrupt;
}), { cleanup: "1 second" });
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "1" });
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "[main] error TaskCleanupTimedOut: its cleanup did not finish within 1s",
      );
      expect(ran.standardError).not.toContain("server cleaned up");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const frozenAfterStartingCleanup = Task.make(
  "a worker that starts a task's cleanup and then freezes does not hang the run, since each task on it ends at its cleanup limit, 5s by default",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Latch } from "effect";
import { Task } from "@sshkeda/task";
const waiterCleanupStarted = Latch.makeUnsafe();
export const waiter = Task.make("waiter", Effect.gen(function* () {
  yield* Effect.addFinalizer(() => waiterCleanupStarted.open.pipe(Effect.andThen(Effect.sleep("20 seconds")), Effect.andThen(Effect.log("waiter cleaned up"))));
  return yield* Task.serve("up");
}));
export const freezer = Task.make("freezer", Effect.gen(function* () {
  yield* Effect.addFinalizer(() => waiterCleanupStarted.await.pipe(Effect.andThen(Effect.log("freezer cleanup started")), Effect.andThen(Effect.sync(() => { while (true) {} }))));
  return yield* Task.serve("up");
}), { cleanup: "1 second" });
export const main = Task.make("main", Effect.gen(function* () {
  yield* waiter;
  yield* freezer;
  yield* Effect.log("ready to stop");
  yield* Effect.never;
}));
if (import.meta.main) Task.main(main);`,
      });
      const command = yield* startTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "1" });
      yield* command.waitForLine("[main] ready to stop");
      yield* command.sendSignal("INT");
      const ran = yield* command.waitForExit;
      expect(ran.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(ran.standardError).toContain("[freezer] freezer cleanup started");
      expect(ran.standardError).toContain(
        "[freezer] error TaskCleanupTimedOut: its cleanup did not finish within 1s",
      );
      expect(ran.standardError).toContain(
        "[waiter] error TaskCleanupTimedOut: its cleanup did not finish within 5s",
      );
      expect(ran.standardError).not.toContain("waiter cleaned up");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const frozenAfterAnsweringPastCleanupLimit = Task.make(
  "a worker whose task's cleanup answers past its limit and freezes only later does not hang the run, since a cleanup waiting on that worker ends at its default 5s cleanup limit",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export const waiter = Task.make("waiter", Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.sleep("20 seconds").pipe(Effect.andThen(Effect.log("waiter cleaned up"))));
  return yield* Task.serve("up");
}));
export const freezer = Task.make("freezer", Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.sleep("1500 millis").pipe(Effect.andThen(Effect.log("freezing now")), Effect.andThen(Effect.sync(() => { while (true) {} }))));
  return yield* Task.serve("up");
}), { cleanup: "1 second" });
export const main = Task.make("main", Effect.andThen(Effect.andThen(waiter, freezer), Effect.fail(new TaskError({ message: "boom" }))));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "1" });
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "[freezer] error TaskCleanupTimedOut: its cleanup did not finish within 1s",
      );
      expect(ran.standardError).toContain("[freezer] freezing now");
      expect(ran.standardError).toContain(
        "[waiter] error TaskCleanupTimedOut: its cleanup did not finish within 5s",
      );
      expect(ran.standardError).not.toContain("waiter cleaned up");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const longSynchronousCleanupWithinItsLimit = Task.make(
  "a task's synchronous cleanup that blocks its worker within its own cleanup limit finishes even when another task on that worker runs past a shorter limit",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { writeFileSync } from "node:fs";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export const quick = Task.make("quick", Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.never);
  return yield* Task.serve("up");
}), { cleanup: "1 second" });
export const slow = Task.make("slow", Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.sleep("200 millis").pipe(Effect.andThen(Effect.sync(() => {
    const end = Date.now() + 4000;
    while (Date.now() < end) {}
    writeFileSync("slow-cleaned-up", "");
  }))));
  return yield* Task.serve("up");
}), { cleanup: "10 seconds" });
export const main = Task.make("main", Effect.andThen(Effect.andThen(slow, quick), Effect.fail(new TaskError({ message: "boom" }))));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "1" });
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        "[quick] error TaskCleanupTimedOut: its cleanup did not finish within 1s",
      );
      expect(ran.standardError).not.toContain("TaskCleanupFrozeWorker");
      expect(yield* exists(`${project.directory}/slow-cleaned-up`)).toBe(true);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const stuckLoopTimesOutThenCleansUp = Task.make(
  "a task with timeout and cleanup whose body is stuck in a synchronous loop ends the run within those durations without any signal, and the trace says why its worker was terminated, though the worker exits before the run waits for its workers",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const spin = Task.make("spin", Effect.gen(function* () {
  yield* Effect.log("spinning");
  yield* Effect.sync(() => { while (true) {} });
}), { timeout: "1 second", cleanup: "1 second" });
export const main = Task.make("main", spin);
if (import.meta.main) Task.main(main);`,
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const frozenAtShutdown = Task.make(
  "a worker frozen after its task passed is shut down at the end without waiting on it, so the run passes and exits",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const freezer = Task.make("freezer", Effect.sync(() => {
  process.once("exit", () => {
    while (true) {}
  });
}));
export const main = Task.make("main", freezer);
if (import.meta.main) Task.main(main);`,
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

const WAITER_CLEANUP_SECONDS = 30;

const MAXIMUM_ELAPSED_SECONDS = 15;

export const frozenBeforeAnotherTaskStartsCleanup = Task.make(
  "a worker that freezes in a task's cleanup before another task on it starts its own is terminated once it misses an answer for that cleanup limit, so the other task ends without waiting out its longer limit",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const waiter = Task.make("waiter", Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("waiter cleaned up"));
  return yield* Task.serve("up");
}), { cleanup: "${WAITER_CLEANUP_SECONDS} seconds" });
export const freezer = Task.make("freezer", Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("freezer cleanup started").pipe(Effect.andThen(Effect.sync(() => { while (true) {} }))));
  return yield* Effect.never;
}), { timeout: "1 second", cleanup: "1 second" });
export const main = Task.make("main", Effect.gen(function* () {
  yield* waiter;
  yield* freezer;
}), { cleanup: "1 second" });
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "1" });
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain("[freezer] error TaskTimedOut: timed out after 1s");
      expect(ran.standardError).toContain("[freezer] freezer cleanup started");
      expect(ran.standardError).toContain(
        "[freezer] error TaskCleanupTimedOut: its cleanup did not finish within 1s",
      );
      const mainCleanupNotStarted =
        "[main] error TaskCleanupNotStarted: its worker did not start its cleanup within 1s, so task terminated that worker\n";
      const workerTerminations = [
        "[freezer] error TaskCleanupFrozeWorker: its cleanup ran past its 1s limit and then stopped its worker from answering, so task terminated that worker\n",
        mainCleanupNotStarted,
      ].filter((line) => ran.standardError.includes(line));
      expect(workerTerminations, ran.standardError).not.toEqual([]);
      expect(ran.standardError).not.toContain("waiter cleaned up");
      expect(ran.standardError).not.toContain("[waiter] error");
      const summary = ran.standardError.includes(mainCleanupNotStarted)
        ? /^failed: freezer, main \(2 of 3 tasks\), (?<seconds>[\d.]+)s$/mu
        : /^failed: freezer \(1 of 3 tasks\), (?<seconds>[\d.]+)s$/mu;
      const { seconds } = summary.exec(ran.standardError)?.groups ?? {};
      const elapsedSeconds = Number(seconds);
      expect(elapsedSeconds).toBeLessThan(MAXIMUM_ELAPSED_SECONDS);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);
