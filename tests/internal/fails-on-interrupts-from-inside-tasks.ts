import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect, FileSystem } from "effect";

import { EXIT_CODE_AFTER_SIGINT } from "../shared/exit-codes-after-stop-signals.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTemporaryDirectory } from "../shared/make-temporary-directories.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";

const printedErrorLinesOf = (standardError: string, taskName: string) =>
  standardError
    .split("\n")
    .filter(
      (line) => line.startsWith(`[${taskName}] `) && /^\[[^\]]+\] (?:\S+: )?error /u.test(line),
    );

const makeMainTaskProject = (mainTaskSource: string) =>
  makeProject({
    "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
${mainTaskSource}
if (import.meta.main) Task.main(main);`,
  });

const runMainTaskSource = (mainTaskSource: string) =>
  Effect.flatMap(makeMainTaskProject(mainTaskSource), (project) =>
    runTaskProcess(project, "main.ts"),
  );

const startMainTaskSource = (mainTaskSource: string) =>
  Effect.flatMap(makeMainTaskProject(mainTaskSource), (project) =>
    startTaskProcess(project, "main.ts"),
  );

const INTERRUPTED_FROM_INSIDE = {
  interrupt: "Effect.interrupt",
  joinOfInterruptedFiber:
    "Effect.gen(function* () { const fiber = yield* Effect.forkChild(Effect.never); yield* Fiber.interrupt(fiber); return yield* Fiber.join(fiber); })",
} as const;

const IMPORT_FIBER = 'import { Fiber } from "effect";';

const interruptedWithoutAFailure = (taskName: string) =>
  `[${taskName}] error CauseWithoutFailure: was interrupted without a failure`;

const requestedByBody = (options: string) =>
  `Task.make("main", Effect.gen(function* () { yield* dependency; })${options})`;

const expectRequesterFailsAfterDependency = (dependencyTask: string, requesterTask: string) =>
  Effect.gen(function* () {
    const ran = yield* runMainTaskSource(
      `${IMPORT_FIBER}
export const dependency = ${dependencyTask};
export const main = ${requesterTask};`,
    );
    expect(ran.exitCode).toBe(1);
    expect(printedErrorLinesOf(ran.standardError, "dependency")).toEqual([
      interruptedWithoutAFailure("dependency"),
    ]);
    expect(printedErrorLinesOf(ran.standardError, "main")).toEqual([
      "[main] error AwaitedTaskFailed: dependency failed",
    ]);
  });

const expectRootTaskFails = (rootTask: string) =>
  Effect.gen(function* () {
    const ran = yield* runMainTaskSource(`${IMPORT_FIBER}
export const main = ${rootTask};`);
    expect(ran.exitCode).toBe(1);
    expect(printedErrorLinesOf(ran.standardError, "main")).toEqual([
      interruptedWithoutAFailure("main"),
    ]);
    expect(ran.standardError).toMatch(/^failed: main \(1 of 1 task\), /mu);
  });

const bodyTask = (body: string) => `Task.make("dependency", ${body})`;

const TIMEOUT_OPTIONS = ', { timeout: "5 seconds", cleanup: "5 seconds" }';

export const bodyInterruptRequestedByBody = Task.make(
  "a body that interrupts itself fails the task a body requests with CauseWithoutFailure",
  Effect.scoped(
    expectRequesterFailsAfterDependency(
      bodyTask(INTERRUPTED_FROM_INSIDE.interrupt),
      requestedByBody(""),
    ),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const bodyJoinOfInterruptedFiberRequestedByBody = Task.make(
  "a body that joins an interrupted fiber fails the task a body requests with CauseWithoutFailure",
  Effect.scoped(
    expectRequesterFailsAfterDependency(
      bodyTask(INTERRUPTED_FROM_INSIDE.joinOfInterruptedFiber),
      requestedByBody(""),
    ),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const bodyInterruptRequestedByBodyWithTimeout = Task.make(
  "a body that interrupts itself fails the task a timed body requests, without blaming the timeout",
  Effect.scoped(
    expectRequesterFailsAfterDependency(
      bodyTask(INTERRUPTED_FROM_INSIDE.interrupt),
      requestedByBody(TIMEOUT_OPTIONS),
    ),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const bodyInterruptOfRootTask = Task.make(
  "a root task body that interrupts itself fails the run with CauseWithoutFailure",
  Effect.scoped(expectRootTaskFails(`Task.make("main", ${INTERRUPTED_FROM_INSIDE.interrupt})`)),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const bodyInterruptWithEndlessCleanup = Task.make(
  "a root task body that interrupts itself fails at once and its cleanup limit applies, even when its cleanup never ends",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runMainTaskSource(
        'export const main = Task.make("main", Effect.gen(function* () { yield* Effect.addFinalizer(() => Effect.never); yield* Effect.interrupt; }), { cleanup: "100 millis" });',
      );
      expect(ran.exitCode).toBe(1);
      expect(printedErrorLinesOf(ran.standardError, "main")).toEqual([
        interruptedWithoutAFailure("main"),
        "[main] error TaskCleanupTimedOut: its cleanup did not finish within 100ms",
      ]);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const bodyInterruptWithFailingCleanup = Task.make(
  "a root task body that interrupts itself and whose cleanup then fails prints both",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runMainTaskSource(
        'export const main = Task.make("main", Effect.gen(function* () { yield* Effect.addFinalizer(() => Effect.die(new Error("cleanup broke"))); yield* Effect.interrupt; }));',
      );
      expect(ran.exitCode).toBe(1);
      const mainErrorLines = printedErrorLinesOf(ran.standardError, "main");
      expect(mainErrorLines[0]).toBe(interruptedWithoutAFailure("main"));
      expect(mainErrorLines.slice(1).join("\n")).toMatch(
        /error TaskCleanupFailed: .*cleanup broke/u,
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

const expectOnlyTimedOutPrinted = (timedTask: string, timedOutLine: string) =>
  Effect.gen(function* () {
    const ran = yield* runMainTaskSource(
      `export const timed = ${timedTask};
export const main = Task.make("main", Effect.log("requesting").pipe(Effect.andThen(timed)));`,
    );
    expect(ran.exitCode).toBe(1);
    expect(printedErrorLinesOf(ran.standardError, "timed"), ran.standardError).toEqual([
      timedOutLine,
    ]);
    expect(ran.standardError).not.toContain("CauseWithoutFailure");
    return ran.standardError.split("\n");
  });

export const bodyStoppedByTimeout = Task.make(
  "a body stopped by its timeout prints only TaskTimedOut",
  Effect.scoped(
    Effect.gen(function* () {
      const lines = yield* expectOnlyTimedOutPrinted(
        'Task.make("timed", Effect.log("waiting").pipe(Effect.andThen(Effect.never)), { timeout: "3 seconds", cleanup: "5 seconds" })',
        "[timed] error TaskTimedOut: timed out after 3s",
      );
      expect(lines.indexOf("[timed] waiting")).toBeGreaterThan(-1);
      expect(lines.indexOf("[timed] waiting")).toBeLessThan(
        lines.indexOf("[timed] error TaskTimedOut: timed out after 3s"),
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const bodiesStoppedByCtrlC = Task.make(
  "two bodies stopped by Ctrl-C print no error and say they stopped",
  Effect.scoped(
    Effect.gen(function* () {
      const command = yield* startMainTaskSource(
        `export const waitingQuiet = Task.make("waiting-quiet", Effect.never);
export const waitingBody = Task.make("waiting-body", Effect.log("waiting").pipe(Effect.andThen(Effect.never)));
export const main = Task.make("main", Effect.all([waitingQuiet, waitingBody], { concurrency: "unbounded", discard: true }));`,
      );
      yield* command.waitForLine("[waiting-body] waiting");
      yield* command.sendSignal("INT");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(exited.standardError).not.toContain("CauseWithoutFailure");
      expect(printedErrorLinesOf(exited.standardError, "waiting-quiet")).toEqual([]);
      expect(printedErrorLinesOf(exited.standardError, "waiting-body")).toEqual([]);
      expect(exited.standardError).toContain(
        "[waiting-quiet] stopped: the run ended before it finished",
      );
      expect(exited.standardError).toContain(
        "[waiting-body] stopped: the run ended before it finished",
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

const WAITING_TASK = `export const waiting = Task.make("waiting", Effect.log("waiting").pipe(Effect.andThen(Effect.never)));`;

const TASKS_AWAITING_A_WAITING_TASK = {
  direct: {
    source: `${WAITING_TASK}
export const a = Task.make("a", Effect.gen(function* () { return yield* waiting; }));`,
    stoppedTaskNames: ["a"],
  },
  chain: {
    source: `${WAITING_TASK}
export const b = Task.make("b", Effect.gen(function* () { return yield* waiting; }));
export const a = Task.make("a", Effect.gen(function* () { return yield* b; }));`,
    stoppedTaskNames: ["a", "b"],
  },
} as const;

const expectTasksStoppedWithoutFailure = (
  tasks: (typeof TASKS_AWAITING_A_WAITING_TASK)[keyof typeof TASKS_AWAITING_A_WAITING_TASK],
  standardError: string,
) => {
  expect(standardError).not.toContain("CauseWithoutFailure");
  for (const taskName of tasks.stoppedTaskNames) {
    expect(printedErrorLinesOf(standardError, taskName)).toEqual([]);
    expect(standardError).toContain(`[${taskName}] stopped: the run ended before it finished`);
  }
};

const expectTasksStoppedByCtrlC = (tasks: Parameters<typeof expectTasksStoppedWithoutFailure>[0]) =>
  Effect.gen(function* () {
    const command = yield* startMainTaskSource(`${tasks.source}
export const main = Task.make("main", Effect.asVoid(a));`);
    yield* command.waitForLine("[waiting] waiting");
    yield* command.sendSignal("INT");
    const exited = yield* command.waitForExit;
    expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
    expectTasksStoppedWithoutFailure(tasks, exited.standardError);
  });

const expectTasksStoppedBySiblingFailure = (
  tasks: Parameters<typeof expectTasksStoppedWithoutFailure>[0],
) =>
  Effect.gen(function* () {
    const ran = yield* runMainTaskSource(`${tasks.source}
export const bad = Task.make("bad", Effect.sleep("1 second").pipe(Effect.andThen(Effect.fail(new Error("bad broke")))));
export const main = Task.make("main", Effect.all([a, bad], { concurrency: "unbounded", discard: true }));`);
    expect(ran.exitCode).toBe(1);
    const badLines = printedErrorLinesOf(ran.standardError, "bad");
    expect(badLines).toHaveLength(1);
    expect(badLines[0]).toMatch(
      /^\[bad\] main\.ts:\d+:\d+: error TaskBodyFailed: its body failed with bad broke$/u,
    );
    expectTasksStoppedWithoutFailure(tasks, ran.standardError);
  });

const expectTasksStoppedBySiblingTimeout = (
  tasks: Parameters<typeof expectTasksStoppedWithoutFailure>[0],
) =>
  Effect.gen(function* () {
    const ran = yield* runMainTaskSource(`${tasks.source}
export const timed = Task.make("timed", Effect.never, { timeout: "3 seconds", cleanup: "5 seconds" });
export const main = Task.make("main", Effect.all([a, timed], { concurrency: "unbounded", discard: true }));`);
    expect(ran.exitCode).toBe(1);
    expect(printedErrorLinesOf(ran.standardError, "timed")).toEqual([
      "[timed] error TaskTimedOut: timed out after 3s",
    ]);
    expectTasksStoppedWithoutFailure(tasks, ran.standardError);
  });

export const bodyAwaitingATaskStoppedByCtrlC = Task.make(
  "a body awaiting a task that Ctrl-C stops prints no error and says it stopped",
  Effect.scoped(expectTasksStoppedByCtrlC(TASKS_AWAITING_A_WAITING_TASK.direct)),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const bodyAwaitingATaskStoppedBySiblingFailure = Task.make(
  "a body awaiting a task that a failing sibling stops prints no error and says it stopped",
  Effect.scoped(expectTasksStoppedBySiblingFailure(TASKS_AWAITING_A_WAITING_TASK.direct)),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const bodyAwaitingATaskStoppedBySiblingTimeout = Task.make(
  "a body awaiting a task that a sibling's timeout stops prints no error and says it stopped",
  Effect.scoped(expectTasksStoppedBySiblingTimeout(TASKS_AWAITING_A_WAITING_TASK.direct)),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const chainedBodiesAwaitingATaskStoppedByCtrlC = Task.make(
  "a chain of bodies awaiting a task that Ctrl-C stops prints no error and every task in it says it stopped",
  Effect.scoped(expectTasksStoppedByCtrlC(TASKS_AWAITING_A_WAITING_TASK.chain)),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const chainedBodiesAwaitingATaskStoppedBySiblingFailure = Task.make(
  "a chain of bodies awaiting a task that a failing sibling stops prints no error and every task in it says it stopped",
  Effect.scoped(expectTasksStoppedBySiblingFailure(TASKS_AWAITING_A_WAITING_TASK.chain)),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const chainedBodiesAwaitingATaskStoppedBySiblingTimeout = Task.make(
  "a chain of bodies awaiting a task that a sibling's timeout stops prints no error and every task in it says it stopped",
  Effect.scoped(expectTasksStoppedBySiblingTimeout(TASKS_AWAITING_A_WAITING_TASK.chain)),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const ctrlCAsABodyInterruptsItself = Task.make(
  "a body that interrupts itself while uninterruptible, after Ctrl-C began the stop, prints stopped: and never CauseWithoutFailure",
  Effect.scoped(
    Effect.gen(function* () {
      const directory = yield* makeTemporaryDirectory();
      const stoppingFile = `${directory}/stopping`;
      const command = yield* startMainTaskSource(
        `export const main = Task.make("main", Effect.log("waiting").pipe(Effect.andThen(Effect.uninterruptible(Effect.promise(async () => { while (!(await Bun.file("${stoppingFile}").exists())) await Bun.sleep(5); }).pipe(Effect.andThen(Effect.interrupt))))));`,
      );
      yield* command.waitForLine("[main] waiting");
      yield* command.sendSignal("INT");
      yield* command.waitForLine("[main] still stopping after 2s");
      yield* (yield* FileSystem.FileSystem).writeFileString(stoppingFile, "");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode, exited.standardError).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(exited.standardError).not.toContain(interruptedWithoutAFailure("main"));
      expect(exited.standardError).toContain("[main] stopped: the run ended before it finished");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const bodyInterruptsItselfWhileTheRunStopsAnotherTask = Task.make(
  "a serving body that interrupts itself while the run is still stopping another serving task fails with CauseWithoutFailure",
  Effect.scoped(
    Effect.gen(function* () {
      const directory = yield* makeTemporaryDirectory();
      const cleaningStartedFile = `${directory}/cleaning-started`;
      const ran = yield* runMainTaskSource(
        `import { Task } from "@sshkeda/task";
export const interrupting = Task.make("interrupting", Effect.raceFirst(Task.serve("up"), Effect.promise(async () => { while (!(await Bun.file("${cleaningStartedFile}").exists())) await Bun.sleep(5); }).pipe(Effect.andThen(Effect.interrupt))));
export const cleaning = Task.make("cleaning", Effect.gen(function* () { yield* Effect.addFinalizer(() => Effect.promise(async () => { await Bun.write("${cleaningStartedFile}", ""); await Bun.sleep(1000); })); return yield* Task.serve("up"); }));
export const main = Task.make("main", Effect.all([interrupting, cleaning], { concurrency: "unbounded", discard: true }));`,
      );
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(ran.standardError).toContain("[main] passed in");
      expect(printedErrorLinesOf(ran.standardError, "interrupting"), ran.standardError).toEqual([
        interruptedWithoutAFailure("interrupting"),
      ]);
      expect(printedErrorLinesOf(ran.standardError, "cleaning")).toEqual([]);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);
