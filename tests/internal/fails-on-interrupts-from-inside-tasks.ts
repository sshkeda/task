import { expect } from "bun:test";

import { Effect, FileSystem } from "effect";

import { EXIT_CODE_AFTER_SIGINT } from "../shared/exit-codes-after-stop-signals.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTemporaryDirectory } from "../shared/make-temporary-directories.ts";
import { makeTest } from "../shared/make-tests.ts";
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
${mainTaskSource}`,
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
  `Task.make({ name: "main", run: Effect.gen(function* () { yield* dependency; })${options} })`;

const expectRequesterFailsAfterDependency = (dependencyTask: string, requesterTask: string) =>
  Effect.gen(function* () {
    const ran = yield* runMainTaskSource(
      `${IMPORT_FIBER}
export const dependency = ${dependencyTask};
export default ${requesterTask};`,
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
export default ${rootTask};`);
    expect(ran.exitCode).toBe(1);
    expect(printedErrorLinesOf(ran.standardError, "main")).toEqual([
      interruptedWithoutAFailure("main"),
    ]);
    expect(ran.standardError).toMatch(/^failed: main \(1 of 1 task\), /mu);
  });

const bodyTask = (body: string) => `Task.make({ name: "dependency", run: ${body} })`;

const TIMEOUT_OPTIONS = ', timeout: "5 seconds", cleanup: "5 seconds"';

export const bodyInterruptRequestedByBody = makeTest(
  "a body that interrupts itself fails the task a body requests with CauseWithoutFailure",
  Effect.scoped(
    expectRequesterFailsAfterDependency(
      bodyTask(INTERRUPTED_FROM_INSIDE.interrupt),
      requestedByBody(""),
    ),
  ),
);

export const bodyJoinOfInterruptedFiberRequestedByBody = makeTest(
  "a body that joins an interrupted fiber fails the task a body requests with CauseWithoutFailure",
  Effect.scoped(
    expectRequesterFailsAfterDependency(
      bodyTask(INTERRUPTED_FROM_INSIDE.joinOfInterruptedFiber),
      requestedByBody(""),
    ),
  ),
);

export const bodyInterruptRequestedByBodyWithTimeout = makeTest(
  "a body that interrupts itself fails the task a timed body requests, without blaming the timeout",
  Effect.scoped(
    expectRequesterFailsAfterDependency(
      bodyTask(INTERRUPTED_FROM_INSIDE.interrupt),
      requestedByBody(TIMEOUT_OPTIONS),
    ),
  ),
);

export const bodyInterruptOfRootTask = makeTest(
  "a root task body that interrupts itself fails the run with CauseWithoutFailure",
  Effect.scoped(
    expectRootTaskFails(`Task.make({ name: "main", run: ${INTERRUPTED_FROM_INSIDE.interrupt} })`),
  ),
);

export const bodyInterruptWithEndlessCleanup = makeTest(
  "a root task body that interrupts itself fails at once and its cleanup limit applies, even when its cleanup never ends",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runMainTaskSource(
        'export default Task.make({ name: "main", run: Effect.gen(function* () { yield* Effect.addFinalizer(() => Effect.never); yield* Effect.interrupt; }), cleanup: "100 millis" });',
      );
      expect(ran.exitCode).toBe(1);
      expect(printedErrorLinesOf(ran.standardError, "main")).toEqual([
        interruptedWithoutAFailure("main"),
        "[main] error TaskCleanupTimedOut: its cleanup did not finish within 100ms, so the run stopped waiting for it",
      ]);
    }),
  ),
);

export const bodyInterruptWithFailingCleanup = makeTest(
  "a root task body that interrupts itself and whose cleanup then fails prints both",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runMainTaskSource(
        'export default Task.make({ name: "main", run: Effect.gen(function* () { yield* Effect.addFinalizer(() => Effect.die(new Error("cleanup broke"))); yield* Effect.interrupt; }) });',
      );
      expect(ran.exitCode).toBe(1);
      const mainErrorLines = printedErrorLinesOf(ran.standardError, "main");
      expect(mainErrorLines[0]).toBe(interruptedWithoutAFailure("main"));
      expect(mainErrorLines.slice(1).join("\n")).toMatch(
        /error TaskCleanupFailed: .*cleanup broke/u,
      );
    }),
  ),
);

const expectOnlyTimedOutPrinted = (timedTask: string, timedOutLine: string) =>
  Effect.gen(function* () {
    const ran = yield* runMainTaskSource(
      `export const timed = ${timedTask};
export default Task.make({ name: "main", run: Effect.log("requesting").pipe(Effect.andThen(timed)) });`,
    );
    expect(ran.exitCode).toBe(1);
    expect(printedErrorLinesOf(ran.standardError, "timed"), ran.standardError).toEqual([
      timedOutLine,
    ]);
    expect(ran.standardError).not.toContain("CauseWithoutFailure");
    return ran.standardError.split("\n");
  });

export const bodyStoppedByTimeout = makeTest(
  "a body stopped by its timeout prints only TaskTimedOut",
  Effect.scoped(
    Effect.gen(function* () {
      const lines = yield* expectOnlyTimedOutPrinted(
        'Task.make({ name: "timed", run: Effect.log("waiting").pipe(Effect.andThen(Effect.never)), timeout: "3 seconds", cleanup: "5 seconds" })',
        "[timed] error TaskTimedOut: timed out after 3s",
      );
      expect(lines.indexOf("[timed] waiting")).toBeGreaterThan(-1);
      expect(lines.indexOf("[timed] waiting")).toBeLessThan(
        lines.indexOf("[timed] error TaskTimedOut: timed out after 3s"),
      );
    }),
  ),
);

export const bodiesStoppedByCtrlC = makeTest(
  "two bodies stopped by Ctrl-C print no error and say they stopped",
  Effect.scoped(
    Effect.gen(function* () {
      const command = yield* startMainTaskSource(
        `export const waitingQuiet = Task.make({ name: "waiting-quiet", run: Effect.never });
export const waitingBody = Task.make({ name: "waiting-body", run: Effect.log("waiting").pipe(Effect.andThen(Effect.never)) });
export default Task.make({ name: "main", run: Effect.all([waitingQuiet, waitingBody], { concurrency: "unbounded", discard: true }) });`,
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
);

const WAITING_TASK = `export const waiting = Task.make({ name: "waiting", run: Effect.log("waiting").pipe(Effect.andThen(Effect.never)) });`;

const TASKS_AWAITING_A_WAITING_TASK = {
  direct: {
    source: `${WAITING_TASK}
export const a = Task.make({ name: "a", run: Effect.gen(function* () { return yield* waiting; }) });`,
    stoppedTaskNames: ["a"],
  },
  chain: {
    source: `${WAITING_TASK}
export const b = Task.make({ name: "b", run: Effect.gen(function* () { return yield* waiting; }) });
export const a = Task.make({ name: "a", run: Effect.gen(function* () { return yield* b; }) });`,
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
export default Task.make({ name: "main", run: Effect.asVoid(a) });`);
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
export const bad = Task.make({ name: "bad", run: Effect.sleep("1 second").pipe(Effect.andThen(Effect.fail(new Error("bad broke")))) });
export default Task.make({ name: "main", run: Effect.all([a, bad], { concurrency: "unbounded", discard: true }) });`);
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
export const timed = Task.make({ name: "timed", run: Effect.never, timeout: "3 seconds", cleanup: "5 seconds" });
export default Task.make({ name: "main", run: Effect.all([a, timed], { concurrency: "unbounded", discard: true }) });`);
    expect(ran.exitCode).toBe(1);
    expect(printedErrorLinesOf(ran.standardError, "timed")).toEqual([
      "[timed] error TaskTimedOut: timed out after 3s",
    ]);
    expectTasksStoppedWithoutFailure(tasks, ran.standardError);
  });

export const bodyAwaitingATaskStoppedByCtrlC = makeTest(
  "a body awaiting a task that Ctrl-C stops prints no error and says it stopped",
  Effect.scoped(expectTasksStoppedByCtrlC(TASKS_AWAITING_A_WAITING_TASK.direct)),
);

export const bodyAwaitingATaskStoppedBySiblingFailure = makeTest(
  "a body awaiting a task that a failing sibling stops prints no error and says it stopped",
  Effect.scoped(expectTasksStoppedBySiblingFailure(TASKS_AWAITING_A_WAITING_TASK.direct)),
);

export const bodyAwaitingATaskStoppedBySiblingTimeout = makeTest(
  "a body awaiting a task that a sibling's timeout stops prints no error and says it stopped",
  Effect.scoped(expectTasksStoppedBySiblingTimeout(TASKS_AWAITING_A_WAITING_TASK.direct)),
);

export const chainedBodiesAwaitingATaskStoppedByCtrlC = makeTest(
  "a chain of bodies awaiting a task that Ctrl-C stops prints no error and every task in it says it stopped",
  Effect.scoped(expectTasksStoppedByCtrlC(TASKS_AWAITING_A_WAITING_TASK.chain)),
);

export const chainedBodiesAwaitingATaskStoppedBySiblingFailure = makeTest(
  "a chain of bodies awaiting a task that a failing sibling stops prints no error and every task in it says it stopped",
  Effect.scoped(expectTasksStoppedBySiblingFailure(TASKS_AWAITING_A_WAITING_TASK.chain)),
);

export const chainedBodiesAwaitingATaskStoppedBySiblingTimeout = makeTest(
  "a chain of bodies awaiting a task that a sibling's timeout stops prints no error and every task in it says it stopped",
  Effect.scoped(expectTasksStoppedBySiblingTimeout(TASKS_AWAITING_A_WAITING_TASK.chain)),
);

export const ctrlCAsABodyInterruptsItself = makeTest(
  "a body that interrupts itself while uninterruptible, after Ctrl-C began the stop, prints stopped: and never CauseWithoutFailure",
  Effect.scoped(
    Effect.gen(function* () {
      const directory = yield* makeTemporaryDirectory();
      const stoppingFile = `${directory}/stopping`;
      const command = yield* startMainTaskSource(
        `export default Task.make({ name: "main", run: Effect.log("waiting").pipe(Effect.andThen(Effect.uninterruptible(Effect.promise(async () => { while (!(await Bun.file("${stoppingFile}").exists())) await Bun.sleep(5); }).pipe(Effect.andThen(Effect.interrupt))))) });`,
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
);

export const bodyInterruptsItselfWhileTheRunStopsAnotherTask = makeTest(
  "a serving body that interrupts itself while the run is still stopping a serving task that requested it fails with CauseWithoutFailure",
  Effect.scoped(
    Effect.gen(function* () {
      const directory = yield* makeTemporaryDirectory();
      const cleaningStartedFile = `${directory}/cleaning-started`;
      const ran = yield* runMainTaskSource(
        `import { Task } from "@sshkeda/task";
export const interrupting = Task.make({ name: "interrupting", run: Effect.raceFirst(Task.serve("up"), Effect.promise(async () => { while (!(await Bun.file("${cleaningStartedFile}").exists())) await Bun.sleep(5); }).pipe(Effect.andThen(Effect.interrupt))) });
export const cleaning = Task.make({ name: "cleaning", run: Effect.gen(function* () { yield* interrupting; yield* Effect.addFinalizer(() => Effect.promise(async () => { await Bun.write("${cleaningStartedFile}", ""); await Bun.sleep(1000); })); return yield* Task.serve("up"); }) });
export default Task.make({ name: "main", run: Effect.all([interrupting, cleaning], { concurrency: "unbounded", discard: true }) });`,
      );
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(ran.standardError).toContain("[main] passed in");
      expect(printedErrorLinesOf(ran.standardError, "interrupting"), ran.standardError).toEqual([
        interruptedWithoutAFailure("interrupting"),
      ]);
      expect(printedErrorLinesOf(ran.standardError, "cleaning")).toEqual([]);
    }),
  ),
);
