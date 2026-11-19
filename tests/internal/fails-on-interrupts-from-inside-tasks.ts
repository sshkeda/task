import { expect } from "bun:test";

import { Effect } from "effect";

import { EXIT_CODE_AFTER_SIGINT } from "../shared/exit-codes-after-stop-signals.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTemporaryDirectory } from "../shared/make-temporary-directories.ts";
import { makeTest } from "../shared/make-tests.ts";
import { readProjectRunsDirectory, readTraces } from "../shared/read-trace-files.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";

const STOPPED_LINE = "stopped: the run ended before it finished";

const RUN_ENDED_EVENT = "run ended before it finished";

const printedErrorLinesOf = (standardError: string, taskName: string) =>
  standardError
    .split("\n")
    .filter(
      (line) =>
        line.startsWith(`error: [${taskName}] `) &&
        /^error: \[[^\]]+\] \S+: [A-Z]\w*: /u.test(line),
    );

const makeMainTaskProject = (mainTaskSource: string) =>
  makeProject({
    "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
${mainTaskSource}`,
  });

const runMainTaskSource = (mainTaskSource: string) =>
  Effect.flatMap(makeMainTaskProject(mainTaskSource), (project) =>
    Effect.map(runTaskProcess(project, "main.ts"), (ran) => ({ ...ran, project })),
  );

const startMainTaskSource = (mainTaskSource: string) =>
  Effect.flatMap(makeMainTaskProject(mainTaskSource), (project) =>
    Effect.map(startTaskProcess(project, "main.ts"), (command) => ({
      waitForLine: command.waitForLine,
      sendSignal: command.sendSignal,
      waitForExit: Effect.map(command.waitForExit, (exited) => ({ ...exited, project })),
    })),
  );

const findTaskNamesWhoseRunEnded = (
  ran: Parameters<typeof readProjectRunsDirectory>[1] & {
    readonly project: Parameters<typeof readProjectRunsDirectory>[0];
  },
  taskNames: readonly string[],
) =>
  Effect.gen(function* () {
    const [trace] = yield* readTraces(yield* readProjectRunsDirectory(ran.project, ran));
    return taskNames.filter((taskName) =>
      (trace?.findTaskEventNames("task", taskName) ?? []).includes(RUN_ENDED_EVENT),
    );
  });

const INTERRUPTED_FROM_INSIDE = {
  interrupt: "Effect.interrupt",
  joinOfInterruptedFiber:
    "Effect.gen(function* () { const fiber = yield* Effect.forkChild(Effect.never); yield* Fiber.interrupt(fiber); return yield* Fiber.join(fiber); })",
} as const;

const IMPORT_FIBER = 'import { Fiber } from "effect";';

const interruptedWithoutAFailure = (taskName: string, location: string) =>
  `error: [${taskName}] ${location}: CauseWithoutFailure: was interrupted without a failure`;

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
      interruptedWithoutAFailure("dependency", "main.ts:4:32"),
    ]);
    expect(printedErrorLinesOf(ran.standardError, "main")).toEqual([]);
  });

const expectRootTaskFails = (rootTask: string) =>
  Effect.gen(function* () {
    const ran = yield* runMainTaskSource(`${IMPORT_FIBER}
export default ${rootTask};`);
    expect(ran.exitCode).toBe(1);
    expect(printedErrorLinesOf(ran.standardError, "main")).toEqual([
      interruptedWithoutAFailure("main", "main.ts:4:21"),
    ]);
    expect(ran.standardError).toMatch(/^failed: \[main\] \(1 of 1 task\), /mu);
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
        interruptedWithoutAFailure("main", "main.ts:3:21"),
        "error: [main] main.ts:3:21: TaskCleanupTimedOut: its cleanup did not finish within 100ms, so the run stopped waiting for it",
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
      expect(printedErrorLinesOf(ran.standardError, "main")).toEqual([
        interruptedWithoutAFailure("main", "main.ts:3:21"),
        "error: [main] main.ts:3:121: Error: cleanup broke",
      ]);
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
        "error: [timed] main.ts:3:27: TaskTimedOut: timed out after 3s",
      );
      expect(lines.indexOf("[timed] waiting")).toBeGreaterThan(-1);
      expect(lines.indexOf("[timed] waiting")).toBeLessThan(
        lines.indexOf("error: [timed] main.ts:3:27: TaskTimedOut: timed out after 3s"),
      );
    }),
  ),
);

export const bodiesStoppedByCtrlC = makeTest(
  "two bodies stopped by Ctrl-C print no error or end line, and the trace records that the run ended before each finished",
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
      expect(exited.standardError).not.toContain(STOPPED_LINE);
      expect(yield* findTaskNamesWhoseRunEnded(exited, ["waiting-quiet", "waiting-body"])).toEqual([
        "waiting-quiet",
        "waiting-body",
      ]);
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
  ran: Parameters<typeof findTaskNamesWhoseRunEnded>[0],
) =>
  Effect.gen(function* () {
    expect(ran.standardError).not.toContain("CauseWithoutFailure");
    expect(ran.standardError).not.toContain(STOPPED_LINE);
    expect(
      tasks.stoppedTaskNames.flatMap((taskName) =>
        printedErrorLinesOf(ran.standardError, taskName),
      ),
    ).toEqual([]);
    expect(yield* findTaskNamesWhoseRunEnded(ran, tasks.stoppedTaskNames)).toEqual([
      ...tasks.stoppedTaskNames,
    ]);
  });

const expectTasksStoppedByCtrlC = (tasks: Parameters<typeof expectTasksStoppedWithoutFailure>[0]) =>
  Effect.gen(function* () {
    const command = yield* startMainTaskSource(`${tasks.source}
export default Task.make({ name: "main", run: Effect.asVoid(a) });`);
    yield* command.waitForLine("[waiting] waiting");
    yield* command.sendSignal("INT");
    const exited = yield* command.waitForExit;
    expect(exited.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
    yield* expectTasksStoppedWithoutFailure(tasks, exited);
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
    expect(badLines[0]).toMatch(/^error: \[bad\] main\.ts:\d+:\d+: Error: bad broke$/u);
    yield* expectTasksStoppedWithoutFailure(tasks, ran);
  });

const expectTasksStoppedBySiblingTimeout = (
  tasks: Parameters<typeof expectTasksStoppedWithoutFailure>[0],
) =>
  Effect.gen(function* () {
    const ran = yield* runMainTaskSource(`${tasks.source}
export const timed = Task.make({ name: "timed", run: Effect.never, timeout: "3 seconds", cleanup: "5 seconds" });
export default Task.make({ name: "main", run: Effect.all([a, timed], { concurrency: "unbounded", discard: true }) });`);
    expect(ran.exitCode).toBe(1);
    const timedLines = printedErrorLinesOf(ran.standardError, "timed");
    expect(timedLines).toHaveLength(1);
    expect(timedLines[0]).toMatch(
      /^error: \[timed\] main\.ts:\d+:\d+: TaskTimedOut: timed out after 3s$/u,
    );
    yield* expectTasksStoppedWithoutFailure(tasks, ran);
  });

export const bodyAwaitingATaskStoppedByCtrlC = makeTest(
  "a body awaiting a task that Ctrl-C stops prints no error, and the trace records that the run ended before it finished",
  Effect.scoped(expectTasksStoppedByCtrlC(TASKS_AWAITING_A_WAITING_TASK.direct)),
);

export const bodyAwaitingATaskStoppedBySiblingFailure = makeTest(
  "a body awaiting a task that a failing sibling stops prints no error, and the trace records that the run ended before it finished",
  Effect.scoped(expectTasksStoppedBySiblingFailure(TASKS_AWAITING_A_WAITING_TASK.direct)),
);

export const bodyAwaitingATaskStoppedBySiblingTimeout = makeTest(
  "a body awaiting a task that a sibling's timeout stops prints no error, and the trace records that the run ended before it finished",
  Effect.scoped(expectTasksStoppedBySiblingTimeout(TASKS_AWAITING_A_WAITING_TASK.direct)),
);

export const chainedBodiesAwaitingATaskStoppedByCtrlC = makeTest(
  "a chain of bodies awaiting a task that Ctrl-C stops prints no error, and the trace records that the run ended before each finished",
  Effect.scoped(expectTasksStoppedByCtrlC(TASKS_AWAITING_A_WAITING_TASK.chain)),
);

export const chainedBodiesAwaitingATaskStoppedBySiblingFailure = makeTest(
  "a chain of bodies awaiting a task that a failing sibling stops prints no error, and the trace records that the run ended before each finished",
  Effect.scoped(expectTasksStoppedBySiblingFailure(TASKS_AWAITING_A_WAITING_TASK.chain)),
);

export const chainedBodiesAwaitingATaskStoppedBySiblingTimeout = makeTest(
  "a chain of bodies awaiting a task that a sibling's timeout stops prints no error, and the trace records that the run ended before each finished",
  Effect.scoped(expectTasksStoppedBySiblingTimeout(TASKS_AWAITING_A_WAITING_TASK.chain)),
);

export const ctrlCAsABodyInterruptsItself = makeTest(
  "a body that interrupts itself while uninterruptible, once the Ctrl-C stop reached it, never prints CauseWithoutFailure, and the trace records that the run ended before it finished",
  Effect.scoped(
    Effect.gen(function* () {
      const command = yield* startMainTaskSource(
        `import { Exit } from "effect";
export default Task.make({ name: "main", run: Effect.log("waiting").pipe(Effect.andThen(Effect.uninterruptible(Effect.exit(Effect.interruptible(Effect.sleep("5 millis"))).pipe(Effect.repeat({ until: Exit.hasInterrupts }), Effect.andThen(Effect.interrupt))))) });`,
      );
      yield* command.waitForLine("[main] waiting");
      yield* command.sendSignal("INT");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode, exited.standardError).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(exited.standardError).not.toContain("CauseWithoutFailure");
      expect(exited.standardError).not.toContain(STOPPED_LINE);
      expect(yield* findTaskNamesWhoseRunEnded(exited, ["main"])).toEqual(["main"]);
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
      expect(ran.standardError).toMatch(/^failed: \[interrupting\] \(1 of 3 tasks\), [\d.]+s$/mu);
      expect(printedErrorLinesOf(ran.standardError, "interrupting"), ran.standardError).toEqual([
        interruptedWithoutAFailure("interrupting", "main.ts:4:34"),
      ]);
      expect(printedErrorLinesOf(ran.standardError, "cleaning")).toEqual([]);
    }),
  ),
);
