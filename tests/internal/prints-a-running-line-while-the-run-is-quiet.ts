import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Clock, Deferred, Effect, Exit, FileSystem, Ref } from "effect";
import { TestClock } from "effect/testing";

import {
  formatRunningLine,
  printRunningLinesWhileQuiet,
  QUIET_MILLISECONDS_BEFORE_A_RUNNING_LINE,
} from "../../src/lib/main-thread/internal/print-running-lines.ts";
import { edit, read } from "../shared/access-files.ts";
import { EXIT_CODE_AFTER_SIGINT } from "../shared/exit-codes-after-stop-signals.ts";
import { REPOSITORY_DIRECTORY } from "../shared/find-repository-directory.ts";
import { makeProjectUsingTaskFrom } from "../shared/make-projects.ts";
import { makeScratchDirectory } from "../shared/make-temporary-directories.ts";
import { makeTest } from "../shared/make-tests.ts";
import { findRunDirectoryPath } from "../shared/name-output-files.ts";
import { startTaskProcess } from "../shared/run-task-processes.ts";
import { startTaskProcessWithPipes } from "../shared/start-task-process-with-pipes.ts";

const SECONDS = 1000;

const QUIET = QUIET_MILLISECONDS_BEFORE_A_RUNNING_LINE;

const NOT_STOPPING = { isStopping: false } as const;

const ownWorkOf = (secondsByTaskName: Readonly<Record<string, number>>) =>
  Object.entries(secondsByTaskName).map(([taskName, seconds]) => ({
    taskName,
    milliseconds: seconds * SECONDS,
  }));

const TWELVE_TASKS_DONE_SECONDS = {
  lint: 12.1,
  e2e: 95,
  unit: 3,
  build: 40.2,
  docs: 1,
  types: 2,
  pack: 4,
  bench: 5,
  audit: 6,
  format: 7,
  size: 8,
  deps: 9,
} as const;

const RUN_SECONDS = 300;

const SHORT_RUN_SECONDS = 3;

const QUIET_MINUTES_WITHOUT_WORK = 3;

const TASKS_DONE = 412;

export const runningLineFormat = makeTest(
  "the running line names the three tasks with the most own work, longest first, then how many more, how many tasks are done and how long the run has run",
  Effect.sync(() => {
    expect(
      formatRunningLine({
        ...NOT_STOPPING,
        ownWork: ownWorkOf(TWELVE_TASKS_DONE_SECONDS),
        endedTaskCount: TASKS_DONE,
        runMilliseconds: RUN_SECONDS * SECONDS,
      }),
    ).toBe("running: [e2e] 95.0s [build] 40.2s [lint] 12.1s and 9 more (412 tasks done), 300.0s");
    expect(
      formatRunningLine({
        ...NOT_STOPPING,
        ownWork: ownWorkOf({ "a]b\tc": 1, lint: 2 }),
        endedTaskCount: 1,
        runMilliseconds: SHORT_RUN_SECONDS * SECONDS,
      }),
    ).toBe(String.raw`running: [lint] 2.0s [a\]b\tc] 1.0s (1 task done), 3.0s`);
    expect(
      formatRunningLine({
        ...NOT_STOPPING,
        ownWork: ownWorkOf({ build: 1, lint: 2, test: 3 }),
        endedTaskCount: 0,
        runMilliseconds: SHORT_RUN_SECONDS * SECONDS,
      }),
    ).toBe("running: [test] 3.0s [lint] 2.0s [build] 1.0s (0 tasks done), 3.0s");
  }),
);

const LONG_TASK_NAME =
  "the build of every page and its assets and its assets and its assets and its assets passes";

const RUNNING_NAME_KEPT_CODE_UNITS = 59;

export const runningLineCutsLongNames = makeTest(
  "the running line cuts each task name longer than 60 characters to its first 59 and an ellipsis, so the line stays short",
  Effect.sync(() => {
    expect(
      formatRunningLine({
        ...NOT_STOPPING,
        ownWork: ownWorkOf({ [LONG_TASK_NAME]: 2, lint: 1 }),
        endedTaskCount: 0,
        runMilliseconds: SHORT_RUN_SECONDS * SECONDS,
      }),
    ).toBe(
      `running: [${LONG_TASK_NAME.slice(0, RUNNING_NAME_KEPT_CODE_UNITS)}…] 2.0s [lint] 1.0s (0 tasks done), 3.0s`,
    );
  }),
);

export const noRunningLineWithoutOwnWork = makeTest(
  "no running line prints when no task is doing its own work, or once the run is stopping",
  Effect.sync(() => {
    expect(
      formatRunningLine({
        ...NOT_STOPPING,
        ownWork: [],
        endedTaskCount: 2,
        runMilliseconds: RUN_SECONDS * SECONDS,
      }),
    ).toBeNull();
    expect(
      formatRunningLine({
        isStopping: true,
        ownWork: ownWorkOf({ build: 1 }),
        endedTaskCount: 0,
        runMilliseconds: RUN_SECONDS * SECONDS,
      }),
    ).toBeNull();
  }),
);

const runLoopOnATestClock = (options: {
  readonly printIsAWrite: boolean;
  readonly steps: (loop: {
    readonly printed: Ref.Ref<readonly string[]>;
    readonly hasWork: Ref.Ref<boolean>;
    readonly writeAt: (atMilliseconds: number, isLineOpen?: boolean) => Effect.Effect<void>;
    readonly setTerminalCommandRunning: (isRunning: boolean) => Effect.Effect<void>;
  }) => Effect.Effect<void>;
}) =>
  Effect.gen(function* () {
    const lastWrite = yield* Ref.make({
      atMilliseconds: 0,
      isLineOpen: false,
      nextWrite: yield* Deferred.make<void>(),
    });
    const printed = yield* Ref.make<readonly string[]>([]);
    const hasWork = yield* Ref.make(true);
    const writeAt = (atMilliseconds: number, isLineOpen = false) =>
      Deferred.make<void>().pipe(
        Effect.flatMap((nextWrite) =>
          Ref.getAndSet(lastWrite, { atMilliseconds, isLineOpen, nextWrite }),
        ),
        Effect.flatMap(({ nextWrite }) => Deferred.done(nextWrite, Exit.void)),
      );
    const terminalCommands = yield* Ref.make({
      isAnyRunning: false,
      lastEndedAtMilliseconds: 0,
      nextChange: yield* Deferred.make<void>(),
    });
    const setTerminalCommandRunning = (isRunning: boolean) =>
      Effect.gen(function* () {
        const nowMilliseconds = yield* Clock.currentTimeMillis;
        const nextChange = yield* Deferred.make<void>();
        const previous = yield* Ref.getAndUpdate(terminalCommands, (current) => ({
          isAnyRunning: isRunning,
          lastEndedAtMilliseconds: isRunning ? current.lastEndedAtMilliseconds : nowMilliseconds,
          nextChange,
        }));
        yield* Deferred.done(previous.nextChange, Exit.void);
      });
    const loop = printRunningLinesWhileQuiet({
      readTerminalCommands: Ref.get(terminalCommands).pipe(
        Effect.map(({ isAnyRunning, lastEndedAtMilliseconds, nextChange }) => ({
          isAnyRunning,
          lastEndedAtMilliseconds,
          awaitChange: Deferred.await(nextChange),
        })),
      ),
      readLastWrite: Ref.get(lastWrite).pipe(
        Effect.map(({ atMilliseconds, isLineOpen, nextWrite }) => ({
          atMilliseconds,
          isLineOpen,
          awaitNextWrite: Deferred.await(nextWrite),
        })),
      ),
      describe: (nowMilliseconds) =>
        Ref.get(hasWork).pipe(
          Effect.map((working) => (working ? `running at ${nowMilliseconds / SECONDS}s` : null)),
        ),
      print: (line) =>
        Ref.update(printed, (lines) => [...lines, line]).pipe(
          Effect.andThen(
            options.printIsAWrite
              ? Clock.currentTimeMillis.pipe(
                  Effect.flatMap((nowMilliseconds) => writeAt(nowMilliseconds)),
                )
              : Effect.void,
          ),
        ),
    });
    yield* Effect.raceFirst(
      loop,
      Effect.yieldNow.pipe(
        Effect.andThen(options.steps({ printed, hasWork, writeAt, setTerminalCommandRunning })),
      ),
    );
  }).pipe(Effect.provide(TestClock.layer()));

const advance = (milliseconds: number) => TestClock.adjust(milliseconds);

export const runningLineAfterAQuietMinute = makeTest(
  "a running line prints after 60 seconds in which nothing was written, and again after each further 60 seconds, its own write restarting the wait",
  runLoopOnATestClock({
    printIsAWrite: true,
    steps: ({ printed }) =>
      Effect.gen(function* () {
        yield* advance(QUIET - 1);
        expect(yield* Ref.get(printed)).toEqual([]);
        yield* advance(1);
        expect(yield* Ref.get(printed)).toEqual(["running at 60s"]);
        yield* advance(QUIET - 1);
        expect(yield* Ref.get(printed)).toEqual(["running at 60s"]);
        yield* advance(1);
        expect(yield* Ref.get(printed)).toEqual(["running at 60s", "running at 120s"]);
      }),
  }),
);

export const hiddenRunningLinesStillWaitAMinute = makeTest(
  "running lines that are not written, as at SSHKEDA_TASK_LOG=error, are still made only once each 60 quiet seconds",
  runLoopOnATestClock({
    printIsAWrite: false,
    steps: ({ printed }) =>
      Effect.gen(function* () {
        yield* advance(QUIET);
        yield* advance(QUIET - 1);
        expect(yield* Ref.get(printed)).toEqual(["running at 60s"]);
        yield* advance(1);
        expect(yield* Ref.get(printed)).toEqual(["running at 60s", "running at 120s"]);
      }),
  }),
);

const HALF_A_MINUTE = 30_000;

export const writtenLineRestartsTheWait = makeTest(
  "any line written to the terminal restarts the 60 seconds before a running line",
  runLoopOnATestClock({
    printIsAWrite: true,
    steps: ({ printed, writeAt }) =>
      Effect.gen(function* () {
        yield* advance(HALF_A_MINUTE);
        yield* writeAt(HALF_A_MINUTE);
        yield* advance(QUIET - 1);
        expect(yield* Ref.get(printed)).toEqual([]);
        yield* advance(1);
        expect(yield* Ref.get(printed)).toEqual(["running at 90s"]);
      }),
  }),
);

export const noRunningLineWhileNoTaskWorks = makeTest(
  "while no task does its own work no running line prints, and once one does, the line comes at the next 60-second mark",
  runLoopOnATestClock({
    printIsAWrite: true,
    steps: ({ printed, hasWork }) =>
      Effect.gen(function* () {
        yield* Ref.set(hasWork, false);
        yield* advance(QUIET * QUIET_MINUTES_WITHOUT_WORK);
        expect(yield* Ref.get(printed)).toEqual([]);
        yield* Ref.set(hasWork, true);
        yield* advance(QUIET);
        expect(yield* Ref.get(printed)).toEqual(["running at 240s"]);
      }),
  }),
);

export const noRunningLineWhileATerminalCommandRuns = makeTest(
  "no running line prints while a command has the terminal, and the 60 seconds start again when it ends",
  runLoopOnATestClock({
    printIsAWrite: true,
    steps: ({ printed, setTerminalCommandRunning }) =>
      Effect.gen(function* () {
        yield* advance(HALF_A_MINUTE);
        yield* setTerminalCommandRunning(true);
        yield* advance(QUIET * QUIET_MINUTES_WITHOUT_WORK);
        expect(yield* Ref.get(printed)).toEqual([]);
        yield* setTerminalCommandRunning(false);
        yield* advance(QUIET - 1);
        expect(yield* Ref.get(printed)).toEqual([]);
        yield* advance(1);
        expect(yield* Ref.get(printed)).toEqual(["running at 270s"]);
      }),
  }),
);

export const noRunningLineInsideAnUnfinishedLine = makeTest(
  "no running line prints while a line on the terminal is unfinished, and the 60 seconds start again when it ends",
  runLoopOnATestClock({
    printIsAWrite: true,
    steps: ({ printed, writeAt }) =>
      Effect.gen(function* () {
        yield* advance(HALF_A_MINUTE);
        yield* writeAt(HALF_A_MINUTE, true);
        yield* advance(QUIET * QUIET_MINUTES_WITHOUT_WORK);
        expect(yield* Ref.get(printed)).toEqual([]);
        const lineEndsAt = HALF_A_MINUTE + QUIET * QUIET_MINUTES_WITHOUT_WORK;
        yield* writeAt(lineEndsAt);
        yield* advance(QUIET - 1);
        expect(yield* Ref.get(printed)).toEqual([]);
        yield* advance(1);
        expect(yield* Ref.get(printed)).toEqual(["running at 270s"]);
      }),
  }),
);

const SHORT_QUIET_MILLISECONDS = 200;

const makeShortQuietProject = (
  files: Readonly<Record<string, string>>,
  quietMilliseconds = SHORT_QUIET_MILLISECONDS,
  heldBackWrite: string | null = null,
) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const repository = yield* REPOSITORY_DIRECTORY;
    const engine = yield* makeScratchDirectory;
    yield* fileSystem.copy(`${repository}/src`, `${engine}/src`);
    yield* fileSystem.copyFile(`${repository}/package.json`, `${engine}/package.json`);
    yield* fileSystem.symlink(`${repository}/node_modules`, `${engine}/node_modules`);
    yield* edit(
      `${engine}/src/lib/main-thread/internal/print-running-lines.ts`,
      `QUIET_MILLISECONDS_BEFORE_A_RUNNING_LINE = ${String(QUIET).replace(/(?=\d{3}$)/u, "_")}`,
      `QUIET_MILLISECONDS_BEFORE_A_RUNNING_LINE = ${quietMilliseconds}`,
    );
    if (heldBackWrite !== null) {
      yield* edit(
        `${engine}/src/lib/task/internal/run-root-task.ts`,
        "Stream.run(stdio.stderr()),",
        `Stream.tap(() => Effect.sleep("${heldBackWrite}")), Stream.run(stdio.stderr()),`,
      );
    }
    return yield* makeProjectUsingTaskFrom({ taskPackageDirectoryPath: engine, files });
  });

const OWN_WORK_PROJECT = `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
const quick = Task.make({ name: "quick", run: Effect.void });
const server = Task.make({ name: "server", output: Schema.String, run: Task.serve("up") });
const child = Task.make({ name: "child", run: Task.exec("cat", ["go"]) });
const parent = Task.make({ name: "parent", run: Effect.gen(function* () { yield* child; }) });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* quick;
  yield* server;
  yield* parent;
}) });`;

const RUNNING_CHILD_LINE = /^running: \[child\] [\d.]+s \(1 task done\), [\d.]+s$/u;

const RUNNING_ONE_TASK_LINE = /^running: \[[^\]]+\] [\d.]+s \(\d+ tasks? done\), [\d.]+s$/u;

const runningLinesOf = (standardError: string) =>
  standardError.split("\n").filter((line) => line.startsWith("running:"));

export const runningLineNamesOnlyOwnWork = makeTest(
  "a quiet run's running line names only the task doing its own work, not the tasks waiting on it or a serving task, prints at SSHKEDA_TASK_LOG=info and debug, and goes to no task file",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeShortQuietProject({ "main.ts": OWN_WORK_PROJECT });
      for (const level of ["info", "debug"]) {
        const started = yield* startTaskProcessWithPipes(project, ["go"], {
          SSHKEDA_TASK_LOG: level,
        });
        yield* started.releaseAfter(["running: [child]"], "go");
        const ran = yield* started.waitForExit;
        yield* Task.exec("rm", ["go"], { cwd: project.directory });
        expect(ran.exitCode, ran.standardError).toBe(0);
        const runningLines = runningLinesOf(ran.standardError);
        expect(
          runningLines.some((line) => RUNNING_CHILD_LINE.test(line)),
          ran.standardError,
        ).toBe(true);
        for (const line of runningLines) {
          expect(line, ran.standardError).toMatch(RUNNING_ONE_TASK_LINE);
        }
        const firstChildLine = runningLines.findIndex((line) => RUNNING_CHILD_LINE.test(line));
        for (const line of runningLines.slice(firstChildLine)) {
          expect(line, ran.standardError).toMatch(RUNNING_CHILD_LINE);
        }
        const lines = ran.standardError.split("\n");
        expect(lines.findLastIndex((line) => line.startsWith("running:"))).toBeLessThan(
          lines.findIndex((line) => line.startsWith("passed:")),
        );
        const runDirectory = yield* Effect.fromNullishOr(findRunDirectoryPath(ran.standardError));
        const fileNames = yield* (yield* FileSystem.FileSystem).readDirectory(runDirectory);
        for (const fileName of fileNames.filter((name) => name.endsWith(".output"))) {
          expect(yield* read(`${runDirectory}/${fileName}`)).not.toContain("running:");
        }
      }
    }),
  ),
);

const OWN_WORK_SECONDS = 1;

const QUIET_MILLISECONDS_BETWEEN_TICKS = 1000;

const TICKS = 25;

export const writtenLinesHoldBackTheRunningLine = makeTest(
  "lines a run writes to the terminal hold back its running line until nothing has been written for the whole wait",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeShortQuietProject(
        {
          "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  for (let tick = 1; tick <= ${TICKS}; tick += 1) {
    yield* Effect.log(\`tick \${tick}\`);
    yield* Effect.sleep("100 millis");
  }
  yield* Task.exec("cat", ["go"]);
}) });`,
        },
        QUIET_MILLISECONDS_BETWEEN_TICKS,
      );
      const started = yield* startTaskProcessWithPipes(project, ["go"]);
      yield* started.releaseAfter(["running: [main]"], "go");
      const ran = yield* started.waitForExit;
      expect(ran.exitCode, ran.standardError).toBe(0);
      const lines = ran.standardError.split("\n");
      expect(
        lines.findIndex((line) => line.startsWith("running:")),
        ran.standardError,
      ).toBeGreaterThan(lines.indexOf(`[main] tick ${TICKS}`));
    }),
  ),
);

export const runningLineHiddenAtTheErrorLevel = makeTest(
  "a quiet run prints a running line at the default log level, and the same run at SSHKEDA_TASK_LOG=error prints none",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeShortQuietProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sleep", ["${OWN_WORK_SECONDS}"]) });`,
      });
      const runAt = (environment: Readonly<Record<string, string>>) =>
        startTaskProcess(project, "main.ts", environment).pipe(
          Effect.flatMap((started) => started.waitForExit),
        );
      const atTheDefaultLevel = yield* runAt({});
      expect(atTheDefaultLevel.exitCode, atTheDefaultLevel.standardError).toBe(0);
      expect(
        runningLinesOf(atTheDefaultLevel.standardError).length,
        atTheDefaultLevel.standardError,
      ).toBeGreaterThan(0);
      const atTheErrorLevel = yield* runAt({ SSHKEDA_TASK_LOG: "error" });
      expect(atTheErrorLevel.exitCode, atTheErrorLevel.standardError).toBe(0);
      expect(runningLinesOf(atTheErrorLevel.standardError)).toEqual([]);
    }),
  ),
);

const QUIET_WAITS_IN_A_HELD_BACK_WRITE = 3;

const HELD_BACK_WRITE = `${String(SHORT_QUIET_MILLISECONDS * QUIET_WAITS_IN_A_HELD_BACK_WRITE)} millis`;

export const runningLineKeepsAPartLineWhole = makeTest(
  "a running line never splits a line a command has only partly written, even when it was decided before that part reached the terminal, and prints once that line ends",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeShortQuietProject(
        {
          "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "partial", run: Task.exec("sh", ["-c", "printf BEGIN; cat end-gate; printf 'END\\n'; cat go"], { print: true }) });`,
        },
        SHORT_QUIET_MILLISECONDS,
        HELD_BACK_WRITE,
      );
      const started = yield* startTaskProcessWithPipes(project, ["end-gate", "go"]);
      yield* started.waitForStandardError("[partial] BEGIN");
      yield* started.release("end-gate", "[partial] BEGIN was written");
      yield* started.waitForLines(["[partial] BEGINEND"], "go");
      yield* started.releaseAfter(["running: [partial]"], "go");
      const ran = yield* started.waitForExit;
      expect(ran.exitCode, ran.standardError).toBe(0);
      const lines = ran.standardError.split("\n");
      const wholeLineIndex = lines.indexOf("[partial] BEGINEND");
      expect(wholeLineIndex, ran.standardError).toBeGreaterThan(0);
      expect(
        lines.slice(wholeLineIndex + 1).some((line) => line.startsWith("running:")),
        ran.standardError,
      ).toBe(true);
    }),
  ),
);

export const noRunningLineWhileOnlyServingOrStopping = makeTest(
  "a run whose only task is serving prints no running line, and a run prints none once it is stopping",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeShortQuietProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const serving = Task.make({ name: "serving", output: Schema.String, run: Task.serve("up") });
export const working = Task.make({ name: "working", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("cleaning up").pipe(Effect.andThen(Task.exec("sleep", ["${OWN_WORK_SECONDS}"]))));
  yield* Task.exec("sleep", ["60"]);
}) });`,
        "serving.ts": `export { serving as default } from "./main.ts";`,
        "working.ts": `export { working as default } from "./main.ts";`,
      });
      const servingRun = yield* startTaskProcess(project, "serving.ts");
      yield* servingRun.waitForLine("serving: [serving]");
      yield* Task.exec("sleep", [String(OWN_WORK_SECONDS)]);
      yield* servingRun.sendSignal("INT");
      const servedRun = yield* servingRun.waitForExit;
      expect(servedRun.exitCode, servedRun.standardError).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(runningLinesOf(servedRun.standardError)).toEqual([]);
      const workingRun = yield* startTaskProcess(project, "working.ts");
      yield* workingRun.waitForLine("running: [working]");
      yield* workingRun.sendSignal("INT");
      const stoppedRun = yield* workingRun.waitForExit;
      expect(stoppedRun.exitCode, stoppedRun.standardError).toBe(EXIT_CODE_AFTER_SIGINT);
      const lines = stoppedRun.standardError.split("\n");
      const stoppingIndex = lines.findIndex((line) => line.startsWith("stopping:"));
      expect(stoppingIndex, stoppedRun.standardError).toBeGreaterThan(0);
      expect(lines.slice(stoppingIndex).filter((line) => line.startsWith("running:"))).toEqual([]);
      expect(stoppedRun.standardError).toContain("[working] cleaning up");
    }),
  ),
);

const HOLDING_THE_TERMINAL = "holding the terminal";

const RELEASED_THE_TERMINAL = "released the terminal";

const CHILD_WAITS = "[child] waits while the command holds the terminal";

const RUNNING_CHILD_LINE_WITH_NO_TASK_DONE =
  /^running: \[child\] [\d.]+s \(0 tasks done\), [\d.]+s$/u;

export const noRunningLineWhileATerminalCommandHasTheTerminal = makeTest(
  'a run prints no running line while a command run with stdio: "inherit" has the terminal, and prints one after it ends while another task still works',
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeShortQuietProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const child = Task.make({ name: "child", run: Effect.gen(function* () {
  yield* Task.exec("cat", ["child-gate"]);
  yield* Effect.log("waits while the command holds the terminal");
  yield* Task.exec("cat", ["go"]);
}) });
export default Task.make({ name: "main", run: Effect.all(
  [Task.exec("sh", ["-c", "echo ${HOLDING_THE_TERMINAL} >&2; cat hold; echo ${RELEASED_THE_TERMINAL} >&2"], { stdio: "inherit" }), child],
  { concurrency: "unbounded", discard: true },
) });`,
      });
      const started = yield* startTaskProcessWithPipes(project, ["child-gate", "hold", "go"]);
      yield* started.releaseAfter([HOLDING_THE_TERMINAL], "child-gate");
      yield* started.waitForLines([CHILD_WAITS], "hold");
      yield* Task.exec("sleep", [String(OWN_WORK_SECONDS)]);
      yield* started.release("hold", `${String(OWN_WORK_SECONDS)}s of quiet`);
      yield* started.releaseAfter([RELEASED_THE_TERMINAL, "running: [child]"], "go");
      const ran = yield* started.waitForExit;
      expect(ran.exitCode, ran.standardError).toBe(0);
      const lines = ran.standardError.split("\n");
      const childWaitsIndex = lines.indexOf(CHILD_WAITS);
      const releasedIndex = lines.indexOf(RELEASED_THE_TERMINAL);
      expect(childWaitsIndex, ran.standardError).toBeGreaterThan(
        lines.indexOf(HOLDING_THE_TERMINAL),
      );
      expect(releasedIndex, ran.standardError).toBeGreaterThan(childWaitsIndex);
      expect(runningLinesOf(lines.slice(childWaitsIndex, releasedIndex).join("\n"))).toEqual([]);
      expect(
        lines.slice(releasedIndex).some((line) => RUNNING_CHILD_LINE_WITH_NO_TASK_DONE.test(line)),
        ran.standardError,
      ).toBe(true);
    }),
  ),
);

export const runningLineAfterAPassedTaskLeftALineUnfinished = makeTest(
  "a run prints its running line at the default level after a task that passed left a line unfinished, since its hidden ended line still ends that line",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeShortQuietProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const download = Task.make({ name: "download", run: Effect.sync(() => { process.stdout.write("no newline from task code"); }) });
const work = Task.make({ name: "work", run: Task.exec("cat", ["go"]) });
export default Task.make({ name: "main", run: Effect.gen(function* () { yield* download; yield* work; }) });`,
      });
      const started = yield* startTaskProcessWithPipes(project, ["go"]);
      yield* started.releaseAfter(["running: [work]"], "go");
      const ran = yield* started.waitForExit;
      expect(ran.exitCode, ran.standardError).toBe(0);
      const lines = ran.standardError.split("\n");
      expect(lines, ran.standardError).toContain("output: no newline from task code");
      expect(
        lines.some((line) => /^running: \[work\] [\d.]+s \(1 task done\), [\d.]+s$/u.test(line)),
        ran.standardError,
      ).toBe(true);
    }),
  ),
);

export const runningLineListsCleanupAfterAFailedBody = makeTest(
  "a running line lists a task running its cleanup after its body failed, as it lists one whose body passed, and no longer once the task has ended",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeShortQuietProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const flaky = Task.make({ name: "flaky", cleanup: "1 minute", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Task.exec("cat", ["go"]).pipe(Effect.orDie));
  return yield* Effect.fail(new Error("flaky failed"));
}) });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.exit(flaky);
  yield* Task.exec("cat", ["after"]);
}) });`,
      });
      const started = yield* startTaskProcessWithPipes(project, ["go", "after"]);
      yield* started.releaseAfter(["running: [flaky]"], "go");
      yield* started.releaseAfter(["ended: [flaky]", "running: [main]"], "after");
      const ran = yield* started.waitForExit;
      const lines = ran.standardError.split("\n");
      const flakyEndedIndex = lines.findIndex((line) => line.startsWith("ended: [flaky]"));
      expect(
        lines
          .slice(0, flakyEndedIndex)
          .some((line) => /^running: (?:.* )?\[flaky\] [\d.]+s /u.test(line)),
        ran.standardError,
      ).toBe(true);
      expect(
        runningLinesOf(lines.slice(flakyEndedIndex).join("\n")).filter((line) =>
          line.includes("[flaky]"),
        ),
        ran.standardError,
      ).toEqual([]);
    }),
  ),
);

const RUNNING_PARENT_SECONDS =
  /^running: \[parent\] (?<ownSeconds>[\d.]+)s \(1 task done\), (?<runSeconds>[\d.]+)s$/u;

export const runningLineLeavesOutWaitingOnOtherTasks = makeTest(
  "a task's time on the running line leaves out the time it waited on the tasks it awaited",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeShortQuietProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const child = Task.make({ name: "child", run: Task.exec("cat", ["child-gate"]) });
export default Task.make({ name: "parent", run: Effect.gen(function* () {
  yield* child;
  yield* Task.exec("cat", ["go"]);
}) });`,
      });
      const started = yield* startTaskProcessWithPipes(project, ["child-gate", "go"]);
      yield* started.waitForLines(["running: [child]"], "child-gate");
      yield* Task.exec("sleep", [String(OWN_WORK_SECONDS)]);
      yield* started.release("child-gate", `${String(OWN_WORK_SECONDS)}s of waiting`);
      yield* started.releaseAfter(["running: [parent]"], "go");
      const ran = yield* started.waitForExit;
      expect(ran.exitCode, ran.standardError).toBe(0);
      const parentLine = ran.standardError
        .split("\n")
        .map((line) => RUNNING_PARENT_SECONDS.exec(line)?.groups)
        .find((groups) => groups !== undefined);
      expect(parentLine, ran.standardError).toBeDefined();
      expect(Number(parentLine?.["ownSeconds"]), ran.standardError).toBeLessThanOrEqual(
        Number(parentLine?.["runSeconds"]) - OWN_WORK_SECONDS,
      );
    }),
  ),
);
