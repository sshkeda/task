import { expect } from "bun:test";

import { Effect, FileSystem } from "effect";

import { read, write } from "../shared/access-files.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { findRunDirectoryPath, withSecondsNamed } from "../shared/name-output-files.ts";
import { readAttribute, readProjectRunsDirectory, readTraces } from "../shared/read-trace-files.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";
import { linesBetweenTheRunLines } from "../shared/split-run-lines.ts";

const ONE_WORKER = { SSHKEDA_TASK_WORKERS: "1" } as const;

const OUTPUT_FILE_NAME = /^\d+\.output$/u;

const readTaskFile = (printed: string, taskName: string) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const runDirectoryPath = findRunDirectoryPath(printed) ?? "";
    const fileNames = (yield* fileSystem.readDirectory(runDirectoryPath)).filter((fileName) =>
      OUTPUT_FILE_NAME.test(fileName),
    );
    const files = yield* Effect.forEach(fileNames, (fileName) =>
      read(`${runDirectoryPath}/${fileName}`),
    );
    const written = files.find((file) => file.startsWith(`started: [${taskName}], `));
    expect(written, printed).toBeDefined();
    return withSecondsNamed(written ?? "")
      .trimEnd()
      .split("\n");
  });

const INTERLEAVED_CALL_COUNT = 50;

export const interleavedBodies = makeTest(
  "console calls from two bodies that interleave on one worker print under each body's own task name and go to that task's file",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const loop = (name: string) => Effect.forEach(Array.from({ length: ${INTERLEAVED_CALL_COUNT} }, (_, call) => call), (call) => Effect.sync(() => console.log(\`\${name} \${call}\`)).pipe(Effect.andThen(Effect.yieldNow)), { discard: true });
const a = Task.make({ name: "a", run: loop("a") });
const b = Task.make({ name: "b", run: loop("b") });
export default Task.make({ name: "main", run: Task.all([a, b]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", ONE_WORKER);
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardOutput).toBe("");
      expect(ran.standardError).not.toContain("output: ");
      for (const taskName of ["a", "b"]) {
        const expected = Array.from(
          { length: INTERLEAVED_CALL_COUNT },
          (_unused, call) => `[${taskName}] ${taskName} ${String(call)}`,
        );
        expect(
          ran.standardError.split("\n").filter((line) => line.startsWith(`[${taskName}] `)),
        ).toEqual(expected);
        const written = yield* readTaskFile(ran.standardError, taskName);
        expect(written.filter((line) => line.startsWith("["))).toEqual(expected);
      }
    }),
  ),
);

export const continuedCalls = makeTest(
  "console calls after an await, in a timer callback, in a forked fiber and in a finalizer print under the name of the body that started them, while another body runs on the same worker",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Fiber } from "effect";
import { Task } from "@sshkeda/task";
const continued = (name: string) => Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.sync(() => console.log(\`\${name} in its cleanup\`)));
  yield* Effect.promise(async () => {
    await new Promise((resolve) => setTimeout(resolve, 20));
    console.log(\`\${name} after await\`);
    await new Promise<void>((resolve) => setTimeout(() => { console.log(\`\${name} in a timer\`); resolve(); }, 20));
  });
  const child = yield* Effect.forkChild(Effect.sleep("20 millis").pipe(Effect.andThen(Effect.sync(() => console.log(\`\${name} in a forked fiber\`)))));
  yield* Fiber.join(child);
});
const a = Task.make({ name: "a", run: continued("a") });
const b = Task.make({ name: "b", run: continued("b") });
export default Task.make({ name: "main", run: Task.all([a, b], { concurrency: "unbounded" }) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", ONE_WORKER);
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).not.toContain("output: ");
      for (const taskName of ["a", "b"]) {
        const expected = ["after await", "in a timer", "in a forked fiber", "in its cleanup"].map(
          (where) => `[${taskName}] ${taskName} ${where}`,
        );
        expect(
          ran.standardError.split("\n").filter((line) => line.startsWith(`[${taskName}] `)),
        ).toEqual(expected);
      }
    }),
  ),
);

export const formattedCalls = makeTest(
  "console calls format their arguments, tables and groups as Bun's console does, with each line printed under the task's name and kept in its file",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.sync(() => {
  console.log("%s has %d items", "cart", 3, { y: [1, 2] });
  console.table([{ a: 1, b: 2 }, { a: 3, b: 4 }]);
  console.group("group");
  console.info("inside");
  console.dir({ deep: { deeper: { deepest: 1 } } }, { depth: 0 });
  console.groupEnd();
  console.debug("after");
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const expected = [
        "[main] cart has 3 items { y: [ 1, 2 ] }",
        "[main] ┌─────────┬───┬───┐",
        "[main] │ (index) │ a │ b │",
        "[main] ├─────────┼───┼───┤",
        "[main] │    0    │ 1 │ 2 │",
        "[main] │    1    │ 3 │ 4 │",
        "[main] └─────────┴───┴───┘",
        "[main] group",
        "[main]   inside",
        "[main]   { deep: [Object] }",
        "[main] after",
      ];
      expect(linesBetweenTheRunLines(ran.standardError)).toEqual([
        ...expected,
        "passed: 1 task, <seconds>",
      ]);
      expect(yield* readTaskFile(ran.standardError, "main")).toEqual([
        "started: [main], <seconds>, 1.output",
        ...expected,
        "ended: [main], <seconds>, passed",
      ]);
    }),
  ),
);

export const standardErrorCalls = makeTest(
  "console.error, console.warn and console.trace print under the task's name like console.log, and nothing reaches standard output",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.sync(() => {
  console.error("broken", 1);
  console.warn("careful");
  console.trace("traced");
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardOutput).toBe("");
      expect(ran.standardError).toContain("\n[main] broken 1\n[main] careful\n[main] ");
      expect(ran.standardError).not.toContain("output: ");
    }),
  ),
);

export const effectConsoleCalls = makeTest(
  "Console.log from Effect in a body prints under the task's name, since Effect's Console uses the global console",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Console, Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Console.log("from Effect's Console");
  yield* Console.error("an error from Effect's Console");
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain(
        "\n[main] from Effect's Console\n[main] an error from Effect's Console\n",
      );
    }),
  ),
);

export const quietConsoleCalls = makeTest(
  "with SSHKEDA_TASK_LOG=error, a body's console calls are left off the terminal and still written to the task's file",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.sync(() => {
  console.log("kept in the file");
  console.error("also kept in the file");
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_LOG: "error" });
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(linesBetweenTheRunLines(ran.standardError)).toEqual(["passed: 1 task, <seconds>"]);
      expect(
        yield* read(`${findRunDirectoryPath(ran.standardError)}/1.output`).pipe(
          Effect.map((written) => withSecondsNamed(written).trimEnd().split("\n")),
        ),
      ).toEqual([
        "started: [main], <seconds>, 1.output",
        "[main] kept in the file",
        "[main] also kept in the file",
        "ended: [main], <seconds>, passed",
      ]);
    }),
  ),
);

export const lateCalls = makeTest(
  "with SSHKEDA_TASK_LOG=debug, a console call from a timer a task set, which fires after that task ended, prints as an output: line after its ended: line and stays out of its file",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { existsSync, writeFileSync } from "node:fs";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const early = Task.make({ name: "early", run: Effect.sync(() => {
  setTimeout(() => { console.log("late from early"); writeFileSync("late", ""); }, 300);
}) });
const waiting = Task.make({ name: "waiting", run: Effect.promise(async () => {
  while (!existsSync("late")) await new Promise((resolve) => setTimeout(resolve, 20));
}) });
export default Task.make({ name: "main", run: Task.all([early, waiting], { concurrency: "unbounded" }) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", {
        ...ONE_WORKER,
        SSHKEDA_TASK_LOG: "debug",
      });
      expect(ran.exitCode, ran.standardError).toBe(0);
      const lines = linesBetweenTheRunLines(ran.standardError);
      expect(lines).toContain("output: late from early");
      expect(lines.indexOf("output: late from early")).toBeGreaterThan(
        lines.indexOf("ended: [early], <seconds>, passed"),
      );
      expect(ran.standardError).not.toContain("[early] late from early");
      expect(yield* readTaskFile(ran.standardError, "early")).toEqual([
        "started: [early], <seconds>, 2.output",
        "ended: [early], <seconds>, passed",
      ]);
    }),
  ),
);

const LATE_STATE_LINE_COUNT = 3;

export const lateCallsKeepTheTaskConsoleState = makeTest(
  "with SSHKEDA_TASK_LOG=debug, a console timer, counter and group a task started carry on in a timer that fires after the task ended, printing as output: lines that stay out of its file",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { existsSync, writeFileSync } from "node:fs";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const early = Task.make({ name: "early", run: Effect.sync(() => {
  console.time("elapsed");
  console.count("counter");
  console.group("group");
  setTimeout(() => {
    console.timeEnd("elapsed");
    console.count("counter");
    console.log("inside the group");
    writeFileSync("late", "");
  }, 300);
}) });
const waiting = Task.make({ name: "waiting", run: Effect.promise(async () => {
  while (!existsSync("late")) await new Promise((resolve) => setTimeout(resolve, 20));
}) });
export default Task.make({ name: "main", run: Task.all([early, waiting], { concurrency: "unbounded" }) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", {
        ...ONE_WORKER,
        SSHKEDA_TASK_LOG: "debug",
      });
      expect(ran.exitCode, ran.standardError).toBe(0);
      const lines = linesBetweenTheRunLines(ran.standardError);
      const endedLine = lines.indexOf("ended: [early], <seconds>, passed");
      const lateLines = lines.slice(endedLine + 1).filter((line) => line.startsWith("output: "));
      expect(lateLines).toHaveLength(LATE_STATE_LINE_COUNT);
      expect(lateLines[0]).toMatch(/^output: {3}elapsed: [\d.]+ms$/u);
      expect(lateLines.slice(1)).toEqual(["output:   counter: 2", "output:   inside the group"]);
      expect(ran.standardError).not.toContain("No such label");
      expect(yield* readTaskFile(ran.standardError, "early")).toEqual([
        "started: [early], <seconds>, 2.output",
        "[early] counter: 1",
        "[early] group",
        "ended: [early], <seconds>, passed",
      ]);
    }),
  ),
);

export const callsOutsideEveryTask = makeTest(
  "a console call from a timer the task file set while it loaded belongs to no task, so it prints as an output: line even while a task runs on that worker",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { existsSync, writeFileSync } from "node:fs";
import { isMainThread } from "node:worker_threads";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
if (!isMainThread) setTimeout(() => { console.log("from the loading file's timer"); writeFileSync("fired", ""); }, 300);
export default Task.make({ name: "main", run: Effect.promise(async () => {
  while (!existsSync("fired")) await new Promise((resolve) => setTimeout(resolve, 20));
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(linesBetweenTheRunLines(ran.standardError)).toContain(
        "output: from the loading file's timer",
      );
      expect(ran.standardError).not.toContain("[main] from the loading file's timer");
    }),
  ),
);

const UNREAD_LINE_COUNT = 64;

const UNREAD_LINE_BYTES = 1_048_576;

const CHECKS_WITHOUT_PROGRESS = 10;

export const unreadStandardErrorHoldsConsoleCalls = makeTest(
  "while nobody reads standard error, a body calling console.log with 64 lines of 1 MiB is held back after about 16 MiB, and every line arrives under the task's name once the reader reads",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { writeFileSync } from "node:fs";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const line = "x".repeat(${UNREAD_LINE_BYTES});
export default Task.make({ name: "chatty", run: Effect.sync(() => {
  for (let written = 1; written <= ${UNREAD_LINE_COUNT}; written++) {
    console.log(line);
    writeFileSync("written", String(written));
  }
}) });`,
        "piped.ts": `const shell = Bun.spawn(["sh", "-c", \`{ \${process.execPath} main.ts 2>&1; echo $? > exit-code; } | (until [ -s written ]; do sleep 0.05; done; seen=$(cat written); same=0; while [ $same -lt ${CHECKS_WITHOUT_PROGRESS} ]; do sleep 0.2; now=$(cat written); if [ "$now" = "$seen" ]; then same=$((same + 1)); else seen=$now; same=0; fi; done; echo $seen > written-while-unread; cat > output)\`], { stdio: ["ignore", "inherit", "inherit"] });
process.exitCode = await shell.exited;`,
      });
      const ran = yield* runTaskProcess(project, "piped.ts");
      const writtenWhileUnread = Number(yield* read(`${project.directory}/written-while-unread`));
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(writtenWhileUnread).toBeGreaterThan(0);
      expect(writtenWhileUnread).toBeLessThan(UNREAD_LINE_COUNT);
      const outputLines = (yield* read(`${project.directory}/output`))
        .split("\n")
        .filter((line) => line === `[chatty] ${"x".repeat(UNREAD_LINE_BYTES)}`);
      expect(outputLines.length).toBe(UNREAD_LINE_COUNT);
      expect((yield* read(`${project.directory}/exit-code`)).trim()).toBe("0");
    }),
  ),
);

const SHOWN_FAILURE_CHARACTERS = 2000;

const BUSY_WAIT_MILLISECONDS = 20_000;

const WAIT_UNTIL_RELEASED = `import { existsSync } from "node:fs";
const waitUntilReleased = (file: string) => {
  const end = Date.now() + ${BUSY_WAIT_MILLISECONDS};
  while (Date.now() < end) if (existsSync(file)) return "released";
  return "gave up";
};`;

export const consoleLinesInPromiseCodeAndTheirSpans = makeTest(
  "a console line in promise code under a span carries that span while its fiber runs and the task's span after an await, when no fiber is running",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.promise(async () => {
  console.log("before the await");
  await new Promise((resolve) => setTimeout(resolve, 10));
  console.log("after the await");
}).pipe(Effect.withSpan("async span")) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      const spanIdOf = (text: string) =>
        trace?.logs.find((log) => log.body.stringValue === text)?.spanId;
      expect(spanIdOf("before the await")).toBe(trace?.findOnlySpan("async span")?.spanId);
      expect(spanIdOf("after the await")).toBe(trace?.findTaskSpan("task", "main")?.spanId);
      expect(spanIdOf("after the await")).toBeDefined();
    }),
  ),
);

const BURST_LINE_COUNT = 50_000;

const SPACED_LINE_COUNT = 20;

export const consoleLinesFromDifferentMillisecondsKeepTheirOwnRecords = makeTest(
  "console lines written in different milliseconds each keep a trace record of their own, even when the main thread prints them in one pass",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.sync(() => {
  for (let line = 0; line < ${BURST_LINE_COUNT}; line++) console.log("burst " + line);
  for (let line = 0; line < ${SPACED_LINE_COUNT}; line++) {
    console.log("spaced " + line);
    const until = performance.now() + 2;
    while (performance.now() < until) {}
  }
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError.slice(-SHOWN_FAILURE_CHARACTERS)).toBe(0);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      const spaced = (trace?.logs ?? []).filter(
        (log) => log.body.stringValue?.includes("spaced ") === true,
      );
      expect(
        spaced.map((log) =>
          log.body.stringValue?.split("\n").filter((line) => line.startsWith("spaced ")),
        ),
      ).toEqual(
        [...Array.from({ length: SPACED_LINE_COUNT }).keys()].map((line) => [
          `spaced ${String(line)}`,
        ]),
      );
      const times = spaced.map((log) => BigInt(log.timeUnixNano));
      expect(times.every((time, index) => index === 0 || time > (times[index - 1] ?? time))).toBe(
        true,
      );
    }),
  ),
);

export const consoleLineBeforeSynchronousWork = makeTest(
  "a console line a body prints before a long synchronous step reaches the terminal while that step runs",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
${WAIT_UNTIL_RELEASED}
export default Task.make({ name: "main", run: Effect.sync(() => {
  console.log("before");
  console.log(waitUntilReleased("release"));
}) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[main] before");
      yield* write(`${project.directory}/release`, "");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode, exited.standardError).toBe(0);
      expect(exited.standardError).toContain("[main] before\n[main] released\n");
    }),
  ),
);

export const consoleLineAfterAnAwaitBeforeSynchronousWork = makeTest(
  "a console line a body prints after an await and before a long synchronous step reaches the terminal while that step runs",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
${WAIT_UNTIL_RELEASED}
export default Task.make({ name: "main", run: Effect.promise(async () => {
  console.log("a");
  await Promise.resolve();
  console.log("starting");
  console.log(waitUntilReleased("release"));
}) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[main] starting");
      yield* write(`${project.directory}/release`, "");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode, exited.standardError).toBe(0);
      expect(exited.standardError).toContain("[main] a\n[main] starting\n[main] released\n");
    }),
  ),
);

const PROGRESS_STEP_COUNT = 3;

export const consoleProgressDuringSynchronousWork = makeTest(
  "each progress line a body prints between synchronous steps, with no await, reaches the terminal before the next step ends",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
${WAIT_UNTIL_RELEASED}
export default Task.make({ name: "main", run: Effect.sync(() => {
  for (let step = 0; step < ${PROGRESS_STEP_COUNT}; step++) {
    console.log("step " + step);
    console.log(waitUntilReleased("release-" + step));
  }
}) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      for (const step of Array.from({ length: PROGRESS_STEP_COUNT }).keys()) {
        yield* command.waitForLine(`[main] step ${String(step)}`);
        yield* write(`${project.directory}/release-${String(step)}`, "");
      }
      const exited = yield* command.waitForExit;
      expect(exited.exitCode, exited.standardError).toBe(0);
      expect(exited.standardError).not.toContain("gave up");
    }),
  ),
);

export const orderedWithOtherLines = makeTest(
  "a body's console lines keep their place among its Effect.log and command lines, in the terminal and in its file, and in its file all come before its ended: line",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.sync(() => console.log("console one"));
  yield* Effect.log("log one");
  yield* Effect.sync(() => console.error("console two"));
  yield* Task.exec("sh", ["-c", "echo command one"], { print: true });
  yield* Effect.sync(() => console.log("console three"));
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const expected = [
        "[main] console one",
        "[main] log one",
        "[main] console two",
        "[main] command one",
        "[main] console three",
      ];
      expect(linesBetweenTheRunLines(ran.standardError)).toEqual([
        ...expected,
        "passed: 1 task, <seconds>",
      ]);
      expect(yield* readTaskFile(ran.standardError, "main")).toEqual([
        "started: [main], <seconds>, 1.output",
        ...expected,
        "ended: [main], <seconds>, passed",
      ]);
    }),
  ),
);

export const consoleLinesAroundEveryOrderedMessage = makeTest(
  "console lines right before and right after an Effect.log line, a warning, a command's start and output, a task it awaits and its own end keep exactly their place, in the terminal and in its file",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const awaited = Task.make({ name: "awaited", run: Effect.log("awaited ran") });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  console.log("first");
  yield* Effect.log("logged");
  console.log("after the log");
  yield* Effect.logWarning("warned");
  console.log("after the warning");
  yield* Task.exec("sh", ["-c", "echo command output"], { print: true });
  console.log("after the command");
  yield* awaited;
  console.log("after the awaited task");
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const mainLines = [
        "[main] first",
        "[main] logged",
        "[main] after the log",
        "[main] warned",
        "[main] after the warning",
        "[main] command output",
        "[main] after the command",
      ];
      expect(linesBetweenTheRunLines(ran.standardError)).toEqual([
        ...mainLines,
        "[awaited] awaited ran",
        "[main] after the awaited task",
        "passed: 2 tasks, <seconds>",
      ]);
      expect(yield* readTaskFile(ran.standardError, "main")).toEqual([
        "started: [main], <seconds>, 1.output",
        ...mainLines,
        "[main] after the awaited task",
        "ended: [main], <seconds>, passed",
      ]);
    }),
  ),
);

const WORKER_EXIT_CODE = 7;

export const callsBeforeProcessExit = makeTest(
  "console lines a body prints right before it calls process.exit print under its name, before its worker's exit",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.sync(() => {
  console.log("first");
  console.log("just before exit");
  process.exit(${WORKER_EXIT_CODE});
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      const lines = ran.standardError.split("\n");
      const exitLine = lines.findIndex((line) =>
        line.includes(`WorkerExited: its worker exited on its own with code ${WORKER_EXIT_CODE}`),
      );
      expect(exitLine, ran.standardError).toBeGreaterThan(lines.indexOf("[main] just before exit"));
      expect(lines.indexOf("[main] first")).toBeGreaterThan(-1);
      expect(lines.indexOf("[main] first")).toBeLessThan(lines.indexOf("[main] just before exit"));
    }),
  ),
);

const BATCH_CHARACTERS = 65_536;

export const consoleCallsWhileArgumentsAreFormatted = makeTest(
  "console calls and Effect logs made while Bun formats a console call's arguments print first, as with Bun's own console, and the call that triggered them prints after them",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const context = yield* Effect.context<never>();
  const loud = { [Symbol.for("nodejs.util.inspect.custom")]: () => {
    console.log("inner");
    Effect.runSyncWith(context)(Effect.log("inner log"));
    console.warn("w".repeat(${BATCH_CHARACTERS}));
    return "loud";
  } };
  console.log("outer", loud);
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError.slice(-SHOWN_FAILURE_CHARACTERS)).toBe(0);
      const expected = [
        "[main] inner",
        "[main] inner log",
        `[main] ${"w".repeat(BATCH_CHARACTERS)}`,
        "[main] outer loud",
      ];
      expect(linesBetweenTheRunLines(ran.standardError)).toEqual([
        ...expected,
        "passed: 1 task, <seconds>",
      ]);
      expect(yield* readTaskFile(ran.standardError, "main")).toEqual([
        "started: [main], <seconds>, 1.output",
        ...expected,
        "ended: [main], <seconds>, passed",
      ]);
    }),
  ),
);

const NESTED_CALL_COUNT = 100_000;

export const manyConsoleCallsWhileArgumentsAreFormatted = makeTest(
  "100,000 console calls made while Bun formats one console call's arguments all print, before the call that made them",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const loud = { [Symbol.for("nodejs.util.inspect.custom")]: () => {
  for (let call = 0; call < ${NESTED_CALL_COUNT}; call++) console.log("nested " + call);
  return "loud";
} };
export default Task.make({ name: "main", run: Effect.sync(() => {
  console.log("outer", loud);
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError.slice(-SHOWN_FAILURE_CHARACTERS)).toBe(0);
      const lines = ran.standardError.split("\n");
      expect(lines.filter((line) => line.startsWith("[main] nested ")).length).toBe(
        NESTED_CALL_COUNT,
      );
      expect(lines.indexOf("[main] outer loud")).toBeGreaterThan(
        lines.indexOf(`[main] nested ${String(NESTED_CALL_COUNT - 1)}`),
      );
    }),
  ),
);

export const consoleCallWhoseFormattingThrows = makeTest(
  "a console call whose argument throws while Bun formats it throws to its caller and prints nothing, and the calls before and after it print",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const broken = { [Symbol.for("nodejs.util.inspect.custom")]: () => { throw new Error("boom"); } };
export default Task.make({ name: "main", run: Effect.sync(() => {
  console.log("before");
  try { console.log("broken", broken); } catch (error) { console.log("caught", (error as Error).message); }
  console.log("after");
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const expected = ["[main] before", "[main] caught boom", "[main] after"];
      expect(linesBetweenTheRunLines(ran.standardError)).toEqual([
        ...expected,
        "passed: 1 task, <seconds>",
      ]);
    }),
  ),
);

export const consoleCallUnderACyclicLogAnnotation = makeTest(
  "a console call under a log annotation that refers to itself prints, and the run passes",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const bad: unknown[] = [];
bad.push(bad);
export default Task.make({ name: "main", run: Effect.sync(() => {
  console.log("hello");
}).pipe(Effect.annotateLogs("bad", bad)) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(linesBetweenTheRunLines(ran.standardError)).toEqual([
        "[main] hello",
        "passed: 1 task, <seconds>",
      ]);
    }),
  ),
);

export const consoleCallsInExitHandlers = makeTest(
  "a console call from an exit handler the body added prints under its name after the body's own line, when the body calls process.exit",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.sync(() => {
  process.on("exit", () => console.log("from exit handler"));
  console.log("body");
  process.exit(${WORKER_EXIT_CODE});
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      const lines = ran.standardError.split("\n");
      expect(lines.indexOf("[main] body"), ran.standardError).toBeGreaterThan(-1);
      expect(lines.indexOf("[main] from exit handler")).toBeGreaterThan(
        lines.indexOf("[main] body"),
      );
      expect(yield* readTaskFile(ran.standardError, "main")).toContain("[main] from exit handler");
    }),
  ),
);

const FULL_BUDGET_CHARACTERS = 16_777_216;

export const consoleCallFromASpanIdGetter = makeTest(
  "a console call from the trace id getter of the span a 16 MiB console line runs under prints first, and the big line and the call after it print within the timeout",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Context, Effect } from "effect";
import type { Tracer } from "effect";
import { Task } from "@sshkeda/task";
const called = { inner: false };
const span: Tracer.ExternalSpan = {
  _tag: "ExternalSpan", sampled: true, annotations: Context.empty(), spanId: "b".repeat(16),
  get traceId() { if (!called.inner) { called.inner = true; console.log("inner"); } return "a".repeat(32); },
};
export default Task.make({ name: "main", timeout: "2 seconds", cleanup: "1 second", run: Effect.sync(() => {
  console.log("z".repeat(${FULL_BUDGET_CHARACTERS}));
  console.log("returned");
}).pipe(Effect.withParentSpan(span)) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError.slice(-SHOWN_FAILURE_CHARACTERS)).toBe(0);
      const lines = ran.standardError.split("\n");
      const bigLine = lines.indexOf(`[main] ${"z".repeat(FULL_BUDGET_CHARACTERS)}`);
      expect(lines.indexOf("[main] inner")).toBeGreaterThan(-1);
      expect(bigLine).toBeGreaterThan(lines.indexOf("[main] inner"));
      expect(lines.indexOf("[main] returned")).toBeGreaterThan(bigLine);
    }),
  ),
);

export const consoleCallUnderASpanWhoseIdGetterThrows = makeTest(
  "a console call under a span whose trace id getter throws prints, and its trace record carries no span ids",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Context, Effect } from "effect";
import type { Tracer } from "effect";
import { Task } from "@sshkeda/task";
const span: Tracer.ExternalSpan = {
  _tag: "ExternalSpan", sampled: true, annotations: Context.empty(), spanId: "b".repeat(16),
  get traceId(): string { throw new Error("no trace id"); },
};
export default Task.make({ name: "main", run: Effect.sync(() => {
  console.log("hello");
}).pipe(Effect.withParentSpan(span)) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(linesBetweenTheRunLines(ran.standardError)).toEqual([
        "[main] hello",
        "passed: 1 task, <seconds>",
      ]);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      const hello = trace?.logs.find((log) => log.body.stringValue === "hello");
      expect(hello).toBeDefined();
      expect(hello?.spanId).toBeUndefined();
    }),
  ),
);

const OUTER_CALL_COUNT = 1000;

export const consoleCallFromASpanIdGetterOnEveryRead = makeTest(
  "a span whose trace id getter makes a console call on every read prints one inner line before each console line under it, since a console call made while span ids are read does not read them again",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Context, Effect } from "effect";
import type { Tracer } from "effect";
import { Task } from "@sshkeda/task";
const span: Tracer.ExternalSpan = {
  _tag: "ExternalSpan", sampled: true, annotations: Context.empty(), spanId: "b".repeat(16),
  get traceId() { console.log("inner"); return "a".repeat(32); },
};
export default Task.make({ name: "main", timeout: "2 seconds", cleanup: "1 second", run: Effect.sync(() => {
  for (let call = 0; call < ${OUTER_CALL_COUNT}; call++) console.log("outer " + call);
  console.log("returned");
}).pipe(Effect.withParentSpan(span)) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError.slice(-SHOWN_FAILURE_CHARACTERS)).toBe(0);
      const printed = linesBetweenTheRunLines(ran.standardError).filter((line) =>
        line.startsWith("[main] "),
      );
      expect(printed).toEqual([
        ...[...Array.from({ length: OUTER_CALL_COUNT }).keys()].flatMap((call) => [
          "[main] inner",
          `[main] outer ${String(call)}`,
        ]),
        "[main] inner",
        "[main] returned",
      ]);
    }),
  ),
);

const MANY_LINES_IN_ONE_CALL = 1_000_000;

export const oneCallWithAMillionLines = makeTest(
  "one console call with a million lines prints every line under the task's name, and the calls before and after it print too",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.sync(() => {
  console.log("before");
  console.log("x\\n".repeat(${MANY_LINES_IN_ONE_CALL}));
  console.log("after");
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError.slice(-SHOWN_FAILURE_CHARACTERS)).toBe(0);
      const lines = ran.standardError.split("\n");
      expect(lines.filter((line) => line === "[main] x").length).toBe(MANY_LINES_IN_ONE_CALL);
      expect(lines.indexOf("[main] before")).toBeLessThan(lines.indexOf("[main] x"));
      expect(lines.indexOf("[main] after")).toBeGreaterThan(lines.lastIndexOf("[main] x"));
    }),
  ),
);

const CLEANUP_CALL_COUNT = 100_000;

const CLEANUP_LINES_THAT_MAY_BE_LOST = 3000;

export const consoleCallsAfterTheBudgetIsLifted = makeTest(
  "after Ctrl-C lifts the output budget, a cleanup that calls console.log 100,000 times and then blocks its worker still prints nearly all of its lines before the worker is terminated",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", cleanup: "2 seconds", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.sync(() => {
    for (let line = 0; line < ${CLEANUP_CALL_COUNT}; line++) console.log("cleanup line " + line);
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
  }));
  yield* Effect.log("ready");
  return yield* Effect.never;
}) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[main] ready");
      yield* command.sendSignal("INT");
      const exited = yield* command.waitForExit;
      const printed = exited.standardError
        .split("\n")
        .filter((line) => line.startsWith("[main] cleanup line "));
      expect(printed.length, exited.standardError.slice(-SHOWN_FAILURE_CHARACTERS)).toBeGreaterThan(
        CLEANUP_CALL_COUNT - CLEANUP_LINES_THAT_MAY_BE_LOST,
      );
      expect(printed[0]).toBe("[main] cleanup line 0");
    }),
  ),
);

export const consoleLinesKeepTheirTraceContext = makeTest(
  "a console line in a span is traced under that span with its text, task and stream and no log annotations, not merged into an earlier line's record",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.sync(() => console.log("outside"));
  yield* Effect.sync(() => console.log("inside")).pipe(Effect.withSpan("inner"), Effect.annotateLogs("marker", "inner"));
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      const inner = trace?.findOnlySpan("inner");
      const inside = trace?.logs.find((log) => log.body.stringValue === "inside");
      const outside = trace?.logs.find((log) => log.body.stringValue === "outside");
      expect(inside?.spanId).toBe(inner?.spanId);
      expect(outside).toBeDefined();
      expect(outside?.spanId).not.toBe(inner?.spanId);
      expect(inside?.attributes.map((attribute) => attribute.key)).toEqual([
        "sshkeda_task.task",
        "sshkeda_task.streamId",
      ]);
      expect(readAttribute(inside?.attributes ?? [], "sshkeda_task.task")).toBe("main");
    }),
  ),
);
