import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Deferred, Effect, Exit, PlatformError, Stream } from "effect";

import { HELD_COMMAND_OUTPUT_BYTES } from "../../src/lib/main-thread/shared/cut-command-output.ts";
import {
  makeHeldCommandOutput,
  RUN_COST_BYTES,
} from "../../src/lib/main-thread/shared/hold-command-output.ts";
import { TaskLines } from "../../src/shared/mark-task-worker-thread-contexts.ts";
import { BYTES_WAITING_TO_BE_WRITTEN } from "../../src/shared/task-output-budget.ts";
import { read } from "../shared/access-files.ts";
import { failureAsValue } from "../shared/failure-as-value.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import {
  findRunDirectoryPath,
  withRunLinePathNamed,
  withSecondsNamed,
} from "../shared/name-output-files.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";
import { linesBetweenTheRunLines } from "../shared/split-run-lines.ts";
import { typeCheckSource } from "../shared/type-check-source.ts";
import { withoutStackFrames } from "../shared/without-stack-frames.ts";

const readFirstTaskFile = (printed: string) =>
  read(`${findRunDirectoryPath(printed)}/1.output`).pipe(
    Effect.map((written) =>
      withRunLinePathNamed(withSecondsNamed(withoutStackFrames(written)), printed)
        .trimEnd()
        .split("\n"),
    ),
  );

const FAILED_COMMAND_EXIT_CODE = 3;

const PRINTED_FAILED_COMMAND_EXIT_CODE = 4;

const throughTheEndedLine = (printed: readonly string[]) =>
  printed.slice(0, printed.indexOf("ended: [main], <seconds>, failed") + 1);

const withLocationsNamed = (lines: readonly string[]) =>
  lines.map((line) => line.replace(/main\.ts:\d+:\d+/u, "main.ts:<location>"));

const runMainTask = (body: string, options = "") =>
  Effect.gen(function* () {
    const project = yield* makeProject({
      "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", ${options}run: Effect.gen(function* () {
${body}
}) });`,
    });
    return yield* runTaskProcess(project, "main.ts");
  });

export const passingCommand = makeTest(
  "a command that passes prints none of its output but keeps it in its task's file and returns it",
  Effect.scoped(
    Effect.gen(function* () {
      const ran =
        yield* runMainTask(`  const listed = yield* Task.exec("bash", ["-c", "echo one; sleep 0.1; echo two >&2; sleep 0.1; echo three"]);
  yield* Effect.log(\`returned \${JSON.stringify(listed)}\`);`);
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(linesBetweenTheRunLines(ran.standardError)).toEqual([
        String.raw`[main] returned "one\nthree\n"`,
        "passed: 1 task, <seconds>",
      ]);
      expect(yield* readFirstTaskFile(ran.standardError)).toEqual([
        "started: [main], <seconds>, 1.output",
        "[main] one",
        "[main] two",
        "[main] three",
        String.raw`[main] returned "one\nthree\n"`,
        "ended: [main], <seconds>, passed",
      ]);
    }),
  ),
);

export const failingCommand = makeTest(
  "a command that fails its task prints its output right before the task's error, and its task's file keeps that output once",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runMainTask(
        `  yield* Task.exec("bash", ["-c", "echo one; sleep 0.1; printf 'two\\\\nthr' >&2; sleep 0.1; echo ee >&2; exit ${String(FAILED_COMMAND_EXIT_CODE)}"]);`,
      );
      expect(ran.exitCode, ran.standardError).toBe(FAILED_COMMAND_EXIT_CODE);
      const printed = withLocationsNamed(linesBetweenTheRunLines(ran.standardError));
      expect(printed).toEqual([
        "[main] one",
        "[main] two",
        "[main] three",
        `error: [main] main.ts:<location>: CommandFailed: bash exited with code ${String(FAILED_COMMAND_EXIT_CODE)}`,
        "error: [main] its output is in <run folder>/1.output",
        "ended: [main], <seconds>, failed",
        `recap: [main] 1.output | main.ts:<location>: CommandFailed: bash exited with code ${String(FAILED_COMMAND_EXIT_CODE)}`,
        "recap: [main] one",
        "recap: [main] two",
        "recap: [main] three",
        "failed: [main] (1 of 1 task), <seconds>",
      ]);
      expect(withLocationsNamed(yield* readFirstTaskFile(ran.standardError))).toEqual([
        "started: [main], <seconds>, 1.output",
        ...throughTheEndedLine(printed),
      ]);
    }),
  ),
);

export const caughtFailedCommand = makeTest(
  "a failed command whose error the task catches prints none of its output when the task passes, though the message the task reads carries it",
  Effect.scoped(
    Effect.gen(function* () {
      const ran =
        yield* runMainTask(`  const caught = yield* Task.exec("bash", ["-c", "echo expected-problem; exit 1"]).pipe(
    Effect.catchTag("CommandFailed", (failure) => Effect.succeed(failure.standardOutput)),
  );
  yield* Effect.log(JSON.stringify(caught));`);
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(linesBetweenTheRunLines(ran.standardError)).toEqual([
        String.raw`[main] "expected-problem\n"`,
        "passed: 1 task, <seconds>",
      ]);
    }),
  ),
);

export const wrappedFailedCommand = makeTest(
  "a task that fails prints the output of every command of it that did not pass, caught ones included, before its errors and one line naming its file",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runMainTask(`  yield* Task.exec("echo", ["passed"]);
  const caught = yield* Task.exec("bash", ["-c", "echo caught-error >&2; exit 1"]).pipe(
    Effect.as("passed"),
    Effect.catchTag("CommandFailed", (failure) => Effect.succeed(failure.message)),
  );
  yield* Effect.log(JSON.stringify(caught));
  yield* Task.exec("bash", ["-c", "echo wrapped-error >&2; exit 1"]).pipe(
    Effect.mapError((failure) => new Error("the deploy failed", { cause: failure })),
  );`);
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(withLocationsNamed(linesBetweenTheRunLines(ran.standardError))).toEqual([
        String.raw`[main] "bash exited with code 1\nstandard error:\ncaught-error"`,
        "[main] caught-error",
        "[main] wrapped-error",
        "error: [main] main.ts:<location>: Error: the deploy failed",
        "error: [main] caused by CommandFailed: bash exited with code 1",
        "error: [main] its output is in <run folder>/1.output",
        "ended: [main], <seconds>, failed",
        "recap: [main] 1.output | main.ts:<location>: Error: the deploy failed",
        "recap: [main] caused by CommandFailed: bash exited with code 1",
        "failed: [main] (1 of 1 task), <seconds>",
      ]);
    }),
  ),
);

export const timedOutCommand = makeTest(
  "a task that times out prints what its running command wrote so far before its error and one line naming its file",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runMainTask(
        `  yield* Task.exec("bash", ["-c", "echo listening; echo request failed >&2; exec sleep 30"]);`,
        'timeout: "1 second", ',
      );
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(withLocationsNamed(linesBetweenTheRunLines(ran.standardError))).toEqual([
        "[main] listening",
        "[main] request failed",
        "error: [main] main.ts:<location>: TaskTimedOut: timed out after 1s",
        "error: [main] bash was still running, called at main.ts:<location>, for <seconds>",
        "error: [main] its output is in <run folder>/1.output",
        "ended: [main], <seconds>, failed",
        "recap: [main] 1.output | main.ts:<location>: TaskTimedOut: timed out after 1s",
        "recap: [main] bash was still running, called at main.ts:<location>, for <seconds>",
        "failed: [main] (1 of 1 task), <seconds>",
      ]);
    }),
  ),
);

export const outputAfterTimeout = makeTest(
  "a task that times out prints what a command its cleanup runs writes after the timeout error, before the line naming its file",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runMainTask(
        `  yield* Effect.never.pipe(
    Effect.onInterrupt(() => Effect.ignore(Task.exec("sh", ["-c", "echo while-stopping; exit 1"]))),
  );`,
        'timeout: "1 second", ',
      );
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(withLocationsNamed(linesBetweenTheRunLines(ran.standardError))).toEqual([
        "error: [main] main.ts:<location>: TaskTimedOut: timed out after 1s",
        "error: [main] no command was running",
        "[main] while-stopping",
        "error: [main] its output is in <run folder>/1.output",
        "ended: [main], <seconds>, failed",
        "recap: [main] 1.output | main.ts:<location>: TaskTimedOut: timed out after 1s",
        "recap: [main] no command was running",
        "failed: [main] (1 of 1 task), <seconds>",
      ]);
    }),
  ),
);

export const failedCleanupAfterFailedCommand = makeTest(
  "a task whose command fails and whose cleanup then fails prints the line naming its file once, after both errors",
  Effect.scoped(
    Effect.gen(function* () {
      const ran =
        yield* runMainTask(`  yield* Effect.addFinalizer(() => Effect.die(new Error("finalizer broke")));
  yield* Task.exec("sh", ["-c", "echo body-output; exit 1"]);`);
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(withLocationsNamed(linesBetweenTheRunLines(ran.standardError))).toEqual([
        "[main] body-output",
        "error: [main] main.ts:<location>: CommandFailed: sh exited with code 1",
        "error: [main] main.ts:<location>: Error: finalizer broke",
        "error: [main] its cleanup failed",
        "error: [main] its output is in <run folder>/1.output",
        "ended: [main], <seconds>, failed",
        "recap: [main] 1.output | main.ts:<location>: Error: finalizer broke",
        "failed: [main] (1 of 1 task), <seconds>",
      ]);
    }),
  ),
);

export const servingCleanupCommand = makeTest(
  "a serving task whose cleanup command fails prints that command's output before its error",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
const server = Task.make({ name: "server", output: Schema.String, run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Task.exec("sh", ["-c", "echo cleanup-output; exit 1"]));
  return yield* Task.serve("up");
}) });
export default Task.make({ name: "main", run: Effect.gen(function* () { yield* server; }) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(
        withLocationsNamed(linesBetweenTheRunLines(ran.standardError)).filter((line) =>
          line.includes("[server]"),
        ),
      ).toEqual([
        "serving: [server], <seconds>",
        "closed: [server]",
        "[server] cleanup-output",
        "error: [server] main.ts:<location>: CommandFailed: sh exited with code 1",
        "error: [server] its body failed after serving",
        "error: [server] its output is in <run folder>/2.output",
        "recap: [server] 2.output | main.ts:<location>: CommandFailed: sh exited with code 1",
        "recap: [server] cleanup-output",
        "failed: [server] (1 of 2 tasks), <seconds>",
      ]);
    }),
  ),
);

const CRASHING_SERVER = `const server = Task.make({ name: "server", output: Schema.String, run: Effect.gen(function* () {
  yield* Effect.forkScoped(Task.exec("sh", ["-c", "echo 'error: missing setting databaseUrl' >&2; echo '    at loadConfig (src/config.ts:17)' >&2; exit 1"]));
  return yield* Task.serve("up");
}) });`;

const runAwaitingCrashingServer = (tasks: string) =>
  Effect.gen(function* () {
    const project = yield* makeProject({
      "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
${CRASHING_SERVER}
${tasks}`,
    });
    return yield* runTaskProcess(project, "main.ts");
  });

export const awaitedServerCrash = makeTest(
  "a task that times out awaiting a server whose quiet command crashed prints that command's output under the server's name before its errors",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runAwaitingCrashingServer(
        `export default Task.make({ name: "e2e", timeout: "1 second", run: Effect.gen(function* () { yield* server; yield* Effect.never; }) });`,
      );
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(withLocationsNamed(linesBetweenTheRunLines(ran.standardError))).toEqual([
        "serving: [server], <seconds>",
        "[server] error: missing setting databaseUrl",
        "[server]     at loadConfig (src/config.ts:17)",
        "error: [e2e] main.ts:<location>: TaskTimedOut: timed out after 1s",
        "error: [e2e] no command was running",
        "error: [e2e] its output is in <run folder>/1.output",
        "ended: [e2e], <seconds>, failed",
        "closed: [server]",
        "recap: [e2e] 1.output | main.ts:<location>: TaskTimedOut: timed out after 1s",
        "recap: [e2e] no command was running",
        "failed: [e2e] (1 of 2 tasks), <seconds>",
      ]);
    }),
  ),
);

export const serverAwaitedByTwoFailedTasks = makeTest(
  "two tasks that time out awaiting one server whose quiet command crashed print that command's output once",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runAwaitingCrashingServer(
        `const awaitServer = (name: string) => Task.make({ name, timeout: "1 second", run: Effect.gen(function* () { yield* server; yield* Effect.never; }) });
export default Task.make({ name: "main", run: Task.allSettled([awaitServer("a"), awaitServer("b")]) });`,
      );
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(
        linesBetweenTheRunLines(ran.standardError).filter((line) => line.startsWith("[server]")),
      ).toEqual([
        "[server] error: missing setting databaseUrl",
        "[server]     at loadConfig (src/config.ts:17)",
      ]);
    }),
  ),
);

export const healthyServerAfterAwaitingTaskFailed = makeTest(
  "a healthy server prints what it held when a task that awaited it fails, and nothing it writes after that",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
const server = Task.make({ name: "server", output: Schema.String, run: Effect.gen(function* () {
  yield* Effect.forkScoped(Task.exec("sh", ["-c", "echo before; sleep 1.5; echo after; exec sleep 30"]));
  return yield* Task.serve("up");
}) });
const e2e = Task.make({ name: "e2e", timeout: "1 second", run: Effect.gen(function* () { yield* server; yield* Effect.never; }) });
const other = Task.make({ name: "other", run: Effect.gen(function* () { yield* server; yield* Effect.sleep("2500 millis"); }) });
export default Task.make({ name: "main", run: Task.allSettled([e2e, other]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(
        linesBetweenTheRunLines(ran.standardError).filter((line) => line.startsWith("[server]")),
      ).toEqual(["[server] before"]);
    }),
  ),
);

export const serverAwaitedByPassingTask = makeTest(
  "a task that passes awaiting a server whose quiet command crashed prints none of that command's output",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runAwaitingCrashingServer(
        `export default Task.make({ name: "main", run: Effect.gen(function* () { yield* server; yield* Effect.sleep("200 millis"); }) });`,
      );
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(linesBetweenTheRunLines(ran.standardError)).toEqual([
        "serving: [server], <seconds>",
        "closed: [server]",
        "passed: 2 tasks, <seconds>",
      ]);
    }),
  ),
);

export const outputAfterAStopSignal = makeTest(
  "a failed task whose cleanup runs a failing command after ctrl-c prints neither that command's output nor the line naming its file",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.gen(function* () {
    yield* Effect.log("cleanup-waiting");
    yield* Effect.sleep("400 millis");
    yield* Effect.ignore(Task.exec("sh", ["-c", "echo after-signal; exit 1"]));
  }));
  yield* Task.exec("sh", ["-c", "echo body-output; exit 1"]);
}) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[main] cleanup-waiting");
      yield* command.sendSignal("INT");
      const exited = yield* command.waitForExit;
      expect(withLocationsNamed(linesBetweenTheRunLines(exited.standardError))).toEqual([
        "[main] body-output",
        "error: [main] main.ts:<location>: CommandFailed: sh exited with code 1",
        "[main] cleanup-waiting",
        "stopping: press ctrl-c again to force",
        "ended: [main], <seconds>, failed",
        "failed: [main] (1 of 1 task), <seconds>",
      ]);
    }),
  ),
);

export const interleavedCommand = makeTest(
  "a command's output that is not printed keeps its place in its task's file among console lines and printed commands' output",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runMainTask(`  console.log("first console line");
  yield* Task.exec("echo", ["first quiet line"]);
  yield* Task.exec("echo", ["printed command line"], { print: true });
  console.log("second console line");
  yield* Task.exec("bash", ["-c", "echo second quiet line >&2"]);
  yield* Effect.log("logged line");`);
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(linesBetweenTheRunLines(ran.standardError)).toEqual([
        "[main] first console line",
        "[main] printed command line",
        "[main] second console line",
        "[main] logged line",
        "passed: 1 task, <seconds>",
      ]);
      expect(yield* readFirstTaskFile(ran.standardError)).toEqual([
        "started: [main], <seconds>, 1.output",
        "[main] first console line",
        "[main] first quiet line",
        "[main] printed command line",
        "[main] second console line",
        "[main] second quiet line",
        "[main] logged line",
        "ended: [main], <seconds>, passed",
      ]);
    }),
  ),
);

const QUIET_LINES = 4400;

const QUIET_LINE_DIGITS = 4095;

const QUIET_OUTPUT_BYTES = QUIET_LINES * (QUIET_LINE_DIGITS + 1);

export const longCommand = makeTest(
  "a command that writes more output than task holds back for standard error, without printing it, passes without waiting on standard error",
  Effect.scoped(
    Effect.gen(function* () {
      expect(QUIET_OUTPUT_BYTES).toBeGreaterThan(BYTES_WAITING_TO_BE_WRITTEN);
      const ran = yield* runMainTask(
        `  const written = yield* Task.exec("bash", ["-c", "seq -f %0${String(QUIET_LINE_DIGITS)}g 1 ${String(QUIET_LINES)}"]);
  yield* Effect.log(\`returned \${String(written.length)} characters\`);`,
      );
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(linesBetweenTheRunLines(ran.standardError)).toEqual([
        `[main] returned ${String(QUIET_OUTPUT_BYTES)} characters`,
        "passed: 1 task, <seconds>",
      ]);
    }),
  ),
);

const LINES_PAST_THE_HELD_BYTES = 1100;

const DIGITS_PER_LINE = 1000;

const RUNS_OF_A_CUT_STANDARD_OUTPUT = 4;

const RECAPPED_OUTPUT_LINES = 3;

const MOST_CODE_UNITS_OF_A_RECAPPED_LINE = 200;

const RECAP_NOTE_OF_A_WHOLE_NUMBER_LINE = ` (${String(DIGITS_PER_LINE)} more characters are not shown)`;

const RECAPPED_DIGITS =
  MOST_CODE_UNITS_OF_A_RECAPPED_LINE - RECAP_NOTE_OF_A_WHOLE_NUMBER_LINE.length;

const RECAPPED_NUMBER_LINE = `recap: [main] ${"0".repeat(RECAPPED_DIGITS)} (${String(DIGITS_PER_LINE - RECAPPED_DIGITS)} more characters are not shown)`;

const numberTexts = (printed: readonly string[]) =>
  printed.filter((line) => line.startsWith("[main] ")).map((line) => line.slice("[main] ".length));

const numberedLines = (printed: readonly string[]) =>
  numberTexts(printed).map((text) => Number(text));

export const cutCommand = makeTest(
  "a command that writes more than task holds and then fails prints only the last of it, after a line that names its task's file, which keeps all of it",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runMainTask(
        `  yield* Task.exec("bash", ["-c", "seq -f %0${String(DIGITS_PER_LINE)}g 1 ${String(LINES_PAST_THE_HELD_BYTES)}; exit ${String(FAILED_COMMAND_EXIT_CODE)}"]);`,
      );
      expect(ran.exitCode, ran.standardError).toBe(FAILED_COMMAND_EXIT_CODE);
      const printed = withLocationsNamed(linesBetweenTheRunLines(ran.standardError));
      const kept = numberedLines(printed);
      const firstKept = kept[0] ?? 0;
      const linesBeforeTheKeptOutput = [
        "warning: [main] only the last 1 MiB of its commands' output prints, and the rest is in <run folder>/1.output",
      ];
      expect(printed.slice(0, linesBeforeTheKeptOutput.length)).toEqual(linesBeforeTheKeptOutput);
      expect(printed.slice(linesBeforeTheKeptOutput.length + kept.length)).toEqual([
        `error: [main] main.ts:<location>: CommandFailed: bash exited with code ${String(FAILED_COMMAND_EXIT_CODE)}`,
        "error: [main] its output is in <run folder>/1.output",
        "ended: [main], <seconds>, failed",
        `recap: [main] 1.output | main.ts:<location>: CommandFailed: bash exited with code ${String(FAILED_COMMAND_EXIT_CODE)}`,
        RECAPPED_NUMBER_LINE,
        `recap: [main] (${String(LINES_PAST_THE_HELD_BYTES - RECAPPED_OUTPUT_LINES)} lines are not shown)`,
        RECAPPED_NUMBER_LINE,
        RECAPPED_NUMBER_LINE,
        "failed: [main] (1 of 1 task), <seconds>",
      ]);
      expect(firstKept).toBeGreaterThan(1);
      expect(kept).toEqual(
        Array.from(
          { length: LINES_PAST_THE_HELD_BYTES - firstKept + 1 },
          (_unused, index) => firstKept + index,
        ),
      );
      const keptBytes = numberTexts(printed).reduce((bytes, text) => bytes + text.length + 1, 0);
      expect(keptBytes).toBeLessThanOrEqual(HELD_COMMAND_OUTPUT_BYTES);
      expect(keptBytes).toBeGreaterThan(
        HELD_COMMAND_OUTPUT_BYTES -
          DIGITS_PER_LINE -
          1 -
          RUNS_OF_A_CUT_STANDARD_OUTPUT * RUN_COST_BYTES,
      );
      expect(numberedLines(yield* readFirstTaskFile(ran.standardError))).toEqual(
        Array.from({ length: LINES_PAST_THE_HELD_BYTES }, (_unused, index) => index + 1),
      );
    }),
  ),
);

const MANY_WRITES = 20_000;

const SMALL_BLOCK_BYTES = 64;

const SMALL_HELD_BYTES = 1024;

const STREAMS = 2;

const textEncoder = new TextEncoder();

export const heldOutputBound = makeTest(
  "what task holds of a command's output stays within its bound however many lines or stream switches the command writes",
  Effect.sync(() => {
    const sizes = { boundBytes: SMALL_HELD_BYTES, blockBytes: SMALL_BLOCK_BYTES };
    const blocksAtMost = SMALL_HELD_BYTES / SMALL_BLOCK_BYTES + 1;
    const oneLinePerWrite = makeHeldCommandOutput(sizes);
    const line = textEncoder.encode("x\n");
    for (const written of Array.from({ length: MANY_WRITES }, () => line)) {
      oneLinePerWrite.hold(0, written);
    }
    const oneLine = oneLinePerWrite.measure();
    expect({ heldBytes: oneLine.heldBytes, runs: oneLine.runs }).toEqual({
      heldBytes: SMALL_HELD_BYTES - RUN_COST_BYTES,
      runs: 1,
    });
    expect(oneLine.blocks).toBeLessThanOrEqual(blocksAtMost);
    const switchingStreams = makeHeldCommandOutput(sizes);
    const byte = textEncoder.encode("y");
    for (const write of Array.from({ length: MANY_WRITES }).keys()) {
      switchingStreams.hold(write % STREAMS, byte);
    }
    const switched = switchingStreams.measure();
    expect(switched.heldBytes + switched.runs * RUN_COST_BYTES).toBeLessThanOrEqual(
      SMALL_HELD_BYTES,
    );
    expect(switched.blocks).toBeLessThanOrEqual(blocksAtMost);
  }),
);

const SMALL_BOUND_BYTES = 60;

const HELD_LINES = 20;

export const heldOutputReplay = makeTest(
  "what task holds of a command's output replays each stream from its first whole line once its start was dropped",
  Effect.sync(() => {
    const held = makeHeldCommandOutput({ boundBytes: SMALL_BOUND_BYTES });
    for (const number of Array.from({ length: HELD_LINES }, (_unused, index) => index + 1)) {
      held.hold(0, textEncoder.encode(`line ${String(number)}\n`));
    }
    held.hold(1, textEncoder.encode("prompt> "));
    expect(held.isCut()).toBe(true);
    expect(
      [...held.replay()].map(({ streamId, pieces }) => ({
        streamId,
        texts: pieces.map((piece) => piece.text),
      })),
    ).toEqual([
      { streamId: 0, texts: ["line 19", "line 20"] },
      { streamId: 1, texts: ["prompt> "] },
      { streamId: 1, texts: [""] },
    ]);
  }),
);

const BYTE_ORDER_MARK = "\uFEFF";

const piecesOfReplay = (held: ReturnType<typeof makeHeldCommandOutput>) =>
  [...held.replay()].map(({ streamId, pieces }) => ({
    streamId,
    texts: pieces.map((piece) => piece.text),
  }));

export const heldOutputByteOrderMark = makeTest(
  "what task holds of a command's output drops a byte order mark only at the start of a stream, as its task's file does, and keeps one after a cut",
  Effect.sync(() => {
    const fromTheStart = makeHeldCommandOutput();
    fromTheStart.hold(0, textEncoder.encode(`${BYTE_ORDER_MARK}start\n`));
    expect(piecesOfReplay(fromTheStart)).toEqual([{ streamId: 0, texts: ["start"] }]);
    const afterACut = makeHeldCommandOutput({ boundBytes: SMALL_BOUND_BYTES });
    afterACut.hold(
      0,
      textEncoder.encode(`${"x".repeat(SMALL_BOUND_BYTES)}\n${BYTE_ORDER_MARK}keep\n`),
    );
    expect(afterACut.isCut()).toBe(true);
    expect(piecesOfReplay(afterACut)).toEqual([{ streamId: 0, texts: [`${BYTE_ORDER_MARK}keep`] }]);
  }),
);

export const heldOutputRelease = makeTest(
  "what task holds of its commands' output lets go of a stream it releases and keeps the rest in order",
  Effect.sync(() => {
    const held = makeHeldCommandOutput();
    const [first, released, last] = [0, 1, STREAMS];
    held.hold(first, textEncoder.encode("kept one\n"));
    held.hold(released, textEncoder.encode("released\n"));
    held.hold(last, textEncoder.encode("kept two\n"));
    held.release(new Set([released]));
    expect(piecesOfReplay(held)).toEqual([
      { streamId: first, texts: ["kept one"] },
      { streamId: last, texts: ["kept two"] },
    ]);
    held.release(new Set([first, last]));
    expect(held.isEmpty()).toBe(true);
    expect(held.measure()).toEqual({ heldBytes: 0, runs: 0, blocks: 0, trackedStreams: 0 });
  }),
);

const EVICTING_BOUND_BYTES = 60;

const EVICTED_STREAM_BYTES = 100;

export const heldOutputReleaseOfEvictedStreams = makeTest(
  "what task holds of its commands' output is no longer cut once every stream it cut is released",
  Effect.sync(() => {
    const held = makeHeldCommandOutput({ boundBytes: EVICTING_BOUND_BYTES });
    held.hold(0, new Uint8Array(EVICTED_STREAM_BYTES));
    held.hold(1, new Uint8Array(EVICTED_STREAM_BYTES));
    held.release(new Set([0]));
    held.release(new Set([1]));
    expect({ isEmpty: held.isEmpty(), isCut: held.isCut() }).toEqual({
      isEmpty: true,
      isCut: false,
    });
  }),
);

const FAILED_COMMANDS = 10_000;

const STREAMS_STILL_HELD_AT_MOST = 4;

export const heldOutputOfManyFailedCommands = makeTest(
  "what task holds of many failed commands' output keeps state only for the streams it still holds, and stays cut",
  Effect.sync(() => {
    const held = makeHeldCommandOutput({ boundBytes: EVICTING_BOUND_BYTES });
    for (const streamId of Array.from({ length: FAILED_COMMANDS }).keys()) {
      held.hold(streamId, new Uint8Array(EVICTED_STREAM_BYTES));
      held.endStream(streamId);
      held.keepFailed(new Set([streamId]));
    }
    expect(held.measure().trackedStreams).toBeLessThanOrEqual(STREAMS_STILL_HELD_AT_MOST);
    expect(held.isCut()).toBe(true);
  }),
);

const COMMANDS_PER_ENDING = 3;

const passWhateverTheCommandDid = <Requirements>(
  command: Effect.Effect<unknown, Effect.Error<ReturnType<typeof Task.exec>>, Requirements>,
) =>
  command.pipe(
    Effect.asVoid,
    Effect.catchTags({
      CommandFailed: () => Effect.void,
      CommandNotStarted: () => Effect.void,
      CommandStandardInputFailed: () => Effect.void,
      PlatformError: () => Effect.void,
    }),
  );

const settleEachCommand = <Failure, Requirements>(
  runCommand: (firstOutput: Deferred.Deferred<void>) => Effect.Effect<void, Failure, Requirements>,
) =>
  Effect.gen(function* () {
    const taskLines = yield* TaskLines;
    const outcomes: string[] = [];
    for (const commandIndex of Array.from({ length: COMMANDS_PER_ENDING }).keys()) {
      const firstOutput = yield* Deferred.make<void>();
      yield* runCommand(firstOutput).pipe(
        Effect.provideService(TaskLines, {
          ...taskLines,
          printStreamOutput: (streamId, pieces, printedTo) =>
            taskLines
              .printStreamOutput(streamId, pieces, printedTo)
              .pipe(Effect.andThen(Deferred.done(firstOutput, Exit.void))),
          settleHeldOutput: (streamIds, outcome) =>
            Effect.sync(() => {
              outcomes[commandIndex] = `${streamIds.length} streams ${outcome}`;
            }),
        }),
      );
    }
    return outcomes;
  });

export const interruptedCommandsSettle = makeTest(
  "a quiet command interrupted after it wrote settles its held output once as not passed",
  Effect.gen(function* () {
    const outcomes = yield* settleEachCommand((firstOutput) =>
      Effect.raceFirst(
        passWhateverTheCommandDid(Task.exec("sh", ["-c", "echo started; exec sleep 30"])),
        Deferred.await(firstOutput),
      ),
    );
    expect(outcomes).toEqual(Array.from({ length: COMMANDS_PER_ENDING }, () => "2 streams failed"));
  }),
);

export const failedStandardInputSettles = makeTest(
  "a quiet command whose standard input fails after it wrote settles its held output once as not passed",
  Effect.gen(function* () {
    const outcomes = yield* settleEachCommand((firstOutput) =>
      passWhateverTheCommandDid(
        Task.exec("sh", ["-c", "echo started; cat > /dev/null; sleep 30"], {
          stdin: Stream.fromEffect(Deferred.await(firstOutput)).pipe(
            Stream.flatMap(() =>
              Stream.fail(
                PlatformError.systemError({
                  _tag: "NotFound",
                  module: "FileSystem",
                  method: "stream",
                  description: "input file missing",
                }),
              ),
            ),
          ),
        }),
      ),
    );
    expect(outcomes).toEqual(Array.from({ length: COMMANDS_PER_ENDING }, () => "2 streams failed"));
  }),
);

export const passedCommandSettles = makeTest(
  "a quiet command that passes settles its held output once as passed",
  Effect.gen(function* () {
    const outcomes = yield* settleEachCommand(() => Effect.asVoid(Task.exec("echo", ["done"])));
    expect(outcomes).toEqual(Array.from({ length: COMMANDS_PER_ENDING }, () => "2 streams passed"));
  }),
);

const CUT_CHARACTER_LEAD_BYTE = 0xe2;

export const heldOutputStreamEnds = makeTest(
  "what task holds of a command's output ends each stream where it ended, before what other streams wrote later",
  Effect.sync(() => {
    const held = makeHeldCommandOutput();
    held.hold(0, Uint8Array.of(CUT_CHARACTER_LEAD_BYTE));
    held.endStream(0);
    held.hold(1, textEncoder.encode("later\n"));
    held.endStream(1);
    expect(piecesOfReplay(held)).toEqual([
      { streamId: 0, texts: ["\uFFFD", ""] },
      { streamId: 1, texts: ["later"] },
    ]);
  }),
);

const LINES_OF_TWO_STREAMS = ["[main] \uFFFD", "[main] later"] as const;

export const failedCommandStreamEnds = makeTest(
  "a command that fails prints a stream that ended inside a character as a line of its own, next to what its other stream wrote in whichever order Task read them, in the same order as its task's file, and its recap lists standard output before standard error",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runMainTask(
        `  yield* Task.exec("sh", ["-c", "printf '\\\\342'; exec 1>&-; echo later >&2; exit ${String(FAILED_COMMAND_EXIT_CODE)}"]);`,
      );
      expect(ran.exitCode, ran.standardError).toBe(FAILED_COMMAND_EXIT_CODE);
      const printed = withLocationsNamed(linesBetweenTheRunLines(ran.standardError));
      const outputLines = printed.slice(0, LINES_OF_TWO_STREAMS.length);
      expect(outputLines.toSorted()).toEqual(LINES_OF_TWO_STREAMS.toSorted());
      expect(printed).toEqual([
        ...outputLines,
        `error: [main] main.ts:<location>: CommandFailed: sh exited with code ${String(FAILED_COMMAND_EXIT_CODE)}`,
        "error: [main] its output is in <run folder>/1.output",
        "ended: [main], <seconds>, failed",
        `recap: [main] 1.output | main.ts:<location>: CommandFailed: sh exited with code ${String(FAILED_COMMAND_EXIT_CODE)}`,
        "recap: [main] \uFFFD",
        "recap: [main] later",
        "failed: [main] (1 of 1 task), <seconds>",
      ]);
      expect(withLocationsNamed(yield* readFirstTaskFile(ran.standardError))).toEqual([
        "started: [main], <seconds>, 1.output",
        ...throughTheEndedLine(printed),
      ]);
    }),
  ),
);

export const printedCommand = makeTest(
  "a command with print: true prints its output as it writes it, and a failure does not print it again, though its error's message carries its standard error",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runMainTask(`  yield* Task.exec("echo", ["live"], { print: true });
  yield* Effect.log("after the first command");
  yield* Task.exec("bash", ["-c", "echo once >&2; exit ${String(PRINTED_FAILED_COMMAND_EXIT_CODE)}"], { print: true });`);
      expect(ran.exitCode, ran.standardError).toBe(PRINTED_FAILED_COMMAND_EXIT_CODE);
      const printed = withLocationsNamed(linesBetweenTheRunLines(ran.standardError));
      expect(printed).toEqual([
        "[main] live",
        "[main] after the first command",
        "[main] once",
        `error: [main] main.ts:<location>: CommandFailed: bash exited with code ${String(PRINTED_FAILED_COMMAND_EXIT_CODE)}`,
        "error: [main] its output is in <run folder>/1.output",
        "ended: [main], <seconds>, failed",
        `recap: [main] 1.output | main.ts:<location>: CommandFailed: bash exited with code ${String(PRINTED_FAILED_COMMAND_EXIT_CODE)}`,
        "recap: [main] once",
        "failed: [main] (1 of 1 task), <seconds>",
      ]);
      expect(withLocationsNamed(yield* readFirstTaskFile(ran.standardError))).toEqual([
        "started: [main], <seconds>, 1.output",
        ...throughTheEndedLine(printed),
      ]);
    }),
  ),
);

export const printOptionTypes = makeTest(
  'Task.exec takes print, secret or stdio: "inherit", but no two of them together',
  Effect.scoped(
    Effect.gen(function* () {
      const failed = yield* failureAsValue(
        typeCheckSource(`import { Effect, Redacted } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const listed: string = yield* Task.exec("ls", [], { print: true });
  const hidden: Redacted.Redacted = yield* Task.exec("ls", [], { secret: true });
  yield* Task.exec("ls", [], { print: true, secret: true });
  yield* Task.exec("ls", [], { print: true, stdio: "inherit" });
  yield* Task.exec("ls", [], { secret: true, stdio: "inherit" });
  const printAndSecret = { print: true, secret: true } as const;
  yield* Task.exec("ls", [], printAndSecret);
  const printAndStdio = { print: true, stdio: "inherit" } as const;
  yield* Task.exec("ls", [], printAndStdio);
  const secretAndStdio = { secret: true, stdio: "inherit" } as const;
  yield* Task.exec("ls", [], secretAndStdio);
  yield* Effect.log(listed, hidden);
}) });`),
      );
      expect(failed.message).toContain(
        "main.ts(6,45): error TS2322: Type 'true' is not assignable to type 'undefined'.",
      );
      expect(failed.message).toContain(
        "main.ts(7,32): error TS2322: Type 'true' is not assignable to type 'never'.",
      );
      expect(failed.message).toContain(
        "main.ts(8,32): error TS2322: Type 'true' is not assignable to type 'never'.",
      );
      const compilerLines = failed.message.split("\n").map((compilerLine) => compilerLine.trim());
      for (const [options, key] of [
        ["{ readonly print: true; readonly secret: true; }", "print"],
        [`{ readonly print: true; readonly stdio: "inherit"; }`, "print"],
        [`{ readonly secret: true; readonly stdio: "inherit"; }`, "secret"],
      ] as const) {
        const errorLine = compilerLines.findIndex((compilerLine) =>
          compilerLine.includes(`error TS2345: Argument of type '${options}' is not assignable`),
        );
        const reasons = [
          `Types of property '${key}' are incompatible.`,
          "Type 'true' is not assignable to type 'never'.",
        ];
        expect(errorLine).toBeGreaterThan(-1);
        expect(compilerLines.slice(errorLine + 1, errorLine + 1 + reasons.length)).toEqual(reasons);
      }
      expect(failed.message).not.toContain("main.ts(4,");
      expect(failed.message).not.toContain("main.ts(5,");
    }),
  ),
);
