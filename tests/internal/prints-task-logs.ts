import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect, FileSystem, Option } from "effect";

import { joinOutput, OutputToJoin } from "../../src/shared/label-output.ts";
import {
  reserveTaskOutputBytes,
  liftTaskOutputBudget,
  makeTaskOutputBudget,
  takeTaskOutputBytes,
  tryToTakeTaskOutputBytes,
  waitForTaskOutputBudgetChange,
} from "../../src/shared/task-output-budget.ts";
import { read } from "../shared/access-files.ts";
import { countOccurrences } from "../shared/count-occurrences.ts";
import { EXIT_CODE_AFTER_SIGINT } from "../shared/exit-codes-after-stop-signals.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { findRunDirectoryPath, withSecondsNamed } from "../shared/name-output-files.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";
import { separateEndedLines } from "../shared/separate-ended-lines.ts";
import { withoutTheFirstRunLine } from "../shared/split-run-lines.ts";

const LOOP_LINE_COUNT = 20_000;

const LOGGED_BEFORE_EXIT_COUNT = 2000;

const WORKER_EXIT_CODE = 7;

const HEAP_GROWTH_LIMIT_MEGABYTES = 50;

export const logs = makeTest(
  "a task's logs print under its name",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.log("hello", { n: 1 });
  yield* Effect.logWarning("careful");
  yield* Effect.logError("broken");
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain('[main] hello {"n":1}\n[main] careful\n[main] broken');
    }),
  ),
);

export const logsBeforeProcessExit = makeTest(
  "every line a body logs right before it calls process.exit prints in order, before its worker's exit",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.forEach(Array.from({ length: ${LOGGED_BEFORE_EXIT_COUNT} }, (_, line) => line), (line) => Effect.log(\`line \${line}\`), { discard: true }).pipe(Effect.andThen(Effect.sync(() => process.exit(${WORKER_EXIT_CODE})))) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      const lines = ran.standardError.split("\n");
      const exitLine = lines.findIndex((line) =>
        line.startsWith(
          `error: [main] main.ts:3:21: WorkerExited: its worker exited on its own with code ${WORKER_EXIT_CODE}`,
        ),
      );
      expect(lines.filter((line) => line.startsWith("[main] line "))).toEqual(
        Array.from({ length: LOGGED_BEFORE_EXIT_COUNT }, (_unused, line) => `[main] line ${line}`),
      );
      expect(exitLine).toBeGreaterThan(
        lines.indexOf(`[main] line ${LOGGED_BEFORE_EXIT_COUNT - 1}`),
      );
    }),
  ),
);

export const logCauseLevels = makeTest(
  "a cause attached to a warning or info log prints under that log level instead of as an error",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Cause, Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.logWarning("retrying", Cause.fail(new Error("timeout")));
  yield* Effect.logInfo("caught", Cause.die(new Error("not found")));
  yield* Effect.logError("gave up", Cause.fail(new Error("still down")));
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toMatch(/^\[main\] retrying\n\[main\] \S+: warn Error: timeout$/mu);
      expect(ran.standardError).toMatch(/^\[main\] caught\n\[main\] \S+: info Error: not found$/mu);
      expect(ran.standardError).toMatch(
        /^\[main\] gave up\n\[main\] \S+: error Error: still down$/mu,
      );
    }),
  ),
);

export const liveLogs = makeTest(
  "a task's log line prints while the task is still running",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.log("waiting");
  const reader = yield* ChildProcess.make("sh", ["-c", "read line < gate"]);
  yield* reader.exitCode;
  yield* Effect.log("done");
}).pipe(Effect.scoped) });`,
      });
      yield* Task.exec("mkfifo", [`${project.directory}/gate`]);

      const command = yield* startTaskProcess(project, "main.ts");
      const gate = yield* fileSystem.open(`${project.directory}/gate`, { flag: "r+" });
      yield* command.waitForLine("[main] waiting");
      yield* gate.write(new TextEncoder().encode("\n"));

      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(0);
      expect(exited.standardError).toContain("[main] waiting\n[main] done");
    }),
  ),
);

export const consoleOutputBeforeExit = makeTest(
  "console output a task writes just before the run ends reaches standard error in full, whether the task passes or fails",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export default Task.make({ name: "chatty", run: Effect.gen(function* () {
  for (let line = 0; line < 20000; line++) console.log(\`ok \${line}\`);
  console.log("last line");
  if (process.env.FAIL === "1") return yield* Effect.fail(new TaskError({ message: "1 test failed" }));
}) });`,
      });
      for (const fail of ["0", "1"]) {
        const ran = yield* runTaskProcess(project, "main.ts", { FAIL: fail });
        expect(ran.exitCode).toBe(Number(fail));
        expect(ran.standardOutput).toBe("");
        const outputLines = ran.standardError
          .split("\n")
          .filter((line) => line.startsWith("[chatty] "));
        expect(outputLines.length).toBe(LOOP_LINE_COUNT + 1);
        expect(outputLines.at(-1)).toBe("[chatty] last line");
      }
    }),
  ),
);

const CONSOLE_OUTPUT_LENGTH = 1_000_000;

const CHATTY_LINE_COUNT = 3;

export const consoleOutputToASlowReader = makeTest(
  "console output a task writes just before the run ends reaches a reader of standard error that waits before it reads, as in 2>&1 | (sleep 3; cat), even when the pipe is non-blocking",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "chatty", run: Effect.sync(() => {
  process.stdout.write("x".repeat(${CONSOLE_OUTPUT_LENGTH}) + "\\nlast line\\n");
}) });`,
        "piped.ts": `const shell = Bun.spawn(["sh", "-c", \`{ perl -MFcntl -e 'fcntl(STDERR, F_SETFL, fcntl(STDERR, F_GETFL, 0) | O_NONBLOCK) or die; exec @ARGV' \${process.execPath} main.ts 2>&1; echo $? > exit-code; } | (sleep 3; cat > output)\`], { stdio: ["ignore", "inherit", "inherit"] });
process.exitCode = await shell.exited;`,
      });
      yield* runTaskProcess(project, "piped.ts");
      const { endedLines, otherLines } = separateEndedLines(
        withSecondsNamed(withoutTheFirstRunLine(yield* read(`${project.directory}/output`))).split(
          "\n",
        ),
      );
      expect(otherLines.slice(0, CHATTY_LINE_COUNT)).toEqual([
        `output: ${"x".repeat(CONSOLE_OUTPUT_LENGTH)}`,
        "output: last line",
        "passed: 1 task, <seconds>",
      ]);
      expect(endedLines).toEqual([]);
      expect((yield* read(`${project.directory}/exit-code`)).trim()).toBe("0");
    }),
  ),
);

const UNREAD_OUTPUT_LINE_COUNT = 64;

const UNREAD_OUTPUT_LINE_BYTES = 1_048_576;

const CHECKS_WITHOUT_PROGRESS = 10;

export const unreadStandardErrorSlowsConsoleOutput = makeTest(
  "while nobody reads standard error, a task writing 64 lines of 1 MiB to standard output is held back after about 16 MiB, and every line arrives once the reader reads",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { writeFileSync } from "node:fs";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const line = "x".repeat(${UNREAD_OUTPUT_LINE_BYTES}) + "\\n";
export default Task.make({ name: "chatty", run: Effect.promise(async () => {
  for (let written = 1; written <= ${UNREAD_OUTPUT_LINE_COUNT}; written++) {
    await new Promise<void>((resolve) => process.stdout.write(line, () => resolve()));
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
      expect(writtenWhileUnread).toBeLessThan(UNREAD_OUTPUT_LINE_COUNT);
      const outputLines = (yield* read(`${project.directory}/output`))
        .split("\n")
        .filter((line) => line === `output: ${"x".repeat(UNREAD_OUTPUT_LINE_BYTES)}`);
      expect(outputLines.length).toBe(UNREAD_OUTPUT_LINE_COUNT);
      expect((yield* read(`${project.directory}/exit-code`)).trim()).toBe("0");
    }),
  ),
);

const FLOOD_SCRIPT = `i=0; while [ $i -lt ${UNREAD_OUTPUT_LINE_COUNT} ]; do head -c ${UNREAD_OUTPUT_LINE_BYTES} /dev/zero | tr '\\0' x; echo; i=$((i+1)); echo $i > written; done`;

export const unreadStandardErrorSlowsCommandOutput = makeTest(
  "while nobody reads standard error, a command writing 64 lines of 1 MiB is held back after about 16 MiB, and every line arrives once the reader reads",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "flood.sh": FLOOD_SCRIPT,
        "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "chatty", run: Task.exec("sh", ["flood.sh"], { print: true }) });`,
        "piped.ts": `const shell = Bun.spawn(["sh", "-c", \`{ \${process.execPath} main.ts 2>&1; echo $? > exit-code; } | (until [ -s written ]; do sleep 0.05; done; seen=$(cat written); same=0; while [ $same -lt ${CHECKS_WITHOUT_PROGRESS} ]; do sleep 0.2; now=$(cat written); if [ "$now" = "$seen" ]; then same=$((same + 1)); else seen=$now; same=0; fi; done; echo $seen > written-while-unread; cat > output)\`], { stdio: ["ignore", "inherit", "inherit"] });
process.exitCode = await shell.exited;`,
      });
      const ran = yield* runTaskProcess(project, "piped.ts");
      const writtenWhileUnread = Number(yield* read(`${project.directory}/written-while-unread`));
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(writtenWhileUnread).toBeGreaterThan(0);
      expect(writtenWhileUnread).toBeLessThan(UNREAD_OUTPUT_LINE_COUNT);
      const outputLines = (yield* read(`${project.directory}/output`))
        .split("\n")
        .filter((line) => line === `[chatty] ${"x".repeat(UNREAD_OUTPUT_LINE_BYTES)}`);
      expect(outputLines.length).toBe(UNREAD_OUTPUT_LINE_COUNT);
      expect((yield* read(`${project.directory}/exit-code`)).trim()).toBe("0");
    }),
  ),
);

const BLANK_LINES_PER_CHUNK = 20_000;

const BLANK_LINE_CHUNK_COUNT = 60;

const LONG_NAME_REPEATS = 40;

const LONG_TASK_NAME = `quiet${"-blank".repeat(LONG_NAME_REPEATS)}`;

export const unreadStandardErrorHoldsBlankLines = makeTest(
  `while nobody reads standard error, a command writing ${String(BLANK_LINE_CHUNK_COUNT * BLANK_LINES_PER_CHUNK)} blank lines is held back, since each blank line costs the bytes of its name and line end, and every line arrives once the reader reads`,
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "blank.sh": `i=0; while [ $i -lt ${BLANK_LINE_CHUNK_COUNT} ]; do head -c ${BLANK_LINES_PER_CHUNK} /dev/zero | tr '\\0' '\\n'; i=$((i+1)); echo $i > written; done`,
        "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "${LONG_TASK_NAME}", run: Task.exec("sh", ["blank.sh"], { print: true }) });`,
        "piped.ts": `const shell = Bun.spawn(["sh", "-c", \`{ \${process.execPath} main.ts 2>&1; echo $? > exit-code; } | (until [ -s written ]; do sleep 0.05; done; seen=$(cat written); same=0; while [ $same -lt ${CHECKS_WITHOUT_PROGRESS} ]; do sleep 0.2; now=$(cat written); if [ "$now" = "$seen" ]; then same=$((same + 1)); else seen=$now; same=0; fi; done; echo $seen > written-while-unread; cat > output)\`], { stdio: ["ignore", "inherit", "inherit"] });
process.exitCode = await shell.exited;`,
      });
      const ran = yield* runTaskProcess(project, "piped.ts");
      const writtenWhileUnread = Number(yield* read(`${project.directory}/written-while-unread`));
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(writtenWhileUnread).toBeGreaterThan(0);
      expect(writtenWhileUnread).toBeLessThan(BLANK_LINE_CHUNK_COUNT);
      const outputLines = (yield* read(`${project.directory}/output`))
        .split("\n")
        .filter((line) => line === `[${LONG_TASK_NAME}] `);
      expect(outputLines.length).toBe(BLANK_LINE_CHUNK_COUNT * BLANK_LINES_PER_CHUNK);
      expect((yield* read(`${project.directory}/exit-code`)).trim()).toBe("0");
    }),
  ),
);

const FLOODING_CLEANUP_TASK = `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "chatty", cleanup: "1 minute", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Task.exec("sh", ["flood.sh"], { print: true }).pipe(Effect.ignore, Effect.andThen(Effect.promise(() => Bun.write("cleanup-done", "")))));
  yield* Effect.promise(() => Bun.write("started", ""));
  return yield* Effect.forever(Effect.log("still going").pipe(Effect.andThen(Effect.sleep("10 millis"))));
}) });`;

export const commandOutputAfterAStopSignal = makeTest(
  "once Ctrl-C stops the run while nobody reads standard error, a command a stopping task runs is no longer held back, so a cleanup that writes 64 MiB still finishes",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "flood.sh": FLOOD_SCRIPT,
        "main.ts": FLOODING_CLEANUP_TASK,
        "unread.pl": `use strict;
pipe(my $reader, my $writer) or die;
my $pid = fork();
if ($pid == 0) { close $reader; open(STDERR, ">&", $writer) or die; exec($ARGV[0], "main.ts") or die; }
close $writer;
select(undef, undef, undef, 0.05) until -e "started";
kill "INT", $pid;
local $SIG{ALRM} = sub { kill "KILL", $pid; };
alarm 40;
waitpid($pid, 0);
print $? & 127 ? "killed\\n" : ($? >> 8) . "\\n";`,
        "unread.ts": `const perl = Bun.spawn(["perl", "unread.pl", process.execPath], { stdio: ["ignore", "inherit", "inherit"] });
process.exitCode = await perl.exited;`,
      });
      const unread = yield* runTaskProcess(project, "unread.ts");
      expect(unread.standardOutput).not.toBe("killed\n");
      expect((yield* read(`${project.directory}/written`)).trim()).toBe(
        String(UNREAD_OUTPUT_LINE_COUNT),
      );
      expect(
        yield* (yield* FileSystem.FileSystem).exists(`${project.directory}/cleanup-done`),
      ).toBe(true);
    }),
  ),
  "50 seconds",
);

export const consoleOutputWhenStandardOutputRefusesWrites = makeTest(
  "console output reaches standard error as output: lines and the run passes even when standard output refuses every write, since Task writes nothing there",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "chatty", run: Effect.sync(() => {
  process.stdout.write("hello from the task\\n");
}) });`,
        "read-only.ts": `const shell = Bun.spawn(["sh", "-c", \`\${process.execPath} main.ts 1</dev/null\`], { stdio: ["ignore", "inherit", "inherit"] });
process.exitCode = await shell.exited;`,
      });
      const ran = yield* runTaskProcess(project, "read-only.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const { endedLines, otherLines } = separateEndedLines(
        withSecondsNamed(withoutTheFirstRunLine(ran.standardError)).split("\n"),
      );
      expect(otherLines.slice(0, -1)).toEqual([
        "output: hello from the task",
        "passed: 1 task, <seconds>",
      ]);
      expect(otherLines.at(-1)).toMatch(/^run: \S+\/$/u);
      expect(endedLines).toEqual([]);
    }),
  ),
);

export const interruptWithConsoleOutputUnread = makeTest(
  "Ctrl-C while standard output is a pipe that nobody reads ends the run with exit 130 and every line on standard error, since Task writes nothing to standard output",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "chatty", run: Effect.gen(function* () {
  process.stdout.write("x".repeat(${CONSOLE_OUTPUT_LENGTH}));
  yield* Effect.promise(() => Bun.write("written", ""));
  return yield* Effect.never;
}) });`,
        "unread.pl": `use strict;
pipe(my $reader, my $writer) or die;
my $pid = fork();
if ($pid == 0) { close $reader; open(STDOUT, ">&", $writer) or die; exec($ARGV[0], "main.ts") or die; }
close $writer;
select(undef, undef, undef, 0.1) until -e "written";
kill "INT", $pid;
local $SIG{ALRM} = sub { kill "KILL", $pid; };
alarm 20;
waitpid($pid, 0);
print $? & 127 ? "killed\\n" : ($? >> 8) . "\\n";`,
        "unread.ts": `const perl = Bun.spawn(["perl", "unread.pl", process.execPath], { stdio: ["ignore", "inherit", "inherit"] });
process.exitCode = await perl.exited;`,
      });
      const unread = yield* runTaskProcess(project, "unread.ts");
      expect(unread.standardOutput).toBe(`${EXIT_CODE_AFTER_SIGINT}\n`);
      expect(withSecondsNamed(withoutTheFirstRunLine(unread.standardError))).toStartWith(
        `output: ${"x".repeat(CONSOLE_OUTPUT_LENGTH)}\n`,
      );
      expect(unread.standardError).toMatch(
        /^stopped: \[chatty\] \(1 of 1 task\), [\d.]+s\nrun: \S+\/$/mu,
      );
    }),
  ),
);

export const liveCommandOutput = makeTest(
  "a command's output line prints under the task's name while the command is still running",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "echo first; read line < gate; echo second"], { print: true }) });`,
      });
      yield* Task.exec("mkfifo", [`${project.directory}/gate`]);

      const command = yield* startTaskProcess(project, "main.ts");
      const gate = yield* fileSystem.open(`${project.directory}/gate`, { flag: "r+" });
      yield* command.waitForLine("[main] first");
      yield* gate.write(new TextEncoder().encode("\n"));

      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(0);
      expect(exited.standardError).toMatch(/\[main\] first\n\[main\] second\npassed: 1 task, /u);
    }),
  ),
  "30 seconds",
);

export const liveCommandOutputFromBothStreams = makeTest(
  "a command's standard output and standard error lines both print under the task's name while the command is still running",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "echo to standard output; echo to standard error >&2; read line < gate; echo done"], { print: true }) });`,
      });
      yield* Task.exec("mkfifo", [`${project.directory}/gate`]);

      const command = yield* startTaskProcess(project, "main.ts");
      const gate = yield* fileSystem.open(`${project.directory}/gate`, { flag: "r+" });
      yield* command.waitForLine("[main] to standard output");
      yield* command.waitForLine("[main] to standard error");
      yield* gate.write(new TextEncoder().encode("\n"));

      const exited = yield* command.waitForExit;
      expect(exited.exitCode, exited.standardError).toBe(0);
      expect(exited.standardError).toMatch(/^\[main\] done\npassed: 1 task, /mu);
    }),
  ),
  "30 seconds",
);

export const commandPromptWithoutANewline = makeTest(
  "text a command writes without a newline, such as a prompt, prints under the task's name before the command reads its answer, and the rest of the line joins it",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "printf 'Password: '; read answer < gate; echo accepted $answer"], { print: true }) });`,
      });
      yield* Task.exec("mkfifo", [`${project.directory}/gate`]);

      const command = yield* startTaskProcess(project, "main.ts");
      const gate = yield* fileSystem.open(`${project.directory}/gate`, { flag: "r+" });
      yield* command.waitForStandardError("\n[main] Password: ");
      yield* gate.write(new TextEncoder().encode("hunter2\n"));

      const exited = yield* command.waitForExit;
      expect(exited.exitCode, exited.standardError).toBe(0);
      expect(exited.standardError).toMatch(/\n\[main\] Password: accepted hunter2\n/u);
      expect(
        withSecondsNamed(yield* read(`${findRunDirectoryPath(exited.standardError)}/1.output`)),
      ).toBe(
        "started: [main], <seconds>, 1.output\n[main] Password: accepted hunter2\nended: [main], <seconds>, passed\n",
      );
    }),
  ),
  "30 seconds",
);

export const promptEndedByAnotherLine = makeTest(
  "a line from elsewhere ends a command's open prompt line first, and the rest of the prompt's line then prints as a new line under the task's name",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.all([
    Task.exec("sh", ["-c", "printf 'Continue? '; read answer < gate; echo $answer"], { print: true }),
    Task.exec("sh", ["-c", "read line < log-gate"]).pipe(Effect.andThen(Effect.log("in between"))),
  ], { concurrency: "unbounded" });
}) });`,
      });
      yield* Task.exec("mkfifo", [`${project.directory}/gate`, `${project.directory}/log-gate`]);

      const command = yield* startTaskProcess(project, "main.ts");
      const gate = yield* fileSystem.open(`${project.directory}/gate`, { flag: "r+" });
      const logGate = yield* fileSystem.open(`${project.directory}/log-gate`, { flag: "r+" });
      yield* command.waitForStandardError("\n[main] Continue? ");
      yield* logGate.write(new TextEncoder().encode("\n"));
      yield* command.waitForLine("[main] in between");
      yield* gate.write(new TextEncoder().encode("yes\n"));

      const exited = yield* command.waitForExit;
      expect(exited.exitCode, exited.standardError).toBe(0);
      expect(exited.standardError).toMatch(
        /\n\[main\] Continue\? \n\[main\] in between\n\[main\] yes\npassed: 1 task, /u,
      );
    }),
  ),
  "30 seconds",
);

export const printedOutputIsReleased = makeTest(
  "a running task keeps no copy of each command output it already printed",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { heapStats } from "bun:jsc";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const heapMegabytes = Effect.sleep("500 millis").pipe(Effect.andThen(Effect.sync(() => {
  Bun.gc(true);
  return Math.round(heapStats().heapSize / 1_000_000);
})));
const printLines = Task.exec("sh", ["-c", "yes $(printf '%04000d' 0) | head -n 5000"], { print: true });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* printLines;
  const before = yield* heapMegabytes;
  for (let printed = 0; printed < 5; printed++) yield* printLines;
  yield* Effect.log(\`heap grew by \${(yield* heapMegabytes) - before} MB\`);
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      const grownMegabytes = Number(
        /\[main\] heap grew by (?<megabytes>-?\d+) MB/u.exec(ran.standardError)?.groups?.[
          "megabytes"
        ],
      );
      expect(grownMegabytes).toBeLessThan(HEAP_GROWTH_LIMIT_MEGABYTES);
    }),
  ),
);

export const interleavedCommandOutput = makeTest(
  "a command's standard output and standard error lines print in the order the command wrote them",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "echo out one; sleep 0.3; echo err one >&2; sleep 0.3; echo out two; sleep 0.3; printf 'err two without a newline' >&2"], { print: true }) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toMatch(
        /\[main\] out one\n\[main\] err one\n\[main\] out two\n\[main\] err two without a newline\npassed: 1 task, /u,
      );
    }),
  ),
);

const FAILED_COMMAND_EXIT_CODE = 3;

const PRINTED_LIVE_AND_IN_THE_RECAP = 2;

export const failedCommandOutput = makeTest(
  "a failed command's output prints once, when the command fails, and not again under its CommandFailed error, only in its recap",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "echo checked; echo broken >&2; exit ${FAILED_COMMAND_EXIT_CODE}"]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(FAILED_COMMAND_EXIT_CODE);
      expect(ran.standardError).toContain(
        `[main] checked\n[main] broken\nerror: [main] main.ts:2:52: CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}\nerror: [main] its output is in ${findRunDirectoryPath(ran.standardError)}/1.output\nended: [main], `,
      );
      expect(ran.standardError).toContain(
        `\nrecap: [main] 1.output | main.ts:2:52: CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}\nrecap: [main] checked\nrecap: [main] broken\nfailed: `,
      );
      expect(countOccurrences(ran.standardError, "broken")).toBe(PRINTED_LIVE_AND_IN_THE_RECAP);
    }),
  ),
);

const WAITING_FOR_LOCK = "waiting for the lock held by pid 4242...";

const COMMAND_STOPS = [
  {
    stoppedBy: "its timeout",
    options: 'timeout: "2 seconds", cleanup: "5 seconds"',
    stop: () => Effect.void,
    exitCode: 1,
  },
  {
    stoppedBy: "Ctrl-C",
    options: 'cleanup: "5 seconds"',
    stop: (command: Effect.Success<ReturnType<typeof startTaskProcess>>) =>
      command.waitForLine("[main] ready").pipe(Effect.andThen(command.sendSignal("INT"))),
    exitCode: EXIT_CODE_AFTER_SIGINT,
  },
] as const;

export const stoppedCommandLastLine = makeTest(
  "a command stopped by its task's timeout or by Ctrl-C still prints the last line it wrote without a newline",
  Effect.forEach(
    COMMAND_STOPS,
    ({ stoppedBy, options, stop, exitCode }) =>
      Effect.scoped(
        Effect.gen(function* () {
          const project = yield* makeProject({
            "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "echo 'full line'; printf '${WAITING_FOR_LOCK}' >&2; echo ready; sleep 30"], { print: true }), ${options} });`,
          });
          const command = yield* startTaskProcess(project, "main.ts");
          yield* stop(command);
          const exited = yield* command.waitForExit;
          expect(exited.exitCode, stoppedBy).toBe(exitCode);
          expect(exited.standardError, stoppedBy).toContain("[main] full line\n");
          expect(exited.standardError, stoppedBy).toContain(`[main] ${WAITING_FOR_LOCK}\n`);
        }),
      ),
    { concurrency: "unbounded", discard: true },
  ),
);

export const stoppedCommandOutputWhileStopping = makeTest(
  "a stopped command's output from handling SIGTERM prints under the task's name",
  Effect.forEach(
    COMMAND_STOPS,
    ({ stoppedBy, options, stop, exitCode }) =>
      Effect.scoped(
        Effect.gen(function* () {
          const project = yield* makeProject({
            "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "trap 'echo released the lock; printf \\"removed the partial build\\" >&2; exit 1' TERM; echo ready; sleep 30 & wait"], { print: true }), ${options} });`,
          });
          const command = yield* startTaskProcess(project, "main.ts");
          yield* stop(command);
          const exited = yield* command.waitForExit;
          expect(exited.exitCode, stoppedBy).toBe(exitCode);
          expect(exited.standardError, stoppedBy).toContain("[main] released the lock\n");
          expect(exited.standardError, stoppedBy).toContain("[main] removed the partial build\n");
        }),
      ),
    { concurrency: "unbounded", discard: true },
  ),
);

export const stoppedCommandWithLingeringChild = makeTest(
  "a stopped command whose child ignores SIGTERM prints its last line without waiting for that child to exit",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "printf '${WAITING_FOR_LOCK}' >&2; (trap '' TERM; sleep 30) & wait"], { print: true }), timeout: "1 second", cleanup: "1 minute" });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(`[main] ${WAITING_FOR_LOCK}\n`);
      expect(ran.standardError).toMatch(/^failed: \[main\] \(1 of 1 task\), [0-9]\.\ds$/mu);
    }),
  ),
);

const LONG_LINE_LENGTH = 3000;

const LONG_LINES_PER_TASK = 400;

const LONG_LINE_TASK_NAMES = ["a", "b", "c"] as const;

const printLongLines = (letter: string) =>
  `Task.exec("awk", ["BEGIN { line = sprintf(\\"%${LONG_LINE_LENGTH}s\\", \\"\\"); gsub(/ /, \\"${letter}\\", line); for (i = 0; i < ${LONG_LINES_PER_TASK}; i++) print line }"], { print: true })`;

export const parallelCommandOutput = makeTest(
  "long output lines from commands in parallel tasks print every character under their own task's name, and each task's file, named on its started line at SSHKEDA_TASK_LOG=debug, holds them whole",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
${LONG_LINE_TASK_NAMES.map((letter) => `export const ${letter} = Task.make({ name: "${letter}", run: ${printLongLines(letter)} });`).join("\n")}
export default Task.make({ name: "main", run: Task.all([${LONG_LINE_TASK_NAMES.join(", ")}]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_LOG: "debug" });
      expect(ran.exitCode).toBe(0);
      const commandLines = ran.standardError
        .split("\n")
        .filter((line) => /^\[[abc]\] /u.test(line));
      expect(
        commandLines.filter((line) => !/^\[(?<letter>[abc])\] \k<letter>+$/u.test(line)),
      ).toEqual([]);
      const runDirectoryPath = findRunDirectoryPath(ran.standardError);
      for (const letter of LONG_LINE_TASK_NAMES) {
        const fileName = new RegExp(
          `^started: \\[${letter}\\], [\\d.]+s, (?<fileName>\\d+\\.output)$`,
          "mu",
        ).exec(ran.standardError)?.groups?.["fileName"];
        const printed = commandLines
          .filter((line) => line.startsWith(`[${letter}] `))
          .map((line) => line.slice(`[${letter}] `.length))
          .join("");
        expect(printed.length, letter).toBe(LONG_LINE_LENGTH * LONG_LINES_PER_TASK);
        const fileLines = (yield* read(`${runDirectoryPath}/${fileName}`))
          .split("\n")
          .filter((line) => line.startsWith(`[${letter}] `));
        expect(fileLines, letter).toEqual(
          Array.from(
            { length: LONG_LINES_PER_TASK },
            () => `[${letter}] ${letter.repeat(LONG_LINE_LENGTH)}`,
          ),
        );
      }
    }),
  ),
);

const PARALLEL_LOG_TASK_NAMES = ["a", "b", "c", "d"] as const;

const runParallelLoggingProject = (lineCount: number, lineLength: number, reader: string) =>
  Effect.gen(function* () {
    const project = yield* makeProject({
      "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const logLines = (letter: string) => Effect.forEach(Array.from({ length: ${lineCount} }, (_, index) => index), (index) => Effect.log(\`\${String(index).padStart(6, "0")} \${letter.repeat(${lineLength})}\`), { discard: true });
${PARALLEL_LOG_TASK_NAMES.map((letter) => `export const ${letter} = Task.make({ name: "${letter}", run: logLines("${letter}") });`).join("\n")}
export default Task.make({ name: "main", run: Task.all([${PARALLEL_LOG_TASK_NAMES.join(", ")}]) });`,
      "piped.ts": `const shell = Bun.spawn(["sh", "-c", \`(\${process.execPath} main.ts; echo $? > exit-code) 2>&1 | ${reader}\`], { stdio: ["ignore", "inherit", "inherit"] });
process.exitCode = await shell.exited;`,
    });
    yield* runTaskProcess(project, "piped.ts");
    return project.directory;
  });

const readParallelLogLines = (projectDirectory: string, lineLength: number) =>
  Effect.gen(function* () {
    const outputLines = (yield* read(`${projectDirectory}/output`)).split("\n");
    const taskNameCharacter = `[${PARALLEL_LOG_TASK_NAMES.join("")}]`;
    const logLine = new RegExp(`^\\[${taskNameCharacter}\\] \\d{6} `, "u");
    const wholeLine = new RegExp(
      `^\\[(?<letter>${taskNameCharacter})\\] \\d{6} \\k<letter>{${lineLength}}$`,
      "u",
    );
    const logLines = outputLines.filter((line) => logLine.test(line));
    return {
      exitCode: (yield* read(`${projectDirectory}/exit-code`)).trim(),
      logLineCount: logLines.length,
      cutLines: logLines.filter((line) => !wholeLine.test(line)),
      summaryLine: outputLines.find((line) => line.startsWith("passed: ")),
    };
  });

const SHORT_LOG_LINES_PER_TASK = 2000;

const SHORT_LOG_LINE_LENGTH = 100;

export const readerThatWaits = makeTest(
  "every log line from parallel tasks arrives whole when standard error shares a pipe with standard output, as in 2>&1, and the reader waits before it reads",
  Effect.scoped(
    Effect.gen(function* () {
      const projectDirectory = yield* runParallelLoggingProject(
        SHORT_LOG_LINES_PER_TASK,
        SHORT_LOG_LINE_LENGTH,
        "(sleep 1; cat > output)",
      );
      const printed = yield* readParallelLogLines(projectDirectory, SHORT_LOG_LINE_LENGTH);
      expect(printed.exitCode).toBe("0");
      expect(printed.logLineCount).toBe(SHORT_LOG_LINES_PER_TASK * PARALLEL_LOG_TASK_NAMES.length);
      expect(printed.cutLines).toEqual([]);
      const taskCount = PARALLEL_LOG_TASK_NAMES.length + 1;
      expect(printed.summaryLine).toMatch(
        new RegExp(`^passed: ${taskCount} tasks, [\\d.]+s$`, "u"),
      );
    }),
  ),
);

const LONG_LOG_LINES_PER_TASK = 20;

const LONG_LOG_LINE_LENGTH = 100_000;

export const logLinesLongerThanThePipe = makeTest(
  "log lines longer than the pipe buffer from parallel tasks arrive whole when standard error shares a pipe with standard output, as in 2>&1 | cat",
  Effect.scoped(
    Effect.gen(function* () {
      const projectDirectory = yield* runParallelLoggingProject(
        LONG_LOG_LINES_PER_TASK,
        LONG_LOG_LINE_LENGTH,
        "cat > output",
      );
      const printed = yield* readParallelLogLines(projectDirectory, LONG_LOG_LINE_LENGTH);
      expect(printed.exitCode).toBe("0");
      expect(printed.logLineCount).toBe(LONG_LOG_LINES_PER_TASK * PARALLEL_LOG_TASK_NAMES.length);
      expect(printed.cutLines).toEqual([]);
    }),
  ),
);

const LOG_LINES_PER_TASK_BEFORE_THE_READER_LEAVES = 1000;

export const readerLeavesAFullPipe = makeTest(
  "when the reader of a full 2>&1 pipe reads a little and goes away before task wrote all its lines, the run exits 141",
  Effect.scoped(
    Effect.gen(function* () {
      const projectDirectory = yield* runParallelLoggingProject(
        LOG_LINES_PER_TASK_BEFORE_THE_READER_LEAVES,
        SHORT_LOG_LINE_LENGTH,
        "(sleep 1; head -c 1000 > output)",
      );
      expect((yield* read(`${projectDirectory}/exit-code`)).trim()).toBe("141");
    }),
  ),
);

export const pipeBlockingAfterTheRun = makeTest(
  "after a run that printed through a 2>&1 pipe, the pipe is blocking again for the next command in the shell",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.log("hello") });`,
        "piped.ts": `const shell = Bun.spawn(["sh", "-c", \`(\${process.execPath} main.ts 2>&1; perl -MFcntl -e 'open(my $after, ">", "after-the-run") or die; print $after fcntl(STDOUT, F_GETFL, 0) & O_NONBLOCK ? "non-blocking" : "blocking"') | cat > output\`], { stdio: ["ignore", "inherit", "inherit"] });
process.exitCode = await shell.exited;`,
      });
      yield* runTaskProcess(project, "piped.ts");
      expect(yield* read(`${project.directory}/output`)).toContain("[main] hello\n");
      expect(yield* read(`${project.directory}/after-the-run`)).toBe("blocking");
    }),
  ),
);

const MANY_TASK_COUNT = 40;

const MANY_TASK_NAMES = Array.from({ length: MANY_TASK_COUNT }, (_unused, index) => `task${index}`);

const runManyTasksOnTheirOwnWorkers = (list: string, run: string) =>
  Effect.gen(function* () {
    const project = yield* makeProject({
      "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
${MANY_TASK_NAMES.map((name) => `export const ${name} = Task.make({ name: "${name}", run: ${run.replaceAll("NAME", name)} });`).join("\n")}
export default Task.make({ name: "main", run: ${list}([${MANY_TASK_NAMES.join(", ")}]) });`,
    });
    return yield* runTaskProcess(project, "main.ts", {
      SSHKEDA_TASK_WORKERS: String(MANY_TASK_COUNT),
    });
  });

export const manyWorkersLogging = makeTest(
  "a run where dozens of tasks log and print at once on their own workers prints every line on standard error and no listener leak warning",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runManyTasksOnTheirOwnWorkers(
        "Task.all",
        `Effect.log("logged by NAME").pipe(Effect.andThen(Effect.sync(() => console.log("printed by NAME"))))`,
      );
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).not.toContain("MaxListenersExceededWarning");
      expect(
        MANY_TASK_NAMES.filter(
          (name) => !ran.standardError.includes(`[${name}] logged by ${name}\n`),
        ),
      ).toEqual([]);
      expect(
        MANY_TASK_NAMES.filter(
          (name) => !ran.standardError.includes(`[${name}] printed by ${name}\n`),
        ),
      ).toEqual([]);
      expect(ran.standardOutput).toBe("");
    }),
  ),
);

export const manyWorkersFailing = makeTest(
  "a run where dozens of tasks fail at once on their own workers prints every failure and no listener leak warning",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runManyTasksOnTheirOwnWorkers(
        "Task.allSettled",
        `Effect.fail("failed in NAME")`,
      );
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).not.toContain("MaxListenersExceededWarning");
      expect(
        MANY_TASK_NAMES.filter((name) => !ran.standardError.includes(`failed in ${name}`)),
      ).toEqual([]);
    }),
  ),
);

const FULL_BUDGET_BYTES = 16_777_216;

export const liftBetweenReadAndWait = Task.make({
  name: "a command held by a full output budget goes on when the budget is lifted after it read the budget and before it started to wait",
  timeout: "10 seconds",
  cleanup: "5 seconds",
  run: Effect.gen(function* () {
    const budget = makeTaskOutputBudget();
    expect(tryToTakeTaskOutputBytes(budget, FULL_BUDGET_BYTES)).toEqual({
      taken: FULL_BUDGET_BYTES,
    });
    const attempt = tryToTakeTaskOutputBytes(budget, 1);
    expect(attempt).toEqual({ waitWhile: FULL_BUDGET_BYTES });
    liftTaskOutputBudget(budget);
    expect(yield* waitForTaskOutputBudgetChange(budget, FULL_BUDGET_BYTES)).toBe("not-equal");
    expect(yield* takeTaskOutputBytes(budget, 1)).toBe(0);
    const waitingBudget = makeTaskOutputBudget();
    tryToTakeTaskOutputBytes(waitingBudget, FULL_BUDGET_BYTES);
    const [woken] = yield* Effect.all(
      [
        waitForTaskOutputBudgetChange(waitingBudget, FULL_BUDGET_BYTES),
        Effect.yieldNow.pipe(
          Effect.andThen(
            Effect.sync(() => {
              liftTaskOutputBudget(waitingBudget);
            }),
          ),
        ),
      ],
      { concurrency: "unbounded" },
    );
    expect(woken).toBe("ok");
  }),
});

const ALTERNATING_PIECE_COUNT = 70_000;

const PROBE_TASK_NAME_LENGTH = 245;

export const alternatingPartialLinesAreReserved = makeTest(
  "command output that alternates partial lines between standard output and standard error never prints more bytes than it reserved from the output budget, even under a long task name",
  Effect.sync(() => {
    const taskName = "n".repeat(PROBE_TASK_NAME_LENGTH);
    const sources = [`[${taskName}] 0`, `[${taskName}] 1`];
    const totals = {
      openSource: Option.none<string>().pipe(Option.getOrNull),
      joinedBytes: 0,
      reservedBytes: 0,
    };
    for (const index of Array.from({ length: ALTERNATING_PIECE_COUNT }).keys()) {
      const piece = { text: "x", continuesLine: index >= sources.length, endsLine: false };
      const [openSource, texts] = joinOutput(
        totals.openSource,
        OutputToJoin.cases.Piece.make({
          source: sources[index % sources.length] ?? "",
          taskName,
          piece,
        }),
      );
      totals.openSource = openSource;
      totals.joinedBytes += Buffer.byteLength(texts.join(""));
      totals.reservedBytes += reserveTaskOutputBytes(taskName, [piece]);
    }
    const closingLineBreakBytes = totals.openSource === null ? 0 : 1;
    expect(totals.joinedBytes + closingLineBreakBytes).toBeLessThanOrEqual(totals.reservedBytes);
  }),
);
