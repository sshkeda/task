import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect, FileSystem } from "effect";

import { read } from "../shared/access-files.ts";
import { EXIT_CODE_AFTER_SIGINT } from "../shared/exit-codes-after-stop-signals.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";

const LOOP_LINE_COUNT = 20_000;

const LINES_AFTER_THE_LOOP = 2;

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
          `[main] error WorkerExited: its worker exited on its own with code ${WORKER_EXIT_CODE}`,
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
      expect(ran.standardError).toMatch(
        /^\[main\] retrying\n\[main\] \S+: warn UntaggedFailure: timeout$/mu,
      );
      expect(ran.standardError).toMatch(
        /^\[main\] caught\n\[main\] \S+: info UntaggedFailure: not found$/mu,
      );
      expect(ran.standardError).toMatch(
        /^\[main\] gave up\n\[main\] \S+: error UntaggedFailure: still down$/mu,
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
  "console output a task writes just before the run ends reaches stdout in full, whether the task passes or fails",
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
        const lines = ran.standardOutput.split("\n");
        expect(lines.length).toBe(LOOP_LINE_COUNT + LINES_AFTER_THE_LOOP);
        expect(lines.at(-LINES_AFTER_THE_LOOP)).toBe("last line");
      }
    }),
  ),
);

const CONSOLE_OUTPUT_LENGTH = 1_000_000;

export const consoleOutputToASlowReader = makeTest(
  "console output a task writes just before the run ends reaches a reader that waits before it reads, as in | (sleep 3; cat), even when the pipe is non-blocking",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "chatty", run: Effect.sync(() => {
  process.stdout.write("x".repeat(${CONSOLE_OUTPUT_LENGTH}) + "\\nlast line\\n");
}) });`,
        "piped.ts": `const shell = Bun.spawn(["sh", "-c", \`{ perl -MFcntl -e 'fcntl(STDOUT, F_SETFL, fcntl(STDOUT, F_GETFL, 0) | O_NONBLOCK) or die; exec @ARGV' \${process.execPath} main.ts; echo $? > exit-code; } | (sleep 3; cat > output)\`], { stdio: ["ignore", "inherit", "inherit"] });
process.exitCode = await shell.exited;`,
      });
      yield* runTaskProcess(project, "piped.ts");
      const output = yield* read(`${project.directory}/output`);
      expect(output.length).toBe(CONSOLE_OUTPUT_LENGTH + "\nlast line\n".length);
      expect(output.endsWith("\nlast line\n")).toBe(true);
      expect((yield* read(`${project.directory}/exit-code`)).trim()).toBe("0");
    }),
  ),
);

export const consoleOutputToAReaderThatLeaves = makeTest(
  "console output to a reader that leaves early, as in | head -n 1, does not crash the run",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "chatty", run: Effect.sync(() => {
  for (let line = 0; line < 20; line++) process.stdout.write("x".repeat(${CONSOLE_OUTPUT_LENGTH}) + "\\n");
}) });`,
        "piped.ts": `const shell = Bun.spawn(["sh", "-c", \`{ \${process.execPath} main.ts 2> errors; echo $? > exit-code; } | head -n 1 > output\`], { stdio: ["ignore", "inherit", "inherit"] });
process.exitCode = await shell.exited;`,
      });
      yield* runTaskProcess(project, "piped.ts");
      expect((yield* read(`${project.directory}/output`)).length).toBe(CONSOLE_OUTPUT_LENGTH + 1);
      expect(yield* read(`${project.directory}/errors`)).toContain("\npassed: 1 task, ");
      expect((yield* read(`${project.directory}/exit-code`)).trim()).toBe("0");
    }),
  ),
);

export const interruptWithConsoleOutputUnread = makeTest(
  "Ctrl-C while standard output is a full pipe that nobody reads still ends the run with exit 130",
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
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "echo first; read line < gate; echo second"]) });`,
      });
      yield* Task.exec("mkfifo", [`${project.directory}/gate`]);

      const command = yield* startTaskProcess(project, "main.ts");
      const gate = yield* fileSystem.open(`${project.directory}/gate`, { flag: "r+" });
      yield* command.waitForLine("[main] first");
      yield* gate.write(new TextEncoder().encode("\n"));

      const exited = yield* command.waitForExit;
      expect(exited.exitCode).toBe(0);
      expect(exited.standardError).toContain("[main] first\n[main] second\n[main] passed in ");
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
const printLines = Task.exec("sh", ["-c", "yes $(printf '%04000d' 0) | head -n 5000"]);
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
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "echo out one; sleep 0.3; echo err one >&2; sleep 0.3; echo out two; sleep 0.3; printf 'err two without a newline' >&2"]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain(
        "[main] out one\n[main] err one\n[main] out two\n[main] err two without a newline\n[main] passed in ",
      );
    }),
  ),
);

const FAILED_COMMAND_EXIT_CODE = 3;

export const failedCommandOutput = makeTest(
  "a failed command's output prints once, as the command writes it, and not again under its CommandFailed error, while the last line of the run still carries it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "echo checked; echo broken >&2; exit ${FAILED_COMMAND_EXIT_CODE}"]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(FAILED_COMMAND_EXIT_CODE);
      expect(ran.standardError).toContain(
        `[main] checked\n[main] broken\n[main] error CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}\nfailed: main (1 of 1 task), `,
      );
      expect(ran.standardError).toEndWith(
        `\nCommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE} | checked | broken`,
      );
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
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "echo 'full line'; printf '${WAITING_FOR_LOCK}' >&2; echo ready; sleep 30"]), ${options} });`,
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
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "trap 'echo released the lock; printf \\"removed the partial build\\" >&2; exit 1' TERM; echo ready; sleep 30 & wait"]), ${options} });`,
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
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "printf '${WAITING_FOR_LOCK}' >&2; (trap '' TERM; sleep 30) & wait"]), timeout: "1 second", cleanup: "1 minute" });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(`[main] ${WAITING_FOR_LOCK}\n`);
      expect(ran.standardError).toMatch(/^failed: main \(1 of 1 task\), [0-9]\.\ds$/mu);
    }),
  ),
);

const LONG_LINE_LENGTH = 3000;

const LONG_LINES_PER_TASK = 400;

const LONG_LINE_TASK_NAMES = ["a", "b", "c"] as const;

export const parallelCommandOutput = makeTest(
  "long output lines from commands in parallel tasks print whole, never cut by another task's lines",
  Effect.scoped(
    Effect.gen(function* () {
      const printLongLines = (letter: string) =>
        `Task.exec("awk", ["BEGIN { line = sprintf(\\"%${LONG_LINE_LENGTH}s\\", \\"\\"); gsub(/ /, \\"${letter}\\", line); for (i = 0; i < ${LONG_LINES_PER_TASK}; i++) print line }"])`;
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
${LONG_LINE_TASK_NAMES.map((letter) => `export const ${letter} = Task.make({ name: "${letter}", run: ${printLongLines(letter)} });`).join("\n")}
export default Task.make({ name: "main", run: Task.all([${LONG_LINE_TASK_NAMES.join(", ")}]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      const outputLines = ran.standardError
        .split("\n")
        .filter((line) => line.length > LONG_LINE_LENGTH);
      expect(outputLines).toHaveLength(LONG_LINES_PER_TASK * LONG_LINE_TASK_NAMES.length);
      expect(
        outputLines.filter((line) => !/^\[(?<letter>[abc])\] \k<letter>+$/u.test(line)),
      ).toEqual([]);
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
  "a run where dozens of tasks log at once on their own workers prints every line on standard output and standard error and no listener leak warning",
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
        MANY_TASK_NAMES.filter((name) => !ran.standardOutput.includes(`printed by ${name}\n`)),
      ).toEqual([]);
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
