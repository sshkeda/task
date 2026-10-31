import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect, FileSystem } from "effect";

import { read } from "../shared/access-files.ts";
import { EXIT_CODE_AFTER_SIGINT } from "../shared/exit-codes-after-stop-signals.ts";
import { makeProject } from "../shared/make-projects.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";

const LOOP_LINE_COUNT = 20_000;

const LINES_AFTER_THE_LOOP = 2;

export const logs = Task.make(
  "a task's logs print under its name",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.gen(function* () {
  yield* Effect.log("hello", { n: 1 });
  yield* Effect.logWarning("careful");
  yield* Effect.logError("broken");
}));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain('[main] hello {"n":1}\n[main] careful\n[main] broken');
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const liveLogs = Task.make(
  "a task's log line prints while the task is still running",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.gen(function* () {
  yield* Effect.log("waiting");
  const reader = yield* ChildProcess.make("sh", ["-c", "read line < gate"]);
  yield* reader.exitCode;
  yield* Effect.log("done");
}).pipe(Effect.scoped));
if (import.meta.main) Task.main(main);`,
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const consoleOutputBeforeExit = Task.make(
  "console output a task writes just before the run ends reaches stdout in full, whether the task passes or fails",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export const chatty = Task.make("chatty", Effect.gen(function* () {
  for (let line = 0; line < 20000; line++) console.log(\`ok \${line}\`);
  console.log("last line");
  if (process.env.FAIL === "1") return yield* Effect.fail(new TaskError({ message: "1 test failed" }));
}));
if (import.meta.main) Task.main(chatty);`,
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const liveCommandOutput = Task.make(
  "a command's output line prints under the task's name while the command is still running",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export const main = Task.make("main", Task.exec("sh", ["-c", "echo first; read line < gate; echo second"]));
if (import.meta.main) Task.main(main);`,
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
  { timeout: "30 seconds", cleanup: "5 seconds" },
);

export const interleavedCommandOutput = Task.make(
  "a command's standard output and standard error lines print in the order the command wrote them",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export const main = Task.make("main", Task.exec("sh", ["-c", "echo out one; sleep 0.3; echo err one >&2; sleep 0.3; echo out two; sleep 0.3; printf 'err two without a newline' >&2"]));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain(
        "[main] out one\n[main] err one\n[main] out two\n[main] err two without a newline\n[main] passed in ",
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

const FAILED_COMMAND_EXIT_CODE = 3;

export const failedCommandOutput = Task.make(
  "a failed command's output prints as the command writes it, and its CommandFailed error still carries that output",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export const main = Task.make("main", Task.exec("sh", ["-c", "echo checked; echo broken >&2; exit ${FAILED_COMMAND_EXIT_CODE}"]));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(FAILED_COMMAND_EXIT_CODE);
      expect(ran.standardError).toContain(
        `[main] checked\n[main] broken\n[main] error CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}\n[main]   checked\n[main]   broken\n`,
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

const WAITING_FOR_LOCK = "waiting for the lock held by pid 4242...";

const COMMAND_STOPS = [
  {
    stoppedBy: "its timeout",
    options: '{ timeout: "2 seconds", cleanup: "5 seconds" }',
    stop: () => Effect.void,
    exitCode: 1,
  },
  {
    stoppedBy: "Ctrl-C",
    options: '{ cleanup: "5 seconds" }',
    stop: (command: Effect.Success<ReturnType<typeof startTaskProcess>>) =>
      command.waitForLine("[main] ready").pipe(Effect.andThen(command.sendSignal("INT"))),
    exitCode: EXIT_CODE_AFTER_SIGINT,
  },
] as const;

export const stoppedCommandLastLine = Task.make(
  "a command stopped by its task's timeout or by Ctrl-C still prints the last line it wrote without a newline",
  Effect.forEach(
    COMMAND_STOPS,
    ({ stoppedBy, options, stop, exitCode }) =>
      Effect.scoped(
        Effect.gen(function* () {
          const project = yield* makeProject({
            "main.ts": `import { Task } from "@sshkeda/task";
export const main = Task.make("main", Task.exec("sh", ["-c", "echo 'full line'; printf '${WAITING_FOR_LOCK}' >&2; echo ready; sleep 30"]), ${options});
if (import.meta.main) Task.main(main);`,
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const stoppedCommandOutputWhileStopping = Task.make(
  "a stopped command's output from handling SIGTERM prints under the task's name",
  Effect.forEach(
    COMMAND_STOPS,
    ({ stoppedBy, options, stop, exitCode }) =>
      Effect.scoped(
        Effect.gen(function* () {
          const project = yield* makeProject({
            "main.ts": `import { Task } from "@sshkeda/task";
export const main = Task.make("main", Task.exec("sh", ["-c", "trap 'echo released the lock; printf \\"removed the partial build\\" >&2; exit 1' TERM; echo ready; sleep 30 & wait"]), ${options});
if (import.meta.main) Task.main(main);`,
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const stoppedCommandWithLingeringChild = Task.make(
  "a stopped command whose child ignores SIGTERM prints its last line without waiting for that child to exit",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export const main = Task.make("main", Task.exec("sh", ["-c", "printf '${WAITING_FOR_LOCK}' >&2; (trap '' TERM; sleep 30) & wait"]), { timeout: "1 second", cleanup: "1 minute" });
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(`[main] ${WAITING_FOR_LOCK}\n`);
      expect(ran.standardError).toMatch(/^failed: main \(1 of 1 task\), [0-9]\.\ds$/mu);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

const LONG_LINE_LENGTH = 3000;

const LONG_LINES_PER_TASK = 400;

const LONG_LINE_TASK_NAMES = ["a", "b", "c"] as const;

export const parallelCommandOutput = Task.make(
  "long output lines from commands in parallel tasks print whole, never cut by another task's lines",
  Effect.scoped(
    Effect.gen(function* () {
      const printLongLines = (letter: string) =>
        `Task.exec("awk", ["BEGIN { line = sprintf(\\"%${LONG_LINE_LENGTH}s\\", \\"\\"); gsub(/ /, \\"${letter}\\", line); for (i = 0; i < ${LONG_LINES_PER_TASK}; i++) print line }"])`;
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
${LONG_LINE_TASK_NAMES.map((letter) => `export const ${letter} = Task.make("${letter}", ${printLongLines(letter)});`).join("\n")}
export const main = Task.make("main", Task.all([${LONG_LINE_TASK_NAMES.join(", ")}]));
if (import.meta.main) Task.main(main);`,
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

const PARALLEL_LOG_TASK_NAMES = ["a", "b", "c", "d"] as const;

const runParallelLoggingProject = (lineCount: number, lineLength: number, reader: string) =>
  Effect.gen(function* () {
    const project = yield* makeProject({
      "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const logLines = (letter: string) => Effect.forEach(Array.from({ length: ${lineCount} }, (_, index) => index), (index) => Effect.log(\`\${String(index).padStart(6, "0")} \${letter.repeat(${lineLength})}\`), { discard: true });
${PARALLEL_LOG_TASK_NAMES.map((letter) => `export const ${letter} = Task.make("${letter}", logLines("${letter}"));`).join("\n")}
export const main = Task.make("main", Task.all([${PARALLEL_LOG_TASK_NAMES.join(", ")}]));
if (import.meta.main) Task.main(main);`,
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

export const readerThatWaits = Task.make(
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

const LONG_LOG_LINES_PER_TASK = 20;

const LONG_LOG_LINE_LENGTH = 100_000;

export const logLinesLongerThanThePipe = Task.make(
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

const LOG_LINES_PER_TASK_BEFORE_THE_READER_LEAVES = 1000;

export const readerLeavesAFullPipe = Task.make(
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const pipeBlockingAfterTheRun = Task.make(
  "after a run that printed through a 2>&1 pipe, the pipe is blocking again for the next command in the shell",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.log("hello"));
if (import.meta.main) Task.main(main);`,
        "piped.ts": `const shell = Bun.spawn(["sh", "-c", \`(\${process.execPath} main.ts 2>&1; perl -MFcntl -e 'open(my $after, ">", "after-the-run") or die; print $after fcntl(STDOUT, F_GETFL, 0) & O_NONBLOCK ? "non-blocking" : "blocking"') | cat > output\`], { stdio: ["ignore", "inherit", "inherit"] });
process.exitCode = await shell.exited;`,
      });
      yield* runTaskProcess(project, "piped.ts");
      expect(yield* read(`${project.directory}/output`)).toContain("[main] hello\n");
      expect(yield* read(`${project.directory}/after-the-run`)).toBe("blocking");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);
