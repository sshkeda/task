import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Deferred, Effect, FileSystem, Path, Queue } from "effect";
import type { Cause } from "effect";

import { makeTaskOutputFiles } from "../../src/lib/main-thread/shared/write-task-output-files.ts";
import { standardErrorLinesDeferred } from "../../src/shared/print-to-standard-error.ts";
import type { makeStandardErrorLine } from "../../src/shared/print-to-standard-error.ts";
import { read } from "../shared/access-files.ts";
import { countOccurrences } from "../shared/count-occurrences.ts";
import { encodeJson } from "../shared/encode-json.ts";
import { EXIT_CODE_AFTER_SIGINT } from "../shared/exit-codes-after-stop-signals.ts";
import { expectOneOfTheOrders } from "../shared/expect-one-of-the-orders.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import {
  findRunDirectoryPath,
  findRunLinePath,
  withSecondsNamed,
} from "../shared/name-output-files.ts";
import { readAttribute, readProjectRunsDirectory, readTraces } from "../shared/read-trace-files.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";
import { withoutStackFrames } from "../shared/without-stack-frames.ts";

const OUTPUT_FILE_NAME_ATTRIBUTE = "sshkeda_task.output.file.name";

const LONG_TASK_NAME_REPEATS = 20;

const LONG_TASK_NAME = `checks that a/b paths, spaces and "quotes" in ${"a very long test name ".repeat(LONG_TASK_NAME_REPEATS)}still work`;

const FAILING_TYPECHECK_PROJECT = {
  "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const typecheck = Task.make({ name: "typecheck", run: Effect.gen(function* () {
  yield* Effect.log("checking 2 files");
  yield* Task.exec("sh", ["-c", "echo 'src/a.ts(1,7): error TS2322: Type string is not assignable to type number.' >&2; echo 'Found 1 error.'; exit 2"]);
}) });
export default Task.make({ name: "main", run: typecheck });`,
} as const;

const COMMAND_LINES_START = 2;

const COMMAND_LINES_END = 4;

const readOutputFileNamesOfTasks = (
  project: Parameters<typeof readProjectRunsDirectory>[0],
  ran: Parameters<typeof readProjectRunsDirectory>[1],
) =>
  Effect.gen(function* () {
    const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
    return Object.fromEntries(
      (trace?.spans ?? [])
        .filter((span) => span.role === "task")
        .map((span) => [
          String(readAttribute(span.attributes, "sshkeda_task.task")),
          readAttribute(span.attributes, OUTPUT_FILE_NAME_ATTRIBUTE),
        ]),
    );
  });

const readRunDirectory = (standardError: string) =>
  Effect.gen(function* () {
    const runDirectory = yield* Effect.fromNullishOr(findRunDirectoryPath(standardError));
    return {
      path: runDirectory,
      fileNames: (yield* (yield* FileSystem.FileSystem).readDirectory(runDirectory)).toSorted(),
    };
  });

export const helloRun = makeTest(
  "a passing task whose command prints one line prints that line, the summary and the run line naming the run folder, which holds the trace and a file with its started line, that line and its ended line",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "hello.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "hello", run: Task.exec("echo", ["hello world"], { print: true }) });`,
      });
      const ran = yield* runTaskProcess(project, "hello.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const runDirectory = yield* readRunDirectory(ran.standardError);
      expect(runDirectory.path).toStartWith(`${project.home}/.sshkeda-task/runs/project/`);
      expect(withSecondsNamed(ran.standardError)).toBe(
        [
          `run: ${runDirectory.path}/`,
          "[hello] hello world",
          "passed: 1 task, <seconds>",
          `run: ${runDirectory.path}/`,
        ].join("\n"),
      );
      expect(runDirectory.fileNames).toEqual(["1.output", "trace.jsonl"]);
      expect(withSecondsNamed(yield* read(`${runDirectory.path}/1.output`))).toBe(
        "started: [hello], <seconds>, 1.output\n[hello] hello world\nended: [hello], <seconds>, passed\n",
      );
    }),
  ),
);

export const silentTaskGetsAFile = makeTest(
  "a task that prints nothing of its own gets an output file with its started and ended lines, numbered in the order the tasks started, and its span names it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const silent = Task.make({ name: "silent", run: Effect.void });
export default Task.make({ name: "main", run: Effect.andThen(silent, Effect.log("done")) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const runDirectory = yield* readRunDirectory(ran.standardError);
      expect(runDirectory.fileNames).toEqual(["1.output", "2.output", "trace.jsonl"]);
      expect(withSecondsNamed(yield* read(`${runDirectory.path}/1.output`))).toBe(
        "started: [main], <seconds>, 1.output\n[main] done\nended: [main], <seconds>, passed\n",
      );
      expect(withSecondsNamed(yield* read(`${runDirectory.path}/2.output`))).toBe(
        "started: [silent], <seconds>, 2.output\nended: [silent], <seconds>, passed\n",
      );
      expect(yield* readOutputFileNamesOfTasks(project, ran)).toEqual({
        silent: "2.output",
        main: "1.output",
      });
    }),
  ),
);

export const spanNamesTheOutputFile = makeTest(
  "each task's span names its output file, numbered in the order the tasks started",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const first = Task.make({ name: "lint", run: Effect.log("lint ran") });
export const second = Task.make({ name: "build", run: Effect.log("build ran") });
export default Task.make({ name: "main", run: second.pipe(Effect.andThen(first), Effect.andThen(Effect.log("main ran"))) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const fileNames = yield* readOutputFileNamesOfTasks(project, ran);
      expect(fileNames).toEqual({ main: "1.output", build: "2.output", lint: "3.output" });
      const runDirectory = yield* readRunDirectory(ran.standardError);
      expect(runDirectory.fileNames).toEqual(["1.output", "2.output", "3.output", "trace.jsonl"]);
      for (const [taskName, fileName] of Object.entries(fileNames)) {
        expect(withSecondsNamed(yield* read(`${runDirectory.path}/${String(fileName)}`))).toBe(
          `started: [${taskName}], <seconds>, ${String(fileName)}\n[${taskName}] ${taskName} ran\nended: [${taskName}], <seconds>, passed\n`,
        );
      }
    }),
  ),
);

export const failedTaskOutputFile = makeTest(
  "a failed task's own lines, its command's output and its error lines go in terminal format to its own file, and the run prints its error lines, the summary and the run line naming the run folder",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject(FAILING_TYPECHECK_PROJECT);
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      const runDirectory = yield* readRunDirectory(ran.standardError);
      const printed = withSecondsNamed(ran.standardError).split("\n");
      const commandLines = printed.slice(COMMAND_LINES_START, COMMAND_LINES_END);
      const beforeTheEnds = [
        `run: ${runDirectory.path}/`,
        "[typecheck] checking 2 files",
        "[typecheck] Found 1 error.",
        "[typecheck] src/a.ts(1,7): error TS2322: Type string is not assignable to type number.",
        "error: [typecheck] main.ts:5:15: CommandFailed: sh exited with code 2",
      ];
      const typecheckEnd = [
        `error: [typecheck] its output is in ${runDirectory.path}/2.output`,
        "ended: [typecheck], <seconds>, failed",
      ];
      const mainEnd = ["ended: [main], <seconds>, failed"];
      const afterTheEnds = [
        "recap: [typecheck] 2.output | main.ts:5:15: CommandFailed: sh exited with code 2",
        "recap: [typecheck] Found 1 error.",
        "recap: [typecheck] src/a.ts(1,7): error TS2322: Type string is not assignable to type number.",
        "failed: [typecheck] (1 of 2 tasks), <seconds>",
        `run: ${runDirectory.path}/`,
      ];
      expectOneOfTheOrders(
        [
          ...printed.slice(0, COMMAND_LINES_START),
          ...commandLines.toSorted(),
          ...printed.slice(COMMAND_LINES_END),
        ],
        [
          [...beforeTheEnds, ...typecheckEnd, ...mainEnd, ...afterTheEnds],
          [...beforeTheEnds, ...mainEnd, ...typecheckEnd, ...afterTheEnds],
        ],
      );
      expect(runDirectory.fileNames).toEqual(["1.output", "2.output", "trace.jsonl"]);
      const file = withSecondsNamed(
        withoutStackFrames(yield* read(`${runDirectory.path}/2.output`)),
      );
      expect(file).toStartWith(
        "started: [typecheck], <seconds>, 2.output\n[typecheck] checking 2 files\n",
      );
      expect(
        file
          .split("\n")
          .filter((line) => line.startsWith("[typecheck] "))
          .toSorted(),
      ).toEqual([
        "[typecheck] Found 1 error.",
        "[typecheck] checking 2 files",
        "[typecheck] src/a.ts(1,7): error TS2322: Type string is not assignable to type number.",
      ]);
      const errorLines = [
        "error: [typecheck] main.ts:5:15: CommandFailed: sh exited with code 2",
        `error: [typecheck] its output is in ${runDirectory.path}/2.output`,
      ];
      expect(file.split("\n").filter((line) => line.startsWith("error: "))).toEqual(errorLines);
      expect(file).toEndWith(`\n${errorLines.join("\n")}\nended: [typecheck], <seconds>, failed\n`);
    }),
  ),
);

export const awaitingTaskOutputFile = makeTest(
  "a task that failed only because a task it awaited failed gets an output file with only its started and ended lines, and the run line names the run folder",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject(FAILING_TYPECHECK_PROJECT);
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(countOccurrences(ran.standardError, "\nrun: ")).toBe(1);
      const runDirectory = yield* readRunDirectory(ran.standardError);
      expect(runDirectory.fileNames).toEqual(["1.output", "2.output", "trace.jsonl"]);
      expect(ran.standardError.trimEnd().split("\n").at(-1)).toBe(`run: ${runDirectory.path}/`);
      expect(withSecondsNamed(yield* read(`${runDirectory.path}/1.output`))).toBe(
        "started: [main], <seconds>, 1.output\nended: [main], <seconds>, failed\n",
      );
      expect(yield* readOutputFileNamesOfTasks(project, ran)).toEqual({
        typecheck: "2.output",
        main: "1.output",
      });
    }),
  ),
);

export const errorNamingAnotherTaskOutputFile = makeTest(
  "a failed task whose error names another task in its taskName field is pointed at by its own output file, in the terminal and in its recap",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskWaitsOnItself } from "@sshkeda/task/errors";
export const dep = Task.make({ name: "dep", run: Effect.log("dep ran") });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* dep;
  return yield* new TaskWaitsOnItself({ taskName: "dep", cycle: ["dep", "dep"] });
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      const runDirectory = yield* readRunDirectory(ran.standardError);
      const pointer = `error: [main] its output is in ${runDirectory.path}/1.output`;
      expect(ran.standardError).toContain(`\n${pointer}\n`);
      expect(ran.standardError).not.toContain("/2.output");
      expect(ran.standardError).toContain(
        "\nrecap: [main] 1.output | main.ts:4:25: TaskWaitsOnItself: waits on itself through [dep] -> [dep]\n",
      );
      const mainFile = yield* read(`${runDirectory.path}/1.output`);
      expect(mainFile).toContain("\nerror: [main] stack frames of TaskWaitsOnItself: ");
      expect(mainFile).toContain(`\n${pointer}\n`);
      expect(yield* read(`${runDirectory.path}/2.output`)).not.toContain("its output is in");
    }),
  ),
);

export const servedTaskPrintingLate = makeTest(
  "a served task's output file holds its started and serving lines, the lines it prints while the run cleans it up and its closed line, and its span names it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const up = Task.make({ name: "up", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("up stopped"));
  return yield* Task.serve("ready");
}) });
export default Task.make({ name: "main", run: Effect.asVoid(up) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const runDirectory = yield* readRunDirectory(ran.standardError);
      expect(runDirectory.fileNames).toEqual(["1.output", "2.output", "trace.jsonl"]);
      expect(withSecondsNamed(yield* read(`${runDirectory.path}/2.output`))).toBe(
        "started: [up], <seconds>, 2.output\nserving: [up], <seconds>\n[up] up stopped\nclosed: [up]\n",
      );
      expect(yield* readOutputFileNamesOfTasks(project, ran)).toEqual({
        up: "2.output",
        main: "1.output",
      });
    }),
  ),
);

export const stoppedTaskPrintingInCleanup = makeTest(
  "a task stopped by Ctrl-C keeps its started line and the lines it prints in its cleanup in its output file, then its ended line, and its span names it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const hang = Task.make({ name: "hang", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("hang stopped"));
  const started = new BroadcastChannel("started");
  started.postMessage("hang");
  started.close();
  return yield* Effect.never;
}) });
const hangStarted = Effect.callback<void>((resume) => {
  const started = new BroadcastChannel("started");
  started.onmessage = () => {
    started.close();
    resume(Effect.void);
  };
  return Effect.sync(() => started.close());
});
export default Task.make({ name: "main", run: Effect.all([hang, Effect.andThen(hangStarted, Effect.log("hang started"))], { concurrency: "unbounded", discard: true }) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[main] hang started");
      yield* command.sendSignal("INT");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode, exited.standardError).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(exited.standardError).toContain("\n[hang] hang stopped\n");
      const runDirectory = yield* readRunDirectory(exited.standardError);
      expect(runDirectory.fileNames).toEqual(["1.output", "2.output", "trace.jsonl"]);
      expect(withSecondsNamed(yield* read(`${runDirectory.path}/2.output`))).toBe(
        "started: [hang], <seconds>, 2.output\n[hang] hang stopped\nended: [hang], <seconds>, stopped\n",
      );
      expect(yield* readOutputFileNamesOfTasks(project, exited)).toEqual({
        main: "1.output",
        hang: "2.output",
      });
    }),
  ),
);

export const longTaskNameOutputFile = makeTest(
  "a task with a long name holding spaces, slashes and quotes gets an output file named by its number",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export default Task.make({ name: ${yield* encodeJson(LONG_TASK_NAME)}, run: Effect.log("ran").pipe(Effect.andThen(Effect.fail(new TaskError({ message: "broke" })))) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      const runDirectory = yield* readRunDirectory(ran.standardError);
      expect(runDirectory.fileNames).toEqual(["1.output", "trace.jsonl"]);
      const [runLine, taskLine, errorLine, pointerLine, endedLine, ...endOfTheRun] =
        ran.standardError.trimEnd().split("\n");
      expect(runLine).toBe(`run: ${runDirectory.path}/`);
      const fileText = yield* read(`${runDirectory.path}/1.output`);
      const startedLine = fileText.split("\n")[0] ?? "";
      expect(withSecondsNamed(startedLine)).toBe(
        `started: [${LONG_TASK_NAME}], <seconds>, 1.output`,
      );
      expect(taskLine).toBe(`[${LONG_TASK_NAME}] ran`);
      expect(errorLine).toStartWith(`error: [${LONG_TASK_NAME}] main.ts:`);
      expect(errorLine).toMatch(/ main\.ts:\d+:\d+: TaskError: broke$/u);
      expect(pointerLine).toBe(
        `error: [${LONG_TASK_NAME}] its output is in ${runDirectory.path}/1.output`,
      );
      expect(withSecondsNamed(endedLine ?? "")).toBe(
        `ended: [${LONG_TASK_NAME}], <seconds>, failed`,
      );
      expect(withSecondsNamed(endOfTheRun.join("\n"))).toBe(
        [
          (errorLine ?? "").replace(
            `error: [${LONG_TASK_NAME}] `,
            `recap: [${LONG_TASK_NAME}] 1.output | `,
          ),
          `failed: [${LONG_TASK_NAME}] (1 of 1 task), <seconds>`,
          `run: ${runDirectory.path}/`,
        ].join("\n"),
      );
      expect(withoutStackFrames(fileText)).toBe(
        `${startedLine}\n${taskLine}\n${errorLine}\n${pointerLine}\n${endedLine}\n`,
      );
    }),
  ),
);

export const unwritableOutputFile = makeTest(
  "an output file that cannot be written does not change the exit code, is reported once, and the run line still names the run folder",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { chmodSync, readdirSync, writeFileSync } from "node:fs";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
const runs = \`\${process.env.HOME}/.sshkeda-task/runs/project\`;
export default Task.make({ name: "main", run: Effect.gen(function* () {
  for (const runDirectoryName of readdirSync(runs)) {
    writeFileSync(\`\${runs}/\${runDirectoryName}/1.output\`, "");
    chmodSync(\`\${runs}/\${runDirectoryName}/1.output\`, 0o400);
  }
  yield* Effect.log("made its output file read-only");
  return yield* Effect.fail(new TaskError({ message: "broke" }));
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(ran.standardError).toContain("[main] made its output file read-only");
      expect(ran.standardError).toMatch(
        /^error: \S+: Error: EACCES: permission denied, open '\S+\/1\.output' \(EACCES\)\nerror: could not write \S+\/1\.output, so the task output files are incomplete$/mu,
      );
      expect(countOccurrences(ran.standardError, "the task output files are incomplete")).toBe(1);
      const runDirectory = yield* readRunDirectory(ran.standardError);
      expect(findRunLinePath(ran.standardError)).toBe(`${runDirectory.path}/`);
      expect(ran.standardError.trimEnd().split("\n").at(-1)).toBe(`run: ${runDirectory.path}/`);
      expect(ran.standardError).toMatch(/^error: \[main\] main\.ts:\d+:\d+: TaskError: broke$/mu);
    }),
  ),
);

export const unwritableOutputFileIsNotNamed = makeTest(
  "when its output file cannot be written, a failed command's error does not name the file, and its output still prints",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { chmodSync, readdirSync, writeFileSync } from "node:fs";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const runs = \`\${process.env.HOME}/.sshkeda-task/runs/project\`;
export default Task.make({ name: "main", run: Effect.gen(function* () {
  for (const runDirectoryName of readdirSync(runs)) {
    writeFileSync(\`\${runs}/\${runDirectoryName}/1.output\`, "");
    chmodSync(\`\${runs}/\${runDirectoryName}/1.output\`, 0o400);
  }
  yield* Task.exec("sh", ["-c", "echo line 01; echo line 02; exit 1"]);
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(ran.standardError).toMatch(
        /^\[main\] line 01\n\[main\] line 02\nerror: \[main\] main\.ts:10:15: CommandFailed: sh exited with code 1\nended: \[main\], /mu,
      );
      expect(ran.standardError).not.toContain("its output is in");
    }),
  ),
);

export const outputFileUnwritableMidCommand = makeTest(
  "when its output file stops being writable while a passing command runs, the command's output prints in order under the task's name, once each line",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.asVoid(Task.exec("sh", ["-c", 'echo early-1; sleep 0.3; chmod 400 "$HOME"/.sshkeda-task/runs/project/*/1.output; sleep 0.3; echo late-2'])) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("so the task output files are incomplete");
      expect(countOccurrences(ran.standardError, "[main] early-1\n")).toBe(1);
      expect(countOccurrences(ran.standardError, "[main] late-2\n")).toBe(1);
      expect(ran.standardError).toContain("[main] early-1\n[main] late-2\n");
    }),
  ),
);

export const commandOutputBatchIsOneAppend = Task.make({
  name: "a batch of command output pieces is written to the task's file in one append and offered to standard error together",
  isolated: true,
  timeout: "1 minute",
  cleanup: "5 seconds",
  run: Effect.gen(function* () {
    const appendedTexts: string[] = [];
    const lines = yield* Queue.unbounded<ReturnType<typeof makeStandardErrorLine>, Cause.Done>();
    yield* Deferred.succeed(standardErrorLinesDeferred, lines);
    const taskOutputFiles = yield* makeTaskOutputFiles({
      runDirectoryPath: "/run",
      isRunDirectoryWritable: true,
      hasStopSignal: Effect.succeed(false),
    }).pipe(
      Effect.provideService(
        FileSystem.FileSystem,
        FileSystem.makeNoop({
          writeFile: (_filePath, data) =>
            Effect.sync(() => {
              appendedTexts.push(new TextDecoder().decode(data));
            }),
        }),
      ),
      Effect.provideService(Path.Path, yield* Path.Path.pipe(Effect.provide(Path.layer))),
    );
    const pieces = [
      { text: "one", continuesLine: false, endsLine: true },
      { text: "two", continuesLine: false, endsLine: true },
      { text: "thr", continuesLine: false, endsLine: false },
      { text: "ee", continuesLine: true, endsLine: true },
    ];
    yield* taskOutputFiles.printStreamOutput("build", 1, pieces, Effect.void, "terminal and file");
    expect(appendedTexts).toEqual(["[build] one\n[build] two\n[build] three\n"]);
    expect(yield* Queue.size(lines)).toBe(pieces.length);
  }),
});
