import { expect } from "bun:test";

import { Effect, FileSystem } from "effect";

import { edit, read, write } from "../shared/access-files.ts";
import { EXIT_CODE_AFTER_SIGINT } from "../shared/exit-codes-after-stop-signals.ts";
import { expectOneOfTheOrders } from "../shared/expect-one-of-the-orders.ts";
import { REPOSITORY_DIRECTORY } from "../shared/find-repository-directory.ts";
import { makeProject, makeProjectUsingTaskFrom } from "../shared/make-projects.ts";
import { makeScratchDirectory } from "../shared/make-temporary-directories.ts";
import { makeTest } from "../shared/make-tests.ts";
import { findRunDirectoryPath, withSecondsNamed } from "../shared/name-output-files.ts";
import { readAttribute, readProjectRunsDirectory, readTraces } from "../shared/read-trace-files.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";
import { linesBetweenTheRunLines } from "../shared/split-run-lines.ts";
import { startTaskProcessWithPipes } from "../shared/start-task-process-with-pipes.ts";
import { withoutStackFrames } from "../shared/without-stack-frames.ts";

const COMMAND_EXIT_CODE = 3;

const ERROR_LOG_LEVEL = { SSHKEDA_TASK_LOG: "error" } as const;

const DEBUG_LOG_LEVEL = { SSHKEDA_TASK_LOG: "debug" } as const;

const STARTED_LINE_FILE_NAME = /^started: .*, (?<fileName>\d+\.output)$/u;

const PARALLEL_TASK_COUNT = 6;

const STARTED_OR_ENDED_LINE = /^(?:started|ended): /u;

const TASK_NAME_AFTER_LABEL = /^(?<label>started|ended|closed): \[(?<name>[^\]]+)\]/u;

const linesOf = (standardError: string) => withSecondsNamed(standardError).split("\n");

const startedAndEndedLinesOf = (standardError: string) =>
  linesOf(standardError).filter((line) => STARTED_OR_ENDED_LINE.test(line));

const namesWithLabels = (standardError: string, labels: readonly string[]) =>
  standardError
    .split("\n")
    .map((line) => TASK_NAME_AFTER_LABEL.exec(line)?.groups)
    .filter((groups) => groups !== undefined && labels.includes(groups["label"] ?? ""))
    .map((groups) => groups?.["name"] ?? "")
    .toSorted();

const expectEveryStartToEnd = (standardError: string) => {
  expect(namesWithLabels(standardError, ["ended", "closed"]), standardError).toEqual(
    namesWithLabels(standardError, ["started"]),
  );
};

const ENDED_LINE = /^ended: \[(?<name>[^\]]+)\], \d+\.\d+s, (?<state>\w+)$/u;

const expectEachEndedLineToSayItsTaskState = (
  project: Effect.Success<ReturnType<typeof makeProject>>,
  ran: { readonly standardError: string; readonly exitCode: number },
) =>
  Effect.gen(function* () {
    const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
    const endedLines = ran.standardError.split("\n").flatMap((line) => {
      const groups = ENDED_LINE.exec(line)?.groups;
      return groups === undefined ? [] : [groups];
    });
    expect(endedLines, ran.standardError).not.toEqual([]);
    for (const { name = "", state } of endedLines) {
      const spanState = readAttribute(
        trace?.findTaskSpan("task", name)?.attributes ?? [],
        "sshkeda_task.state",
      );
      expect(state, name).toBe(spanState === "cancelled" ? "stopped" : String(spanState));
    }
  });

const indexOfLine = (standardError: string, wanted: string) => {
  const index = linesOf(standardError).indexOf(wanted);
  expect(index, standardError).toBeGreaterThan(-1);
  return index;
};

const lastIndexOfLineStarting = (standardError: string, prefix: string) => {
  const index = linesOf(standardError).findLastIndex((line) => line.startsWith(prefix));
  expect(index, standardError).toBeGreaterThan(-1);
  return index;
};

export const startedBeforeAndEndedAfterTheTaskLines = makeTest(
  "with SSHKEDA_TASK_LOG=debug, a task's started line prints before its first line and its ended line after its last, each with the seconds since the run began, and both open and close its file",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const build = Task.make({ name: "build", run: Effect.gen(function* () {
  yield* Effect.log("first");
  yield* Task.exec("sh", ["-c", "echo compiled"]);
  yield* Effect.log("last");
}) });
export default Task.make({ name: "main", run: build });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", DEBUG_LOG_LEVEL);
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(startedAndEndedLinesOf(ran.standardError)).toEqual([
        "started: [main], <seconds>, 1.output",
        "started: [build], <seconds>, 2.output",
        "ended: [build], <seconds>, passed",
        "ended: [main], <seconds>, passed",
      ]);
      expect(indexOfLine(ran.standardError, "started: [build], <seconds>, 2.output")).toBeLessThan(
        indexOfLine(ran.standardError, "[build] first"),
      );
      expect(indexOfLine(ran.standardError, "[build] last")).toBeLessThan(
        indexOfLine(ran.standardError, "ended: [build], <seconds>, passed"),
      );
      const runDirectoryPath = findRunDirectoryPath(ran.standardError);
      expect(withSecondsNamed(yield* read(`${runDirectoryPath}/2.output`))).toBe(
        [
          "started: [build], <seconds>, 2.output",
          "[build] first",
          "[build] compiled",
          "[build] last",
          "ended: [build], <seconds>, passed",
          "",
        ].join("\n"),
      );
    }),
  ),
);

export const everyStartedTaskEnds = makeTest(
  "with SSHKEDA_TASK_LOG=debug, when one of two tasks fails, each of the three tasks prints one ended line, the failed task's after its error and the awaiting task's after that error, all before the summary",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const lint = Task.make({ name: "lint", run: Effect.void });
const e2e = Task.make({ name: "e2e", run: Task.exec("cat", ["release-e2e"]).pipe(Effect.andThen(Effect.fail(new Error("expected 200, got 500")))) });
export default Task.make({ name: "ci", run: Task.allSettled([lint, e2e]) });`,
      });
      const started = yield* startTaskProcessWithPipes(project, ["release-e2e"], DEBUG_LOG_LEVEL);
      yield* started.releaseAfter(["ended: [lint]"], "release-e2e");
      const ran = yield* started.waitForExit;
      expect(ran.exitCode, ran.standardError).toBe(1);
      const beforeTheEnds = [
        "started: [ci], <seconds>, 1.output",
        "started: [lint], <seconds>, 2.output",
        "started: [e2e], <seconds>, 3.output",
        "ended: [lint], <seconds>, passed",
        "error: [e2e] main.ts:4:113: Error: expected 200, got 500",
      ];
      const e2eEnd = [
        "error: [e2e] its output is in <run folder>/3.output",
        "ended: [e2e], <seconds>, failed",
      ];
      const ciEnd = ["ended: [ci], <seconds>, failed"];
      const afterTheEnds = [
        "recap: [e2e] 3.output | main.ts:4:113: Error: expected 200, got 500",
        "failed: [e2e] (1 of 3 tasks), <seconds>",
      ];
      expectOneOfTheOrders(linesBetweenTheRunLines(ran.standardError), [
        [...beforeTheEnds, ...e2eEnd, ...ciEnd, ...afterTheEnds],
        [...beforeTheEnds, ...ciEnd, ...e2eEnd, ...afterTheEnds],
      ]);
      expectEveryStartToEnd(ran.standardError);
      yield* expectEachEndedLineToSayItsTaskState(project, ran);
    }),
  ),
);

export const failedTaskEndsAfterItsLastErrorLine = makeTest(
  "a task whose command fails prints its ended line right after the last line of its error, the one naming its file",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "echo first; echo second; exit ${COMMAND_EXIT_CODE}"]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(COMMAND_EXIT_CODE);
      const endedIndex = indexOfLine(ran.standardError, "ended: [main], <seconds>, failed");
      const lines = linesOf(ran.standardError);
      expect(lastIndexOfLineStarting(ran.standardError, "error: [main] ")).toBe(endedIndex - 1);
      expect(lines[endedIndex - 1]).toBe(
        `error: [main] its output is in ${findRunDirectoryPath(ran.standardError)}/1.output`,
      );
      expect(lines[endedIndex + 1]).toBe(
        `recap: [main] 1.output | main.ts:2:52: CommandFailed: sh exited with code ${COMMAND_EXIT_CODE}`,
      );
      const recapOutputLines = ["recap: [main] first", "recap: [main] second"];
      const recapOutputStart = endedIndex + 1 + 1;
      const summaryIndex = recapOutputStart + recapOutputLines.length;
      expect(lines.slice(recapOutputStart, summaryIndex)).toEqual(recapOutputLines);
      expect(lines[summaryIndex]).toStartWith("failed: [main] ");
      yield* expectEachEndedLineToSayItsTaskState(project, ran);
    }),
  ),
);

export const failedCleanupEndsAfterItsErrorLines = makeTest(
  "a task whose cleanup fails prints its ended line after the cleanup's error lines",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "db", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.die(new Error("could not drop the test database")));
  yield* Effect.log("seeded");
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(linesBetweenTheRunLines(ran.standardError)).toEqual([
        "[db] seeded",
        "error: [db] main.ts:4:51: Error: could not drop the test database",
        "error: [db] its cleanup failed",
        "error: [db] its output is in <run folder>/1.output",
        "ended: [db], <seconds>, failed",
        "recap: [db] 1.output | main.ts:4:51: Error: could not drop the test database",
        "failed: [db] (1 of 1 task), <seconds>",
      ]);
      yield* expectEachEndedLineToSayItsTaskState(project, ran);
    }),
  ),
);

export const timedOutTaskEnds = makeTest(
  "a task that times out prints its ended line after its timeout error",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "slow", timeout: "300 millis", run: Effect.never });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(linesBetweenTheRunLines(ran.standardError)).toEqual([
        "error: [slow] main.ts:3:21: TaskTimedOut: timed out after 300ms",
        "error: [slow] no command was running",
        "error: [slow] its output is in <run folder>/1.output",
        "ended: [slow], <seconds>, failed",
        "recap: [slow] 1.output | main.ts:3:21: TaskTimedOut: timed out after 300ms",
        "recap: [slow] no command was running",
        "failed: [slow] (1 of 1 task), <seconds>",
      ]);
      yield* expectEachEndedLineToSayItsTaskState(project, ran);
    }),
  ),
);

export const timedOutTaskNamesItsRunningCommands = makeTest(
  "a task that times out names each command still running, where it was called and for how long, in its error and its recap",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "migrate", timeout: "1 second", run: Effect.gen(function* () {
  yield* Effect.forkChild(Task.exec("sleep", ["30"]));
  yield* Effect.sleep("300 millis");
  yield* Task.exec("sh", ["-c", "echo applying 002_users.sql; sleep 30"]);
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(linesBetweenTheRunLines(ran.standardError)).toEqual([
        "[migrate] applying 002_users.sql",
        "error: [migrate] main.ts:3:21: TaskTimedOut: timed out after 1s",
        "error: [migrate] sleep was still running, called at main.ts:4:32, for <seconds>",
        "error: [migrate] sh was still running, called at main.ts:6:15, for <seconds>",
        "error: [migrate] its output is in <run folder>/1.output",
        "ended: [migrate], <seconds>, failed",
        "recap: [migrate] 1.output | main.ts:3:21: TaskTimedOut: timed out after 1s",
        "recap: [migrate] sleep was still running, called at main.ts:4:32, for <seconds>",
        "recap: [migrate] sh was still running, called at main.ts:6:15, for <seconds>",
        "failed: [migrate] (1 of 1 task), <seconds>",
      ]);
    }),
  ),
);

export const timedOutTaskWithOnlyEndedCommands = makeTest(
  "a task that times out after its commands have ended says that no command was running",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "e2e", timeout: "1 second", run: Effect.gen(function* () {
  yield* Task.exec("sh", ["-c", "echo build done"]);
  yield* Effect.never;
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(linesBetweenTheRunLines(ran.standardError)).toContain(
        "error: [e2e] no command was running",
      );
      expect(linesBetweenTheRunLines(ran.standardError)).toContain(
        "recap: [e2e] no command was running",
      );
    }),
  ),
);

export const servingTaskGetsNoEndedLine = makeTest(
  "with SSHKEDA_TASK_LOG=debug, a task that serves prints its started, serving and closed lines in that order and no ended line, while the task that used it ends",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
const web = Task.make({ name: "web", output: Schema.String, run: Task.serve("http://web.test/") });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.log(\`using \${yield* web}\`);
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", DEBUG_LOG_LEVEL);
      expect(ran.exitCode, ran.standardError).toBe(0);
      const webLines = linesOf(ran.standardError).filter((line) => line.includes(": [web]"));
      expect(webLines).toEqual([
        "started: [web], <seconds>, 2.output",
        "serving: [web], <seconds>",
        "closed: [web]",
      ]);
      expect(startedAndEndedLinesOf(ran.standardError)).toEqual([
        "started: [main], <seconds>, 1.output",
        "started: [web], <seconds>, 2.output",
        "ended: [main], <seconds>, passed",
      ]);
      expectEveryStartToEnd(ran.standardError);
      yield* expectEachEndedLineToSayItsTaskState(project, ran);
    }),
  ),
);

export const stoppedTasksEnd = makeTest(
  "Ctrl-C prints an ended line for each task it stopped and closed for a task that served, before the summary, and no ended line for a task that passed",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
const quick = Task.make({ name: "quick", run: Effect.void });
const web = Task.make({ name: "web", output: Schema.String, run: Task.serve("http://web.test/") });
const waiting = Task.make({ name: "waiting", run: Effect.never });
export default Task.make({ name: "dev", run: Effect.gen(function* () {
  yield* quick;
  yield* web;
  yield* Effect.log("waiting");
  return yield* waiting;
}) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[dev] waiting");
      yield* command.sendSignal("INT");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode, exited.standardError).toBe(EXIT_CODE_AFTER_SIGINT);
      const lines = linesOf(exited.standardError.trimEnd());
      expect(lines.filter((line) => line.startsWith("ended: ")).toSorted()).toEqual([
        "ended: [dev], <seconds>, stopped",
        "ended: [waiting], <seconds>, stopped",
      ]);
      expect(lines.filter((line) => line.startsWith("closed: "))).toEqual(["closed: [web]"]);
      const summaryIndex = lines.findIndex((line) => line.startsWith("stopped: "));
      expect(lines.slice(summaryIndex + 1).map((line) => line.split(" ")[0])).toEqual(["run:"]);
      expect(lastIndexOfLineStarting(exited.standardError, "ended: ")).toBeLessThan(summaryIndex);
      expect(lines.filter((line) => line.startsWith("started: "))).toEqual([]);
      yield* expectEachEndedLineToSayItsTaskState(project, exited);
    }),
  ),
);

const SECONDS_AT_THE_END = /, (?<seconds>\d+\.\d)s(?:, \d+\.output|, passed)?$/u;

const secondsOf = (line: string) => Number(SECONDS_AT_THE_END.exec(line)?.groups?.["seconds"]);

const STEP_COUNT = 8;

const LINES_PER_PASSED_TASK = 2;

export const startedAndEndedTimesNeverGoBack = makeTest(
  "with SSHKEDA_TASK_LOG=debug, the seconds on started and ended lines never go down from one line to the next and never pass the summary's",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const tasks = Array.from({ length: ${STEP_COUNT} }, (_, index) => Task.make({ name: \`step-\${index}\`, run: Effect.sleep(\`\${index * 30} millis\`) }));
export default Task.make({ name: "main", run: Task.all(tasks) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", DEBUG_LOG_LEVEL);
      expect(ran.exitCode, ran.standardError).toBe(0);
      const seconds = ran.standardError
        .split("\n")
        .filter((line) => STARTED_OR_ENDED_LINE.test(line))
        .map((line) => secondsOf(line));
      expect(seconds.length, ran.standardError).toBe((STEP_COUNT + 1) * LINES_PER_PASSED_TASK);
      expect(seconds, ran.standardError).toEqual(seconds.toSorted((left, right) => left - right));
      const summarySeconds = secondsOf(
        ran.standardError.split("\n").find((line) => line.startsWith("passed: ")) ?? "",
      );
      expect(Math.max(...seconds), ran.standardError).toBeLessThanOrEqual(summarySeconds);
      expectEveryStartToEnd(ran.standardError);
      yield* expectEachEndedLineToSayItsTaskState(project, ran);
    }),
  ),
);

export const errorLevelHidesStartedAndEnded = makeTest(
  "SSHKEDA_TASK_LOG=error hides started and ended lines on the terminal and still writes them to each task's file",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.fail(new Error("broke")) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", ERROR_LOG_LEVEL);
      expect(ran.exitCode, ran.standardError).toBe(1);
      const runDirectoryPath = findRunDirectoryPath(ran.standardError);
      expect(withSecondsNamed(ran.standardError.trimEnd())).toBe(
        [
          `run: ${runDirectoryPath}/`,
          "error: [main] main.ts:3:63: Error: broke",
          `error: [main] its output is in ${runDirectoryPath}/1.output`,
          "recap: [main] 1.output | main.ts:3:63: Error: broke",
          "failed: [main] (1 of 1 task), <seconds>",
          `run: ${runDirectoryPath}/`,
        ].join("\n"),
      );
      expect(
        withSecondsNamed(withoutStackFrames(yield* read(`${runDirectoryPath}/1.output`))),
      ).toBe(
        [
          "started: [main], <seconds>, 1.output",
          "error: [main] main.ts:3:63: Error: broke",
          `error: [main] its output is in ${runDirectoryPath}/1.output`,
          "ended: [main], <seconds>, failed",
          "",
        ].join("\n"),
      );
    }),
  ),
);

export const runLineOpensAndClosesTheRun = makeTest(
  "a run's first line and last line are the same run line, and with SSHKEDA_TASK_LOG=debug each started line names a different file in the run folder whose first line is that started line",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const tasks = Array.from({ length: ${PARALLEL_TASK_COUNT} }, (_, index) => Task.make({ name: \`step-\${index}\`, run: Effect.log("ran") }));
export default Task.make({ name: "main", run: Task.all(tasks) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", DEBUG_LOG_LEVEL);
      expect(ran.exitCode, ran.standardError).toBe(0);
      const runDirectoryPath = findRunDirectoryPath(ran.standardError);
      const lines = ran.standardError.trimEnd().split("\n");
      expect(lines[0]).toBe(`run: ${runDirectoryPath}/`);
      expect(lines.at(-1)).toBe(lines[0]);
      const startedLines = lines.filter((line) => line.startsWith("started: "));
      expect(startedLines).toHaveLength(PARALLEL_TASK_COUNT + 1);
      const fileNames = startedLines.map(
        (line) => STARTED_LINE_FILE_NAME.exec(line)?.groups?.["fileName"] ?? "",
      );
      expect(new Set(fileNames).size, ran.standardError).toBe(startedLines.length);
      for (const [index, fileName] of fileNames.entries()) {
        const file = yield* read(`${runDirectoryPath}/${fileName}`);
        expect(file.split("\n")[0]).toBe(startedLines[index]);
      }
    }),
  ),
);

const HELD_BACK_WRITE = "200 millis";

export const runLineComesBeforeWhatTheTaskFileWritesAsItLoads = makeTest(
  "the run line is written before the run process imports the task file, even when each write to standard error is held back, so lines the task file writes straight to standard error as it loads there come after it",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const repository = yield* REPOSITORY_DIRECTORY;
      const engine = yield* makeScratchDirectory;
      yield* fileSystem.copy(`${repository}/src`, `${engine}/src`);
      yield* fileSystem.copyFile(`${repository}/package.json`, `${engine}/package.json`);
      yield* fileSystem.symlink(`${repository}/node_modules`, `${engine}/node_modules`);
      yield* edit(
        `${engine}/src/lib/task/internal/run-root-task.ts`,
        "Stream.run(stdio.stderr()),",
        `Stream.tap(() => Effect.sleep("${HELD_BACK_WRITE}")), Stream.run(stdio.stderr()),`,
      );
      const project = yield* makeProjectUsingTaskFrom({
        taskPackageDirectoryPath: engine,
        files: {
          "main.ts": `import { isMainThread } from "node:worker_threads";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
if (Bun.main !== import.meta.path && isMainThread) {
  console.error("loading in the run process");
  await Bun.write(Bun.stderr, "written to standard error in the run process\\n");
}
export default Task.make({ name: "main", run: Effect.void });`,
        },
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(linesBetweenTheRunLines(ran.standardError)).toEqual([
        "loading in the run process",
        "written to standard error in the run process",
        "passed: 1 task, <seconds>",
      ]);
    }),
  ),
);

export const commandOutputReachesTheFileAsItIsWritten = makeTest(
  "with SSHKEDA_TASK_LOG=debug, a running command's line is in its task's file, named on the started line, while the command still runs",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "echo first line; until [ -e go ]; do sleep 0.05; done; echo last line"], { print: true }) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts", DEBUG_LOG_LEVEL);
      const printed = yield* command.waitForStandardError("[main] first line\n");
      const lines = printed.split("\n");
      const runDirectoryPath = lines[0]?.replace(/^run: /u, "") ?? "";
      const fileName =
        STARTED_LINE_FILE_NAME.exec(lines.find((line) => line.startsWith("started: ")) ?? "")
          ?.groups?.["fileName"] ?? "";
      expect(yield* read(`${runDirectoryPath}${fileName}`)).toContain("\n[main] first line\n");
      yield* write(`${project.directory}/go`, "");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode, exited.standardError).toBe(0);
      expect(withSecondsNamed(yield* read(`${runDirectoryPath}${fileName}`))).toBe(
        [
          `started: [main], <seconds>, ${fileName}`,
          "[main] first line",
          "[main] last line",
          "ended: [main], <seconds>, passed",
          "",
        ].join("\n"),
      );
    }),
  ),
);

const SEQUENTIAL_PROJECT = `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const build = Task.make({ name: "build", run: Effect.log("built") });
export default Task.make({ name: "main", run: build });`;

export const passingRunPrintsNoProgressLines = makeTest(
  "a passing run prints no started or ended line on the terminal, and each task's file still opens with its started line and closes with its ended line",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": SEQUENTIAL_PROJECT });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const runDirectoryPath = findRunDirectoryPath(ran.standardError);
      expect(withSecondsNamed(ran.standardError.trimEnd())).toBe(
        [
          `run: ${runDirectoryPath}/`,
          "[build] built",
          "passed: 2 tasks, <seconds>",
          `run: ${runDirectoryPath}/`,
        ].join("\n"),
      );
      expect(withSecondsNamed(yield* read(`${runDirectoryPath}/1.output`))).toBe(
        ["started: [main], <seconds>, 1.output", "ended: [main], <seconds>, passed", ""].join("\n"),
      );
      expect(withSecondsNamed(yield* read(`${runDirectoryPath}/2.output`))).toBe(
        [
          "started: [build], <seconds>, 2.output",
          "[build] built",
          "ended: [build], <seconds>, passed",
          "",
        ].join("\n"),
      );
    }),
  ),
);

export const debugLevelPrintsProgressLines = makeTest(
  "SSHKEDA_TASK_LOG=debug prints every task's started and ended lines on the terminal, passed ones included",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": SEQUENTIAL_PROJECT });
      const ran = yield* runTaskProcess(project, "main.ts", DEBUG_LOG_LEVEL);
      expect(ran.exitCode, ran.standardError).toBe(0);
      const runDirectoryPath = findRunDirectoryPath(ran.standardError);
      expect(withSecondsNamed(ran.standardError.trimEnd())).toBe(
        [
          `run: ${runDirectoryPath}/`,
          "started: [main], <seconds>, 1.output",
          "started: [build], <seconds>, 2.output",
          "[build] built",
          "ended: [build], <seconds>, passed",
          "ended: [main], <seconds>, passed",
          "passed: 2 tasks, <seconds>",
          `run: ${runDirectoryPath}/`,
        ].join("\n"),
      );
    }),
  ),
);

export const onlyFailedTasksPrintEndedLines = makeTest(
  "when one of two tasks fails, the failed task and the task awaiting it print ended lines on the terminal, the passed task prints none, no task prints a started line, and each task's file holds its own started and ended lines",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const lint = Task.make({ name: "lint", run: Effect.void });
const e2e = Task.make({ name: "e2e", run: lint.pipe(Effect.andThen(Effect.fail(new Error("expected 200, got 500")))) });
export default Task.make({ name: "ci", run: Task.allSettled([lint, e2e]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      const beforeTheEnds = ["error: [e2e] main.ts:4:84: Error: expected 200, got 500"];
      const e2eEnd = [
        "error: [e2e] its output is in <run folder>/3.output",
        "ended: [e2e], <seconds>, failed",
      ];
      const ciEnd = ["ended: [ci], <seconds>, failed"];
      const afterTheEnds = [
        "recap: [e2e] 3.output | main.ts:4:84: Error: expected 200, got 500",
        "failed: [e2e] (1 of 3 tasks), <seconds>",
      ];
      expectOneOfTheOrders(linesBetweenTheRunLines(ran.standardError), [
        [...beforeTheEnds, ...e2eEnd, ...ciEnd, ...afterTheEnds],
        [...beforeTheEnds, ...ciEnd, ...e2eEnd, ...afterTheEnds],
      ]);
      yield* expectEachEndedLineToSayItsTaskState(project, ran);
      const runDirectoryPath = findRunDirectoryPath(ran.standardError);
      for (const [fileName, taskName, state] of [
        ["1.output", "ci", "failed"],
        ["2.output", "lint", "passed"],
        ["3.output", "e2e", "failed"],
      ] as const) {
        const fileLines = withSecondsNamed(yield* read(`${runDirectoryPath}/${fileName}`))
          .trimEnd()
          .split("\n");
        expect(fileLines[0]).toBe(`started: [${taskName}], <seconds>, ${fileName}`);
        expect(fileLines.at(-1)).toBe(`ended: [${taskName}], <seconds>, ${state}`);
      }
    }),
  ),
);

const HIDDEN_AT_THE_DEFAULT_LEVEL = /^(?:started: |ended: .*, passed$)/u;

export const hiddenLinesStillEndPartLines = makeTest(
  "a line task code left unfinished ends where a started or passed ended line would have ended it, so the terminal shows what SSHKEDA_TASK_LOG=debug shows without those lines",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const write = (text: string) => Effect.sync(() => { process.stdout.write(text); });
const lookup = Task.make({ name: "lookup", run: Effect.void });
const download = Task.make({ name: "download", run: write("no newline from task code") });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* write("asking ");
  yield* lookup;
  yield* write("answered\\n");
  yield* download;
  yield* Effect.log("after the download");
}) });`,
      });
      const atTheDefaultLevel = yield* runTaskProcess(project, "main.ts");
      expect(atTheDefaultLevel.exitCode, atTheDefaultLevel.standardError).toBe(0);
      const atTheDebugLevel = yield* runTaskProcess(project, "main.ts", DEBUG_LOG_LEVEL);
      expect(atTheDebugLevel.exitCode, atTheDebugLevel.standardError).toBe(0);
      const printed = linesBetweenTheRunLines(atTheDefaultLevel.standardError);
      expect(printed).toEqual([
        "output: asking ",
        "output: answered",
        "output: no newline from task code",
        "[main] after the download",
        "passed: 3 tasks, <seconds>",
      ]);
      expect(
        linesBetweenTheRunLines(atTheDebugLevel.standardError).filter(
          (line) => !HIDDEN_AT_THE_DEFAULT_LEVEL.test(line),
        ),
      ).toEqual(printed);
    }),
  ),
);
