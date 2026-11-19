import { expect } from "bun:test";

import { Duration, Effect, FileSystem, Queue, Stream } from "effect";
import type { Cause } from "effect";

import { decodeOutputText, splitOutputPieces } from "../../src/shared/label-output.ts";
import { read, write } from "../shared/access-files.ts";
import { countOccurrences } from "../shared/count-occurrences.ts";
import { EXIT_CODE_AFTER_SIGINT } from "../shared/exit-codes-after-stop-signals.ts";
import { expectOneOfTheOrders } from "../shared/expect-one-of-the-orders.ts";
import { findKilledProcesses } from "../shared/find-killed-processes.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { findRunDirectoryPath, withSecondsNamed } from "../shared/name-output-files.ts";
import { readAttribute, readProjectRunsDirectory, readTraces } from "../shared/read-trace-files.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";
import { separateEndedLines } from "../shared/separate-ended-lines.ts";
import { linesBetweenTheRunLines, withoutTheFirstRunLine } from "../shared/split-run-lines.ts";
import { withoutStackFrames } from "../shared/without-stack-frames.ts";

const TASK_LINE = /^\[(?<taskName>(?:[^\]\\]|\\.)+)\] /u;

const STARTED_FILE_NAME = /(?<=^started: .*), (?<fileName>\d+\.output)$/u;

const TASK_PRINTED_LINE =
  /^(?:started|ended|error|recap|warning|serving|closed|running|stopping|killed|passed|failed|stopped|run|output):(?: |$)/u;

const ERROR_LOG_LEVEL = { SSHKEDA_TASK_LOG: "error" } as const;

const SUMMARY_AND_RUN_LINE_COUNT = 2;

const ERROR_AND_CONTEXT_LINE_COUNT = 2;

const ERROR_CONTEXT_AND_OUTPUT_FILE_LINE_COUNT = 3;

const ERROR_AND_RECAP_LINES = 2;

const linesOf = (standardError: string) => standardError.split("\n");

const withRunDirectoryReplaced = (standardError: string) =>
  withSecondsNamed(standardError).replaceAll(
    `run: ${findRunDirectoryPath(standardError)}/`,
    "run: <run folder>/",
  );

const summaryAndRunLines = (standardError: string) =>
  linesOf(withRunDirectoryReplaced(standardError)).slice(-SUMMARY_AND_RUN_LINE_COUNT);

const EVERY_KIND_OF_LINE_PROJECT = {
  "main.ts": `import { Effect, Schema } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
const web = Task.make({ name: "web", output: Schema.String, run: Task.serve("http://web.test/") });
const leaver = Task.make({ name: "leaver", run: ChildProcess.make("sh", ["-c", "trap '' TERM; sleep 1000 > /dev/null 2>&1 & until ps -o command= -p $! | grep -qx 'sleep 1000'; do :; done; echo $! > leaver-pid"]).pipe(Effect.flatMap((child) => child.exitCode), Effect.asVoid) });
const typecheck = Task.make({ name: "typecheck", run: Effect.asVoid(Task.exec("sh", ["-c", "echo 'src/a.ts(1,7): error TS2322' >&2; exit 2"])) });
const lint = Task.make({ name: "lint", run: Effect.sync(() => { throw new Error("2 files are not formatted"); }) });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Task.allSettled([typecheck, lint, leaver]).pipe(Effect.ignore);
  const url = yield* web;
  yield* Effect.log(\`ready at \${url}\`);
  return yield* Effect.never;
}) });`,
} as const;

export const everyLineFollowsTheRule = makeTest(
  "in a run with errors, a serving task, a killed process and Ctrl-C at SSHKEDA_TASK_LOG=debug, every line that starts with [ is a task's own line and every other line is a lowercase label, a colon and a value",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject(EVERY_KIND_OF_LINE_PROJECT);
      const command = yield* startTaskProcess(project, "main.ts", { SSHKEDA_TASK_LOG: "debug" });
      yield* command.waitForLine("[main] ready at ");
      yield* command.sendSignal("INT");
      const ran = yield* command.waitForExit;
      expect(ran.exitCode, ran.standardError).toBe(EXIT_CODE_AFTER_SIGINT);
      const lines = linesOf(ran.standardError);
      const taskLines = lines.filter((line) => TASK_LINE.test(line));
      expect(taskLines.toSorted()).toEqual([
        "[main] ready at http://web.test/",
        "[typecheck] src/a.ts(1,7): error TS2322",
      ]);
      expect(
        lines.filter((line) => !TASK_LINE.test(line) && !TASK_PRINTED_LINE.test(line)),
      ).toEqual([]);
      expect(lines.filter((line) => line.includes("::"))).toEqual([]);
      const leaverProcessId = Number((yield* read(`${project.directory}/leaver-pid`)).trim());
      const runDirectoryPath = findRunDirectoryPath(ran.standardError);
      const printedLines = withRunDirectoryReplaced(ran.standardError).split("\n");
      const startedFileNames = printedLines.flatMap(
        (line) => STARTED_FILE_NAME.exec(line)?.groups?.["fileName"] ?? [],
      );
      expect(startedFileNames.toSorted(), ran.standardError).toEqual([
        "1.output",
        "2.output",
        "3.output",
        "4.output",
        "5.output",
      ]);
      const printedLinesWithoutFileNames = printedLines.map((line) =>
        line.replace(STARTED_FILE_NAME, ""),
      );
      const typecheckFileName = printedLines
        .find((line) => line.startsWith("started: [typecheck], "))
        ?.match(STARTED_FILE_NAME)?.groups?.["fileName"];
      for (const requiredLine of [
        "started: [main], <seconds>",
        "started: [leaver], <seconds>",
        "ended: [leaver], <seconds>, passed",
        "started: [web], <seconds>",
        "[typecheck] src/a.ts(1,7): error TS2322",
        "error: [typecheck] main.ts:6:74: CommandFailed: sh exited with code 2",
        `error: [typecheck] its output is in ${runDirectoryPath}/${typecheckFileName}`,
        "error: [lint] main.ts:7:75: Error: 2 files are not formatted",
        `killed: [leaver] ${leaverProcessId} (sleep)`,
        "serving: [web], <seconds>",
        "[main] ready at http://web.test/",
        "stopping: press ctrl-c again to force",
        "closed: [web]",
      ]) {
        expect(printedLinesWithoutFileNames, ran.standardError).toContain(requiredLine);
      }
      expect(
        printedLines.filter((line) => line.startsWith("ended: ")).toSorted(),
        ran.standardError,
      ).toEqual([
        "ended: [leaver], <seconds>, passed",
        "ended: [lint], <seconds>, failed",
        "ended: [main], <seconds>, stopped",
        "ended: [typecheck], <seconds>, failed",
      ]);
      expect(printedLines.slice(-SUMMARY_AND_RUN_LINE_COUNT)).toEqual([
        "failed: [typecheck] [lint] (2 of 5 tasks), 2 stopped, <seconds>",
        "run: <run folder>/",
      ]);
      expect(lines.at(-1)).toBe(`run: ${runDirectoryPath}/`);
      expect(findKilledProcesses(ran.standardError)).toEqual([
        { processId: leaverProcessId, command: "sleep", by: "[leaver]" },
      ]);
    }),
  ),
);

export const errorsPrintOnceWhenTheyHappen = makeTest(
  "a failure a task catches prints its error line once, when it happens, before the lines its task prints after it, and the run still passes",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const migrate = Task.make({ name: "migrate", run: Effect.sync(() => { throw new Error("the database is not reachable"); }) });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.log("before");
  yield* migrate.pipe(Effect.ignore);
  yield* Effect.log("after");
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const errorLine = "error: [migrate] main.ts:3:81: Error: the database is not reachable";
      expect(countOccurrences(ran.standardError, "the database is not reachable")).toBe(1);
      const runDirectoryPath = findRunDirectoryPath(ran.standardError);
      const beforeTheEnds = [`run: ${runDirectoryPath}/`, "[main] before", errorLine];
      const migrateEnd = [
        `error: [migrate] its output is in ${runDirectoryPath}/2.output`,
        "ended: [migrate], <seconds>, failed",
      ];
      const afterTheEnds = [
        "passed: 2 tasks, 1 caught failure, <seconds>",
        `run: ${runDirectoryPath}/`,
      ];
      const mainAfter = "[main] after";
      expectOneOfTheOrders(withSecondsNamed(ran.standardError).split("\n"), [
        [...beforeTheEnds, ...migrateEnd, mainAfter, ...afterTheEnds],
        [...beforeTheEnds, mainAfter, ...migrateEnd, ...afterTheEnds],
      ]);
    }),
  ),
);

const failingTasks = (taskNames: readonly string[]) => `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
${taskNames.map((taskName) => `const ${taskName} = Task.make({ name: "${taskName}", run: Effect.sync(() => { throw new Error("${taskName} broke"); }) });`).join("\n")}
export default Task.make({ name: "main", run: Task.allSettled([${taskNames.join(", ")}]) });`;

export const summaryNamesAtMostThreeTasks = makeTest(
  "the failed summary names each failed task in brackets in the order they were found, at most three of them, and counts the rest",
  Effect.scoped(
    Effect.gen(function* () {
      const threeFailed = yield* makeProject({
        "main.ts": failingTasks(["alpha", "beta", "gamma"]),
      });
      const fiveFailed = yield* makeProject({
        "main.ts": failingTasks(["alpha", "beta", "gamma", "delta", "epsilon"]),
      });
      const ranThree = yield* runTaskProcess(threeFailed, "main.ts");
      const ranFive = yield* runTaskProcess(fiveFailed, "main.ts");
      expect(ranThree.exitCode, ranThree.standardError).toBe(1);
      expect(ranFive.exitCode, ranFive.standardError).toBe(1);
      expect(summaryAndRunLines(ranThree.standardError)[0]).toBe(
        "failed: [alpha] [beta] [gamma] (3 of 4 tasks), <seconds>",
      );
      expect(summaryAndRunLines(ranFive.standardError)[0]).toBe(
        "failed: [alpha] [beta] [gamma] and 2 more (5 of 6 tasks), <seconds>",
      );
    }),
  ),
);

export const summaryOfOneTask = makeTest(
  "a run of one task ends with passed: 1 task, failed: [name] (1 of 1 task) or stopped: [name] (1 of 1 task), then the run line",
  Effect.scoped(
    Effect.gen(function* () {
      const passing = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.void });`,
      });
      const failing = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.sync(() => { throw new Error("broke"); }) });`,
      });
      const stopped = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "deploy", run: Effect.log("deploying").pipe(Effect.andThen(Effect.never)) });`,
      });
      const passed = yield* runTaskProcess(passing, "main.ts");
      const failed = yield* runTaskProcess(failing, "main.ts");
      const stopping = yield* startTaskProcess(stopped, "main.ts");
      yield* stopping.waitForLine("[deploy] deploying");
      yield* stopping.sendSignal("INT");
      const stoppedRun = yield* stopping.waitForExit;
      expect(passed.exitCode).toBe(0);
      expect(summaryAndRunLines(passed.standardError)).toEqual([
        "passed: 1 task, <seconds>",
        "run: <run folder>/",
      ]);
      expect(failed.exitCode).toBe(1);
      expect(summaryAndRunLines(failed.standardError)).toEqual([
        "failed: [main] (1 of 1 task), <seconds>",
        "run: <run folder>/",
      ]);
      expect(stoppedRun.exitCode).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(summaryAndRunLines(stoppedRun.standardError)).toEqual([
        "stopped: [deploy] (1 of 1 task), <seconds>",
        "run: <run folder>/",
      ]);
    }),
  ),
);

const PRINTING_PROJECT = {
  "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "hello", run: Effect.gen(function* () {
  yield* Effect.log("logged by the task");
  yield* Task.exec("echo", ["written by a command"], { print: true });
}) });`,
} as const;

export const linesRouteByWhereTheyComeFrom = makeTest(
  "a task's own logs and its command's output both print live and go to its file, and the error level drops both while keeping every line task writes",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject(PRINTING_PROJECT);
      const byDefault = yield* runTaskProcess(project, "main.ts");
      const atErrorLevel = yield* runTaskProcess(project, "main.ts", ERROR_LOG_LEVEL);
      expect(linesBetweenTheRunLines(byDefault.standardError)).toEqual([
        "[hello] logged by the task",
        "[hello] written by a command",
        "passed: 1 task, <seconds>",
      ]);
      expect(linesBetweenTheRunLines(atErrorLevel.standardError)).toEqual([
        "passed: 1 task, <seconds>",
      ]);
      for (const ran of [byDefault, atErrorLevel]) {
        const runDirectoryPath = findRunDirectoryPath(ran.standardError);
        expect(linesOf(ran.standardError).at(-1)).toBe(`run: ${runDirectoryPath}/`);
        expect(withSecondsNamed(yield* read(`${runDirectoryPath}/1.output`))).toBe(
          [
            "started: [hello], <seconds>, 1.output",
            "[hello] logged by the task",
            "[hello] written by a command",
            "ended: [hello], <seconds>, passed",
            "",
          ].join("\n"),
        );
      }
    }),
  ),
);

export const outputFileStartsWithItsTask = makeTest(
  "each task's file holds exactly its own lines in terminal format, starting with its started line, and the run folder holds no index file",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const build = Task.make({ name: "build", run: Effect.log("built") });
const test = Task.make({ name: "test", run: Effect.asVoid(Task.exec("sh", ["-c", "echo 1 failing >&2; exit 1"])) });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* build;
  yield* test.pipe(Effect.ignore);
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const runDirectoryPath = findRunDirectoryPath(ran.standardError) ?? "";
      const fileSystem = yield* FileSystem.FileSystem;
      expect((yield* fileSystem.readDirectory(runDirectoryPath)).toSorted()).toEqual([
        "1.output",
        "2.output",
        "3.output",
        "trace.jsonl",
      ]);
      expect(withSecondsNamed(yield* read(`${runDirectoryPath}/1.output`))).toBe(
        "started: [main], <seconds>, 1.output\nended: [main], <seconds>, passed\n",
      );
      expect(withSecondsNamed(yield* read(`${runDirectoryPath}/2.output`))).toBe(
        "started: [build], <seconds>, 2.output\n[build] built\nended: [build], <seconds>, passed\n",
      );
      expect(
        withSecondsNamed(withoutStackFrames(yield* read(`${runDirectoryPath}/3.output`))),
      ).toBe(
        [
          "started: [test], <seconds>, 3.output",
          "[test] 1 failing",
          "error: [test] main.ts:4:64: CommandFailed: sh exited with code 1",
          `error: [test] its output is in ${runDirectoryPath}/3.output`,
          "ended: [test], <seconds>, failed",
          "",
        ].join("\n"),
      );
    }),
  ),
);

export const commandOutputIgnoresTheTaskLogLevel = makeTest(
  "a command's output prints live and reaches its task's file, which its failure names, even when the task sets the minimum log level to Error and removes every logger",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Logger, References } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.asVoid(Task.exec("sh", ["-c", "echo LOST_OUTPUT; exit 1"], { print: true })).pipe(
  Effect.provideService(References.MinimumLogLevel, "Error"),
  Effect.provideService(Logger.CurrentLoggers, new Set()),
) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(linesBetweenTheRunLines(ran.standardError)).toEqual([
        "[main] LOST_OUTPUT",
        "error: [main] main.ts:3:66: CommandFailed: sh exited with code 1",
        "error: [main] its output is in <run folder>/1.output",
        "ended: [main], <seconds>, failed",
        "recap: [main] 1.output | main.ts:3:66: CommandFailed: sh exited with code 1",
        "recap: [main] LOST_OUTPUT",
        "failed: [main] (1 of 1 task), <seconds>",
      ]);
      expect(
        withSecondsNamed(yield* read(`${findRunDirectoryPath(ran.standardError)}/1.output`)),
      ).toStartWith("started: [main], <seconds>, 1.output\n[main] LOST_OUTPUT\n");
    }),
  ),
);

export const servingFailurePrintsWhenItHappens = makeTest(
  "a serving task whose body fails prints its error when it fails, before the lines of its cleanup and its closed line",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
const web = Task.make({ name: "web", output: Schema.String, run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("cleaning up").pipe(Effect.andThen(Effect.sleep("1 second"))));
  return yield* Effect.raceFirst(Task.serve("http://web.test/"), Effect.sleep("300 millis").pipe(Effect.andThen(Effect.fail(new Error("SERVER FAILED")))));
}) });
export default Task.make({ name: "main", run: Effect.andThen(web, Effect.never) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      const lines = linesBetweenTheRunLines(ran.standardError);
      const errorLine = lines.findIndex((line) => line.includes("SERVER FAILED"));
      expect(lines.slice(errorLine, errorLine + ERROR_AND_CONTEXT_LINE_COUNT)).toEqual([
        "error: [web] main.ts:5:129: Error: SERVER FAILED",
        "error: [web] its body failed after serving",
      ]);
      expect(errorLine).toBeLessThan(lines.indexOf("[web] cleaning up"));
      expect(lines.indexOf("[web] cleaning up")).toBeLessThan(lines.indexOf("closed: [web]"));
      expect(countOccurrences(ran.standardError, "SERVER FAILED")).toBe(ERROR_AND_RECAP_LINES);
      expect(lines.slice(0, -1).at(-1)).toBe(
        "recap: [web] 2.output | main.ts:5:129: Error: SERVER FAILED",
      );
    }),
  ),
);

export const everyServingFailurePrints = makeTest(
  "when two serving tasks fail, each prints its error once when it fails, the end of the run recaps each, and the summary counts both as failed",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
const a = Task.make({ name: "a", output: Schema.String, run: Effect.raceFirst(Task.serve("a"), Effect.sleep("300 millis").pipe(Effect.andThen(Effect.fail(new Error("FIRST FAILURE"))))) });
const b = Task.make({ name: "b", output: Schema.String, run: Task.serve("b").pipe(Effect.onInterrupt(() => Effect.die(new Error("SECOND FAILURE")))) });
export default Task.make({ name: "main", run: Effect.andThen(b, Effect.andThen(a, Effect.never)) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      const lines = linesBetweenTheRunLines(ran.standardError);
      const firstFailure = lines.findIndex((line) => line.endsWith(": Error: FIRST FAILURE"));
      const secondFailure = lines.findIndex((line) => line.endsWith(": Error: SECOND FAILURE"));
      expect(lines[firstFailure]).toStartWith("error: [a] main.ts:");
      expect(lines[firstFailure + 1]).toBe("error: [a] its body failed after serving");
      expect(lines[secondFailure]).toStartWith("error: [b] main.ts:");
      expect(lines[secondFailure + 1]).toBe("error: [b] its body failed after serving");
      expect(firstFailure).toBeLessThan(lines.indexOf("closed: [a]"));
      expect(secondFailure).toBeLessThan(lines.indexOf("closed: [b]"));
      expect(countOccurrences(ran.standardError, "FIRST FAILURE")).toBe(ERROR_AND_RECAP_LINES);
      expect(countOccurrences(ran.standardError, "SECOND FAILURE")).toBe(ERROR_AND_RECAP_LINES);
      const recapLines = [
        (lines[firstFailure] ?? "").replace(/^error: \[a\] /u, "recap: [a] 3.output | "),
        (lines[secondFailure] ?? "").replace(/^error: \[b\] /u, "recap: [b] 2.output | "),
      ];
      expect(lines.slice(-recapLines.length - 1, -1)).toEqual(recapLines);
      expect(lines.at(-1)).toBe("failed: [a] [b] (2 of 3 tasks), 1 stopped, <seconds>");
    }),
  ),
);

const DEV_SERVER = `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
const web = Task.make({ name: "web", output: Schema.String, run: Task.serve("http://localhost:3000") });`;

export const servingTaskStoppedByCtrlC = makeTest(
  "Ctrl-C while a task serves counts the serving task as stopped, and its trace says so",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `${DEV_SERVER}
export default Task.make({ name: "dev", run: Effect.gen(function* () {
  yield* Effect.log(\`web is at \${yield* web}\`);
  yield* Effect.never;
}) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("[dev] web is at http://localhost:3000");
      yield* command.sendSignal("INT");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode, exited.standardError).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(linesOf(withRunDirectoryReplaced(exited.standardError))).toEqual([
        "run: <run folder>/",
        "serving: [web], <seconds>",
        "[dev] web is at http://localhost:3000",
        "stopping: press ctrl-c again to force",
        "ended: [dev], <seconds>, stopped",
        "closed: [web]",
        "stopped: [web] [dev] (2 of 2 tasks), <seconds>",
        "run: <run folder>/",
      ]);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, exited));
      for (const taskName of ["web", "dev"]) {
        expect(
          readAttribute(
            trace?.findTaskSpan("task", taskName)?.attributes ?? [],
            "sshkeda_task.state",
          ),
          taskName,
        ).toBe("cancelled");
      }
    }),
  ),
);

export const servingTaskPassesWithoutAStop = makeTest(
  "a task that serves until a run ends without a stop counts as passed",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `${DEV_SERVER}
export default Task.make({ name: "dev", run: Effect.gen(function* () {
  yield* Effect.log(\`web is at \${yield* web}\`);
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(linesOf(withRunDirectoryReplaced(ran.standardError))).toEqual([
        "run: <run folder>/",
        "serving: [web], <seconds>",
        "[dev] web is at http://localhost:3000",
        "closed: [web]",
        "passed: 2 tasks, <seconds>",
        "run: <run folder>/",
      ]);
    }),
  ),
);

export const finalizerErrorWhoseMessageChanges = makeTest(
  "a finalizer's error whose message a later finalizer changes prints once",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const thrown = new Error("ORIGINAL MESSAGE");
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.sync(() => { thrown.message = "CHANGED MESSAGE"; }));
  yield* Effect.addFinalizer(() => Effect.die(thrown));
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      const errorLines = linesOf(ran.standardError).filter((line) => line.startsWith("error: "));
      expect(errorLines).toHaveLength(ERROR_CONTEXT_AND_OUTPUT_FILE_LINE_COUNT);
      expect(linesOf(ran.standardError).filter((line) => line.startsWith("recap: "))).toEqual([
        (errorLines[0] ?? "").replace(/^error: \[main\] /u, "recap: [main] 1.output | "),
      ]);
      expect(errorLines.join("\n")).toMatch(
        /: Error: (?:ORIGINAL|CHANGED) MESSAGE\nerror: \[main\] its cleanup failed\nerror: \[main\] its output is in \S+\/1\.output$/u,
      );
    }),
  ),
);

export const failedSummaryAfterDiscoveryFails = makeTest(
  "a run whose task file throws while the run process imports it prints the import error, a failed summary of no tasks and the run line",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
if (Bun.main !== import.meta.path) throw new Error("discovery broke");
export default Task.make({ name: "main", run: Effect.log("body ran") });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      const lines = linesOf(withRunDirectoryReplaced(ran.standardError));
      expect(lines).toEqual([
        "run: <run folder>/",
        "error: main.ts:3:46: Error: discovery broke",
        "error: could not import task module",
        "failed: 0 of 0 tasks, <seconds>",
        "run: <run folder>/",
      ]);
    }),
  ),
);

export const stoppedSummaryAfterCtrlCDuringDiscovery = makeTest(
  "Ctrl-C while the run process imports the task file prints the stopping line, a stopped summary of no tasks and the run line",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
if (Bun.main !== import.meta.path) {
  console.error("discovery waiting");
  await new Promise(() => {});
}
export default Task.make({ name: "main", run: Effect.log("body ran") });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForLine("discovery waiting");
      yield* command.sendSignal("INT");
      const exited = yield* command.waitForExit;
      expect(exited.exitCode, exited.standardError).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(linesOf(withRunDirectoryReplaced(exited.standardError))).toEqual([
        "run: <run folder>/",
        "discovery waiting",
        "stopping: press ctrl-c again to force",
        "stopped: 0 of 0 tasks, <seconds>",
        "run: <run folder>/",
      ]);
    }),
  ),
);

export const killedLineInTheTaskFile = makeTest(
  "a task that prints nothing but leaves a process that ignores SIGTERM gets a file holding the killed line about it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "root", run: ChildProcess.make("sh", ["-c", "trap '' TERM; sleep 1000 > /dev/null 2>&1 & until ps -o command= -p $! | grep -qx 'sleep 1000'; do :; done; echo $! > leaver-pid"]).pipe(Effect.flatMap((child) => child.exitCode), Effect.asVoid) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const leaverProcessId = Number((yield* read(`${project.directory}/leaver-pid`)).trim());
      const killedLine = `killed: [root] ${leaverProcessId} (sleep)`;
      expect(linesOf(ran.standardError)).toContain(killedLine);
      const runDirectoryPath = findRunDirectoryPath(ran.standardError);
      expect(linesOf(yield* read(`${runDirectoryPath}/1.output`))).toContain(killedLine);
    }),
  ),
);

export const runLineWhenTheRunDirectoryIsNotMade = makeTest(
  "a run whose state directory cannot be made reports it once and still ends with the summary and the run line naming the folder it tried to make",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject(PRINTING_PROJECT);
      const ran = yield* runTaskProcess(project, "main.ts", {
        SSHKEDA_TASK_STATE_DIRECTORY: "/dev/null/state",
      });
      expect(ran.exitCode, ran.standardError).toBe(0);
      const lines = linesOf(withSecondsNamed(ran.standardError));
      const errorLines = lines.filter((line) => line.startsWith("error: "));
      expect(errorLines).toHaveLength(ERROR_AND_CONTEXT_LINE_COUNT);
      expect(errorLines[0]).toMatch(
        /^error: \/dev\/null\/state\/runs\/\S+: Error: ENOTDIR: not a directory, mkdir '\/dev\/null\/state\/runs\/\S+' \(ENOTDIR\)$/u,
      );
      expect(errorLines[1]).toMatch(
        /^error: could not make \/dev\/null\/state\/runs\/\S+, so the trace file is incomplete$/u,
      );
      const [summaryLine, runLine] = lines.slice(-SUMMARY_AND_RUN_LINE_COUNT);
      expect(summaryLine).toBe("passed: 1 task, <seconds>");
      expect(runLine).toStartWith("run: /dev/null/state/runs/project/");
    }),
  ),
);

const NAME_THAT_LOOKS_LIKE_LINES = `a]b\\c\nerror: [d] fake\r\te\u001Bf`;

const ESCAPED_NAME = String.raw`[a\]b\\c\nerror: [d\] fake\r\te\u001Bf]`;

export const taskNamesAreEscaped = makeTest(
  String.raw`a task name with ], \, a line break, a carriage return, a tab and a control character is escaped wherever Task writes it in brackets, so it fakes no line, and the trace keeps the name as made`,
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const odd = Task.make({ name: "a]b\\\\c\\nerror: [d] fake\\r\\te\\u001Bf", run: Effect.gen(function* () {
  yield* Effect.log("logged");
  yield* odd;
}) });
export default Task.make({ name: "main", run: Task.allSettled([odd]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      const oddLines = [
        `${ESCAPED_NAME} logged`,
        `error: ${ESCAPED_NAME} main.ts:3:18: TaskWaitsOnItself: waits on itself through ${ESCAPED_NAME} -> ${ESCAPED_NAME}`,
      ];
      const pointerLine = `error: ${ESCAPED_NAME} its output is in ${findRunDirectoryPath(ran.standardError)}/2.output`;
      const taskLines = [...oddLines, pointerLine];
      const recapLine = `recap: ${ESCAPED_NAME} 2.output | main.ts:3:18: TaskWaitsOnItself: waits on itself through ${ESCAPED_NAME} -> ${ESCAPED_NAME}`;
      const beforeTheEnds = ["run: <run folder>/", ...oddLines];
      const oddEnd = [pointerLine, `ended: ${ESCAPED_NAME}, <seconds>, failed`];
      const mainEnd = ["ended: [main], <seconds>, failed"];
      const afterTheEnds = [
        recapLine,
        `failed: ${ESCAPED_NAME} (1 of 2 tasks), <seconds>`,
        "run: <run folder>/",
      ];
      expectOneOfTheOrders(linesOf(withRunDirectoryReplaced(ran.standardError)), [
        [...beforeTheEnds, ...oddEnd, ...mainEnd, ...afterTheEnds],
        [...beforeTheEnds, ...mainEnd, ...oddEnd, ...afterTheEnds],
      ]);
      expect(
        withSecondsNamed(
          withoutStackFrames(yield* read(`${findRunDirectoryPath(ran.standardError)}/2.output`)),
        ),
      ).toBe(
        `started: ${ESCAPED_NAME}, <seconds>, 2.output\n${taskLines.join("\n")}\nended: ${ESCAPED_NAME}, <seconds>, failed\n`,
      );
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      expect(trace?.findTaskSpan("task", NAME_THAT_LOOKS_LIKE_LINES)).toBeDefined();
    }),
  ),
);

export const everyLoggedLineIsPrefixed = makeTest(
  String.raw`a log whose text breaks lines with \n, \r\n and \r prints as one [name] line per line, on the terminal and in the task's file`,
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.log("one\\ntwo\\r\\nthree\\rfour") });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const taskLines = ["[main] one", "[main] two", "[main] three", "[main] four"];
      expect(linesBetweenTheRunLines(ran.standardError)).toEqual([
        ...taskLines,
        "passed: 1 task, <seconds>",
      ]);
      expect(
        withSecondsNamed(yield* read(`${findRunDirectoryPath(ran.standardError)}/1.output`)),
      ).toBe(
        `${[
          "started: [main], <seconds>, 1.output",
          ...taskLines,
          "ended: [main], <seconds>, passed",
        ].join("\n")}\n`,
      );
    }),
  ),
);

export const workerOutputIsLabelled = makeTest(
  "what task code writes straight to standard output or standard error on a worker, when its file loads or in a body, prints on standard error as output: lines, one per line, including a last line with no line break, while a body's console calls print under its name, and nothing reaches standard output",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { isMainThread } from "node:worker_threads";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
if (!isMainThread) console.error("loaded on a worker");
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.sync(() => {
    console.log("to standard output\\nits second line");
    console.error("to standard error");
    process.stdout.write("no line break");
  });
  yield* Effect.log("logged");
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardOutput).toBe("");
      const lines = linesBetweenTheRunLines(ran.standardError);
      expect(lines.toSorted()).toEqual(
        [
          "[main] to standard output",
          "[main] its second line",
          "output: no line break",
          "output: loaded on a worker",
          "[main] to standard error",
          "[main] logged",
          "passed: 1 task, <seconds>",
        ].toSorted(),
      );
      expect(lines.at(-1)).toBe("passed: 1 task, <seconds>");
    }),
  ),
);

export const runProcessDefectPrintsAsAnError = makeTest(
  "a defect that escapes the run process prints as Task's error line, not as Effect's log line",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import "./node_modules/@sshkeda/task/src/lib/task/internal/start-the-run.ts";`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toBe(
        "error: InternalInvariantBroken: run process broke the internal invariant that the supervisor passes the run argument",
      );
    }),
  ),
);

export const promptShowsBeforeInput = makeTest(
  "a prompt a root task writes on a worker without a line break shows as output: before the task gets the input it waits for",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Stdio, Stream } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.sync(() => { process.stdout.write("Continue? "); });
  const answer = yield* Stdio.Stdio.use((stdio) => Stream.mkString(Stream.decodeText(stdio.stdin)));
  yield* Effect.log(\`answered \${answer.trim()}\`);
}) });`,
      });
      const standardInput = yield* Queue.unbounded<Uint8Array, Cause.Done>();
      const command = yield* startTaskProcess(
        project,
        "main.ts",
        {},
        project.directory,
        [],
        Stream.fromQueue(standardInput),
      );
      yield* command.waitForStandardError("output: Continue? ");
      yield* Queue.offer(standardInput, new TextEncoder().encode("yes\n"));
      yield* Queue.end(standardInput);
      const ran = yield* command.waitForExit;
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardOutput).toBe("");
      expect(linesBetweenTheRunLines(ran.standardError)).toEqual([
        "output: Continue? ",
        "[main] answered yes",
        "passed: 1 task, <seconds>",
      ]);
    }),
  ),
);

export const outputBytesStayDistinct = makeTest(
  String.raw`output that is not UTF-8 prints each invalid byte as \xNN, and a character split across two writes still prints whole`,
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.sync(() => {
  process.stdout.write(Buffer.from([0xff, 10, 0xfe, 10]));
  process.stdout.write(Buffer.from([0x61, 0xe2]));
  process.stdout.write(Buffer.from([0x82, 0xac, 10]));
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardOutput).toBe("");
      const { endedLines, otherLines } = separateEndedLines(
        linesBetweenTheRunLines(ran.standardError),
      );
      expect(otherLines).toEqual([
        String.raw`output: \xFF`,
        String.raw`output: \xFE`,
        "output: a€",
        "passed: 1 task, <seconds>",
      ]);
      expect(endedLines).toEqual([]);
    }),
  ),
);

const OPENING_LINE_COUNT = 1;

export const partialLineShowsWhileTheWorkerRuns = makeTest(
  "text a task writes without a line break shows as output: at once, while its worker keeps running, and its line ends when the worker ends",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.sync(() => { process.stdout.write("partial"); });
  while (!(yield* Effect.promise(() => Bun.file("go").exists()))) {
    yield* Effect.sleep("10 millis");
  }
  yield* Effect.sync(() => { process.stdout.write(" and the rest"); });
  yield* Effect.sync(() => process.exit(3));
}) });`,
      });
      const command = yield* startTaskProcess(project, "main.ts");
      yield* command.waitForStandardError("output: partial");
      yield* write(`${project.directory}/go`, "");
      const ran = yield* command.waitForExit;
      expect(ran.exitCode).toBe(1);
      expect(ran.standardOutput).toBe("");
      expect(
        linesOf(withSecondsNamed(withoutTheFirstRunLine(ran.standardError))).slice(
          0,
          OPENING_LINE_COUNT,
        ),
      ).toEqual(["output: partial and the rest"]);
    }),
  ),
);

const LETTER_A = 0x61;

const LETTER_B = 0x62;

const LINE_FEED = 0x0a;

const INVALID_BYTE = 0xff;

const BYTE_ORDER_MARK = new TextEncoder().encode("\uFEFF");

const LARGE_CHUNK_MEBIBYTES = 16;

const BYTES_PER_MEBIBYTE = 1_048_576;

const LARGE_CHUNK_BYTES = LARGE_CHUNK_MEBIBYTES * BYTES_PER_MEBIBYTE;

const SLOWDOWN_ALLOWED_FOR_ONE_INVALID_BYTE = 5;

const TIMED_DECODES_OF_EACH_CHUNK = 3;

const MILLISECONDS_ALLOWED_BESIDES = 500;

const decodeChunks = (chunks: readonly (readonly number[])[]) =>
  Stream.fromIterable(chunks.map((chunk) => Uint8Array.from(chunk))).pipe(
    decodeOutputText,
    Stream.runCollect,
    Effect.map((texts) => texts.join("")),
  );

export const byteOrderMarkStaysInOutput = makeTest(
  "a byte order mark in output prints as U+FEFF, after an invalid byte and at the start of a write alike",
  Effect.gen(function* () {
    expect(
      yield* decodeChunks([[LETTER_A, INVALID_BYTE, ...BYTE_ORDER_MARK, LETTER_B, LINE_FEED]]),
    ).toBe(`${String.raw`a\xFF`}\uFEFFb\n`);
    expect(yield* decodeChunks([[LETTER_A], [...BYTE_ORDER_MARK, LETTER_B, LINE_FEED]])).toBe(
      "a\uFEFFb\n",
    );
  }),
);

const splitTexts = (texts: readonly string[]) =>
  Stream.fromIterable(texts).pipe(splitOutputPieces, Stream.runCollect);

export const emptyTextKeepsTheLineBreakAfterIt = makeTest(
  "an empty text, like a write of only a byte order mark, leaves the next line break its own line, and a carriage return still pairs with a line feed across it",
  Effect.gen(function* () {
    expect(yield* splitTexts(["", "\nOK\n"])).toEqual([
      { text: "", continuesLine: false, endsLine: true },
      { text: "OK", continuesLine: false, endsLine: true },
    ]);
    expect(yield* splitTexts(["a\r", "", "\nb\n"])).toEqual([
      { text: "a", continuesLine: false, endsLine: true },
      { text: "b", continuesLine: false, endsLine: true },
    ]);
  }),
);

const decodeOnce = (chunk: Uint8Array) =>
  Stream.make(chunk).pipe(
    decodeOutputText,
    Stream.runCollect,
    Effect.map((texts) => texts.join("")),
    Effect.timed,
    Effect.map(([took, decoded]) => ({ milliseconds: Duration.toMillis(took), decoded })),
  );

export const oneInvalidByteKeepsDecodingLinear = makeTest(
  String.raw`a 16 MiB write whose last byte is not UTF-8 decodes about as fast as the same write with every byte valid, and prints that byte as \xFF`,
  Effect.gen(function* () {
    const valid = new Uint8Array(LARGE_CHUNK_BYTES).fill(LETTER_A);
    const invalidAtTheEnd = Uint8Array.from(valid);
    invalidAtTheEnd.set([INVALID_BYTE], LARGE_CHUNK_BYTES - 1);
    const timings = yield* Effect.forEach(Array.from({ length: TIMED_DECODES_OF_EACH_CHUNK }), () =>
      Effect.all([decodeOnce(valid), decodeOnce(invalidAtTheEnd)]),
    );
    const fastest = (index: 0 | 1) =>
      Math.min(...timings.map((timing) => timing[index].milliseconds));
    expect(timings[0]?.[1].decoded).toBe(`${"a".repeat(LARGE_CHUNK_BYTES - 1)}${String.raw`\xFF`}`);
    expect(fastest(1)).toBeLessThanOrEqual(
      SLOWDOWN_ALLOWED_FOR_ONE_INVALID_BYTE * fastest(0) + MILLISECONDS_ALLOWED_BESIDES,
    );
  }),
);

const OPEN_LINE_EXAMPLE_LINE_COUNT = 3;

const OPEN_LINE_TASK_FILE = `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const waitFor = (fileName: string) => Effect.gen(function* () {
  while (!(yield* Effect.promise(() => Bun.file(fileName).exists()))) {
    yield* Effect.sleep("10 millis");
  }
});
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.sync(() => { process.stdout.write("partial"); });
  yield* waitFor("log");
  yield* Effect.log("logged");
  yield* waitFor("rest");
  yield* Effect.sync(() => { process.stdout.write(" rest\\n"); });
}) });`;

export const otherLineEndsAnOpenOutputLine = makeTest(
  "a line Task prints while an output: line is open ends that line first, and the rest of it prints after as a new output: line, on standard error and under 2>&1 alike",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": OPEN_LINE_TASK_FILE,
        "merged.ts": `const merged = Bun.spawn(["sh", "-c", \`exec \${process.execPath} main.ts 2>&1\`], { stdio: ["ignore", "inherit", "inherit"] });
process.exitCode = await merged.exited;`,
      });
      for (const merged of [false, true]) {
        yield* Effect.all([
          FileSystem.FileSystem.pipe(
            Effect.flatMap((files) => files.remove(`${project.directory}/log`, { force: true })),
          ),
          FileSystem.FileSystem.pipe(
            Effect.flatMap((files) => files.remove(`${project.directory}/rest`, { force: true })),
          ),
        ]);
        const command = yield* startTaskProcess(project, merged ? "merged.ts" : "main.ts");
        const waitForWritten = merged
          ? command.waitForStandardOutput
          : command.waitForStandardError;
        yield* waitForWritten("output: partial");
        yield* write(`${project.directory}/log`, "");
        yield* waitForWritten("[main] logged\n");
        yield* write(`${project.directory}/rest`, "");
        const ran = yield* command.waitForExit;
        expect(ran.exitCode, ran.standardError).toBe(0);
        const written = merged ? ran.standardOutput : ran.standardError;
        expect(
          separateEndedLines(
            withSecondsNamed(withoutTheFirstRunLine(written)).split("\n"),
          ).otherLines.slice(0, OPEN_LINE_EXAMPLE_LINE_COUNT),
        ).toEqual(["output: partial", "[main] logged", "output:  rest"]);
        expect(merged ? ran.standardError : ran.standardOutput).toBe("");
      }
    }),
  ),
);

const ESCAPED_LINE_SEPARATOR = String.raw`\u2028`;

const ESCAPED_PARAGRAPH_SEPARATOR = String.raw`\u2029`;

export const liveLinesEscapeLineSeparators = makeTest(
  "a task's logged, console and command lines escape U+2028 and U+2029 on the terminal and in its file, so no reader takes them as line breaks, while Task.exec still returns them as written",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.log("log A\\u2028failed: [forged]");
  yield* Effect.sync(() => console.log("console A\\u2029error: [forged]"));
  const returned = yield* Task.exec("sh", ["-c", "printf 'command A\\\\342\\\\200\\\\250B\\\\ttab\\\\n'"], { print: true });
  yield* Effect.log(\`returned as written: \${String(returned === "command A\\u2028B\\ttab\\n")}\`);
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const taskLines = [
        `[main] log A${ESCAPED_LINE_SEPARATOR}failed: [forged]`,
        `[main] console A${ESCAPED_PARAGRAPH_SEPARATOR}error: [forged]`,
        `[main] command A${ESCAPED_LINE_SEPARATOR}B\ttab`,
        "[main] returned as written: true",
      ];
      expect(linesBetweenTheRunLines(ran.standardError)).toEqual([
        ...taskLines,
        "passed: 1 task, <seconds>",
      ]);
      expect(
        withSecondsNamed(yield* read(`${findRunDirectoryPath(ran.standardError)}/1.output`)),
      ).toBe(
        `${[
          "started: [main], <seconds>, 1.output",
          ...taskLines,
          "ended: [main], <seconds>, passed",
        ].join("\n")}\n`,
      );
    }),
  ),
);
