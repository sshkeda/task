import { expect } from "bun:test";

import { Cause, Effect, FileSystem, Schema } from "effect";

import { CommandFailed, CommandStandardInputFailed } from "../../src/lib/errors/index.ts";
import {
  causeWithPrintableErrors,
  describeTraceExceptions,
  reportCause,
} from "../../src/shared/report-cause.ts";
import { read, write } from "../shared/access-files.ts";
import { countOccurrences } from "../shared/count-occurrences.ts";
import { encodeJson } from "../shared/encode-json.ts";
import { expectOneOfTheOrders, withEitherEndFirst } from "../shared/expect-one-of-the-orders.ts";
import { findMisprintedErrorLines } from "../shared/find-misprinted-error-lines.ts";
import { REPOSITORY_DIRECTORY } from "../shared/find-repository-directory.ts";
import { makeProject } from "../shared/make-projects.ts";
import {
  makeScratchDirectory,
  makeTemporaryDirectory,
} from "../shared/make-temporary-directories.ts";
import { makeTest } from "../shared/make-tests.ts";
import {
  findRunDirectoryPath,
  findTraceFilePath,
  withRunDirectoryNamed,
  withRunLinePathNamed,
  withSecondsNamed,
} from "../shared/name-output-files.ts";
import {
  readAttribute,
  readProjectRunsDirectory,
  readTracedFailures,
  readTraces,
} from "../shared/read-trace-files.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";
import { startTaskProcessWithPipes } from "../shared/start-task-process-with-pipes.ts";
import { findStackFrameBlocks, withoutStackFrames } from "../shared/without-stack-frames.ts";

const FEWEST_EXPECTED_ERROR_CLASSES = 50;

const PRINTED_ZERO_COUNT = 500;

const FAILURE_VALUE_CHARACTER_LIMIT = 200;

const FAILED_COMMAND_EXIT_CODE = 3;

const LONG_STANDARD_OUTPUT_LINES = 25;

const LONG_STANDARD_ERROR_LINES = 22;

const NUMBER_DIGITS = 2;

const ROOT_TASK_COMMAND_PREFIX = "[main] main.ts:7:52";

const ROOT_TASK_FAILURE_PREFIX = "[main] main.ts:7:63";

const CHECK_ARCHIVE_FAILURE_PREFIX = "[check-archive] main.ts:4:85";

const CHECK_B_FAILURE_PREFIX = "[b] main.ts:5:67";

const BUILD_COMMAND_PREFIX = "[build] main.ts:2:59";

const COMMAND_IN_TASK_MADE_IN_BODY_PREFIX = "[made-in-body] main.ts:4:54";

const SUMMARY_LINE = /^(?:passed|failed|stopped): /u;

const startedLineOf = (taskName: string, fileName: string) =>
  `started: [${taskName}], <seconds>, ${fileName}`;

const MAIN_STARTED_LINE = startedLineOf("main", "1.output");

const endedLineOf = (taskName: string, state: "failed" | "passed" | "stopped") =>
  `ended: [${taskName}], <seconds>, ${state}`;

const RUN_LINE = "run: <run folder>/";

const ERROR_LOG_LEVEL = { SSHKEDA_TASK_LOG: "error" } as const;

const outputFileLineOf = (taskName: string, fileName: string) =>
  `error: [${taskName}] its output is in <run folder>/${fileName}`;

const MAIN_OUTPUT_FILE_LINE = outputFileLineOf("main", "1.output");

const OUTPUT_FILE_AND_LAST_ERROR_LINES = 2;

const ERROR_AND_RECAP_LINES = 2;

const PRINTED_LIVE_AND_IN_THE_RECAP = 2;

const recapLineOf = (errorLine: string, fileName: string) =>
  errorLine.replace(/^error: (?<taskName>\[[^\]]+\]) /u, `recap: $<taskName> ${fileName} | `);

const findLineBeforeTheEndedLine = (standardError: string) => {
  const lines = withRunLinePathNamed(standardError.trimEnd()).split("\n");
  const summaryIndex = lines.findLastIndex((line) => SUMMARY_LINE.test(line));
  const recapIndex = lines.findIndex((line) => line.startsWith("recap: "));
  const endedIndex = recapIndex - 1;
  expect(lines[recapIndex]).toBe(
    recapLineOf(lines.find((line) => line.startsWith("error: [main] ")) ?? "", "1.output"),
  );
  expect(
    lines.slice(recapIndex, summaryIndex).filter((line) => !line.startsWith("recap: [main] ")),
  ).toEqual([]);
  expect(withSecondsNamed(lines[endedIndex] ?? "")).toBe(endedLineOf("main", "failed"));
  expect(lines[endedIndex - 1]).toBe(MAIN_OUTPUT_FILE_LINE);
  return lines[endedIndex - 1 - 1];
};

const findErrorLines = (standardError: string) =>
  withRunLinePathNamed(standardError)
    .split("\n")
    .filter((line) => line.startsWith("error: "));

const TWO_COMMAND_LINES_END = 2;

const withCommandLinesSorted = (lines: readonly string[]) => [
  ...lines.slice(0, TWO_COMMAND_LINES_END).toSorted(),
  ...lines.slice(TWO_COMMAND_LINES_END),
];

const numberedLines = (prefix: string, first: number, last: number) =>
  Array.from(
    { length: last - first + 1 },
    (_unused, index) => `${prefix} ${String(first + index).padStart(NUMBER_DIGITS, "0")}`,
  );

export const failHard = makeTest(
  "the first failure fails the run and the summary names it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export const slow = Task.make({ name: "slow", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.log("slow cleaned up"));
  yield* Effect.log("slow started");
  yield* Effect.never;
}) });
export const bad = Task.make({ name: "bad", run: Effect.gen(function* () {
  yield* slow.pipe(Effect.forkChild);
  return yield* Effect.fail(new TaskError({ message: "bad broke" }));
}) });
export default Task.make({ name: "main", run: Effect.all([slow, bad], { concurrency: "unbounded", discard: true }) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(findErrorLines(ran.standardError).join("\n")).toMatch(
        /^error: \[bad\] \S+: TaskError: bad broke\nerror: \[bad\] its output is in <run folder>\/3\.output$/u,
      );
      expect(ran.standardError).toMatch(
        /^recap: \[bad\] 3\.output \| \S+: TaskError: bad broke$/mu,
      );
      expect(ran.standardError).toMatch(/^failed: \[bad\] \(1 of 3 tasks\), 1 stopped, [\d.]+s$/mu);
    }),
  ),
);

export const manyFailedTasks = makeTest(
  "the summary names the first three failed tasks in the order they were made and counts the rest",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export const lint = Task.make({ name: "lint", run: Effect.fail(new TaskError({ message: "lint broke" })) });
export const build = Task.make({ name: "build", run: Effect.fail(new TaskError({ message: "build broke" })) });
export const ok = Task.make({ name: "ok", run: Effect.void });
export const types = Task.make({ name: "types", run: Effect.fail(new TaskError({ message: "types broke" })) });
export const docs = Task.make({ name: "docs", run: Effect.fail(new TaskError({ message: "docs broke" })) });
export const e2e = Task.make({ name: "e2e", run: Effect.fail(new TaskError({ message: "e2e broke" })) });
export const checks = Task.make({ name: "checks", run: Task.allSettled([e2e, docs, ok, types, build, lint]) });
export default Task.make({ name: "main", run: Task.allSettled([checks]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).not.toContain("SettledTasksFailed");
      const tracedFailures = yield* readTracedFailures(project, ran);
      expect(tracedFailures).toContainEqual({
        taskName: "checks",
        failure:
          "error SettledTasksFailed: 5 of 6 tasks failed: [e2e], [docs], [types], [build], [lint]",
      });
      expect(tracedFailures).toContainEqual({
        taskName: "main",
        failure: "error SettledTasksFailed: 1 of 1 task failed: [checks]",
      });
      expect(ran.standardError).toMatch(
        /^failed: \[lint\] \[build\] \[types\] and 2 more \(5 of 8 tasks\), [\d.]+s$/mu,
      );
    }),
  ),
);

export const errorOutput = makeTest(
  "a TaskError failure or defect prints once with its tag and the user line that made it, and the trace keeps the stack",
  Effect.scoped(
    Effect.gen(function* () {
      for (const mode of ["expected", "defect"] as const) {
        const project = yield* makeProject({
          "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
const failure = Effect.${mode === "defect" ? "die" : "fail"}(new TaskError({ message: "dev is not running" }));
export default Task.make({ name: "main", run: failure });`,
        });
        const ran = yield* runTaskProcess(project, "main.ts");
        expect(ran.exitCode).toBe(1);
        expect(countOccurrences(ran.standardError, "dev is not running")).toBe(
          ERROR_AND_RECAP_LINES,
        );
        const [, lineNumber, columnNumber] =
          /^error: \[main\] main\.ts:(?<lineNumber>\d+):(?<columnNumber>\d+): TaskError: dev is not running$/mu.exec(
            ran.standardError,
          ) ?? [];
        expect(findLineBeforeTheEndedLine(ran.standardError)).toBe(
          `error: [main] main.ts:${lineNumber}:${columnNumber}: TaskError: dev is not running`,
        );
        const constructingLine = (yield* read(`${project.directory}/main.ts`)).split("\n")[
          Number(lineNumber) - 1
        ];
        expect(constructingLine?.slice(Number(columnNumber) - 1)).toStartWith("new TaskError(");
        expect(ran.standardError).not.toMatch(/^(?:error: )?\[main\]\s+at /mu);
        const tracePath = yield* Effect.fromNullishOr(findTraceFilePath(ran.standardError));
        const fileSystem = yield* FileSystem.FileSystem;
        const trace = yield* fileSystem.readFileString(tracePath);
        expect(trace).toContain("TaskError: dev is not running");
        expect(trace).toContain("main.ts:");
      }
    }),
  ),
);

export const namedErrors = makeTest(
  "every named error a task fails with prints its message and tag without a stack, or its fields when it has no message, and the trace keeps the details",
  Effect.scoped(
    Effect.gen(function* () {
      const failures = [
        {
          failure: `Task.exec("sh", ["-c", "echo broken >&2; exit ${FAILED_COMMAND_EXIT_CODE}"])`,
          tag: "CommandFailed",
          exitCode: FAILED_COMMAND_EXIT_CODE,
          headline: new RegExp(
            String.raw`^error: \[main\] main\.ts:9:22: CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}$`,
            "u",
          ),
          detailLines: ["error: [main] its output is in <run folder>/1.output"],
        },
        {
          failure: `Task.exec("sh", ["-c", "echo hint: printed by the command >&2; exit ${FAILED_COMMAND_EXIT_CODE}"])`,
          tag: "CommandFailed",
          exitCode: FAILED_COMMAND_EXIT_CODE,
          headline: new RegExp(
            String.raw`^error: \[main\] main\.ts:9:22: CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}$`,
            "u",
          ),
          detailLines: ["error: [main] its output is in <run folder>/1.output"],
        },
        {
          failure:
            'Effect.fail(new DevServerMissing({ message: "dev server is not running", hint: "start it with bun scripts/dev.ts" }))',
          tag: "DevServerMissing",
          exitCode: 1,
          headline:
            /^error: \[main\] main\.ts:9:\d+: DevServerMissing: dev server is not running$/u,
          detailLines: [MAIN_OUTPUT_FILE_LINE],
        },
        {
          failure: "Effect.fail(new PortTaken({ port: 3000 }))",
          tag: "PortTaken",
          exitCode: 1,
          headline: /^error: \[main\] main\.ts:9:\d+: PortTaken: port 3000 is taken$/u,
          detailLines: [MAIN_OUTPUT_FILE_LINE],
        },
        {
          failure: 'Effect.fail(new DeployRejected({ env: "staging", status: 409 }))',
          tag: "DeployRejected",
          exitCode: 1,
          headline:
            /^error: \[main\] main\.ts:9:\d+: DeployRejected: \{"env":"staging","status":409\}$/u,
          detailLines: [MAIN_OUTPUT_FILE_LINE],
        },
      ];
      for (const expected of failures) {
        const project = yield* makeProject({
          "main.ts": `import { Data, Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
class DevServerMissing extends Schema.TaggedError<DevServerMissing>()("DevServerMissing", { message: Schema.String, hint: Schema.String }) {}
class PortTaken extends Data.TaggedError("PortTaken")<{ readonly port: number }> {
  override get message() { return \`port \${this.port} is taken\`; }
  get hint() { return "stop the other server or pick another port"; }
}
class DeployRejected extends Data.TaggedError("DeployRejected")<{ readonly env: string; readonly status: number }> {}
const failure = ${expected.failure};
export default Task.make({ name: "main", run: failure });`,
        });
        const ran = yield* runTaskProcess(project, "main.ts");
        expect(ran.exitCode).toBe(expected.exitCode);
        const [headline, ...detailLines] = findErrorLines(ran.standardError);
        expect(headline).toMatch(expected.headline);
        expect(detailLines).toEqual(expected.detailLines);
        expect(findLineBeforeTheEndedLine(ran.standardError)).toBe(
          [headline, ...detailLines].at(-OUTPUT_FILE_AND_LAST_ERROR_LINES),
        );
        expect(countOccurrences(ran.standardError, `${expected.tag}:`)).toBe(ERROR_AND_RECAP_LINES);
        expect(ran.standardError).not.toMatch(/^(?:error: )?\[main\]\s+at /mu);
        const tracePath = yield* Effect.fromNullishOr(findTraceFilePath(ran.standardError));
        const fileSystem = yield* FileSystem.FileSystem;
        const trace = yield* fileSystem.readFileString(tracePath);
        expect(trace).toContain(`${expected.tag}:`);
        expect(trace).toContain(" at ");
      }
    }),
  ),
);

export const carriageReturnsInErrors = makeTest(
  "a failed command whose output moves back to the start of the line with a carriage return prints each part as its own line under the task's name, and its error names its file instead of repeating them, whether or not SSHKEDA_TASK_LOG=error",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "printf 'downloading 10%%\\\\rdownloading 100%%\\\\r\\\\n'; printf 'error: disk full\\\\r\\\\n' >&2; exit ${FAILED_COMMAND_EXIT_CODE}"]) });`,
        "piped.ts": `const shell = Bun.spawn(["sh", "-c", \`\${process.execPath} main.ts 2>&1\`], { stdio: ["ignore", "inherit", "inherit"] });
process.exitCode = await shell.exited;`,
      });
      const errorLines = [
        `error: [main] main.ts:2:52: CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}`,
        "error: [main] its output is in <run folder>/1.output",
      ];
      const recapLines = [
        `recap: [main] 1.output | main.ts:2:52: CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}`,
        "recap: [main] downloading 10%",
        "recap: [main] downloading 100%",
        "recap: [main] error: disk full",
      ];
      const endOfTheRun = ["failed: [main] (1 of 1 task), <seconds>", RUN_LINE, ""];
      const piped = yield* runTaskProcess(project, "piped.ts");
      expect(piped.exitCode, piped.standardOutput).toBe(FAILED_COMMAND_EXIT_CODE);
      const lines = withSecondsNamed(withRunDirectoryNamed(piped.standardOutput, project)).split(
        "\n",
      );
      const commandLines = lines.filter((line) => line.startsWith("[main] "));
      expect(commandLines.toSorted()).toEqual([
        "[main] downloading 10%",
        "[main] downloading 100%",
        "[main] error: disk full",
      ]);
      expect(lines.filter((line) => !line.startsWith("[main] "))).toEqual([
        RUN_LINE,
        ...errorLines,
        endedLineOf("main", "failed"),
        ...recapLines,
        ...endOfTheRun,
      ]);
      expect(lines.indexOf(errorLines[0] ?? "")).toBeGreaterThan(
        lines.indexOf(commandLines.at(-1) ?? ""),
      );
      const quiet = yield* runTaskProcess(project, "piped.ts", ERROR_LOG_LEVEL);
      expect(quiet.exitCode, quiet.standardOutput).toBe(FAILED_COMMAND_EXIT_CODE);
      expect(withSecondsNamed(withRunDirectoryNamed(quiet.standardOutput, project))).toBe(
        [RUN_LINE, ...errorLines, ...recapLines, ...endOfTheRun].join("\n"),
      );
    }),
  ),
);

export const longCommandOutput = makeTest(
  "a failed command with long output prints each of its lines once, when the command fails, its error names its file instead of repeating them, and its recap repeats only its first line and its last two, whether or not SSHKEDA_TASK_LOG=error, while the trace keeps the error",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "seq -f 'report line %02g' 1 ${LONG_STANDARD_OUTPUT_LINES}; seq -f 'warning %02g' 1 ${LONG_STANDARD_ERROR_LINES} >&2; exit ${FAILED_COMMAND_EXIT_CODE}"]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(FAILED_COMMAND_EXIT_CODE);
      const errorLines = [
        `error: [main] main.ts:2:52: CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}`,
        "error: [main] its output is in <run folder>/1.output",
      ];
      const recapLines = [
        `recap: [main] 1.output | main.ts:2:52: CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}`,
        "recap: [main] report line 01",
        "recap: [main] …",
        `recap: [main] warning ${String(LONG_STANDARD_ERROR_LINES - 1)}`,
        `recap: [main] warning ${String(LONG_STANDARD_ERROR_LINES)}`,
      ];
      const endOfTheRun = ["failed: [main] (1 of 1 task), <seconds>", RUN_LINE];
      const lines = withSecondsNamed(withRunDirectoryNamed(ran.standardError, project)).split("\n");
      const commandLines = lines.filter((line) => line.startsWith("[main] "));
      expect(commandLines.filter((line) => line.startsWith("[main] report line "))).toEqual(
        numberedLines("[main] report line", 1, LONG_STANDARD_OUTPUT_LINES),
      );
      expect(commandLines.filter((line) => line.startsWith("[main] warning "))).toEqual(
        numberedLines("[main] warning", 1, LONG_STANDARD_ERROR_LINES),
      );
      expect(commandLines).toHaveLength(LONG_STANDARD_OUTPUT_LINES + LONG_STANDARD_ERROR_LINES);
      expect(lines.filter((line) => !line.startsWith("[main] "))).toEqual([
        RUN_LINE,
        ...errorLines,
        endedLineOf("main", "failed"),
        ...recapLines,
        ...endOfTheRun,
      ]);
      const quiet = yield* runTaskProcess(project, "main.ts", ERROR_LOG_LEVEL);
      expect(quiet.exitCode, quiet.standardError).toBe(FAILED_COMMAND_EXIT_CODE);
      expect(withSecondsNamed(withRunDirectoryNamed(quiet.standardError, project))).toBe(
        [RUN_LINE, ...errorLines, ...recapLines, ...endOfTheRun].join("\n"),
      );
      const tracePath = yield* Effect.fromNullishOr(findTraceFilePath(ran.standardError));
      const trace = yield* (yield* FileSystem.FileSystem).readFileString(tracePath);
      expect(trace).toContain(`CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}`);
      expect(countOccurrences(trace, "report line 01"), "only the logged line").toBe(1);
      const [readTrace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      const loggedLines = (readTrace?.logs ?? [])
        .map((log) => log.body.stringValue ?? "")
        .join("\n")
        .split("\n");
      expect(loggedLines.filter((line) => line.startsWith("report line "))).toEqual(
        numberedLines("report line", 1, LONG_STANDARD_OUTPUT_LINES),
      );
      expect(loggedLines.filter((line) => line.startsWith("warning "))).toEqual(
        numberedLines("warning", 1, LONG_STANDARD_ERROR_LINES),
      );
    }),
  ),
);

export const failingInADependency = makeTest(
  "a body that fails inside an installed package prints no location inside node_modules",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, FileSystem } from "effect";
import { Task } from "@sshkeda/task";
const version = FileSystem.FileSystem.pipe(Effect.flatMap((fs) => fs.readFileString(".bun-version")));
export default Task.make({ name: "main", run: version });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(
        /^error: \[main\] main\.ts:4:21: PlatformError: NotFound: .*\.bun-version.*$/mu,
      );
      expect(ran.standardError).not.toContain("node_modules");
    }),
  ),
);

const FILE_INSTALLED_PACKAGE = {
  "package.json": `{ "name": "@acme/tools", "type": "module", "exports": { ".": "./src/format.ts" } }`,
  "src/format.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
class ToolExited extends Schema.TaggedError<ToolExited>()("ToolExited", { message: Schema.String }) {}
export const format = Task.make({ name: "format", run: Effect.suspend(() => Effect.fail(new ToolExited({ message: "fmt exited with code 1" }))) });`,
  "scripts/tools.ts": 'export { format as default } from "../src/format.ts";',
  "scripts/broken.ts": `import { isMainThread } from "node:worker_threads";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
if (!isMainThread) throw new Error("worker import broke");
export default Task.make({ name: "broken", run: Effect.void });`,
} as const;

export const failingInAFileInstalledPackage = makeTest(
  "a task from a package copied into node_modules prints no location inside the package, and one node_modules links to a folder outside it prints the full path of the line in that folder, whether the run starts in the project or from the package's own script",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const project = yield* makeProject({
        "main.ts": 'export { format as default } from "@acme/tools";',
        ...Object.fromEntries(
          Object.entries(FILE_INSTALLED_PACKAGE).map(([name, text]) => [
            `node_modules/@acme/tools/${name}`,
            text,
          ]),
        ),
      });
      const linkedDirectory = yield* makeScratchDirectory;
      for (const [name, text] of Object.entries(FILE_INSTALLED_PACKAGE)) {
        yield* write(`${linkedDirectory}/${name}`, text);
      }
      const runEveryWay = Effect.forEach(
        ["main.ts", "node_modules/@acme/tools/scripts/tools.ts"],
        (file) =>
          Effect.map(runTaskProcess(project, file), (ran) => findErrorLines(ran.standardError)),
      );
      const fromTheCopy = yield* runEveryWay;
      yield* fileSystem.remove(`${project.directory}/node_modules/@acme/tools`, {
        recursive: true,
      });
      yield* fileSystem.symlink(linkedDirectory, `${project.directory}/node_modules/@acme/tools`);
      const fromTheLink = yield* runEveryWay;
      for (const errorLines of fromTheCopy) {
        expect(errorLines).toEqual([
          "error: [format] ToolExited: fmt exited with code 1",
          "error: [format] its output is in <run folder>/1.output",
        ]);
      }
      for (const [firstLine, ...otherLines] of fromTheLink) {
        expect(firstLine).toMatch(
          new RegExp(
            `^error: \\[format\\] ${RegExp.escape(linkedDirectory)}/src/format\\.ts:4:\\d+: ToolExited: fmt exited with code 1$`,
            "u",
          ),
        );
        expect(otherLines).toEqual(["error: [format] its output is in <run folder>/1.output"]);
      }
      const brokenOnAWorker = yield* runTaskProcess(
        project,
        "node_modules/@acme/tools/scripts/broken.ts",
      );
      expect(findErrorLines(brokenOnAWorker.standardError)).toEqual([
        `error: [broken] ${linkedDirectory}/scripts/broken.ts:4:30: Error: worker import broke`,
        "error: [broken] could not import task module",
        "error: [broken] its output is in <run folder>/1.output",
      ]);
    }),
  ),
);

export const failingInAnInstalledPackageStartedOutsideTheProject = makeTest(
  "a task from an installed package prints no location inside the package when the run starts in a folder with no package.json above it or inside the package's own folder",
  Effect.scoped(
    Effect.gen(function* () {
      const packageDirectory = "node_modules/@acme/tools";
      const project = yield* makeProject({
        "main.ts": 'export { format as default } from "@acme/tools";',
        ...Object.fromEntries(
          Object.entries(FILE_INSTALLED_PACKAGE).map(([name, text]) => [
            `${packageDirectory}/${name}`,
            text,
          ]),
        ),
      });
      const directoryWithoutPackageJson = yield* makeTemporaryDirectory();
      for (const workingDirectory of [
        directoryWithoutPackageJson,
        `${project.directory}/${packageDirectory}`,
      ]) {
        const ran = yield* runTaskProcess(
          project,
          `${packageDirectory}/scripts/tools.ts`,
          {},
          workingDirectory,
        );
        expect(findErrorLines(ran.standardError), workingDirectory).toEqual([
          "error: [format] ToolExited: fmt exited with code 1",
          "error: [format] its output is in <run folder>/1.output",
        ]);
      }
    }),
  ),
);

const findLinesNamingPaths = (standardError: string) =>
  standardError
    .split("\n")
    .filter((line) => /^(?:error: |\[lib\] \S+: warn )/u.test(line))
    .filter((line) => !line.includes("its output is in"))
    .map((line) => line.replaceAll(/(?<fileAndLine>\.ts:\d+):\d+:/gu, "$<fileAndLine>:<column>:"))
    .toSorted();

export const pathsFromTheStartDirectory = makeTest(
  "a printed path is relative to the folder the run started in, climbs with ../ to a file of the project outside it, and prints in full outside the project or when the run starts outside it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "package.json": '{ "name": "monorepo", "type": "module", "workspaces": ["apps/*"] }',
        "apps/web/package.json": '{ "name": "web", "type": "module" }',
        "apps/web/lib/fail.ts": `import { Data } from "effect";
class LibFailed extends Data.TaggedError("LibFailed")<{ readonly message: string }> {}
export const fail = () => new LibFailed({ message: "broke in lib" });`,
        "apps/web/scripts/ci.ts": `import { Cause, Effect } from "effect";
import { Task } from "@sshkeda/task";
import { ReportedErrors } from "@sshkeda/task/errors";
import { fail } from "../lib/fail.ts";
export const here = Task.make({ name: "here", run: Effect.fail(new Error("broke here")) });
export const lib = Task.make({ name: "lib", isolated: true, run: Effect.gen(function* () {
  yield* Effect.logWarning("retrying", Cause.fail(fail()));
  return yield* Effect.fail(fail());
}) });
export const build = Task.make({ name: "build", run: Task.exec("sh", ["-c", "exit ${FAILED_COMMAND_EXIT_CODE}"]) });
export const elsewhere = Task.make({ name: "elsewhere", run: Effect.fail(new ReportedErrors({ reportedErrors: [{ tag: "Outside", location: "/outside/x.ts:1:2", message: "outside the project" }] })) });
export default Task.make({ name: "main", run: Task.allSettled([here, lib, build, elsewhere]) });`,
      });
      const expectedLines = (scripts: string, library: string) => [
        `[lib] ${library}fail.ts:3:<column>: warn LibFailed: broke in lib`,
        `error: [build] ${scripts}ci.ts:10:<column>: CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}`,
        "error: [elsewhere] /outside/x.ts:1:<column>: Outside: outside the project",
        `error: [here] ${scripts}ci.ts:5:<column>: Error: broke here`,
        `error: [lib] ${library}fail.ts:3:<column>: LibFailed: broke in lib`,
      ];
      const directoryWithoutPackageJson = yield* makeTemporaryDirectory();
      for (const [startDirectory, scripts, library] of [
        [project.directory, "apps/web/scripts/", "apps/web/lib/"],
        [`${project.directory}/apps`, "web/scripts/", "web/lib/"],
        [`${project.directory}/apps/web`, "scripts/", "lib/"],
        [`${project.directory}/apps/web/scripts`, "", "../lib/"],
        [
          directoryWithoutPackageJson,
          `${project.directory}/apps/web/scripts/`,
          `${project.directory}/apps/web/lib/`,
        ],
      ] as const) {
        const ran = yield* runTaskProcess(project, "apps/web/scripts/ci.ts", {}, startDirectory);
        expect(ran.exitCode, startDirectory).toBe(1);
        expect(findLinesNamingPaths(ran.standardError), startDirectory).toEqual(
          expectedLines(scripts, library).toSorted(),
        );
      }
    }),
  ),
);

export const failingInASiblingPackage = makeTest(
  "an error thrown in a sibling workspace package outside the project the run starts in prints the full path of the line that threw it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "package.json": '{ "name": "monorepo", "type": "module", "workspaces": ["packages/*"] }',
        "packages/app/package.json": '{ "name": "app", "type": "module" }',
        "packages/shared/package.json": '{ "name": "shared", "type": "module" }',
        "packages/shared/bug.ts": `export const compile = () => {
  throw new Error("SIBLING_MARK");
};`,
        "packages/app/sibling.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { compile } from "../shared/bug.ts";
export default Task.make({ name: "build", run: Effect.sync(compile) });`,
      });
      const ran = yield* runTaskProcess(
        project,
        "packages/app/sibling.ts",
        {},
        `${project.directory}/packages/app`,
      );
      expect(ran.exitCode).toBe(1);
      expect(findErrorLines(ran.standardError)).toEqual([
        `error: [build] ${project.directory}/packages/shared/bug.ts:2:13: Error: SIBLING_MARK`,
        "error: [build] its output is in <run folder>/1.output",
      ]);
    }),
  ),
);

const withColumnsNamed = (lines: readonly string[]) =>
  lines.map((line) =>
    line.replaceAll(/(?<fileAndLine>\.ts:\d+):\d+:/gu, "$<fileAndLine>:<column>:"),
  );

const NAMED_LIKE_TASK_ERRORS = {
  "error-named.ts":
    'Effect.fail(Object.assign(new Error("NAMED_MARK"), { name: "AwaitedTaskFailed" }))',
  "error-tagged.ts":
    'Effect.fail(Object.assign(new Error("TAGGED_MARK"), { _tag: "SettledTasksFailed" }))',
} as const;

export const userErrorsNamedLikeTaskErrors = makeTest(
  "a task's own error named or tagged like the error Task fails a task with when a task it awaited failed prints, while Task's own such error does not",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        ...Object.fromEntries(
          Object.entries(NAMED_LIKE_TASK_ERRORS).map(([file, failure]) => [
            file,
            `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: ${failure} });`,
          ]),
        ),
        "with-awaited.ts": `import { Cause, Effect, Exit } from "effect";
import { Task } from "@sshkeda/task";
const dep = Task.make({ name: "dep", run: Effect.fail(new Error("DEP_MARK")) });
const own = Object.assign(new Error("OWN_MARK"), { name: "AwaitedTaskFailed" });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const depExit = yield* Effect.exit(dep);
  return yield* Effect.failCause(Exit.isFailure(depExit) ? Cause.combine(depExit.cause, Cause.fail(own)) : Cause.fail(own));
}) });`,
      });
      for (const [file, line] of [
        ["error-named.ts", "AwaitedTaskFailed: NAMED_MARK"],
        ["error-tagged.ts", "SettledTasksFailed: TAGGED_MARK"],
      ] as const) {
        const ran = yield* runTaskProcess(project, file);
        expect(ran.exitCode, file).toBe(1);
        expect(withColumnsNamed(findErrorLines(ran.standardError)), file).toEqual([
          `error: [main] ${file}:3:<column>: ${line}`,
          "error: [main] its output is in <run folder>/1.output",
        ]);
        expect(
          withColumnsNamed(ran.standardError.split("\n")).filter((recap) =>
            recap.startsWith("recap: "),
          ),
          file,
        ).toEqual([`recap: [main] 1.output | ${file}:3:<column>: ${line}`]);
      }
      const withAwaited = yield* runTaskProcess(project, "with-awaited.ts");
      expect(withAwaited.exitCode).toBe(1);
      const mainErrorLines = findErrorLines(withAwaited.standardError).filter((line) =>
        line.startsWith("error: [main] "),
      );
      expect(withColumnsNamed(mainErrorLines)).toEqual([
        "error: [main] with-awaited.ts:4:<column>: AwaitedTaskFailed: OWN_MARK",
        "error: [main] its output is in <run folder>/1.output",
      ]);
      expect(withAwaited.standardError).not.toContain("[dep] failed");
    }),
  ),
);

export const plainUserFailureNamedLikeTaskError = makeTest(
  "a task's own plain object failure named like the error Task fails a task with when a task it awaited failed prints",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.fail({ name: "AwaitedTaskFailed", message: "PLAIN_MARK" }) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(withColumnsNamed(findErrorLines(ran.standardError))).toEqual([
        "error: [main] main.ts:3:<column>: AwaitedTaskFailed: PLAIN_MARK",
        "error: [main] its output is in <run folder>/1.output",
      ]);
      expect(
        withColumnsNamed(ran.standardError.split("\n")).filter((recap) =>
          recap.startsWith("recap: "),
        ),
      ).toEqual(["recap: [main] 1.output | main.ts:3:<column>: AwaitedTaskFailed: PLAIN_MARK"]);
    }),
  ),
);

export const sourceFileNamedNodeModules = makeTest(
  "an error made in an extensionless source file named node_modules keeps its location in that file",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "lib/node_modules": `import { TaskError } from "@sshkeda/task/errors";
export const makeError = () =>
  new TaskError({ message: "made in lib/node_modules" });`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { makeError } from "./lib/node_modules";
export default Task.make({ name: "main", run: Effect.suspend(() => Effect.fail(makeError())) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(
        /^error: \[main\] lib\/node_modules:3:\d+: TaskError: made in lib\/node_modules$/mu,
      );
    }),
  ),
);

export const unsetConfigKey = makeTest(
  "an unset Config a task reads fails naming the key, and names the environment variable only when @sshkeda/task's environment config provider read it",
  Effect.scoped(
    Effect.gen(function* () {
      const cases = [
        {
          config: 'Config.String("HOST").pipe(Config.nested("TASK_TEST_UNSET_APP"))',
          lines:
            "main.ts:3:21: ConfigKeyNotSet: configuration key TASK_TEST_UNSET_APP.HOST is not set: the environment has no TASK_TEST_UNSET_APP_HOST",
        },
        {
          config: 'Config.schema(Schema.Struct({ x: Schema.String }), "TASK_TEST_UNSET_STRUCT")',
          lines:
            "main.ts:3:21: ConfigKeyNotSet: configuration key TASK_TEST_UNSET_STRUCT is not set",
        },
      ];
      for (const { config, lines } of cases) {
        const project = yield* makeProject({
          "main.ts": `import { Config, Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: ${config} });`,
        });
        const ran = yield* runTaskProcess(project, "main.ts");
        expect(ran.exitCode).toBe(1);
        expect(findLineBeforeTheEndedLine(ran.standardError)).toBe(`error: [main] ${lines}`);
        expect(ran.standardError).not.toContain("ConfigError");
        expect(findMisprintedErrorLines(ran.standardError)).toEqual([]);
      }
    }),
  ),
);

export const emptyValueForDefaultedConfig = makeTest(
  "an empty environment value is set, so a defaulted Config fails with Effect's ConfigError, which keeps the schema's allowed values, instead of using the default",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Config, Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.log("number " + (yield* Config.Int("TASK_TEST_NUMBER").pipe(Config.withDefault(3))));
  yield* Effect.log("mode " + (yield* Config.Literals(["dev", "prod"], "TASK_TEST_MODE").pipe(Config.withDefault("dev"))));
}) });`,
      });
      const unset = yield* runTaskProcess(project, "main.ts");
      expect(unset.standardError).toContain("[main] number 3");
      expect(unset.standardError).toContain("[main] mode dev");
      const emptyMode = yield* runTaskProcess(project, "main.ts", { TASK_TEST_MODE: "" });
      expect(emptyMode.exitCode).toBe(1);
      expect(emptyMode.standardError).toContain('\nerror: [main]   at ["TASK_TEST_MODE"])\n');
      expect(findErrorLines(emptyMode.standardError)[0]).toBe(
        'error: [main] main.ts:3:21: ConfigError: SchemaError(Expected "dev" | "prod"',
      );
      const emptyNumber = yield* runTaskProcess(project, "main.ts", { TASK_TEST_NUMBER: "" });
      expect(emptyNumber.exitCode).toBe(1);
      expect(emptyNumber.standardError).toContain('\nerror: [main]   at ["TASK_TEST_NUMBER"])\n');
      expect(findErrorLines(emptyNumber.standardError)[0]).toBe(
        "error: [main] main.ts:3:21: ConfigError: SchemaError(Expected a string representing a finite number",
      );
    }),
  ),
);

export const configSetWithWrongShape = makeTest(
  "a Config whose key is set to a value of the wrong shape, or that a task's own ConfigProvider reads, is not reported as unset",
  Effect.scoped(
    Effect.gen(function* () {
      for (const config of [
        'Effect.provide(Config.String("X"), ConfigProvider.layer(ConfigProvider.fromUnknown({ X: { a: 1 } })))',
        'Effect.provide(Config.String("X"), ConfigProvider.layer(ConfigProvider.fromUnknown({})))',
      ]) {
        const project = yield* makeProject({
          "main.ts": `import { Config, ConfigProvider, Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: ${config} });`,
        });
        const ran = yield* runTaskProcess(project, "main.ts");
        expect(ran.exitCode).toBe(1);
        expect(ran.standardError).toContain("error: [main] main.ts:3:21: ConfigError:");
        expect(ran.standardError).not.toContain("ConfigKeyNotSet");
      }
    }),
  ),
);

export const realErrorsInsteadOfWrappers = makeTest(
  "a thrown Error, a typed failure, a thrown non-Error and a failed cleanup each print the real error's type, message and line under the task's name, with what Task adds on a line of its own, and the trace records the same type",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Data, Effect } from "effect";
import { Task } from "@sshkeda/task";
class BuildBroke extends Data.TaggedError("BuildBroke")<{ readonly message: string }> {}
export const migrate = Task.make({ name: "migrate", run: Effect.sync(() => { throw new Error("the database is not reachable"); }) });
export const build = Task.make({ name: "build", run: Effect.fail(new BuildBroke({ message: "build broke" })) });
export const deploy = Task.make({ name: "deploy", run: Effect.sync(() => { throw "the deploy key expired"; }) });
export const db = Task.make({ name: "db", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.sync(() => { throw new Error("the container did not stop"); }));
}) });
export default Task.make({ name: "main", run: Task.allSettled([migrate, build, deploy, db]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      const lines = ran.standardError.split("\n");
      expect(lines).toContain(
        "error: [migrate] main.ts:4:88: Error: the database is not reachable",
      );
      expect(lines).toContain("error: [build] main.ts:5:70: BuildBroke: build broke");
      expect(lines).toContain(
        "error: [deploy] main.ts:6:28: UntaggedFailure: the deploy key expired",
      );
      expect(ran.standardError).toContain(
        "error: [db] main.ts:8:66: Error: the container did not stop\nerror: [db] its cleanup failed\n",
      );
      expect(ran.standardError).not.toMatch(
        /TaskBodyFailed|TaskCleanupFailed|its body failed with/u,
      );
      expect(findMisprintedErrorLines(ran.standardError)).toEqual([]);
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      expect(
        ["migrate", "build", "deploy", "db"].map((taskName) =>
          readAttribute(trace?.findTaskSpan("task", taskName)?.attributes ?? [], "error.type"),
        ),
      ).toEqual(["Error", "BuildBroke", "UntaggedFailure", "Error"]);
    }),
  ),
);

export const realErrorsOfModulesAndWorkers = makeTest(
  "a task module that throws on import and an uncaught error that stops a worker print the real error's type, message and line, then what Task was doing on a line of its own",
  Effect.scoped(
    Effect.gen(function* () {
      const importing = yield* makeProject({
        "setup.ts": `import { isMainThread } from "node:worker_threads";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const setup = Task.make({ name: "setup", run: Effect.void });
if (!isMainThread) throw new TypeError("config.port is not a number");`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { setup } from "./setup.ts";
export default Task.make({ name: "main", run: Effect.asVoid(setup) });`,
      });
      const importFailed = yield* runTaskProcess(importing, "main.ts");
      expect(importFailed.exitCode).toBe(1);
      expect(importFailed.standardError).toMatch(
        /^error: \[main\] setup\.ts:5:\d+: TypeError: config\.port is not a number\nerror: \[main\] could not import task module$/mu,
      );
      const [importTrace] = yield* readTraces(
        yield* readProjectRunsDirectory(importing, importFailed),
      );
      expect(readAttribute(importTrace?.findRunSpan()?.attributes ?? [], "error.type")).toBe(
        "TypeError",
      );
      const watching = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "watch", run: Effect.gen(function* () {
  setTimeout(() => { throw new RangeError("the watcher queue overflowed"); }, 0);
  yield* Effect.never;
}) });`,
      });
      const uncaught = yield* runTaskProcess(watching, "main.ts");
      expect(uncaught.exitCode).toBe(1);
      expect(uncaught.standardError).toMatch(
        /^error: \[watch\] main\.ts:4:32: RangeError: the watcher queue overflowed\nerror: \[watch\] its worker stopped on this uncaught error$/mu,
      );
      const [uncaughtTrace] = yield* readTraces(
        yield* readProjectRunsDirectory(watching, uncaught),
      );
      expect(
        readAttribute(uncaughtTrace?.findTaskSpan("task", "watch")?.attributes ?? [], "error.type"),
      ).toBe("RangeError");
    }),
  ),
);

export const realErrorKeepsItsOwnLocation = makeTest(
  "a task module that calls a helper which throws while a worker imports it prints the line in the helper that made the error, not the line of the module that called it, then that it could not import the module",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "config.ts": `export const readPort = (): number => {
  throw new TypeError("bad port");
};`,
        "main.ts": `import { isMainThread } from "node:worker_threads";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { readPort } from "./config.ts";
if (!isMainThread) readPort();
export default Task.make({ name: "main", run: Effect.void });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(
        /^error: \[main\] config\.ts:2:13: TypeError: bad port\nerror: \[main\] could not import task module$/mu,
      );
      expect(ran.standardError).not.toContain("main.ts:5");
    }),
  ),
);

export const nonErrorThrownOnImport = makeTest(
  "a task module that throws a value that is not an Error while it loads prints it as UntaggedFailure, then that it could not import the module, and the trace records UntaggedFailure",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "setup.ts": `import { isMainThread } from "node:worker_threads";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const setup = Task.make({ name: "setup", run: Effect.void });
if (!isMainThread) throw "configuration unavailable";`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { setup } from "./setup.ts";
export default Task.make({ name: "main", run: Effect.asVoid(setup) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(
        /^error: \[main\] \S+: UntaggedFailure: configuration unavailable\nerror: \[main\] could not import task module$/mu,
      );
      expect(ran.standardError).not.toContain("TaskModuleImportFailed");
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      expect(readAttribute(trace?.findRunSpan()?.attributes ?? [], "error.type")).toBe(
        "UntaggedFailure",
      );
    }),
  ),
);

export const rootStandardInputFailure = makeTest(
  "a root task whose standard input fails prints that failure with its own type, then that it could not read standard input, and the trace records the same type",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Stdio, Stream } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "reader", run: Effect.gen(function* () {
  const stdio = yield* Stdio.Stdio;
  return yield* Stream.mkString(Stream.decodeText(stdio.stdin));
}) });`,
        "from-a-directory.ts": `const shell = Bun.spawn(["sh", "-c", \`\${process.execPath} main.ts < .\`], { stdio: ["ignore", "inherit", "inherit"] });
process.exitCode = await shell.exited;`,
      });
      const ran = yield* runTaskProcess(project, "from-a-directory.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(
        /^error: \[reader\] main\.ts:3:21: Error: EISDIR: illegal operation on a directory, \w+ \(EISDIR\)\nerror: \[reader\] could not read standard input$/mu,
      );
      expect(ran.standardError).not.toContain("PlatformError");
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      expect(
        readAttribute(trace?.findTaskSpan("task", "reader")?.attributes ?? [], "error.type"),
      ).toBe("Error");
    }),
  ),
);

export const platformErrorCausedByItself = makeTest(
  "a standard input stream that fails with a PlatformError whose cause is itself still prints that error and which command's standard input failed, and the run ends",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { PlatformError, Stream } from "effect";
import { Task } from "@sshkeda/task";
const failure = PlatformError.systemError({ _tag: "Unknown", module: "Upstream", method: "read" });
Object.assign(failure.reason, { cause: failure });
export default Task.make({ name: "main", run: Task.exec("cat", [], { stdin: Stream.fail(failure) }) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(
        /^error: \[main\] main\.ts:3:\d+: PlatformError: Unknown: Upstream\.read\n(?:error: \[main\] .*\n)*error: \[main\] the standard input stream of cat failed$/mu,
      );
      expect(ran.standardError).toMatch(/^failed: \[main\] \(1 of 1 task\), [\d.]+s$/mu);
    }),
  ),
);

export const userPlatformErrorCarryingReportedErrors = makeTest(
  "a PlatformError of the task's own that carries ReportedErrors keeps its own headline, whether the body fails with it or a Task.exec standard input stream does",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, PlatformError, Stream } from "effect";
import { Task } from "@sshkeda/task";
import { ReportedErrors } from "@sshkeda/task/errors";
const rejected = () => PlatformError.systemError({ _tag: "PermissionDenied", module: "Cloud", method: "deploy", pathOrDescriptor: "production", description: "production deployment rejected", cause: new ReportedErrors({ reportedErrors: [{ tag: "ValidationError", message: "invalid credentials", location: "/project/validate.ts:8:5" }] }) });
export const deploy = Task.make({ name: "deploy", run: Effect.fail(rejected()) });
export const upload = Task.make({ name: "upload", run: Task.exec("cat", [], { stdin: Stream.fail(rejected()) }) });
export default Task.make({ name: "main", run: Task.allSettled([deploy, upload]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      const headline =
        "PlatformError: PermissionDenied: Cloud.deploy (production): production deployment rejected";
      expect(ran.standardError).toContain(
        `error: [deploy] main.ts:5:68: ${headline}\nerror: [deploy] caused by ReportedErrors: invalid credentials\n`,
      );
      expect(ran.standardError).toContain(
        `error: [upload] main.ts:6:98: ${headline}\nerror: [upload] caused by ReportedErrors: invalid credentials\nerror: [upload] the standard input stream of cat failed\n`,
      );
      expect(ran.standardError).not.toMatch(/^error: (?:\[\w+\] )?\/project\/validate\.ts:8:5: /mu);
    }),
  ),
);

export const userErrorLocations = makeTest(
  "a user error, tagged error or defect prints the user line that made it, and a module that throws on import prints its line",
  Effect.scoped(
    Effect.gen(function* () {
      for (const [failure, tag] of [
        ['Effect.fail(new Error("boom"))', "Error"],
        ['Effect.fail(new Deploy({ reason: "boom" }))', "Deploy"],
        ['Effect.sync(() => { throw new TypeError("boom"); })', "TypeError"],
        ['Effect.fail(new UserNotFound({ name: "alice" }))', "UserNotFound"],
        ['Effect.fail(new BucketMissing({ location: "us-east-1" }))', "BucketMissing"],
        [
          'Effect.fail(new Wrapped({ reportedErrors: [{ tag: "Fake", message: "fake" }] }))',
          "Wrapped",
        ],
      ] as const) {
        const project = yield* makeProject({
          "main.ts": `import { Data, Effect } from "effect";
import { Task } from "@sshkeda/task";
class Deploy extends Data.TaggedError("Deploy")<{ readonly reason: string }> {}
class UserNotFound extends Data.TaggedError("UserNotFound")<{ readonly name: string }> {}
class BucketMissing extends Data.TaggedError("BucketMissing")<{ readonly location: string }> {}
class Wrapped extends Data.TaggedError("Wrapped")<{ readonly reportedErrors: ReadonlyArray<unknown> }> {}
export default Task.make({ name: "main", run: ${failure} });`,
        });
        const ran = yield* runTaskProcess(project, "main.ts");
        expect(ran.exitCode).toBe(1);
        expect(ran.standardError).toMatch(
          new RegExp(String.raw`^error: \[main\] main\.ts:7:\d+: ${tag}: `, "mu"),
        );
      }
      const throwing = yield* makeProject({
        "throws.ts": `import { isMainThread } from "node:worker_threads";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const thrown = Task.make({ name: "thrown", run: Effect.void });
if (!isMainThread) throw new Error("module refused");`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { thrown } from "./throws.ts";
export default Task.make({ name: "main", run: Effect.asVoid(thrown) });`,
      });
      const importFailed = yield* runTaskProcess(throwing, "main.ts");
      expect(importFailed.exitCode).toBe(1);
      expect(importFailed.standardError).toMatch(
        /^error: \[main\] throws\.ts:5:\d+: Error: module refused\nerror: \[main\] could not import task module$/mu,
      );
    }),
  ),
);

export const failuresWithoutAName = makeTest(
  "a thrown system error prints Node's text with its path and its error code, and a failure value without a message prints as JSON cut to 200 characters",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { readFileSync } from "node:fs";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const read = Task.make({ name: "read", run: Effect.sync(() => readFileSync("content/contact.html", "utf8")) });
export const invalid = Task.make({ name: "invalid", run: Effect.fail({ service: "worker", field: "port", problem: "is not a port number" }) });
export const long = Task.make({ name: "long", run: Effect.fail({ values: Array.from({ length: 100 }, (_unused, index) => index) }) });
export default Task.make({ name: "main", run: Task.allSettled([read, invalid, long]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(
        /^error: \[read\] main\.ts:4:26: Error: ENOENT: no such file or directory, open 'content\/contact\.html' \(ENOENT\)$/mu,
      );
      expect(ran.standardError).toContain(
        'error: [invalid] main.ts:5:29: UntaggedFailure: {"service":"worker","field":"port","problem":"is not a port number"}\n',
      );
      const longValue = yield* encodeJson({
        values: Array.from({ length: 100 }, (_unused, index) => index),
      });
      expect(ran.standardError).toContain(
        `error: [long] main.ts:6:26: UntaggedFailure: ${longValue.slice(0, FAILURE_VALUE_CHARACTER_LIMIT - 1)}…\n`,
      );
      expect(findMisprintedErrorLines(ran.standardError)).toEqual([]);
    }),
  ),
);

export const cyclicErrorCauses = makeTest(
  "an error whose cause contains a cycle keeps its original message and stack",
  Effect.scoped(
    Effect.gen(function* () {
      for (const cause of ["failure", 'new Error("inner failure", { cause: failure })']) {
        const project = yield* makeProject({
          "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.sync(() => {
  const failure = new Error("original cyclic failure");
  failure.cause = ${cause};
  throw failure;
}) });`,
        });
        const ran = yield* runTaskProcess(project, "main.ts");
        expect(ran.exitCode).toBe(1);
        expect(ran.standardError).toMatch(
          /^error: \[main\] main\.ts:4:\d+: Error: original cyclic failure$/mu,
        );
        expect(ran.standardError).not.toContain("RangeError");
        const tracePath = yield* Effect.fromNullishOr(findTraceFilePath(ran.standardError));
        const fileSystem = yield* FileSystem.FileSystem;
        const trace = yield* fileSystem.readFileString(tracePath);
        expect(trace).toContain("original cyclic failure");
        expect(trace).toContain("main.ts:4:");
      }
    }),
  ),
);

export const deepErrorCauses = makeTest(
  "an error with a cause chain 20,000 deep is reported with every cause in the trace, whatever its messages say",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
let failure = new Error("deepest cause from Generator.next");
for (let depth = 0; depth < 20_000; depth++) failure = new Error(\`cause \${depth}\`, { cause: failure });
export default Task.make({ name: "main", run: Effect.fail(failure) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(/^error: \[main\] main\.ts:4:\d+: Error: cause 19999$/mu);
      const tracePath = yield* Effect.fromNullishOr(findTraceFilePath(ran.standardError));
      const fileSystem = yield* FileSystem.FileSystem;
      const trace = yield* fileSystem.readFileString(tracePath);
      expect(trace).toContain("[cause]: Error: deepest cause from Generator.next");
      expect(trace).not.toContain("Error: Error: cause 19999");
      expect(ran.standardError).not.toContain("still stopping");
    }),
  ),
);

export const deepPlainObjectCauses = makeTest(
  "an error whose cause chain is 20,000 plain objects deep is reported with every cause in the trace",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
let cause: object = { message: "deepest cause" };
for (let depth = 0; depth < 20_000; depth++) cause = { message: \`cause \${depth}\`, cause };
export default Task.make({ name: "main", run: Effect.fail(new Error("deploy failed", { cause })) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      const tracePath = yield* Effect.fromNullishOr(findTraceFilePath(ran.standardError));
      const fileSystem = yield* FileSystem.FileSystem;
      const trace = yield* fileSystem.readFileString(tracePath);
      expect(trace).toContain("deepest cause");
      expect(ran.standardError).not.toContain("still stopping");
    }),
  ),
);

export const thrownValuesWithoutMessages = makeTest(
  "an empty thrown string or Error says that the thrown value has no message",
  Effect.scoped(
    Effect.gen(function* () {
      for (const [failure, tag] of [
        ['throw "";', "UntaggedFailure"],
        ["throw new Error();", "Error"],
      ] as const) {
        const project = yield* makeProject({
          "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.sync(() => { ${failure} }) });`,
        });
        const ran = yield* runTaskProcess(project, "main.ts");
        expect(ran.exitCode).toBe(1);
        expect(findLineBeforeTheEndedLine(ran.standardError)).toMatch(
          new RegExp(
            String.raw`^error: \[main\] main\.ts:3:\d+: ${tag}: a thrown value that has no message$`,
            "u",
          ),
        );
      }
    }),
  ),
);

export const defectsWithoutSourceExcerpts = makeTest(
  "a TypeError from the engine or from the task prints its whole message, source excerpt included",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const notAnEffect: any = [];
export const piped = Task.make({ name: "piped", run: Effect.suspend(() => notAnEffect.pipe(
  Effect.asVoid,
)) });
export const deploy = Task.make({ name: "deploy", run: Effect.sync(() => { throw new Error("upload rejected (In 'us-east-1': quota exceeded)"); }) });
export const configure = Task.make({ name: "configure", run: Effect.sync(() => { throw new TypeError("invalid setting (In 'production')"); }) });
export default Task.make({ name: "main", run: Task.allSettled([piped, deploy, configure]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(
        /^error: \[piped\] main\.ts:4:\d+: TypeError: notAnEffect\.pipe is not a function\. \(In 'notAnEffect\.pipe\(Effect\.asVoid\)', 'notAnEffect\.pipe' is undefined\)$/mu,
      );
      expect(ran.standardError).toContain("upload rejected (In 'us-east-1': quota exceeded)");
      expect(ran.standardError).toMatch(
        /^error: \[configure\] main\.ts:\d+:\d+: TypeError: invalid setting \(In 'production'\)$/mu,
      );
    }),
  ),
);

export const wrappedCauseKeepsOuterMessage = makeTest(
  "a task's own error that wraps a TaskError as its cause is printed with its own message, tag and user line, without a detecting line",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Data, Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
class DeployFailed extends Data.TaggedError("DeployFailed")<{ readonly cause: TaskError }> {
  override get message() { return "deploy failed"; }
}
export default Task.make({ name: "main", run: Effect.fail(new DeployFailed({
  cause: new TaskError({ message: "inner failure" }),
})) });`,
      });
      const result = yield* runTaskProcess(project, "main.ts");
      expect(result.exitCode, result.standardError).toBe(1);
      expect(result.standardError).toMatch(
        /^error: \[main\] main\.ts:7:\d+: DeployFailed: deploy failed$/mu,
      );
      expect(result.standardError).toMatch(
        /^error: \[main\] caused by TaskError: inner failure$/mu,
      );
    }),
  ),
);

export const causeEndingTheWrapperMessage = makeTest(
  "a cause whose message ends the message of the error it caused is still printed with its own tag",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
class TlsHandshakeError extends Error { override name = "TlsHandshakeError"; }
export default Task.make({ name: "main", run: Effect.fail(new Error("request: timeout", { cause: new TlsHandshakeError("timeout") })) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(/^error: \[main\] caused by TlsHandshakeError: timeout$/mu);
    }),
  ),
);

export const failureCausesArePrinted = makeTest(
  "a failure from Effect.tryPromise without a catch prints the error it caught, such as a refused connection",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const server = Bun.serve({ port: 0, fetch: () => new Response() });
const closedPort = server.port;
await server.stop(true);
export default Task.make({ name: "main", run: Effect.tryPromise(() => fetch(\`http://127.0.0.1:\${closedPort}\`)) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(
        /^run: \S+\/\nerror: \[main\] main\.ts:6:21: UnknownError: An error occurred in Effect\.tryPromise\nerror: \[main\] caused by TypeError: ECONNREFUSED: Unable to connect\. Is the computer able to access the url\?\nerror: \[main\] its output is in \S+\/1\.output\nended: \[main\], [\d.]+s, failed\nrecap: \[main\] 1\.output \| main\.ts:6:21: UnknownError: An error occurred in Effect\.tryPromise\nfailed: /u,
      );
    }),
  ),
);

export const innerErrorsArePrinted = makeTest(
  "a failure from Promise.any prints each error it rejected with, up to five",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const fetchFromMirrors = () => Promise.any(["a", "b", "c", "d", "e", "f", "g"].map(async (mirror) => {
  throw new TypeError(\`mirror \${mirror} is down\`);
}));
export default Task.make({ name: "main", run: Effect.tryPromise(fetchFromMirrors) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(ran.standardError).toContain(
        [
          "error: [main] main.ts:6:21: UnknownError: An error occurred in Effect.tryPromise",
          "error: [main] caused by AggregateError",
          "error: [main]   TypeError: mirror a is down",
          "error: [main]   TypeError: mirror b is down",
          "error: [main]   TypeError: mirror c is down",
          "error: [main]   TypeError: mirror d is down",
          "error: [main]   TypeError: mirror e is down",
          "error: [main]   and 2 more",
          "error: [main] its output is in ",
        ].join("\n"),
      );
    }),
  ),
);

export const errorsFieldOfOtherErrorsIsNotPrinted = makeTest(
  "an error that is not an AggregateError prints no lines for an errors field it has",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Data, Effect } from "effect";
import { Task } from "@sshkeda/task";
class ConfigInvalid extends Data.TaggedError("ConfigInvalid")<{ readonly message: string; readonly errors: ReadonlyArray<string> }> {}
export default Task.make({ name: "main", run: Effect.fail(new ConfigInvalid({ message: "config is invalid", errors: ["port must be a number"] })) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(ran.standardError).toMatch(
        /^error: \[main\] main\.ts:4:\d+: ConfigInvalid: config is invalid$/mu,
      );
      expect(ran.standardError).not.toContain("port must be a number");
    }),
  ),
);

export const causesInsideTheMessageArePrinted = makeTest(
  "a cause whose message is only part of an earlier line, or is empty, is still printed",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Data, Effect } from "effect";
import { Task } from "@sshkeda/task";
class ConfigFailed extends Data.TaggedError("ConfigFailed")<{ readonly cause: unknown }> {
  override get message() { return "could not read config file"; }
}
export default Task.make({ name: "main", run: Effect.fail(new ConfigFailed({
  cause: new Error("file", { cause: new Error("") }),
})) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(ran.standardError).toMatch(
        /^error: \[main\] main\.ts:6:\d+: ConfigFailed: could not read config file\nerror: \[main\] caused by Error: file\nerror: \[main\] caused by Error$/mu,
      );
    }),
  ),
);

const USER_CONSTRUCTED_ERROR_CLASSES = new Set(["TaskError", "ReportedErrors"]);

export const everyTaskErrorTagIsConstructedInSource = makeTest(
  "every Task error class is tagged with its own name and constructed in src, so searching the shipped source for new <Tag>( finds where it is raised",
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const repository = yield* REPOSITORY_DIRECTORY;
    const sourceFilePaths = (yield* fileSystem.readDirectory(`${repository}/src`, {
      recursive: true,
    })).filter((sourceFilePath) => sourceFilePath.endsWith(".ts"));
    const sourceText = (yield* Effect.forEach(sourceFilePaths, (sourceFilePath) =>
      fileSystem.readFileString(`${repository}/src/${sourceFilePath}`),
    )).join("\n");
    const declarations = yield* Effect.forEach(
      sourceText.matchAll(
        /class (?<className>\w+) extends Schema\.TaggedError<\k<className>>\(\)\(\s*(?:"(?<tagLiteral>\w+)"|(?<tagConstant>\w+))/gu,
      ),
      (declaration) =>
        Schema.decodeUnknownEffect(
          Schema.Tuple([
            Schema.String,
            Schema.String,
            Schema.UndefinedOr(Schema.String),
            Schema.UndefinedOr(Schema.String),
          ]),
        )([...declaration]).pipe(
          Effect.map(([, className, tagLiteral, tagConstant]) => ({
            className,
            tagLiteral,
            tagConstant,
          })),
        ),
    );
    expect(declarations.length).toBeGreaterThan(FEWEST_EXPECTED_ERROR_CLASSES);
    expect(
      declarations.filter(({ className, tagLiteral, tagConstant }) =>
        tagLiteral === undefined
          ? !sourceText.includes(`const ${tagConstant} = "${className}";`)
          : tagLiteral !== className,
      ),
    ).toEqual([]);
    expect(
      declarations
        .map(({ className }) => className)
        .filter(
          (className) =>
            !USER_CONSTRUCTED_ERROR_CLASSES.has(className) &&
            !sourceText.includes(`new ${className}(`),
        ),
    ).toEqual([]);
  }),
);

const OUTPUT_TAIL = "OUTPUT_TAIL_FROM_THE_COMMAND";

const ERROR_TAIL = "ERROR_TAIL_FROM_THE_COMMAND";

const WRAPPING_LAYERS = 6;

const failedCommand = () =>
  new CommandFailed({
    command: "c",
    code: 1,
    standardOutput: `${OUTPUT_TAIL}\n`,
    standardError: `${ERROR_TAIL}\n`,
  });

const wrappedInLayers = (layerCount: number): Error =>
  layerCount === 0
    ? failedCommand()
    : new Error(`layer ${String(layerCount)}`, { cause: wrappedInLayers(layerCount - 1) });

const printedTexts = (failure: Error) => {
  const traced = describeTraceExceptions(causeWithPrintableErrors(Cause.fail(failure)));
  return [
    reportCause(Cause.fail(failure)).details,
    ...traced.flatMap(({ message, stacktrace }) => [message, stacktrace]),
  ].join("\n");
};

const traceErrorsOf = (failure: Error) =>
  describeTraceExceptions(causeWithPrintableErrors(Cause.fail(failure))).map(
    ({ errorType, message }) => ({ errorType, message }),
  );

export const prettyTextLeavesOutCommandOutput = makeTest(
  "the details and trace stack of a failed command leave out the output its message carries, through any number of wrapping causes, while the trace keeps each error's own type and message",
  Effect.sync(() => {
    const deeplyWrapped = wrappedInLayers(WRAPPING_LAYERS);
    const aggregate = new AggregateError([new Error("INNER_DIAGNOSTIC")], "aggregate", {
      cause: failedCommand(),
    });
    const standardInputFailure = new CommandStandardInputFailed({
      command: "c",
      cause: failedCommand(),
    });
    const throughAPlainRecord = new Error("deploy failed", {
      cause: { message: "command failed", cause: failedCommand() },
    });
    for (const failure of [
      failedCommand(),
      deeplyWrapped,
      aggregate,
      standardInputFailure,
      throughAPlainRecord,
    ]) {
      const printed = printedTexts(failure);
      expect(printed, failure.name).toContain("c exited with code 1");
      expect(printed, failure.name).not.toContain(OUTPUT_TAIL);
      expect(printed, failure.name).not.toContain(ERROR_TAIL);
    }
    expect(traceErrorsOf(aggregate)).toEqual([
      {
        errorType: "AggregateError",
        message:
          "aggregate\n  Error: INNER_DIAGNOSTIC\ncaused by CommandFailed: c exited with code 1",
      },
    ]);
    expect(traceErrorsOf(standardInputFailure)).toEqual([
      {
        errorType: "CommandFailed",
        message: "c exited with code 1\nthe standard input stream of c failed",
      },
    ]);
    const looped = new Error("looped");
    const loopedBack = new Error("looped back", { cause: looped });
    Object.assign(looped, { cause: loopedBack });
    expect(reportCause(Cause.fail(looped)).details).toContain("looped back");
  }),
);

export const plainRecordCauseKeepsOutputOutOfTheTrace = makeTest(
  "a failed command wrapped through a plain record with a cause prints its output once and its trace holds it once",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "echo PLAIN_RECORD_OUT; echo PLAIN_RECORD_ERR >&2; exit 1"]).pipe(Effect.mapError((cause) => new Error("deploy failed", { cause: { message: "command failed", cause } }))) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      const tracePath = yield* Effect.fromNullishOr(findTraceFilePath(ran.standardError));
      const trace = yield* (yield* FileSystem.FileSystem).readFileString(tracePath);
      expect(trace).toContain("deploy failed");
      for (const marker of ["PLAIN_RECORD_OUT", "PLAIN_RECORD_ERR"]) {
        expect(countOccurrences(ran.standardError, marker), marker).toBe(1);
        expect(countOccurrences(trace, marker), marker).toBe(1);
      }
    }),
  ),
);

export const printedErrorFormatCheck = makeTest(
  "the printed-error format check refuses an error line without the error label, a tag or a message, and a hint line under any error, CommandFailed included, whose printed message never carries the command's output",
  Effect.sync(() => {
    expect(
      new CommandFailed({ command: "c", code: 1, standardOutput: "", standardError: "" }).message,
    ).toBe("c exited with code 1");
    const failed = new CommandFailed({
      command: "c",
      code: 1,
      standardOutput: "out",
      standardError: "err",
    });
    expect(failed.message).toBe(
      "c exited with code 1\nstandard output:\nout\nstandard error:\nerr",
    );
    expect(failed.printedMessage).toBe("c exited with code 1");
    expect([failed.standardOutput, failed.standardError]).toEqual(["out", "err"]);
    const commandFailure =
      "error: [main] CommandFailed: sh exited with code 1\nerror: [main] its output is in /runs/project/run/1.output";
    for (const printed of [
      "error: [main] TaskError: it broke\nerror: [main] fatal: broken\nerror: [main] hint: printed by a tool",
      `${commandFailure}\nerror: [main] hint: printed by the command`,
      `${commandFailure}\nerror: [main] TaskError: second\nerror: [main] hint: written by task`,
      "error: [main] TaskError: first\nerror: [main]\nerror: [main]   hint: written by task",
      "error: TaskError: first\nerror:\nerror: hint: written by task",
      "error TaskError: it broke",
      "TaskError: it broke",
      "error: [main] it broke",
      "error: [main] main: TaskError: it broke",
    ]) {
      expect(() => {
        expect(findMisprintedErrorLines(printed)).toEqual([]);
      }, printed).toThrow();
    }
    expect(findMisprintedErrorLines(commandFailure)).toEqual([]);
    expect(
      findMisprintedErrorLines(
        "[main] error TaskError: logged by the task\nerror: [main] main.ts:1:1: TaskError: it broke\nfailed: [main] (1 of 1 task), 0.1s\nrun: /runs/project/run/",
      ),
    ).toEqual([]);
  }),
);

export const directoryWithSpace = makeTest(
  "errors from a project directory whose name has a space keep their locations, including ReportedErrors violations",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "sp ace/package.json": '{ "name": "spaced", "type": "module" }',
        "sp ace/main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { ReportedErrors } from "@sshkeda/task/errors";
export const thrown = Task.make({ name: "thrown", run: Effect.fail(new Error("boom")) });
export const checked = Task.make({ name: "checked", run: Effect.fail(new ReportedErrors({ reportedErrors: [
  { tag: "LintViolation", location: import.meta.dir + "/a b.ts:1:2", message: "first violation", hint: "use Effect FileSystem" },
  { tag: "OtherViolation", location: import.meta.dir + "/c d.ts:4:5", message: "second violation" },
] })) });
export default Task.make({ name: "main", run: Task.allSettled([thrown, checked]) });`,
      });
      const spacedDirectory = `${project.directory}/sp ace`;
      const ran = yield* runTaskProcess(project, "sp ace/main.ts", {}, spacedDirectory);
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(/^error: \[thrown\] main\.ts:4:\d+: Error: boom$/mu);
      expect(ran.standardError).toContain(
        "error: [checked] a b.ts:1:2: LintViolation: first violation\nerror: [checked] c d.ts:4:5: OtherViolation: second violation\n",
      );
    }),
  ),
);

const splitTheEndOfTheRun = (standardError: string) => {
  const [firstLine, ...lines] = standardError.trimEnd().split("\n");
  const summaryIndex = lines.findLastIndex((line) => SUMMARY_LINE.test(line));
  const [summaryLine = "", runLine = "", ...linesAfterTheRunLine] = lines.slice(summaryIndex);
  expect(firstLine, standardError).toBe(runLine);
  const linesBeforeTheSummary = lines
    .slice(0, summaryIndex)
    .map((line) => withRunLinePathNamed(withSecondsNamed(line), standardError));
  return {
    linesBeforeTheSummary,
    summaryLine,
    runLine,
    linesAfterTheRunLine,
    errorLines: linesBeforeTheSummary.filter((line) => line.startsWith("error: ")),
    recapLines: linesBeforeTheSummary.filter((line) => line.startsWith("recap: ")),
  };
};

const expectEachEndedLineRightAfter = (
  lines: readonly string[],
  outputFileAndEndedLines: readonly (readonly [string, string])[],
) => {
  for (const [outputFileLine, endedLine] of outputFileAndEndedLines) {
    expect(lines[lines.indexOf(outputFileLine) + 1], lines.join("\n")).toBe(endedLine);
  }
};

const readTaskOutputFile = (
  project: Parameters<typeof readProjectRunsDirectory>[0],
  ran: Parameters<typeof readProjectRunsDirectory>[1],
  taskName: string,
) =>
  Effect.gen(function* () {
    const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
    const fileName = readAttribute(
      trace?.findTaskSpan("task", taskName)?.attributes ?? [],
      "sshkeda_task.output.file.name",
    );
    const runDirectory = yield* Effect.fromNullishOr(findRunDirectoryPath(ran.standardError));
    return withRunLinePathNamed(
      withoutStackFrames(yield* read(`${runDirectory}/${String(fileName)}`)),
      ran.standardError,
    );
  });

const runFailureReasonProject = (rootTaskBody: string) =>
  Effect.gen(function* () {
    const project = yield* makeProject({
      "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
class EvidenceRejected extends Schema.TaggedError<EvidenceRejected>()("EvidenceRejected", { message: Schema.String }) {}
export const checkArchive = Task.make({ name: "check-archive", run: Effect.fail(new EvidenceRejected({ message: "archive reference is required" })) });
export const checkB = Task.make({ name: "b", run: Effect.fail(new EvidenceRejected({ message: "b rejected the evidence" })) });
export const checkLater = Task.make({ name: "check-later", run: Effect.sleep("2 seconds").pipe(Effect.andThen(Effect.fail(new EvidenceRejected({ message: "later failure" })))) });
export default Task.make({ name: "main", run: ${rootTaskBody} });`,
    });
    const ran = yield* runTaskProcess(project, "main.ts");
    return { ...ran, ...splitTheEndOfTheRun(ran.standardError), project };
  });

export const failureReasonOfRootTask = makeTest(
  "a failed run prints its error once as error: [task] location: Tag: message, then its recap line, the summary and the run line naming the run folder",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runFailureReasonProject(
        `Effect.fail(new EvidenceRejected({ message: "archive reference is required" }))`,
      );
      expect(ran.exitCode).toBe(1);
      expect(ran.summaryLine).toMatch(/^failed: \[main\] \(1 of 1 task\), [\d.]+s$/u);
      expect(ran.runLine).toMatch(/^run: \/.+\/runs\/project\/[^/]+\/$/u);
      expect(ran.linesAfterTheRunLine).toEqual([]);
      expect(ran.linesBeforeTheSummary).toEqual([
        `error: ${ROOT_TASK_FAILURE_PREFIX}: EvidenceRejected: archive reference is required`,
        MAIN_OUTPUT_FILE_LINE,
        endedLineOf("main", "failed"),
        recapLineOf(
          `error: ${ROOT_TASK_FAILURE_PREFIX}: EvidenceRejected: archive reference is required`,
          "1.output",
        ),
      ]);
      expect(countOccurrences(ran.standardError, "archive reference is required")).toBe(
        ERROR_AND_RECAP_LINES,
      );
    }),
  ),
);

export const failureReasonOfAwaitedTask = makeTest(
  "the run's error line names the original failure of the awaited task, not AwaitedTaskFailed wrapping it",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runFailureReasonProject(
        `Effect.all([checkArchive, checkLater], { concurrency: "unbounded", discard: true })`,
      );
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).not.toContain("AwaitedTaskFailed");
      expect(ran.runLine).toStartWith("run: ");
      expect(ran.errorLines).toEqual([
        `error: ${CHECK_ARCHIVE_FAILURE_PREFIX}: EvidenceRejected: archive reference is required`,
        outputFileLineOf("check-archive", "2.output"),
      ]);
      expect(ran.recapLines).toEqual([
        recapLineOf(
          `error: ${CHECK_ARCHIVE_FAILURE_PREFIX}: EvidenceRejected: archive reference is required`,
          "2.output",
        ),
      ]);
    }),
  ),
);

export const awaitedFailureWithFinalizerFailure = makeTest(
  "of an awaited task failure and an independent finalizer failure in the same cause, the awaited task's own failure and the finalizer failure each print once, not AwaitedTaskFailed, and the trace keeps both",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export const dependency = Task.make({ name: "dependency", run: Effect.fail(new TaskError({ message: "dependency broke" })) });
export default Task.make({ name: "main", run: Effect.scoped(Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.sync(() => { throw new Error("finalizer broke independently"); }));
  yield* dependency;
})) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).not.toContain("AwaitedTaskFailed");
      const dependencyErrorLine = "error: [dependency] main.ts:4:76: TaskError: dependency broke";
      const dependencyOutputFileLine = outputFileLineOf("dependency", "2.output");
      const mainErrorLine = "error: [main] main.ts:6:66: Error: finalizer broke independently";
      expectOneOfTheOrders(splitTheEndOfTheRun(ran.standardError).errorLines, [
        [dependencyErrorLine, dependencyOutputFileLine, mainErrorLine, MAIN_OUTPUT_FILE_LINE],
        [dependencyErrorLine, mainErrorLine, dependencyOutputFileLine, MAIN_OUTPUT_FILE_LINE],
        [dependencyErrorLine, mainErrorLine, MAIN_OUTPUT_FILE_LINE, dependencyOutputFileLine],
      ]);
      expectEachEndedLineRightAfter(splitTheEndOfTheRun(ran.standardError).linesBeforeTheSummary, [
        [dependencyOutputFileLine, endedLineOf("dependency", "failed")],
        [MAIN_OUTPUT_FILE_LINE, endedLineOf("main", "failed")],
      ]);
      const mainFailures = (yield* readTracedFailures(project, ran))
        .filter((traced) => traced.taskName === "main")
        .map((traced) => traced.failure)
        .join("\n");
      expect(mainFailures).toContain("AwaitedTaskFailed: [dependency] failed");
      expect(mainFailures).toContain("finalizer broke independently");
    }),
  ),
);

const countTracedAwaitedFailuresWhenAFinalizerAwaitsItToo = (mainBody: string) =>
  Effect.gen(function* () {
    const project = yield* makeProject({
      "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export const dependency = Task.make({ name: "dependency", run: Effect.fail(new TaskError({ message: "dependency broke" })) });
export const main = Task.make({ name: "main", run: ${mainBody} });
export default Task.make({ name: "root", run: Effect.andThen(main, Effect.void) });`,
    });
    const ran = yield* runTaskProcess(project, "main.ts");
    expect(ran.exitCode).toBe(1);
    expect(ran.standardError).not.toContain("AwaitedTaskFailed");
    expect(splitTheEndOfTheRun(ran.standardError).errorLines.join("\n")).toMatch(
      /^error: \[dependency\] main\.ts:4:\d+: TaskError: dependency broke\nerror: \[dependency\] its output is in <run folder>\/3\.output$/u,
    );
    return countOccurrences(
      (yield* readTracedFailures(project, ran))
        .filter((traced) => traced.taskName === "main")
        .map((traced) => traced.failure)
        .join("\n"),
      "AwaitedTaskFailed: [dependency] failed",
    );
  });

export const awaitedFailureAlsoAwaitedInAScopedFinalizer = makeTest(
  "a failed task awaited by the body and by a finalizer of its own scope is recorded once in the trace, and the run's only error line names its own failure",
  Effect.scoped(
    Effect.gen(function* () {
      const count = yield* countTracedAwaitedFailuresWhenAFinalizerAwaitsItToo(
        `Effect.scoped(Effect.gen(function* () {
  yield* Effect.addFinalizer(() => dependency.pipe(Effect.catch((error) => Effect.die(error))));
  yield* dependency;
}))`,
      );
      expect(count).toBe(1);
    }),
  ),
);

export const awaitedFailureAlsoAwaitedInATaskScopeFinalizer = makeTest(
  "a failed task awaited by the body and by a finalizer of the task scope is recorded once in the trace, and the run's only error line names its own failure",
  Effect.scoped(
    Effect.gen(function* () {
      const count = yield* countTracedAwaitedFailuresWhenAFinalizerAwaitsItToo(
        `Effect.gen(function* () {
  yield* Effect.addFinalizer(() => dependency.pipe(Effect.catch((error) => Effect.die(error))));
  yield* dependency;
})`,
      );
      expect(count).toBe(1);
    }),
  ),
);

export const failureReasonOfUncaughtFailure = makeTest(
  "a failure an awaiting task caught prints as an error line when it happens, before the failure that ended the run, and each goes to its own task's file",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runFailureReasonProject(
        `Effect.gen(function* () { yield* Effect.ignore(checkArchive); yield* checkB; })`,
      );
      expect(ran.exitCode).toBe(1);
      expect(withSecondsNamed(yield* readTaskOutputFile(ran.project, ran, "b"))).toBe(
        `${startedLineOf("b", "3.output")}\nerror: ${CHECK_B_FAILURE_PREFIX}: EvidenceRejected: b rejected the evidence\n${outputFileLineOf("b", "3.output")}\n${endedLineOf("b", "failed")}\n`,
      );
      expect(withSecondsNamed(yield* readTaskOutputFile(ran.project, ran, "check-archive"))).toBe(
        `${startedLineOf("check-archive", "2.output")}\nerror: ${CHECK_ARCHIVE_FAILURE_PREFIX}: EvidenceRejected: archive reference is required\n${outputFileLineOf("check-archive", "2.output")}\n${endedLineOf("check-archive", "failed")}\n`,
      );
      const checkArchiveErrorLine = `error: ${CHECK_ARCHIVE_FAILURE_PREFIX}: EvidenceRejected: archive reference is required`;
      const checkArchiveOutputFileLine = outputFileLineOf("check-archive", "2.output");
      const bErrorLine = `error: ${CHECK_B_FAILURE_PREFIX}: EvidenceRejected: b rejected the evidence`;
      const bOutputFileLine = outputFileLineOf("b", "3.output");
      const checkArchiveRecapLine = recapLineOf(checkArchiveErrorLine, "2.output");
      const bRecapLine = recapLineOf(bErrorLine, "3.output");
      expectOneOfTheOrders(
        [...ran.errorLines, ...ran.linesBeforeTheSummary.slice(-ran.recapLines.length)],
        [
          [
            checkArchiveErrorLine,
            checkArchiveOutputFileLine,
            bErrorLine,
            bOutputFileLine,
            checkArchiveRecapLine,
            bRecapLine,
          ],
          [
            checkArchiveErrorLine,
            bErrorLine,
            checkArchiveOutputFileLine,
            bOutputFileLine,
            checkArchiveRecapLine,
            bRecapLine,
          ],
          [
            checkArchiveErrorLine,
            bErrorLine,
            bOutputFileLine,
            checkArchiveOutputFileLine,
            bRecapLine,
            checkArchiveRecapLine,
          ],
        ],
      );
      expectEachEndedLineRightAfter(ran.linesBeforeTheSummary, [
        [checkArchiveOutputFileLine, endedLineOf("check-archive", "failed")],
        [bOutputFileLine, endedLineOf("b", "failed")],
      ]);
      expect(countOccurrences(ran.standardError, "archive reference is required")).toBe(
        ERROR_AND_RECAP_LINES,
      );
    }),
  ),
);

export const failureReasonOfMultiLineMessage = makeTest(
  "the error of a multi-line message prints its headline and then each later line as an error line, without trailing blank lines",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runFailureReasonProject(
        `Effect.fail(new EvidenceRejected({ message: "first line\\n  second line\\n\\nthird line\\n" }))`,
      );
      expect(ran.exitCode).toBe(1);
      expect(ran.runLine).toStartWith("run: ");
      expect(ran.linesBeforeTheSummary).toEqual([
        `error: ${ROOT_TASK_FAILURE_PREFIX}: EvidenceRejected: first line`,
        "error: [main]   second line",
        "error: [main]",
        "error: [main] third line",
        MAIN_OUTPUT_FILE_LINE,
        endedLineOf("main", "failed"),
        recapLineOf(`error: ${ROOT_TASK_FAILURE_PREFIX}: EvidenceRejected: first line`, "1.output"),
      ]);
    }),
  ),
);

export const failureReasonOfCommand = makeTest(
  "the error line of a failed command holds the command and its exit code, and its output prints once before it under the task's name, and again in its recap",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runFailureReasonProject(
        `Task.exec("sh", ["-c", "echo checked >&1; echo broken >&2; exit ${FAILED_COMMAND_EXIT_CODE}"])`,
      );
      expect(ran.exitCode).toBe(FAILED_COMMAND_EXIT_CODE);
      expect(ran.runLine).toStartWith("run: ");
      expect(withCommandLinesSorted(ran.linesBeforeTheSummary)).toEqual([
        "[main] broken",
        "[main] checked",
        `error: ${ROOT_TASK_COMMAND_PREFIX}: CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}`,
        "error: [main] its output is in <run folder>/1.output",
        endedLineOf("main", "failed"),
        recapLineOf(
          `error: ${ROOT_TASK_COMMAND_PREFIX}: CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}`,
          "1.output",
        ),
        "recap: [main] checked",
        "recap: [main] broken",
      ]);
      expect(countOccurrences(ran.standardError, "broken")).toBe(PRINTED_LIVE_AND_IN_THE_RECAP);
    }),
  ),
);

export const failureReasonOfAwaitedCommand = makeTest(
  "the error line of a failed command in an awaited task starts with where its Task.exec call is",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export const build = Task.make({ name: "build", run: Task.exec("sh", ["-c", "echo broken >&2; exit ${FAILED_COMMAND_EXIT_CODE}"]) });
export default Task.make({ name: "main", run: build });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      const errorLine = `error: ${BUILD_COMMAND_PREFIX}: CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}`;
      const recapLine = recapLineOf(errorLine, "2.output");
      expectOneOfTheOrders(
        splitTheEndOfTheRun(ran.standardError).linesBeforeTheSummary,
        withEitherEndFirst(
          ["[build] broken", errorLine],
          ["error: [build] its output is in <run folder>/2.output", endedLineOf("build", "failed")],
          [endedLineOf("main", "failed")],
          [recapLine, "recap: [build] broken"],
        ),
      );
    }),
  ),
);

export const failureReasonOfCommandInTaskMadeInBody = makeTest(
  "the error line of a failed command in a task made in a body starts with where its Task.exec call is",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Task.make({ name: "made-in-body", run: Task.exec("sh", ["-c", "echo broken >&2; exit ${FAILED_COMMAND_EXIT_CODE}"]) });
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      const errorLine = `error: ${COMMAND_IN_TASK_MADE_IN_BODY_PREFIX}: CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}`;
      const recapLine = recapLineOf(errorLine, "2.output");
      expectOneOfTheOrders(
        splitTheEndOfTheRun(ran.standardError).linesBeforeTheSummary,
        withEitherEndFirst(
          ["[made-in-body] broken", errorLine],
          [
            "error: [made-in-body] its output is in <run folder>/2.output",
            endedLineOf("made-in-body", "failed"),
          ],
          [endedLineOf("main", "failed")],
          [recapLine, "recap: [made-in-body] broken"],
        ),
      );
    }),
  ),
);

export const failureReasonKeepsLocationNamedLikeTask = makeTest(
  "the error line keeps a reported location that is spelled like a task's name instead of putting where that task was made",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { ReportedErrors } from "@sshkeda/task/errors";
export const build = Task.make({ name: "build", run: Effect.void });
export default Task.make({ name: "main", run: Effect.fail(new ReportedErrors({ reportedErrors: [{ tag: "StepRejected", location: "build", message: "the build step was rejected" }] })) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(splitTheEndOfTheRun(ran.standardError).linesBeforeTheSummary).toEqual([
        "error: [main] build: StepRejected: the build step was rejected",
        MAIN_OUTPUT_FILE_LINE,
        endedLineOf("main", "failed"),
        recapLineOf("error: [main] build: StepRejected: the build step was rejected", "1.output"),
      ]);
    }),
  ),
);

export const failureReasonOfLongMessage = makeTest(
  "the error line keeps the whole first line of a long message",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runFailureReasonProject(
        `Effect.fail(new EvidenceRejected({ message: "${"0".repeat(PRINTED_ZERO_COUNT)}" }))`,
      );
      expect(ran.exitCode).toBe(1);
      expect(ran.runLine).toStartWith("run: ");
      expect(ran.errorLines).toEqual([
        `error: ${ROOT_TASK_FAILURE_PREFIX}: EvidenceRejected: ${"0".repeat(PRINTED_ZERO_COUNT)}`,
        MAIN_OUTPUT_FILE_LINE,
      ]);
      expect(countOccurrences(ran.standardError, "0".repeat(PRINTED_ZERO_COUNT))).toBe(
        ERROR_AND_RECAP_LINES,
      );
    }),
  ),
);

export const failureReasonLeavesOutDiagnostics = makeTest(
  "the error headline of a failed command names the command and its exit code even when a line of its output, printed before it, reads like a diagnostic",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runFailureReasonProject(
        `Task.exec("sh", ["-c", "echo 'progress 01'; echo 'assertion failed at src/app.ts:3:5' >&2; exit ${FAILED_COMMAND_EXIT_CODE}"])`,
      );
      expect(ran.exitCode).toBe(FAILED_COMMAND_EXIT_CODE);
      expect(withCommandLinesSorted(ran.linesBeforeTheSummary)).toEqual([
        "[main] assertion failed at src/app.ts:3:5",
        "[main] progress 01",
        `error: ${ROOT_TASK_COMMAND_PREFIX}: CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}`,
        "error: [main] its output is in <run folder>/1.output",
        endedLineOf("main", "failed"),
        recapLineOf(
          `error: ${ROOT_TASK_COMMAND_PREFIX}: CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}`,
          "1.output",
        ),
        "recap: [main] progress 01",
        "recap: [main] assertion failed at src/app.ts:3:5",
      ]);
    }),
  ),
);

export const noFailureReasonOnSuccess = makeTest(
  "a passing run prints no error and nothing else before the summary, and ends with the run line naming its run folder",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runFailureReasonProject("Effect.void");
      expect(ran.exitCode).toBe(0);
      expect(ran.summaryLine).toMatch(/^passed: 1 task, [\d.]+s$/u);
      expect(ran.runLine).toMatch(/^run: \/.+\/runs\/project\/[^/]+\/$/u);
      expect(ran.linesBeforeTheSummary).toEqual([]);
      expect(ran.linesAfterTheRunLine).toEqual([]);
    }),
  ),
);

export const singleFailurePrintsOnce = makeTest(
  "a single failed task prints its error exactly once as it fails, after its command's output and naming the file that holds it, and its recap lines before the summary and the run line",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export const check = Task.make({ name: "check", run: Task.exec("sh", ["-c", "echo 'TS2554: Expected 0 arguments, but got 1.'; exit 1"]) });
export default Task.make({ name: "main", run: check });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      const end = splitTheEndOfTheRun(ran.standardError);
      const errorLine = "error: [check] main.ts:2:59: CommandFailed: sh exited with code 1";
      const recapLine = recapLineOf(errorLine, "2.output");
      expectOneOfTheOrders(
        end.linesBeforeTheSummary,
        withEitherEndFirst(
          ["[check] TS2554: Expected 0 arguments, but got 1.", errorLine],
          ["error: [check] its output is in <run folder>/2.output", endedLineOf("check", "failed")],
          [endedLineOf("main", "failed")],
          [recapLine, "recap: [check] TS2554: Expected 0 arguments, but got 1."],
        ),
      );
      expect(end.summaryLine).toMatch(/^failed: \[check\] \(1 of 2 tasks\), [\d.]+s$/u);
      expect(withRunDirectoryNamed(end.runLine, project)).toBe("run: <run folder>/");
      expect(end.linesAfterTheRunLine).toEqual([]);
      expect(countOccurrences(ran.standardError, "CommandFailed")).toBe(ERROR_AND_RECAP_LINES);
      expect(countOccurrences(ran.standardError, "TS2554")).toBe(PRINTED_LIVE_AND_IN_THE_RECAP);
    }),
  ),
);

const SETTLED_FAILURES_PROJECT = `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export const first = Task.make({ name: "first", run: Effect.fail(new TaskError({ message: "first broke" })) });
export const second = Task.make({ name: "second", run: Task.exec("cat", ["release-second"]).pipe(Effect.andThen(Effect.fail(new TaskError({ message: "second broke" })))) });
export const third = Task.make({ name: "third", run: Effect.sleep("500 millis").pipe(Effect.andThen(Task.exec("sh", ["-c", "echo third output; exit 2"]))) });`;

const runSecondAfterFirstEnded = (project: Effect.Success<ReturnType<typeof makeProject>>) =>
  Effect.gen(function* () {
    const started = yield* startTaskProcessWithPipes(project, ["release-second"]);
    yield* started.releaseAfter(["ended: [first]"], "release-second");
    return yield* started.waitForExit;
  });

export const settledFailuresPrintAtTheEnd = makeTest(
  "two failures under Task.allSettled each print their error once as they fail, the end of the run recaps both, the summary names both, and each error goes to its task's file",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `${SETTLED_FAILURES_PROJECT}
export default Task.make({ name: "main", run: Task.allSettled([first, second]) });`,
      });
      const ran = yield* runSecondAfterFirstEnded(project);
      expect(ran.exitCode).toBe(1);
      const end = splitTheEndOfTheRun(ran.standardError);
      expect(end.summaryLine).toMatch(/^failed: \[first\] \[second\] \(2 of 3 tasks\), [\d.]+s$/u);
      expect(withSecondsNamed(yield* readTaskOutputFile(project, ran, "first"))).toBe(
        `${startedLineOf("first", "2.output")}\nerror: [first] main.ts:4:66: TaskError: first broke\n${outputFileLineOf("first", "2.output")}\n${endedLineOf("first", "failed")}\n`,
      );
      const secondErrorLine = "error: [second] main.ts:5:125: TaskError: second broke";
      const firstRecapLine = recapLineOf(
        "error: [first] main.ts:4:66: TaskError: first broke",
        "2.output",
      );
      expectOneOfTheOrders(
        end.linesBeforeTheSummary,
        withEitherEndFirst(
          [
            "error: [first] main.ts:4:66: TaskError: first broke",
            outputFileLineOf("first", "2.output"),
            endedLineOf("first", "failed"),
            secondErrorLine,
          ],
          [outputFileLineOf("second", "3.output"), endedLineOf("second", "failed")],
          [endedLineOf("main", "failed")],
          [firstRecapLine, recapLineOf(secondErrorLine, "3.output")],
        ),
      );
      expect(withSecondsNamed(yield* readTaskOutputFile(project, ran, "second"))).toBe(
        `${startedLineOf("second", "3.output")}\nerror: [second] main.ts:5:125: TaskError: second broke\n${outputFileLineOf("second", "3.output")}\n${endedLineOf("second", "failed")}\n`,
      );
      expect(countOccurrences(ran.standardError, "first broke")).toBe(ERROR_AND_RECAP_LINES);
      expect(countOccurrences(ran.standardError, "second broke")).toBe(ERROR_AND_RECAP_LINES);
    }),
  ),
);

export const otherFailedTasksOutputFiles = makeTest(
  "each failed task's lines go to its own file in the run folder the run line names, each file named on its task's span",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `${SETTLED_FAILURES_PROJECT}
export default Task.make({ name: "main", run: Task.allSettled([first, second, third]) });`,
      });
      const ran = yield* runSecondAfterFirstEnded(project);
      expect(ran.exitCode).toBe(1);
      const end = splitTheEndOfTheRun(ran.standardError);
      const runDirectory = yield* Effect.fromNullishOr(findRunDirectoryPath(ran.standardError));
      const [trace] = yield* readTraces(yield* readProjectRunsDirectory(project, ran));
      const fileNamesByTask = new Map(
        (trace?.spans ?? [])
          .filter((span) => span.role === "task")
          .flatMap((span) => {
            const fileName = readAttribute(span.attributes, "sshkeda_task.output.file.name");
            return fileName === undefined
              ? []
              : [[readAttribute(span.attributes, "sshkeda_task.task"), fileName] as const];
          }),
      );
      expect(new Set(fileNamesByTask.keys())).toEqual(
        new Set(["main", "first", "second", "third"]),
      );
      expect(end.runLine).toBe(`run: ${runDirectory}/`);
      const readFileOf = (taskName: string) =>
        read(`${runDirectory}/${String(fileNamesByTask.get(taskName))}`).pipe(
          Effect.map((text) =>
            withRunLinePathNamed(withSecondsNamed(withoutStackFrames(text)), ran.standardError),
          ),
        );
      expect(yield* readFileOf("main")).toBe(
        `${MAIN_STARTED_LINE}\n${endedLineOf("main", "failed")}\n`,
      );
      expect(yield* readFileOf("first")).toBe(
        `${startedLineOf("first", "2.output")}\nerror: [first] main.ts:4:66: TaskError: first broke\n${outputFileLineOf("first", "2.output")}\n${endedLineOf("first", "failed")}\n`,
      );
      expect(yield* readFileOf("second")).toBe(
        `${startedLineOf("second", "3.output")}\nerror: [second] main.ts:5:125: TaskError: second broke\n${outputFileLineOf("second", "3.output")}\n${endedLineOf("second", "failed")}\n`,
      );
      expect(yield* readFileOf("third")).toMatch(
        /^started: \[third\], <seconds>, 4\.output\n\[third\] third output\nerror: \[third\] main\.ts:6:\d+: CommandFailed: sh exited with code 2\nerror: \[third\] its output is in <run folder>\/4\.output\nended: \[third\], <seconds>, failed\n$/u,
      );
      expect(
        (yield* (yield* FileSystem.FileSystem).readDirectory(runDirectory)).toSorted(),
      ).toEqual(["1.output", "2.output", "3.output", "4.output", "trace.jsonl"]);
      expect(end.errorLines.filter((line) => !line.startsWith("error: [third] "))).toEqual([
        "error: [first] main.ts:4:66: TaskError: first broke",
        outputFileLineOf("first", "2.output"),
        "error: [second] main.ts:5:125: TaskError: second broke",
        outputFileLineOf("second", "3.output"),
      ]);
      expect(
        end.errorLines.filter((line) => line.startsWith("error: [third] ")).join("\n"),
      ).toMatch(
        /^error: \[third\] main\.ts:6:\d+: CommandFailed: sh exited with code 2\nerror: \[third\] its output is in <run folder>\/4\.output$/u,
      );
      expect(end.recapLines.filter((line) => !line.startsWith("recap: [third] "))).toEqual([
        recapLineOf("error: [first] main.ts:4:66: TaskError: first broke", "2.output"),
        recapLineOf("error: [second] main.ts:5:125: TaskError: second broke", "3.output"),
      ]);
    }),
  ),
);

export const decidingBodyFailureAfterItsCleanupFailure = makeTest(
  "an uncaught body failure that decides the run prints as it happens, before the failure of the same task's cleanup that follows it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.die(new TaskError({ message: "cleanup broke" })));
  return yield* Effect.fail(new TaskError({ message: "body broke" }));
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(splitTheEndOfTheRun(ran.standardError).linesBeforeTheSummary).toEqual([
        "error: [main] main.ts:6:29: TaskError: body broke",
        "error: [main] main.ts:5:47: TaskError: cleanup broke",
        "error: [main] its cleanup failed",
        MAIN_OUTPUT_FILE_LINE,
        endedLineOf("main", "failed"),
        recapLineOf("error: [main] main.ts:5:47: TaskError: cleanup broke", "1.output"),
      ]);
    }),
  ),
);

export const cleanupFailureWithTheBodyFailureText = makeTest(
  "a cleanup failure with the same text as the body failure an awaiting task caught is a failure of its own, so it prints as an error line of its own and in its task's file",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { existsSync, writeFileSync } from "node:fs";
import { Effect, Schedule } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
const broke = () => new TaskError({ message: "build broke" });
export const build = Task.make({ name: "build", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.sync(() => existsSync("main-caught-it")).pipe(
    Effect.repeat({ until: (caught) => caught, schedule: Schedule.spaced("10 millis") }),
    Effect.andThen(Effect.suspend(() => Effect.die(broke()))),
  ));
  return yield* Effect.suspend(() => Effect.fail(broke()));
}) });
export default Task.make({ name: "main", run: build.pipe(Effect.catchTag("AwaitedTaskFailed", () => Effect.log("main caught it").pipe(Effect.andThen(Effect.sync(() => writeFileSync("main-caught-it", ""))), Effect.andThen(Effect.never)))) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain("[main] main caught it");
      const end = splitTheEndOfTheRun(ran.standardError);
      const errorLine = "error: [build] main.ts:5:21: TaskError: build broke";
      const contextLine = "error: [build] its cleanup failed";
      const outputFileLine = outputFileLineOf("build", "2.output");
      expect(end.errorLines).toEqual([errorLine, errorLine, contextLine, outputFileLine]);
      expect(end.recapLines).toEqual([recapLineOf(errorLine, "2.output")]);
      expect(withSecondsNamed(yield* readTaskOutputFile(project, ran, "build"))).toBe(
        `${startedLineOf("build", "2.output")}\n${errorLine}\n${errorLine}\n${contextLine}\n${outputFileLine}\n${endedLineOf("build", "failed")}\n`,
      );
    }),
  ),
);

export const wrappedCommandFailureNamesItsFile = makeTest(
  "an error that wraps a failed command, or is caused by one further down its cause chain, names the command's output file once, whether or not SSHKEDA_TASK_LOG=error",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
class BuildFailed extends Schema.TaggedError<BuildFailed>()("BuildFailed", { cause: Schema.Unknown }) {}
export const plain = Task.make({ name: "plain", run: Task.exec("sh", ["-c", "echo broken; exit ${FAILED_COMMAND_EXIT_CODE}"]).pipe(Effect.mapError((cause) => new Error("build failed", { cause }))) });
export const nested = Task.make({ name: "nested", run: Task.exec("sh", ["-c", "echo also broken >&2; exit ${FAILED_COMMAND_EXIT_CODE}"]).pipe(Effect.mapError((cause) => new Error("release failed", { cause: new BuildFailed({ cause }) }))) });
export default Task.make({ name: "main", run: Effect.all([plain, nested], { mode: "result" }).pipe(Effect.asVoid) });`,
      });
      const errorLines = [
        "error: [plain] main.ts:4:27: Error: build failed",
        `error: [plain] caused by CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}`,
        "error: [plain] its output is in <run folder>/2.output",
        "error: [nested] main.ts:5:28: Error: release failed",
        "error: [nested] caused by BuildFailed",
        `error: [nested] caused by CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}`,
        "error: [nested] its output is in <run folder>/3.output",
      ];
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(findErrorLines(withSecondsNamed(ran.standardError))).toEqual(errorLines);
      expect(ran.standardError).toContain("[plain] broken\n");
      expect(ran.standardError).toContain("[nested] also broken\n");
      const quiet = yield* runTaskProcess(project, "main.ts", ERROR_LOG_LEVEL);
      expect(quiet.exitCode, quiet.standardError).toBe(0);
      expect(findErrorLines(quiet.standardError)).toEqual(errorLines);
      expect(quiet.standardError).not.toContain("broken\n");
    }),
  ),
);

const MOST_WRITTEN_STACK_FRAMES = 100;

export const stackFramesInTheTaskFile = makeTest(
  "a failed task's file holds a block of stack frames for its error, each inner error and its cause, each named by its error, after its error lines and before the line naming its file, at most 100 frames per error, while the terminal prints none of them",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
Error.stackTraceLimit = 200;
const deep = (depth: number): number => {
  if (depth === 0) throw new AggregateError([new Error("INNER_MARK")], "OUTER_MARK", { cause: new Error("CAUSE_MARK") });
  return 1 + deep(depth - 1);
};
export default Task.make({ name: "main", run: Effect.sync(() => deep(150)) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      const errorLines = [
        "error: [main] main.ts:5:30: AggregateError: OUTER_MARK",
        "error: [main]   Error: INNER_MARK",
        "error: [main] caused by Error: CAUSE_MARK",
      ];
      const outputLine = "error: [main] its output is in <run folder>/1.output";
      expect(findErrorLines(ran.standardError)).toEqual([...errorLines, outputLine]);
      expect(ran.standardError).not.toContain("stack frames");
      const file = withRunLinePathNamed(
        yield* read(`${findRunDirectoryPath(ran.standardError) ?? ""}/1.output`),
        ran.standardError,
      );
      const blocks = findStackFrameBlocks(file);
      expect(blocks.map((block) => block.error)).toEqual([
        "AggregateError: OUTER_MARK",
        "Error: INNER_MARK",
        "Error: CAUSE_MARK",
      ]);
      const [outerBlock] = blocks;
      expect(outerBlock?.frames[0]).toMatch(
        new RegExp(`^at deep \\(${RegExp.escape(project.directory)}/main\\.ts:5:\\d+\\)$`, "u"),
      );
      expect(outerBlock?.frames.length).toBe(MOST_WRITTEN_STACK_FRAMES + 1);
      expect(outerBlock?.frames.at(-1)).toMatch(
        /^\d+ more stack frames left out, past the first 100$/u,
      );
      const fileLines = file.split("\n");
      const lastErrorLine = fileLines.indexOf(errorLines.at(-1) ?? "");
      expect(fileLines[lastErrorLine + 1]).toBe(
        "error: [main] stack frames of AggregateError: OUTER_MARK",
      );
      expect(withoutStackFrames(file)).toContain(`${errorLines.join("\n")}\n${outputLine}\n`);
    }),
  ),
);

export const cleanupFailureStackFramesInTheFile = makeTest(
  "a cleanup that fails writes its error's stack frames to the task's file after those of its body's failure, and the terminal prints no stack frames",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.die(new Error("CLEANUP_MARK")));
  return yield* Effect.fail(new Error("BODY_MARK"));
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).not.toContain("stack frames");
      const file = yield* read(`${findRunDirectoryPath(ran.standardError) ?? ""}/1.output`);
      const blocks = findStackFrameBlocks(file);
      expect(blocks.map((block) => block.error)).toEqual([
        "Error: BODY_MARK",
        "Error: CLEANUP_MARK",
      ]);
      expect(blocks[1]?.frames[0]).toMatch(
        new RegExp(`^at .*\\(${RegExp.escape(project.directory)}/main\\.ts:4:\\d+\\)$`, "u"),
      );
    }),
  ),
);

export const cleanupReportBeforeStackFrames = makeTest(
  "a cleanup error prints as it was when it failed, even when reading an inner error's stack for the task's file changes it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.sync(() => {
    const failure = new Error("ORIGINAL_CLEANUP_DIAGNOSTIC");
    const detail = Object.defineProperty(new Error("internal detail"), "stack", { get() { failure.message = ""; return undefined; } });
    Object.defineProperty(failure, "errors", { value: [detail] });
    throw failure;
  }));
  return yield* Effect.fail(new Error("body error"));
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toMatch(
        /^error: \[main\] main\.ts:\d+:\d+: Error: ORIGINAL_CLEANUP_DIAGNOSTIC$/mu,
      );
      expect(ran.standardError).toMatch(
        /^recap: \[main\] 1\.output \| main\.ts:\d+:\d+: Error: ORIGINAL_CLEANUP_DIAGNOSTIC$/mu,
      );
    }),
  ),
);

export const unreadableStackLeavesOutOnlyItsFrames = makeTest(
  "an inner error whose stack throws when read, or is not text, prints as it does without stack frames, and only the readable stacks go to the task's file",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";

export default Task.make({
  name: "main",
  run: Effect.sync(() => {
    const throwing = new Error("THROWING_MARK");
    Object.defineProperty(throwing, "stack", { get() { throw new Error("STACK_GETTER_MARK"); } });
    const numbered = Object.assign(new Error("NUMBERED_MARK"), { stack: 42 });
    throw new AggregateError([throwing, numbered], "OUTER_MARK");
  }),
});`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      const errorLines = [
        "error: [main] main.ts:10:15: AggregateError: OUTER_MARK",
        "error: [main]   Error: THROWING_MARK",
        "error: [main]   Error: NUMBERED_MARK",
      ];
      expect(findErrorLines(ran.standardError)).toEqual([
        ...errorLines,
        "error: [main] its output is in <run folder>/1.output",
      ]);
      expect(ran.standardError).not.toContain("STACK_GETTER_MARK");
      const file = yield* read(`${findRunDirectoryPath(ran.standardError) ?? ""}/1.output`);
      expect(file).not.toContain("STACK_GETTER_MARK");
      expect(findStackFrameBlocks(file).map((block) => block.error)).toEqual([
        "AggregateError: OUTER_MARK",
      ]);
    }),
  ),
);

export const hugeSparseInnerErrorList = makeTest(
  "an error whose errors list is a huge sparse array prints as it does without stack frames, without reading the whole list",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.sync(() => {
  const failure = new Error("SPARSE_FAILURE");
  Object.defineProperty(failure, "errors", { value: Array(0xffff_ffff) });
  throw failure;
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(findErrorLines(ran.standardError)).toEqual([
        "error: [main] main.ts:4:23: Error: SPARSE_FAILURE",
        "error: [main] its output is in <run folder>/1.output",
      ]);
    }),
  ),
  "30 seconds",
);

export const stackGetterThatChangesItsError = makeTest(
  "an inner error whose stack getter replaces its message with a throwing getter and then throws prints as it does without stack frames",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.sync(() => {
  const inner = new Error("INNER_ORIGINAL");
  Object.defineProperty(inner, "stack", {
    get() {
      Object.defineProperty(inner, "message", { get() { throw new Error("GETTER_FAILURE"); } });
      throw new Error("STACK_UNAVAILABLE");
    },
  });
  throw new AggregateError([inner], "OUTER");
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(findErrorLines(ran.standardError)).toEqual([
        "error: [main] main.ts:11:13: AggregateError: OUTER",
        "error: [main]   Error: INNER_ORIGINAL",
        "error: [main] its output is in <run folder>/1.output",
      ]);
    }),
  ),
);

export const stackFramesOfAnErrorWithAMultilineName = makeTest(
  "the line naming an error's stack frames in its task's file holds only the first line of its name, so it fakes no line",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.sync(() => {
  throw Object.assign(new Error("BROKEN"), { name: "Custom: one\\nended: [other], 0ms, passed" });
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      const file = yield* read(`${findRunDirectoryPath(ran.standardError) ?? ""}/1.output`);
      expect(file).not.toMatch(/^ended: \[other\]/mu);
      expect(findStackFrameBlocks(file).map((block) => block.error)).toEqual(["Custom: one"]);
    }),
  ),
);
