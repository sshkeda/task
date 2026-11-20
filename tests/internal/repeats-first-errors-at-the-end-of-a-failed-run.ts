import { expect } from "bun:test";

import { Effect, FileSystem } from "effect";

import { formatRecapLines } from "../../src/lib/main-thread/shared/format-recap-lines.ts";
import { countLineBreaks, countLineFeeds } from "../../src/shared/count-line-breaks.ts";
import { nameTask } from "../../src/shared/label-lines.ts";
import { countOccurrences } from "../shared/count-occurrences.ts";
import { EXIT_CODE_AFTER_SIGINT } from "../shared/exit-codes-after-stop-signals.ts";
import { expectOneOfTheOrders, withEitherEndFirst } from "../shared/expect-one-of-the-orders.ts";
import { findMisprintedErrorLines } from "../shared/find-misprinted-error-lines.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import {
  findRunDirectoryPath,
  withRunLinePathNamed,
  withSecondsNamed,
} from "../shared/name-output-files.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";
import { startTaskProcessWithPipes } from "../shared/start-task-process-with-pipes.ts";

const FAILED_COMMAND_EXIT_CODE = 3;

const IN_THE_ERROR_AND_THE_RECAP = 2;

const RECAP_LINES_OF_TWO_LINE_ERROR = 2;

const linesOf = (standardError: string) => withSecondsNamed(standardError).trimEnd().split("\n");

const expectTheRunToEndWith = (standardError: string, lastLines: string[]) => {
  expect(linesOf(standardError).slice(-lastLines.length), standardError).toEqual(lastLines);
};

const printedLinesOf = (standardError: string) =>
  withRunLinePathNamed(withSecondsNamed(standardError)).trimEnd().split("\n");

const expectEachErrorLineOnce = (standardError: string) => {
  const errorLines = linesOf(standardError).filter((line) => line.startsWith("error: "));
  expect([...new Set(errorLines)], standardError).toEqual(errorLines);
};

const findSummaryIndex = (standardError: string) =>
  linesOf(standardError).findLastIndex((line) => /^(?:passed|failed|stopped): /u.test(line));

export const oneFailedTask = makeTest(
  "a failed run recaps its failed task right before the summary with a recap line naming its output file and the first line of its error, then the error's next lines, and prints each error line once",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Task.exec("sh", ["-c", "echo checked; exit ${FAILED_COMMAND_EXIT_CODE}"]).pipe(Effect.ignore);
  return yield* new TaskError({ message: "the check failed\\nsee above" });
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      const runDirectoryPath = findRunDirectoryPath(ran.standardError);
      const errorLine = "error: [main] main.ts:6:17: TaskError: the check failed";
      const pointerLine = `error: [main] its output is in ${runDirectoryPath}/1.output`;
      expectTheRunToEndWith(ran.standardError, [
        "ended: [main], <seconds>, failed",
        "recap: [main] 1.output | main.ts:6:17: TaskError: the check failed",
        "recap: [main] see above",
        "failed: [main] (1 of 1 task), <seconds>",
        `run: ${runDirectoryPath}/`,
      ]);
      expect(countOccurrences(ran.standardError, `${errorLine}\n`)).toBe(1);
      expect(countOccurrences(ran.standardError, `${pointerLine}\n`)).toBe(1);
      expect(countOccurrences(ran.standardError, "recap: ")).toBe(RECAP_LINES_OF_TWO_LINE_ERROR);
      expectEachErrorLineOnce(ran.standardError);
      expect(countOccurrences(ran.standardError, "see above")).toBe(IN_THE_ERROR_AND_THE_RECAP);
      expect(countOccurrences(ran.standardError, "[main] checked")).toBe(1);
    }),
  ),
);

export const twoFailedTasksInFailedLineOrder = makeTest(
  "a failed run recaps each failed task in the order the failed: line names them, even when they failed in another order",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
const late = Task.make({ name: "late", run: Task.exec("cat", ["release-late"]).pipe(Effect.andThen(Effect.fail(new TaskError({ message: "late broke" })))) });
const early = Task.make({ name: "early", run: Effect.fail(new TaskError({ message: "early broke" })) });
export default Task.make({ name: "main", run: Task.allSettled([late, early]) });`,
      });
      const started = yield* startTaskProcessWithPipes(project, ["release-late"]);
      yield* started.releaseAfter(["ended: [early]"], "release-late");
      const ran = yield* started.waitForExit;
      expect(ran.exitCode, ran.standardError).toBe(1);
      expectOneOfTheOrders(
        printedLinesOf(ran.standardError),
        withEitherEndFirst(
          [
            "run: <run folder>/",
            "error: [early] main.ts:5:59: TaskError: early broke",
            "error: [early] its output is in <run folder>/3.output",
            "ended: [early], <seconds>, failed",
            "error: [late] main.ts:4:112: TaskError: late broke",
          ],
          [
            "error: [late] its output is in <run folder>/2.output",
            "ended: [late], <seconds>, failed",
          ],
          ["ended: [main], <seconds>, failed"],
          [
            "recap: [late] 2.output | main.ts:4:112: TaskError: late broke",
            "recap: [early] 3.output | main.ts:5:59: TaskError: early broke",
            "failed: [late] [early] (2 of 3 tasks), <seconds>",
            "run: <run folder>/",
          ],
        ),
      );
    }),
  ),
);

export const awaitedFailureIsNotRepeated = makeTest(
  "a task that failed only because a task it awaited failed gets no recap line at the end of the run",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
const dependency = Task.make({ name: "dependency", run: Effect.fail(new TaskError({ message: "dependency broke" })) });
export default Task.make({ name: "main", run: Effect.asVoid(dependency) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expectOneOfTheOrders(
        printedLinesOf(ran.standardError),
        withEitherEndFirst(
          ["run: <run folder>/", "error: [dependency] main.ts:4:69: TaskError: dependency broke"],
          [
            "error: [dependency] its output is in <run folder>/2.output",
            "ended: [dependency], <seconds>, failed",
          ],
          ["ended: [main], <seconds>, failed"],
          [
            "recap: [dependency] 2.output | main.ts:4:69: TaskError: dependency broke",
            "failed: [dependency] (1 of 2 tasks), <seconds>",
            "run: <run folder>/",
          ],
        ),
      );
    }),
  ),
);

export const repeatWithoutOutputFileLine = makeTest(
  "a failed task whose output file could not be written gets a recap line with the first line of its error and no file name",
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
  return yield* Effect.fail(new TaskError({ message: "broke" }));
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(ran.standardError).not.toContain("its output is in");
      const lines = linesOf(ran.standardError);
      const summaryIndex = findSummaryIndex(ran.standardError);
      expect(lines.slice(summaryIndex - 1, summaryIndex + 1), ran.standardError).toEqual([
        "recap: [main] main.ts:11:29: TaskError: broke",
        "failed: [main] (1 of 1 task), <seconds>",
      ]);
      expect(ran.standardError).not.toContain(" | ");
      expect(countOccurrences(ran.standardError, "TaskError: broke")).toBe(
        IN_THE_ERROR_AND_THE_RECAP,
      );
      expectEachErrorLineOnce(ran.standardError);
    }),
  ),
);

export const nothingRepeatedAfterCtrlC = makeTest(
  "a run stopped with Ctrl-C after a task failed prints no recap line",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
const broke = Task.make({ name: "broke", run: Effect.fail(new TaskError({ message: "broke early" })) });
const hang = Task.make({ name: "hang", run: Effect.log("hanging").pipe(Effect.andThen(Effect.never)) });
export default Task.make({ name: "main", run: Task.allSettled([broke, hang]) });`,
      });
      const started = yield* startTaskProcess(project, "main.ts");
      yield* started.waitForLines(["ended: [broke]", "[hang] hanging"]);
      yield* started.sendSignal("INT");
      const exited = yield* started.waitForExit;
      expect(exited.exitCode, exited.standardError).toBe(EXIT_CODE_AFTER_SIGINT);
      expect(countOccurrences(exited.standardError, "TaskError: broke early")).toBe(1);
      expect(countOccurrences(exited.standardError, "its output is in")).toBe(1);
      const lines = linesOf(exited.standardError);
      const summaryIndex = findSummaryIndex(exited.standardError);
      expect(exited.standardError).not.toContain("recap: ");
      expect(lines[summaryIndex - 1], exited.standardError).not.toStartWith("error:");
      expect(lines[summaryIndex], exited.standardError).toStartWith("failed: [broke] ");
    }),
  ),
);

export const repeatAtTheErrorLogLevel = makeTest(
  "a failed run with SSHKEDA_TASK_LOG=error still prints the recap line of each failed task before the summary",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export default Task.make({ name: "main", run: Effect.log("hidden").pipe(Effect.andThen(Effect.fail(new TaskError({ message: "broke" })))) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_LOG: "error" });
      expect(ran.exitCode, ran.standardError).toBe(1);
      const runDirectoryPath = findRunDirectoryPath(ran.standardError);
      const errorLine = "error: [main] main.ts:4:100: TaskError: broke";
      const pointerLine = `error: [main] its output is in ${runDirectoryPath}/1.output`;
      expect(withSecondsNamed(ran.standardError)).toBe(
        [
          `run: ${runDirectoryPath}/`,
          errorLine,
          pointerLine,
          "recap: [main] 1.output | main.ts:4:100: TaskError: broke",
          "failed: [main] (1 of 1 task), <seconds>",
          `run: ${runDirectoryPath}/`,
        ].join("\n"),
      );
    }),
  ),
);

export const nothingRepeatedWhenTheRunPasses = makeTest(
  "a run that passes prints no recap line, even when it caught a task's failure",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
const migrate = Task.make({ name: "migrate", run: Effect.fail(new TaskError({ message: "migrate broke" })) });
export default Task.make({ name: "main", run: migrate.pipe(Effect.ignore) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(countOccurrences(ran.standardError, "TaskError: migrate broke")).toBe(1);
      expect(countOccurrences(ran.standardError, "its output is in")).toBe(1);
      expect(ran.standardError).not.toContain("recap: ");
      expectOneOfTheOrders(
        printedLinesOf(ran.standardError),
        withEitherEndFirst(
          ["run: <run folder>/", "error: [migrate] main.ts:4:63: TaskError: migrate broke"],
          [
            "error: [migrate] its output is in <run folder>/2.output",
            "ended: [migrate], <seconds>, failed",
          ],
          [],
          ["passed: 2 tasks, 1 caught failure, <seconds>", "run: <run folder>/"],
        ),
      );
    }),
  ),
);

export const repeatShowsTheTimeoutAfterACaughtError = makeTest(
  "a task whose printed error was caught and whose cleanup then ran past its limit recaps the cleanup time limit error, not the caught error",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
const broken = Task.make({ name: "broken", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.never);
  return yield* Effect.fail(new TaskError({ message: "broken broke" }));
}), cleanup: "300 millis" });
export default Task.make({ name: "main", run: broken.pipe(
  Effect.catchTag("AwaitedTaskFailed", () => Effect.log("main caught it")),
) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(ran.standardError).toContain("[main] main caught it");
      const runDirectoryPath = findRunDirectoryPath(ran.standardError);
      expectTheRunToEndWith(ran.standardError, [
        "recap: [broken] 2.output | main.ts:4:21: TaskCleanupTimedOut: its cleanup did not finish within 300ms, so the run stopped waiting for it",
        "failed: [broken] (1 of 2 tasks), <seconds>",
        `run: ${runDirectoryPath}/`,
      ]);
      expect(countOccurrences(ran.standardError, "TaskCleanupTimedOut")).toBe(
        IN_THE_ERROR_AND_THE_RECAP,
      );
      expect(countOccurrences(ran.standardError, "TaskError: broken broke")).toBe(1);
    }),
  ),
);

export const caughtFailureIsRepeated = makeTest(
  "a task whose failure an awaiting task caught still ended failed, so a run that fails for another reason recaps it with the other failed task, in the order they ended",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
const caught = Task.make({ name: "caught", run: Effect.fail(new TaskError({ message: "caught broke" })) });
const other = Task.make({ name: "other", run: Effect.fail(new TaskError({ message: "other broke" })) });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* caught.pipe(Effect.catchTag("AwaitedTaskFailed", () => Task.exec("cat", ["release-main"]).pipe(Effect.andThen(Effect.log("main caught it")))));
  yield* other;
}) });`,
      });
      const started = yield* startTaskProcessWithPipes(project, ["release-main"]);
      yield* started.releaseAfter(["ended: [caught]"], "release-main");
      const ran = yield* started.waitForExit;
      expect(ran.exitCode, ran.standardError).toBe(1);
      expectOneOfTheOrders(
        printedLinesOf(ran.standardError),
        withEitherEndFirst(
          [
            "run: <run folder>/",
            "error: [caught] main.ts:4:61: TaskError: caught broke",
            "error: [caught] its output is in <run folder>/2.output",
            "ended: [caught], <seconds>, failed",
            "[main] main caught it",
            "error: [other] main.ts:5:59: TaskError: other broke",
          ],
          [
            "error: [other] its output is in <run folder>/3.output",
            "ended: [other], <seconds>, failed",
          ],
          ["ended: [main], <seconds>, failed"],
          [
            "recap: [caught] 2.output | main.ts:4:61: TaskError: caught broke",
            "recap: [other] 3.output | main.ts:5:59: TaskError: other broke",
            "failed: [caught] [other] (2 of 3 tasks), <seconds>",
            "run: <run folder>/",
          ],
        ),
      );
    }),
  ),
);

export const failureInAFinalizerAfterTheOwnError = makeTest(
  "a task that fails on its own error and then on a task its finalizer awaited is recapped with the awaited task, in the order the two tasks ended",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
const dependency = Task.make({ name: "dependency", run: Effect.fail(new TaskError({ message: "dependency broke" })) });
export default Task.make({ name: "main", run: Effect.scoped(Effect.gen(function* () {
  yield* Effect.addFinalizer(() => dependency.pipe(Effect.orDie));
  return yield* Effect.fail(new TaskError({ message: "main broke" }));
})) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      const beforeTheEnds = [
        "run: <run folder>/",
        "error: [dependency] main.ts:4:69: TaskError: dependency broke",
      ];
      const dependencyEnd = [
        "error: [dependency] its output is in <run folder>/2.output",
        "ended: [dependency], <seconds>, failed",
      ];
      const mainError = "error: [main] main.ts:7:29: TaskError: main broke";
      const mainEnd = [
        "error: [main] its output is in <run folder>/1.output",
        "ended: [main], <seconds>, failed",
      ];
      const dependencyRecap =
        "recap: [dependency] 2.output | main.ts:4:69: TaskError: dependency broke";
      const mainRecap = "recap: [main] 1.output | main.ts:7:29: TaskError: main broke";
      const summary = [
        "failed: [dependency] [main] (2 of 2 tasks), <seconds>",
        "run: <run folder>/",
      ];
      expectOneOfTheOrders(printedLinesOf(ran.standardError), [
        [
          ...beforeTheEnds,
          ...dependencyEnd,
          mainError,
          ...mainEnd,
          dependencyRecap,
          mainRecap,
          ...summary,
        ],
        [
          ...beforeTheEnds,
          mainError,
          ...dependencyEnd,
          ...mainEnd,
          dependencyRecap,
          mainRecap,
          ...summary,
        ],
        [
          ...beforeTheEnds,
          mainError,
          ...mainEnd,
          ...dependencyEnd,
          mainRecap,
          dependencyRecap,
          ...summary,
        ],
      ]);
    }),
  ),
);

export const sharedFailureIsRepeatedOnce = makeTest(
  "with SSHKEDA_TASK_LOG=debug, a failure that one awaiting task caught and another did not is recapped once, under the task that failed",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
const dependency = Task.make({ name: "dependency", run: Effect.fail(new TaskError({ message: "dependency broke" })) });
const waitFor = (pipeName: string) => Effect.ignore(Task.exec("cat", [pipeName]));
const catcher = Task.make({ name: "catcher", run: dependency.pipe(Effect.ignore, Effect.andThen(waitFor("release-catcher"))) });
const uncaught = Task.make({ name: "uncaught", run: Effect.asVoid(dependency).pipe(Effect.ensuring(waitFor("release-uncaught"))) });
export default Task.make({ name: "main", run: Task.allSettled([catcher, uncaught]).pipe(Effect.ensuring(waitFor("release-main"))) });`,
      });
      const started = yield* startTaskProcessWithPipes(
        project,
        ["release-catcher", "release-uncaught", "release-main"],
        { SSHKEDA_TASK_LOG: "debug" },
      );
      yield* started.releaseAfter(["ended: [dependency]"], "release-catcher");
      yield* started.releaseAfter(["ended: [catcher]"], "release-uncaught");
      yield* started.releaseAfter(["ended: [uncaught]"], "release-main");
      const ran = yield* started.waitForExit;
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(printedLinesOf(ran.standardError), ran.standardError).toEqual([
        "run: <run folder>/",
        "started: [main], <seconds>, 1.output",
        "started: [catcher], <seconds>, 2.output",
        "started: [uncaught], <seconds>, 3.output",
        "started: [dependency], <seconds>, 4.output",
        "error: [dependency] main.ts:4:69: TaskError: dependency broke",
        "error: [dependency] its output is in <run folder>/4.output",
        "ended: [dependency], <seconds>, failed",
        "ended: [catcher], <seconds>, passed",
        "ended: [uncaught], <seconds>, failed",
        "ended: [main], <seconds>, failed",
        "recap: [dependency] 4.output | main.ts:4:69: TaskError: dependency broke",
        "failed: [dependency] (1 of 4 tasks), <seconds>",
        "run: <run folder>/",
      ]);
      expect(countOccurrences(ran.standardError, "TaskError: dependency broke")).toBe(
        IN_THE_ERROR_AND_THE_RECAP,
      );
    }),
  ),
);

const CLEANUP_PROJECT = (finalizer: string) => `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const broken = Task.make({ name: "broken", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => ${finalizer});
  return yield* Effect.fail(new Error("body broke"));
}), cleanup: "200 millis" });
export default Task.make({ name: "main", run: Effect.ignore(broken) });`;

export const cleanupTimeoutIsRepeated = makeTest(
  "a task whose body failed and whose cleanup then ran past its limit recaps the cleanup time limit error, the last error it printed",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": CLEANUP_PROJECT("Effect.never") });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      const runDirectoryPath = findRunDirectoryPath(ran.standardError);
      expectTheRunToEndWith(ran.standardError, [
        "recap: [broken] 2.output | main.ts:3:21: TaskCleanupTimedOut: its cleanup did not finish within 200ms, so the run stopped waiting for it",
        "failed: [broken] (1 of 2 tasks), <seconds>",
        `run: ${runDirectoryPath}/`,
      ]);
      expect(countOccurrences(ran.standardError, "body broke")).toBe(1);
    }),
  ),
);

export const cleanupFailureIsRepeated = makeTest(
  "a task whose body failed and whose cleanup then failed recaps the cleanup failure, the last error it printed",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": CLEANUP_PROJECT('Effect.die(new Error("cleanup broke"))'),
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      const runDirectoryPath = findRunDirectoryPath(ran.standardError);
      expectTheRunToEndWith(ran.standardError, [
        "recap: [broken] 2.output | main.ts:4:51: Error: cleanup broke",
        "failed: [broken] (1 of 2 tasks), 1 stopped, <seconds>",
        `run: ${runDirectoryPath}/`,
      ]);
      expect(countOccurrences(ran.standardError, "body broke")).toBe(1);
    }),
  ),
);

export const multiLineErrorNameIsSplit = makeTest(
  "an error whose name spans lines prints each of its lines as its own error line, and its recap repeats them",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const error = new Error("broke");
error.name = "Custom\\nerror: [main] inner";
export default Task.make({ name: "main", run: Effect.fail(error) });`,
      });
      const started = yield* startTaskProcess(project, "main.ts");
      const standardError = yield* started.waitForStandardErrorToClose;
      const runDirectoryPath = findRunDirectoryPath(standardError);
      const mainErrorLines = linesOf(standardError).filter((line) =>
        line.startsWith("error: [main]"),
      );
      expect(mainErrorLines, standardError).toEqual([
        "error: [main] main.ts:3:19: Custom",
        "error: [main] error: [main] inner: broke",
        `error: [main] its output is in ${runDirectoryPath}/1.output`,
      ]);
      expect(
        findMisprintedErrorLines(standardError).map(({ line }) => line),
        standardError,
      ).toEqual(["error: [main] main.ts:3:19: Custom"]);
      expectTheRunToEndWith(standardError, [
        "ended: [main], <seconds>, failed",
        "recap: [main] 1.output | main.ts:3:19: Custom",
        "recap: [main] error: [main] inner: broke",
        "failed: [main] (1 of 1 task), <seconds>",
        `run: ${runDirectoryPath}/`,
      ]);
    }),
  ),
);

const FAILED_COMMAND_RECAP =
  /^recap: \[main\] 1\.output \| main\.ts:\d+:\d+: CommandFailed: sh exited with code 3$/u;

const recapLinesOf = (standardError: string) =>
  linesOf(standardError).filter((line) => line.startsWith("recap: "));

const runFailingScript = (script: string, execOptions = "") =>
  Effect.gen(function* () {
    const project = yield* makeProject({
      "script.sh": `${script}\nexit ${FAILED_COMMAND_EXIT_CODE}\n`,
      "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sh", ["script.sh"]${execOptions}) });`,
    });
    const ran = yield* runTaskProcess(project, "main.ts");
    expect(ran.exitCode, ran.standardError).toBe(FAILED_COMMAND_EXIT_CODE);
    const [firstRecapLine, ...outputRecapLines] = recapLinesOf(ran.standardError);
    expect(firstRecapLine, ran.standardError).toMatch(FAILED_COMMAND_RECAP);
    return outputRecapLines;
  });

export const failedCommandRecapsItsFirstAndLastLines = makeTest(
  "a failed command's recap prints its first non-empty line, a note counting the lines between that it leaves out, and its last two non-empty lines, taking standard output's lines before standard error's",
  Effect.scoped(
    Effect.gen(function* () {
      expect(
        yield* runFailingScript(
          String.raw`printf 'first\n\nsecond\n'; printf 'third\n  \nfourth\n' >&2`,
        ),
      ).toEqual([
        "recap: [main] first",
        "recap: [main] (2 lines are not shown)",
        "recap: [main] third",
        "recap: [main] fourth",
      ]);
    }),
  ),
);

const taskRunning = (name: string, command: string) =>
  `import { Task } from "@sshkeda/task";
export default Task.make({ name: "${name}", run: ${command} });`;

const failingInnerTask = taskRunning("inner", 'Task.exec("sh", ["-c", "echo bad >&2; exit 1"])');

const NESTED_RUN_LINE_PATH = /(?<=\] run: )\/\S+\/$/u;

const nestedRecapLinesOf = (standardError: string) =>
  recapLinesOf(standardError).map((line) => line.replace(NESTED_RUN_LINE_PATH, "<nested run>/"));

export const nestedFailureRecap = makeTest(
  "a failed command running Task recaps the inner failure",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": taskRunning("main", 'Task.exec("bun", ["inner.ts"])'),
        "inner.ts": failingInnerTask,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(nestedRecapLinesOf(ran.standardError), ran.standardError).toEqual([
        "recap: [main] 1.output | main.ts:2:52: CommandFailed: bun exited with code 1",
        "recap: [main] run: <nested run>/",
        "recap: [main] [inner] 1.output | inner.ts:2:53: CommandFailed: sh exited with code 1",
        "recap: [main] [inner] bad",
        "recap: [main] (6 lines are not shown)",
        "recap: [main] failed: [inner] (1 of 1 task), <seconds>",
        "recap: [main] run: <nested run>/",
      ]);
    }),
  ),
);

export const threeLevelFailureRecap = makeTest(
  "a failed command recaps three levels of Task failures",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": taskRunning("main", 'Task.exec("bun", ["middle.ts"])'),
        "middle.ts": taskRunning("middle", 'Task.exec("bun", ["inner.ts"])'),
        "inner.ts": failingInnerTask,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(nestedRecapLinesOf(ran.standardError), ran.standardError).toEqual([
        "recap: [main] 1.output | main.ts:2:52: CommandFailed: bun exited with code 1",
        "recap: [main] run: <nested run>/",
        "recap: [main] [middle] 1.output | middle.ts:2:54: CommandFailed: bun exited with code 1",
        "recap: [main] [middle] run: <nested run>/",
        "recap: [main] [middle] [inner] 1.output | inner.ts:2:53: CommandFailed: sh exited with code 1",
        "recap: [main] [middle] [inner] bad",
        "recap: [main] [middle] (6 lines are not shown)",
        "recap: [main] [middle] failed: [inner] (1 of 1 task), <seconds>",
        "recap: [main] [middle] run: <nested run>/",
        "recap: [main] (19 lines are not shown)",
        "recap: [main] failed: [middle] (1 of 1 task), <seconds>",
        "recap: [main] run: <nested run>/",
      ]);
    }),
  ),
);

const WRAPPER_EXIT_CODE = 3;

const MAX_NESTED_RECAP_LINES = 20;

export const wrappedNestedFailureRecap = makeTest(
  "a failed command that runs Task and then fails for its own reason recaps both",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": taskRunning(
          "main",
          `Task.exec("sh", ["-c", "bun inner.ts; echo own reason >&2; exit ${WRAPPER_EXIT_CODE}"])`,
        ),
        "inner.ts": failingInnerTask,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(WRAPPER_EXIT_CODE);
      const recapLines = nestedRecapLinesOf(ran.standardError);
      expect(recapLines, ran.standardError).toContain(
        "recap: [main] [inner] 1.output | inner.ts:2:53: CommandFailed: sh exited with code 1",
      );
      expect(recapLines.at(-1), ran.standardError).toBe("recap: [main] own reason");
    }),
  ),
);

export const longFailureRecapIsCut = makeTest(
  "a recap file of more than 20 lines recaps its first 20 and a note counting the rest",
  Effect.scoped(
    Effect.gen(function* () {
      const recapLines = yield* runFailingScript(
        String.raw`printf '[%s]' "$(seq -f '"line %g"' 1 25 | paste -sd, -)" > "$SSHKEDA_TASK_RECAP_FILE"
echo out`,
      );
      expect(recapLines).toEqual([
        "recap: [main] out",
        ...Array.from(
          { length: MAX_NESTED_RECAP_LINES },
          (_unused, index) => `recap: [main] line ${index + 1}`,
        ),
        "recap: [main] (5 nested recap lines are not shown)",
      ]);
    }),
  ),
);

export const malformedFailureRecap = makeTest(
  "a malformed recap file falls back to the first and last output lines",
  Effect.scoped(
    Effect.gen(function* () {
      expect(
        yield* runFailingScript(
          String.raw`if [ -n "$SSHKEDA_TASK_RECAP_FILE" ] && printf 'garbage' > "$SSHKEDA_TASK_RECAP_FILE"; then echo first; else echo 'no recap file path'; fi
printf 'second\nthird\nfourth\n' >&2`,
        ),
      ).toEqual([
        "recap: [main] first",
        "recap: [main] (1 line is not shown)",
        "recap: [main] third",
        "recap: [main] fourth",
      ]);
    }),
  ),
);

export const secretCommandHasNoRecapPath = makeTest(
  "a secret command receives no recap file path",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Redacted } from "effect";
import { Task } from "@sshkeda/task";
const args = ["-c", 'if [ "\${SSHKEDA_TASK_RECAP_FILE+x}" ]; then echo present; else echo absent; fi'];
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const secret = yield* Task.exec("sh", args, { secret: true });
  yield* Effect.log(Redacted.value(secret).trim());
  yield* Effect.log((yield* Task.exec("sh", args)).trim());
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(linesOf(ran.standardError).filter((line) => line.startsWith("[main] "))).toEqual([
        "[main] absent",
        "[main] present",
      ]);
    }),
  ),
);

export const passingNestedRunWritesNoRecap = makeTest(
  "a passing nested Task run receives a recap path and writes no file",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": taskRunning("main", 'Task.exec("bun", ["inner.ts"], { print: true })'),
        "inner.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
// Task.exec supplies a fresh SSHKEDA_TASK_RECAP_FILE path unless secret or stdio: "inherit" is set.
// A nested Task run writes its recap there only when it prints a failure recap.
export default Task.make({ name: "inner", run: Effect.log("recap path " + process.env.SSHKEDA_TASK_RECAP_FILE) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      const prefix = "[main] [inner] recap path ";
      const recapPath = linesOf(ran.standardError)
        .find((line) => line.startsWith(prefix))
        ?.slice(prefix.length);
      expect(recapPath, ran.standardError).toStartWith(
        `${findRunDirectoryPath(ran.standardError)}/`,
      );
      const fileSystem = yield* FileSystem.FileSystem;
      expect(yield* fileSystem.exists(String(recapPath))).toBe(false);
    }),
  ),
);

export const failedCommandRecapsFewLines = makeTest(
  "a failed command that wrote one, two or three non-empty lines recaps all of them with no note, and one that wrote nothing recaps only its error",
  Effect.scoped(
    Effect.gen(function* () {
      expect(yield* runFailingScript("")).toEqual([]);
      expect(yield* runFailingScript("echo only")).toEqual(["recap: [main] only"]);
      expect(yield* runFailingScript("echo one; echo two >&2")).toEqual([
        "recap: [main] one",
        "recap: [main] two",
      ]);
      expect(yield* runFailingScript("echo one; echo; echo two; echo three >&2")).toEqual([
        "recap: [main] one",
        "recap: [main] two",
        "recap: [main] three",
      ]);
    }),
  ),
);

export const failedCommandRecapsOneStream = makeTest(
  "a failed command that wrote only to standard output, or only to standard error, recaps that stream's first line, a note counting the lines it leaves out, and its last two",
  Effect.scoped(
    Effect.gen(function* () {
      const fiveLines = [
        "recap: [main] 1",
        "recap: [main] (2 lines are not shown)",
        "recap: [main] 4",
        "recap: [main] 5",
      ];
      expect(yield* runFailingScript("seq 1 5")).toEqual(fiveLines);
      expect(yield* runFailingScript("seq 1 5 >&2")).toEqual(fiveLines);
    }),
  ),
);

const LONG_OUTPUT_LINE_LENGTH = 300;

const MOST_CODE_UNITS_OF_A_RECAPPED_LINE = 200;

const COMBINING_MARKS_AFTER_ONE_LETTER = 300;

const CONTROL_CHARACTERS_THAT_ESCAPE_PAST_THE_CUT = 60;

const CONTROL_CHARACTERS_SHOWN_ESCAPED = 27;

const roomBeforeTheNote = (lineLength: number) =>
  MOST_CODE_UNITS_OF_A_RECAPPED_LINE -
  ` (${String(lineLength)} more characters are not shown)`.length;

const COMBINING_MARKS_SHOWN = roomBeforeTheNote(COMBINING_MARKS_AFTER_ONE_LETTER + 1) - 1;

const LONG_OUTPUT_LINE_SHOWN = roomBeforeTheNote(LONG_OUTPUT_LINE_LENGTH);

export const failedCommandRecapEscapesAndCuts = makeTest(
  "a failed command's recapped lines escape control characters and every character a reader may take as a line break, and are cut to 200 code units with their note, never inside an escape, the note counting the code units left out, however few characters those units make, also when only escaping makes a line that long",
  Effect.scoped(
    Effect.gen(function* () {
      expect(
        yield* runFailingScript(
          String.raw`printf 'tab\there\033[31mred\n'; printf 'begin\342\200\250failed: [forged]\342\200\251error: [forged]\302\205vt\013ff\014end\n'; printf 'a'; seq ${COMBINING_MARKS_AFTER_ONE_LETTER} | while read -r mark; do printf '\314\201'; done; echo; printf '%0${LONG_OUTPUT_LINE_LENGTH}d\n' 0 >&2`,
        ),
      ).toEqual([
        String.raw`recap: [main] tab\there\u001B[31mred`,
        "recap: [main] (1 line is not shown)",
        `recap: [main] a${"́".repeat(COMBINING_MARKS_SHOWN)} (${String(COMBINING_MARKS_AFTER_ONE_LETTER - COMBINING_MARKS_SHOWN)} more characters are not shown)`,
        `recap: [main] ${"0".repeat(LONG_OUTPUT_LINE_SHOWN)} (${String(LONG_OUTPUT_LINE_LENGTH - LONG_OUTPUT_LINE_SHOWN)} more characters are not shown)`,
      ]);
      expect(
        yield* runFailingScript(
          String.raw`printf 'begin\342\200\250failed: [forged]\342\200\251error: [forged]\302\205vt\013ff\014end\n'`,
        ),
      ).toEqual([
        String.raw`recap: [main] begin failed: [forged] error: [forged]\u0085vt\u000Bff\u000Cend`,
      ]);
      expect(
        yield* runFailingScript(
          String.raw`printf '%0${CONTROL_CHARACTERS_THAT_ESCAPE_PAST_THE_CUT}d\n' 0 | tr 0 '\001'`,
        ),
      ).toEqual([
        `recap: [main] ${String.raw`\u0001`.repeat(CONTROL_CHARACTERS_SHOWN_ESCAPED)} (${String(CONTROL_CHARACTERS_THAT_ESCAPE_PAST_THE_CUT - CONTROL_CHARACTERS_SHOWN_ESCAPED)} more characters are not shown)`,
      ]);
    }),
  ),
);

export const taskNamesEscapeLineBreaks = makeTest(
  "a task name escapes every character a reader may take as a line break",
  Effect.sync(() => {
    expect(nameTask("a b c\u0085d\ve\ff")).toBe(String.raw`[a b c\u0085d\u000Be\u000Cf]`);
  }),
);

export const secretCommandRecapsNoOutput = makeTest(
  "a failed command recaps its output lines, and the same command run with secret: true recaps only its error, none of its output",
  Effect.scoped(
    Effect.gen(function* () {
      const runCommand = (options: string) =>
        Effect.gen(function* () {
          const project = yield* makeProject({
            "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "echo hunter2; echo hunter3 >&2; exit ${FAILED_COMMAND_EXIT_CODE}"]${options}) });`,
          });
          const ran = yield* runTaskProcess(project, "main.ts");
          expect(ran.exitCode, ran.standardError).toBe(FAILED_COMMAND_EXIT_CODE);
          return ran.standardError;
        });
      expect(recapLinesOf(yield* runCommand(""))).toEqual([
        `recap: [main] 1.output | main.ts:2:52: CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}`,
        "recap: [main] hunter2",
        "recap: [main] hunter3",
      ]);
      const secret = yield* runCommand(", { secret: true }");
      expect(recapLinesOf(secret)).toEqual([
        `recap: [main] 1.output | main.ts:2:52: CapturedCommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}, and its captured output is not shown`,
      ]);
      expect(secret).not.toContain("hunter");
    }),
  ),
);

const TWO_ERRORS_PROJECT = (errors: string) => `import { Cause, Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const commandFailed = yield* Effect.flip(Task.exec("sh", ["-c", "echo wrote this; exit ${FAILED_COMMAND_EXIT_CODE}"]));
  const taskError = new TaskError({ message: "the check failed" });
  return yield* Effect.failCause(Cause.combine(${errors}));
}) });`;

export const twoErrorsAtOnceRecapTheLast = makeTest(
  "a task that fails with two errors at once prints both and recaps the last, with the failed command's output only when the failed command is the last",
  Effect.scoped(
    Effect.gen(function* () {
      const commandLast = yield* makeProject({
        "main.ts": TWO_ERRORS_PROJECT("Cause.fail(taskError), Cause.fail(commandFailed)"),
      });
      const ranCommandLast = yield* runTaskProcess(commandLast, "main.ts");
      expect(ranCommandLast.exitCode, ranCommandLast.standardError).toBe(1);
      expect(ranCommandLast.standardError).toContain(
        "error: [main] main.ts:6:21: TaskError: the check failed\n",
      );
      expect(recapLinesOf(ranCommandLast.standardError)).toEqual([
        `recap: [main] 1.output | main.ts:5:49: CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}`,
        "recap: [main] wrote this",
      ]);
      const taskErrorLast = yield* makeProject({
        "main.ts": TWO_ERRORS_PROJECT("Cause.fail(commandFailed), Cause.fail(taskError)"),
      });
      const ranTaskErrorLast = yield* runTaskProcess(taskErrorLast, "main.ts");
      expect(ranTaskErrorLast.exitCode, ranTaskErrorLast.standardError).toBe(1);
      expect(ranTaskErrorLast.standardError).toContain(
        `error: [main] main.ts:5:49: CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}\n`,
      );
      expect(recapLinesOf(ranTaskErrorLast.standardError)).toEqual([
        "recap: [main] 1.output | main.ts:6:21: TaskError: the check failed",
      ]);
    }),
  ),
);

export const recapLinesAtTheErrorLevel = makeTest(
  "at SSHKEDA_TASK_LOG=error a failed command's recap still prints its output lines with the other recap lines",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "seq 1 5; exit ${FAILED_COMMAND_EXIT_CODE}"]) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_LOG: "error" });
      expect(ran.exitCode, ran.standardError).toBe(FAILED_COMMAND_EXIT_CODE);
      expect(recapLinesOf(ran.standardError)).toEqual([
        `recap: [main] 1.output | main.ts:2:52: CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}`,
        "recap: [main] 1",
        "recap: [main] (2 lines are not shown)",
        "recap: [main] 4",
        "recap: [main] 5",
      ]);
    }),
  ),
);

const TASKS_IN_FAILED_LINE_ORDER = ["d", "b", "a", "c"] as const;

const TASKS_IN_ENDING_ORDER = ["c", "a", "b", "d"] as const;

const PARALLEL_WORKERS = { SSHKEDA_TASK_WORKERS: "5" } as const;

export const recapOrderFollowsTheFailedLine = makeTest(
  "with more than three failed tasks, the recap lines follow the failed: line's order, not the order the tasks ended, and list the tasks it counts as more",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
const fail = (name: string) => Task.make({ name, run: Task.exec("cat", [\`gate-\${name}\`]).pipe(Effect.andThen(Effect.fail(new TaskError({ message: \`\${name} broke\` })))) });
export default Task.make({ name: "main", run: Task.allSettled([${TASKS_IN_FAILED_LINE_ORDER.map((name) => `fail("${name}")`).join(", ")}]) });`,
      });
      const started = yield* startTaskProcessWithPipes(
        project,
        TASKS_IN_ENDING_ORDER.map((name) => `gate-${name}`),
        PARALLEL_WORKERS,
      );
      yield* Effect.forEach(
        TASKS_IN_ENDING_ORDER,
        (name, index) => {
          const previous = TASKS_IN_ENDING_ORDER[index - 1];
          return started.releaseAfter(
            previous === undefined ? ["run: "] : [`ended: [${previous}]`],
            `gate-${name}`,
          );
        },
        { discard: true },
      );
      const ran = yield* started.waitForExit;
      expect(ran.exitCode, ran.standardError).toBe(1);
      const endedOrder = linesOf(ran.standardError).flatMap((line) => {
        const name = /^ended: \[(?<name>[a-d])\]/u.exec(line)?.groups?.["name"];
        return name === undefined ? [] : [name];
      });
      expect(endedOrder, ran.standardError).toEqual([...TASKS_IN_ENDING_ORDER]);
      expect(
        linesOf(ran.standardError).find((line) => line.startsWith("failed: ")),
        ran.standardError,
      ).toMatch(/^failed: \[d\] \[b\] \[a\] and 1 more \(4 of 5 tasks\), /u);
      expect(
        recapLinesOf(ran.standardError).map(
          (line) => /^recap: \[(?<name>\w)\]/u.exec(line)?.groups?.["name"],
        ),
        ran.standardError,
      ).toEqual([...TASKS_IN_FAILED_LINE_ORDER]);
    }),
  ),
);

export const longErrorRecap = makeTest(
  "an error of many lines recaps its first line, its next ten non-blank lines and a note counting the lines after them",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.die(new Error(["differs", "+ actual - expected", "", "  [", "    {", "      current: null,", "      model: 'fast',", "+     refused: null,", "-     refused: 'broken',", "      speed: 'fast'", "    },", "    {", "      current: null,", "      model: 'standard',"].join("\\n"))) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(recapLinesOf(ran.standardError), ran.standardError).toEqual([
        "recap: [main] 1.output | main.ts:3:62: Error: differs",
        "recap: [main] + actual - expected",
        "recap: [main]   [",
        "recap: [main]     {",
        "recap: [main]       current: null,",
        "recap: [main]       model: 'fast',",
        "recap: [main] +     refused: null,",
        "recap: [main] -     refused: 'broken',",
        "recap: [main]       speed: 'fast'",
        "recap: [main]     },",
        "recap: [main]     {",
        "recap: [main] (2 lines are not shown)",
      ]);
    }),
  ),
);

const FAILED_INNER_TASKS = 25;

const INNER_RECAP_LINES_SHOWN = 19;

export const longNestedRecapIsCountedByItsWriter = makeTest(
  "a nested run with more than 20 recap lines writes its first 19 and a note counting the rest, so the parent recaps that note",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": taskRunning("main", 'Task.exec("bun", ["inner.ts"])'),
        "inner.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const failing = Array.from({ length: ${String(FAILED_INNER_TASKS)} }, (_unused, index) =>
  Task.make({ name: \`t\${String(index)}\`, run: Effect.die(new Error("bad")) }));
export default Task.make({ name: "inner", run: Task.allSettled(failing) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      const recapLines = recapLinesOf(ran.standardError);
      expect(recapLines, ran.standardError).toContain(
        `recap: [main] (${String(FAILED_INNER_TASKS - INNER_RECAP_LINES_SHOWN)} nested recap lines are not shown)`,
      );
      expect(
        recapLines.filter((line) => line.startsWith("recap: [main] [t")),
        ran.standardError,
      ).toHaveLength(INNER_RECAP_LINES_SHOWN);
    }),
  ),
);

const LONG_MESSAGE_CHARACTERS = 201;

const MOST_CODE_UNITS_OF_A_RECAP_LINE = 200;

const LONG_HEADLINE_RECAP =
  /^recap: \[main\] 1\.output \| (?<body>(?<shown>.+) \((?<notShown>\d+) more characters are not shown\))$/u;

export const longRecapHeadlineIsCut = makeTest(
  "a recap's first line longer than 200 code units is cut to 200 with a note counting the code units left out",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.die(new Error("x".repeat(${String(LONG_MESSAGE_CHARACTERS)}))) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      const errorLine =
        linesOf(ran.standardError).find((line) => line.startsWith("error: [main] main.ts:")) ?? "";
      const [recapLine] = recapLinesOf(ran.standardError);
      const {
        body = "",
        shown = "",
        notShown = "",
      } = LONG_HEADLINE_RECAP.exec(recapLine ?? "")?.groups ?? {};
      expect(body.length, ran.standardError).toBeLessThanOrEqual(MOST_CODE_UNITS_OF_A_RECAP_LINE);
      expect(shown.length + Number(notShown)).toBe(errorLine.length - "error: [main] ".length);
    }),
  ),
);

export const recapEscapesGivenOutputLines = makeTest(
  "recap output lines given in a reported error print escaped, so they can't start a line of their own or carry terminal codes",
  Effect.sync(() => {
    expect(
      formatRecapLines("t", { text: "Step: bad", outputLines: ["a\nb\u001B[31m"] }, "1.output"),
    ).toEqual(["recap: [t] 1.output | Step: bad", String.raw`recap: [t] a\nb\u001B[31m`]);
  }),
);

export const lineBreaksAreCountedOnceEach = makeTest(
  "a line break is a line feed, a carriage return or the two together, counted once",
  Effect.sync(() => {
    expect(
      ["", "a", "a\n", "a\r\nb\r\n", "\r\r\n\n", "a\rb\nc", "\n\r"].map((text) =>
        String(countLineBreaks(text)),
      ),
    ).toEqual(["0", "0", "1", "2", "3", "2", "2"]);
  }),
);

const GIVEN_LINE_CHARACTERS = 198;

const GIVEN_LINE_CHARACTERS_SHOWN = 164;

const LONG_GIVEN_LINE = `${"x".repeat(GIVEN_LINE_CHARACTERS)}\u001B`;

export const recapCutsGivenOutputLines = makeTest(
  "a recap output line given in a reported error that is longer than 200 code units once escaped is cut to 200 with a note",
  Effect.sync(() => {
    expect(
      formatRecapLines("t", { text: "Step: bad", outputLines: [LONG_GIVEN_LINE] }, "1.output"),
    ).toEqual([
      "recap: [t] 1.output | Step: bad",
      `recap: [t] ${"x".repeat(GIVEN_LINE_CHARACTERS_SHOWN)} (35 more characters are not shown)`,
    ]);
  }),
);

const recapOfOneOutputLine = (outputLine: string) =>
  formatRecapLines("t", { text: "Step: bad", outputLines: [outputLine] }, "1.output");

export const cutRecapLineIsNotCutAgain = makeTest(
  "a recap line that was already cut prints unchanged when it is recapped again",
  Effect.sync(() => {
    const [, cutOnce = ""] = recapOfOneOutputLine(LONG_GIVEN_LINE);
    expect(recapOfOneOutputLine(cutOnce.slice("recap: [t] ".length))).toEqual([
      "recap: [t] 1.output | Step: bad",
      cutOnce,
    ]);
  }),
);

const MANY_LINES = 1_000_000;

export const manyLineBreaksAreCounted = makeTest(
  "a million line breaks are counted",
  Effect.sync(() => {
    expect(countLineBreaks("\r\nx".repeat(MANY_LINES))).toBe(MANY_LINES);
    expect(countLineFeeds("\n".repeat(MANY_LINES))).toBe(MANY_LINES);
  }),
);

const WIDE_TASK_NAME_CHARACTERS = 2000;

const WIDE_INNER_DETAIL_LINES = 10;

export const wideNestedRecapFitsItsFile = makeTest(
  "a nested run whose recap lines are over 64 KiB together writes the lines that fit and a note counting the rest, so the parent recaps them",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": taskRunning("main", 'Task.exec("bun", ["inner.ts"])'),
        "inner.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const details = Array.from({ length: ${String(WIDE_INNER_DETAIL_LINES)} }, (_unused, index) => \`detail \${String(index)}\`);
export default Task.make({ name: "界".repeat(${String(WIDE_TASK_NAME_CHARACTERS)}), run: Effect.die(new Error(["bad", ...details].join("\\n"))) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      const recapLines = recapLinesOf(ran.standardError);
      expect(recapLines, ran.standardError).toContain(
        "recap: [main] (1 nested recap line is not shown)",
      );
      expect(
        recapLines.filter((line) => line.startsWith("recap: [main] [界")),
        ran.standardError,
      ).toHaveLength(WIDE_INNER_DETAIL_LINES);
    }),
  ),
);
