import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import { read } from "../shared/access-files.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { withRunLinePathNamed } from "../shared/name-output-files.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

const FAILED_COMMAND_EXIT_CODE = 3;

const findLocatedLines = (standardError: string) =>
  withRunLinePathNamed(standardError)
    .split("\n")
    .filter((line) => /^(?:error|recap): \[[^\]]*\] (?:\d+\.output \| )?\S+:\d+:\d+: /u.test(line));

const linesOf = (taskName: string, outputFile: string, place: string, error: string) => [
  `error: [${taskName}] ${place}: ${error}`,
  `recap: [${taskName}] ${outputFile} | ${place}: ${error}`,
];

const COMMAND_FAILED = `CommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}`;

const HELPER_FUNCTIONS = `const calls: (readonly string[])[] = [];
export const runReturned = (args: readonly string[]) => Task.exec("sh", args);
export const runWithStatement = (args: readonly string[]) => {
  const command = Task.exec("sh", args);
  calls.push(args);
  return command;
};`;

const TASKS_USING_HELPERS = `const fails = ["-c", "exit ${FAILED_COMMAND_EXIT_CODE}"];
export const returned = Task.make({ name: "returned", run: Effect.gen(function* () {
  yield* runReturned(fails);
}) });
export const withStatement = Task.make({ name: "with-statement", run: Effect.gen(function* () {
  yield* runWithStatement(fails);
}) });
export default Task.make({ name: "main", run: Task.allSettled([returned, withStatement], { concurrency: 1 }) });`;

const IMPORTS = `import { Effect } from "effect";
import { Task } from "@sshkeda/task";`;

const helpersFile = `import { Task } from "@sshkeda/task";
${HELPER_FUNCTIONS}`;

const tasksImportingHelpersFrom = (helpers: string) => `${IMPORTS}
import { runReturned, runWithStatement } from "${helpers}";
${TASKS_USING_HELPERS}`;

export const commandOfSeveral = makeTest(
  "when one of several commands in a task fails, its error and recap lines name the line and column of the Task.exec call that started it, not where the task was made",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "two-execs.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";

export default Task.make({
  name: "main",
  run: Effect.gen(function* () {
    yield* Task.exec("sh", ["-c", "echo first ok"]);
    yield* Task.exec("sh", ["-c", "echo second fails; exit ${FAILED_COMMAND_EXIT_CODE}"]);
    yield* Task.exec("sh", ["-c", "echo third never runs"]);
  }),
});`,
      });
      const ran = yield* runTaskProcess(project, "two-execs.ts");
      expect(ran.exitCode, ran.standardError).toBe(FAILED_COMMAND_EXIT_CODE);
      expect(findLocatedLines(ran.standardError)).toEqual(
        linesOf("main", "1.output", "two-execs.ts:8:17", COMMAND_FAILED),
      );
      expect(ran.standardError).not.toContain("third never runs");
    }),
  ),
);

export const commandAtTheTopOfRun = makeTest(
  "a command whose Task.exec call is wrapped in Effect.asVoid at the top level of run: names that call when it fails",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({
  name: "main",
  run: Effect.asVoid(Task.exec("sh", ["-c", "exit ${FAILED_COMMAND_EXIT_CODE}"])),
});`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(FAILED_COMMAND_EXIT_CODE);
      expect(findLocatedLines(ran.standardError)).toEqual(
        linesOf("main", "1.output", "main.ts:5:27", COMMAND_FAILED),
      );
    }),
  ),
);

export const helperInTheSameFile = makeTest(
  "a command started by a helper in the same file names the helper's Task.exec call, or the helper's caller when the helper returns the call directly and Bun drops its frame",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `${IMPORTS}
${HELPER_FUNCTIONS}
${TASKS_USING_HELPERS}`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      const lines = findLocatedLines(ran.standardError);
      expect(lines.toSorted()).toEqual(
        [
          ...linesOf("returned", "2.output", "main.ts:12:10", COMMAND_FAILED),
          ...linesOf("with-statement", "3.output", "main.ts:6:24", COMMAND_FAILED),
        ].toSorted(),
      );
    }),
  ),
);

export const helperInASiblingPackage = makeTest(
  "a command started by a helper in a sibling workspace package names the full path of the helper's Task.exec call, or the helper's caller when the helper returns the call directly",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "package.json": '{ "name": "monorepo", "type": "module", "workspaces": ["packages/*"] }',
        "packages/app/package.json": '{ "name": "app", "type": "module" }',
        "packages/tools/package.json": '{ "name": "tools", "type": "module" }',
        "packages/tools/index.ts": helpersFile,
        "packages/app/main.ts": tasksImportingHelpersFrom("../tools/index.ts"),
      });
      const ran = yield* runTaskProcess(
        project,
        "packages/app/main.ts",
        {},
        `${project.directory}/packages/app`,
      );
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(findLocatedLines(ran.standardError).toSorted()).toEqual(
        [
          ...linesOf("returned", "2.output", "main.ts:6:10", COMMAND_FAILED),
          ...linesOf(
            "with-statement",
            "3.output",
            `${project.directory}/packages/tools/index.ts:5:24`,
            COMMAND_FAILED,
          ),
        ].toSorted(),
      );
    }),
  ),
);

export const helperInAnInstalledPackage = makeTest(
  "a command started by a helper in a package under node_modules never names a line inside node_modules: it names where its task was made, as before, or the helper's caller when the helper returns the call directly",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "node_modules/@acme/run/package.json":
          '{ "name": "@acme/run", "type": "module", "exports": "./index.ts" }',
        "node_modules/@acme/run/index.ts": helpersFile,
        "main.ts": tasksImportingHelpersFrom("@acme/run"),
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(findLocatedLines(ran.standardError).toSorted()).toEqual(
        [
          ...linesOf("returned", "2.output", "main.ts:6:10", COMMAND_FAILED),
          ...linesOf("with-statement", "3.output", "main.ts:8:35", COMMAND_FAILED),
        ].toSorted(),
      );
      expect(ran.standardError).not.toContain("node_modules/@acme");
    }),
  ),
);

export const otherCommandErrors = makeTest(
  'a command that fails with its output captured, with stdio: "inherit", without starting, in a working directory that does not exist, refused by the process service, or whose standard input fails, names the Task.exec call that started it',
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Redacted, Stream } from "effect";
import { Task } from "@sshkeda/task";
const fails = ["-c", "exit ${FAILED_COMMAND_EXIT_CODE}"];
export const secret = Task.make({ name: "secret", run: Effect.gen(function* () {
  yield* Task.exec("sh", fails, { secret: true });
}) });
export const notStarted = Task.make({ name: "not-started", run: Effect.gen(function* () {
  yield* Task.exec("no-such-command-anywhere", []);
}) });
export const badDirectory = Task.make({ name: "bad-directory", run: Effect.gen(function* () {
  yield* Task.exec("sh", fails, { cwd: "/no/such/directory" });
}) });
export const refused = Task.make({ name: "refused", run: Effect.gen(function* () {
  yield* Task.exec("sh", fails, { env: { SECRET: Redacted.make("a\\u0000b") } });
}) });
export const standardInputFailed = Task.make({ name: "stdin-failed", run: Effect.gen(function* () {
  yield* Task.exec("cat", [], { stdin: Stream.fail("stream broke") });
}) });
export default Task.make({
  name: "main",
  run: Task.allSettled([secret, notStarted, badDirectory, refused, standardInputFailed], { concurrency: 1 }),
});`,
        "inherit.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "inherit", run: Effect.gen(function* () {
  yield* Task.exec("sh", ["-c", "exit ${FAILED_COMMAND_EXIT_CODE}"], { stdio: "inherit" });
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      const firstLines = findLocatedLines(ran.standardError).filter((line) =>
        line.includes(".ts:"),
      );
      expect(firstLines.toSorted()).toEqual(
        [
          ...linesOf(
            "secret",
            "2.output",
            "main.ts:5:15",
            `CapturedCommandFailed: sh exited with code ${FAILED_COMMAND_EXIT_CODE}, and its captured output is not shown`,
          ),
          ...linesOf(
            "not-started",
            "3.output",
            "main.ts:8:15",
            'Error: Executable not found in $PATH: "no-such-command-anywhere"',
          ),
          ...linesOf(
            "bad-directory",
            "4.output",
            "main.ts:11:15",
            "WorkingDirectoryUnusable: the working directory /no/such/directory doesn't exist",
          ),
          ...linesOf(
            "refused",
            "5.output",
            "main.ts:14:15",
            "RedactedValueWithNulRefused: task's process service refused the environment variable SECRET for sh, a Redacted value holding a NUL character",
          ),
          ...linesOf("stdin-failed", "6.output", "main.ts:17:15", "UntaggedFailure: stream broke"),
        ].toSorted(),
      );
      const inherited = yield* runTaskProcess(project, "inherit.ts");
      expect(inherited.exitCode, inherited.standardError).toBe(FAILED_COMMAND_EXIT_CODE);
      expect(findLocatedLines(inherited.standardError)).toEqual(
        linesOf("inherit", "1.output", "inherit.ts:4:15", COMMAND_FAILED),
      );
    }),
  ),
);

export const commandsInCleanup = makeTest(
  "a command run by a task's cleanup names its Task.exec call, and a call an Effect callback returns directly, with no frame of the user's left, names where the task was made, as before",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const fails = ["-c", "exit ${FAILED_COMMAND_EXIT_CODE}"];
export const arrow = Task.make({ name: "arrow", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.orDie(Task.exec("sh", fails)));
}) });
export const generator = Task.make({ name: "generator", run: Effect.gen(function* () {
  yield* Effect.addFinalizer(() => Effect.gen(function* () {
    yield* Effect.orDie(Task.exec("sh", fails));
  }));
}) });
export const flatMapped = Task.make({
  name: "flat-mapped",
  run: Effect.void.pipe(Effect.flatMap(() => Task.exec("sh", fails))),
});
export default Task.make({
  name: "main",
  run: Task.allSettled([arrow, generator, flatMapped], { concurrency: 1 }),
});`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(
        findLocatedLines(ran.standardError)
          .filter((line) => line.includes(".ts:"))
          .toSorted(),
      ).toEqual(
        [
          ...linesOf("arrow", "2.output", "main.ts:5:54", COMMAND_FAILED),
          ...linesOf("generator", "3.output", "main.ts:9:30", COMMAND_FAILED),
          ...linesOf("flat-mapped", "4.output", "main.ts:12:32", COMMAND_FAILED),
        ].toSorted(),
      );
    }),
  ),
);

export const commandsAfterAResume = makeTest(
  "a command a callback returns directly, in a fiber resumed by a Node callback API in another file or by a timer, names where its task was made, as before, not the line that resumed the fiber",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "lib/read.ts": `import { readFile } from "node:fs";
import { Effect } from "effect";
export const readText = (path: string) =>
  Effect.callback<string, Error>((resume) => {
    readFile(path, "utf8", (error, text) => {
      resume(error ? Effect.fail(error) : Effect.succeed(text));
    });
  });`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { readText } from "./lib/read.ts";
const fails = ["-c", "exit ${FAILED_COMMAND_EXIT_CODE}"];
const timer = Effect.callback<void>((resume) => {
  setTimeout(() => {
    resume(Effect.void);
  }, 1);
});
export const read = Task.make({
  name: "read",
  run: readText("package.json").pipe(Effect.flatMap(() => Task.exec("sh", fails))),
});
export const timed = Task.make({
  name: "timed",
  run: timer.pipe(Effect.flatMap(() => Task.exec("sh", fails))),
});
export default Task.make({ name: "main", run: Task.allSettled([read, timed], { concurrency: 1 }) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(findLocatedLines(ran.standardError).toSorted()).toEqual(
        [
          ...linesOf("read", "2.output", "main.ts:10:26", COMMAND_FAILED),
          ...linesOf("timed", "3.output", "main.ts:14:27", COMMAND_FAILED),
        ].toSorted(),
      );
    }),
  ),
);

export const locationField = makeTest(
  "a failed command's error carries the place of its Task.exec call as an absolute location, and its message, command and other fields stay as they were",
  Effect.gen(function* () {
    const failure = yield* Task.exec("sh", ["-c", `exit ${FAILED_COMMAND_EXIT_CODE}`]).pipe(
      Effect.flatMap((output) => Effect.die(`the command succeeded and printed ${output}`)),
      Effect.catchTag("CommandFailed", (commandFailed) => Effect.succeed(commandFailed)),
    );
    expect(failure.command).toBe("sh");
    expect(failure.message).toBe(`sh exited with code ${FAILED_COMMAND_EXIT_CODE}`);
    const [, filePath, line, column] =
      /^(?<filePath>.+):(?<line>\d+):(?<column>\d+)$/u.exec(failure.location ?? "") ?? [];
    expect(filePath).toBe(import.meta.path);
    const sourceLines = (yield* read(import.meta.path)).split("\n");
    expect(sourceLines[Number(line) - 1]?.slice(Number(column) - 1)).toStartWith("exec(");
  }),
);
