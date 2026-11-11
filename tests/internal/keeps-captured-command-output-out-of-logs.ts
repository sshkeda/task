import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect, FileSystem, Redacted } from "effect";

import { TASK_STATE_DIRECTORY_NAME } from "../../src/shared/names-from-package-json.ts";
import { failureAsValue } from "../shared/failure-as-value.ts";
import { makeProject } from "../shared/make-projects.ts";
import type { makeProjectUsingTaskFrom } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";
import { typeCheckSource } from "../shared/type-check-source.ts";

const FAKE_SECRET_OUTPUT = "fake-token-7f3a9c";

const FAKE_SECRET_ERROR = "fake-error-2b81";

const PRINT_FAKE_SECRETS = "printf '%s%s' fake-token- 7f3a9c; printf '%s%s' fake-error- 2b81 >&2";

const FAILING_EXIT_CODE = 3;

const CAPTURED_FAILURE_LINE =
  "[main] error CapturedCommandFailed: sh exited with code 3, and its captured output is not shown";

const readStateDirectoryFiles = (
  project: Effect.Success<ReturnType<typeof makeProjectUsingTaskFrom>>,
) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const directory = `${project.home}/${TASK_STATE_DIRECTORY_NAME}`;
    const files = yield* Effect.forEach(
      yield* fileSystem.readDirectory(directory, { recursive: true }),
      (path) =>
        Effect.gen(function* () {
          if ((yield* fileSystem.stat(`${directory}/${path}`)).type !== "File") {
            return [];
          }
          return [
            {
              path,
              text: new TextDecoder().decode(yield* fileSystem.readFile(`${directory}/${path}`)),
            },
          ];
        }),
    );
    return files.flat();
  });

const expectNoFakeSecrets = (
  ran: { readonly standardOutput: string; readonly standardError: string },
  stateFiles: readonly { readonly path: string; readonly text: string }[],
) => {
  expect(stateFiles.some((file) => file.path.startsWith("traces/"))).toBe(true);
  for (const secret of [FAKE_SECRET_OUTPUT, FAKE_SECRET_ERROR]) {
    expect(ran.standardOutput).not.toContain(secret);
    expect(ran.standardError).not.toContain(secret);
    expect(
      stateFiles.filter((file) => file.text.includes(secret)).map((file) => file.path),
    ).toEqual([]);
  }
};

export const capturedOutputStaysOutOfLogs = makeTest(
  "Task.exec with secret: true logs no line of the command, so its output is in neither the run's output nor any file in the state directory",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Redacted } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const token = yield* Task.exec("sh", ["-c", "${PRINT_FAKE_SECRETS}"], { secret: true });
  yield* Effect.log(\`captured \${Redacted.value(token).length} characters\`);
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      const stateFiles = yield* readStateDirectoryFiles(project);
      expectNoFakeSecrets(ran, stateFiles);
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain(
        `[main] captured ${FAKE_SECRET_OUTPUT.length} characters\n`,
      );
      expect(
        stateFiles.some(
          (file) =>
            file.path.startsWith("traces/") &&
            file.text.includes(`captured ${FAKE_SECRET_OUTPUT.length} characters`),
        ),
      ).toBe(true);
    }),
  ),
);

export const capturedOutputUnwraps = Task.make({
  name: "Task.exec with secret: true returns standard output as a Redacted that unwraps to the exact text",
  run: Effect.gen(function* () {
    const output = yield* Task.exec("sh", ["-c", String.raw`printf '  first line\nsecond\n'`], {
      secret: true,
    });
    expect(Redacted.isRedacted(output)).toBe(true);
    expect(Redacted.value(output)).toBe("  first line\nsecond\n");
  }),
});

export const capturedFailureHidesOutput = makeTest(
  "a failed command run with secret: true keeps both outputs Redacted on its CapturedCommandFailed, and out of its message, the run's output and the trace, and the run exits with its code",
  Effect.scoped(
    Effect.gen(function* () {
      yield* Task.exec("sh", ["-c", `${PRINT_FAKE_SECRETS}; exit ${FAILING_EXIT_CODE}`], {
        secret: true,
      }).pipe(
        Effect.flatMap(() => Effect.die("the command succeeded")),
        Effect.catchTag("CapturedCommandFailed", (failure) =>
          Effect.sync(() => {
            expect(failure.code).toBe(FAILING_EXIT_CODE);
            expect(failure.message).toBe(
              "sh exited with code 3, and its captured output is not shown",
            );
            expect(Redacted.value(failure.standardOutput)).toBe(FAKE_SECRET_OUTPUT);
            expect(Redacted.value(failure.standardError)).toBe(FAKE_SECRET_ERROR);
          }),
        ),
      );
      const project = yield* makeProject({
        "main.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Task.exec("sh", ["-c", "${PRINT_FAKE_SECRETS}; exit ${FAILING_EXIT_CODE}"], { secret: true }) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(FAILING_EXIT_CODE);
      expect(
        ran.standardError.split("\n").filter((line) => line.startsWith("[main] error ")),
      ).toEqual([CAPTURED_FAILURE_LINE]);
      expectNoFakeSecrets(ran, yield* readStateDirectoryFiles(project));
    }),
  ),
);

const ROOT_TASK_COMMAND_EXIT_CODE = 4;

const CONSUMER_SECRET_OPTION_SOURCE = `import { Effect, Redacted } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const byDefault: string = yield* Task.exec("sh", ["-c", "echo default line"]);
  const notSecret: string = yield* Task.exec("sh", ["-c", "echo not secret line"], { secret: false });
  const captured: Redacted.Redacted<string> = yield* Task.exec("sh", ["-c", "echo captured line"], { secret: true });
  yield* Effect.log(\`returned \${JSON.stringify([byDefault, notSecret, Redacted.value(captured)])}\`);
  yield* Task.exec("sh", ["-c", "echo checked; echo broken >&2; exit 3"], { secret: false }).pipe(
    Effect.catchTag("CommandFailed", (failure) => Effect.log(\`caught \${failure.standardOutput.trim()} \${failure.standardError.trim()}\`)),
  );
  yield* Task.exec("sh", ["-c", "echo failed; exit ${ROOT_TASK_COMMAND_EXIT_CODE}"]);
}) });
`;

export const defaultOutputLogs = makeTest(
  "Task.exec logs each line and returns a string by default and with secret: false",
  Effect.scoped(
    Effect.gen(function* () {
      yield* typeCheckSource(CONSUMER_SECRET_OPTION_SOURCE);
      const project = yield* makeProject({ "main.ts": CONSUMER_SECRET_OPTION_SOURCE });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(ROOT_TASK_COMMAND_EXIT_CODE);
      expect(ran.standardError).toContain(
        `[main] default line\n[main] not secret line\n[main] returned ["default line\\n","not secret line\\n","captured line\\n"]\n[main] checked\n[main] broken\n[main] caught checked broken\n[main] failed\n[main] error CommandFailed: sh exited with code ${ROOT_TASK_COMMAND_EXIT_CODE}\nfailed: main (1 of 1 task), `,
      );
    }),
  ),
);

export const optionalSecretOptionsCompile = makeTest(
  "Task.exec takes options whose secret is an optional boolean, typed as they are or derived from its parameters",
  Effect.scoped(
    typeCheckSource(`import { Effect, Redacted } from "effect";
import { Task } from "@sshkeda/task";
const options: { cwd?: string; secret?: boolean } = { cwd: "." };
const derived: NonNullable<Parameters<typeof Task.exec>[2]> = { cwd: "." };
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const fromOptions: string | Redacted.Redacted<string> = yield* Task.exec("echo", ["hello"], options);
  const fromDerived: string | Redacted.Redacted<string> = yield* Task.exec("echo", ["derived"], derived);
  yield* Effect.log(fromOptions, fromDerived);
}) });`),
  ),
);

export const secretTypeNeedsTheSecretOption = makeTest(
  "Task.exec typed as secret without secret: true does not compile",
  Effect.scoped(
    Effect.gen(function* () {
      const failed = yield* failureAsValue(
        typeCheckSource(`import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const bare = Task.exec<true>("echo", ["bare"]);
export const other = Task.exec<true>("echo", ["other"], { cwd: "." });
export default Task.make({ name: "main", run: Effect.void });`),
      );
      expect(failed.message).toContain(
        "main.ts(3,26): error TS2554: Expected 3 arguments, but got 2.",
      );
      expect(failed.message).toContain("main.ts(4,57): error TS2741");
      expect(failed.message).toContain("Property 'secret' is missing");
    }),
  ),
);
