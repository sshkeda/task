import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Cause, Effect, FileSystem, Redacted } from "effect";

import { TASK_STATE_DIRECTORY_NAME } from "../../src/shared/names-from-package-json.ts";
import { read } from "../shared/access-files.ts";
import { failureAsValue } from "../shared/failure-as-value.ts";
import { makeProject } from "../shared/make-projects.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";
import { typeCheckSource } from "../shared/type-check-source.ts";

const FAKE_SECRET = "fake-env-secret-5d1e";

const SPLIT_FAKE_SECRET_SOURCE = `"fake-env-sec" + "ret-5d1e"`;

const SECRET_SEEN_EXIT_CODE = 3;

const SECRET_NUL_SOURCE = String.raw`"fake-env-sec\0" + "ret-5d1e"`;

const SHOWN_SECRETS = [FAKE_SECRET, "sec\0ret-5d1e", "sec ret-5d1e"];

const CONSUMER_SOURCE = `import { Effect, Redacted } from "effect";
import { Task } from "@sshkeda/task";
const secret = Redacted.make(${SPLIT_FAKE_SECRET_SOURCE});
export const seen = Task.make("seen", Effect.gen(function* () {
  const length = yield* Task.exec("sh", ["-c", 'printf "%s" "$SECRET" | wc -c'], {
    env: { PLAIN: "plain", SECRET: secret },
  });
  yield* Effect.log(\`the command saw \${length.trim()} characters\`);
  yield* Task.exec("sh", ["-c", '[ "$SECRET" = "$EXPECTED" ] && exit ${SECRET_SEEN_EXIT_CODE}; exit 1'], {
    env: { EXPECTED: Redacted.value(secret), SECRET: secret },
  });
}));
export const nul = Task.make("nul", Task.exec("sh", ["-c", "true"], {
  env: { SECRET: Redacted.make(${SECRET_NUL_SOURCE}) },
}));
export const main = Task.make("main", Task.allSettled([seen, nul]));
if (import.meta.main) Task.main(main);
`;

const RETURNED_SECRET = "fake-returned-secret-7c4a";

const RETURNING_SOURCE = `import { Effect, Redacted } from "effect";
import { Task } from "@sshkeda/task";
export const token = Task.make("token", Task.exec("cat", ["secret.txt"], { secret: true }));
export const tokens = Task.make("tokens", Effect.gen(function* () {
  const value = yield* token;
  return { nested: { value }, list: [value] };
}));
export const main = Task.make("main", Effect.gen(function* () {
  const direct: Redacted.Redacted<string> = yield* token;
  const { nested, list } = yield* tokens;
  const nestedValue: Redacted.Redacted<string> = nested.value;
  yield* Effect.log(\`direct \${direct}, nested \${nestedValue}, listed \${list.length}\`);
  yield* Task.exec("sh", ["-c", 'printf "%s|%s|%s" "$DIRECT" "$NESTED" "$LISTED" > received.txt'], {
    env: { DIRECT: direct, NESTED: nestedValue, LISTED: list[0] ?? "missing" },
  });
  return { direct, nested };
}));
if (import.meta.main) Task.main(main);
`;

const readStateDirectoryTexts = (home: string) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const directory = `${home}/${TASK_STATE_DIRECTORY_NAME}`;
    const paths = yield* fileSystem.readDirectory(directory, { recursive: true });
    const files = yield* Effect.forEach(paths, (path) =>
      Effect.gen(function* () {
        if ((yield* fileSystem.stat(`${directory}/${path}`)).type !== "File") {
          return [];
        }
        return [{ path, text: yield* fileSystem.readFileString(`${directory}/${path}`) }];
      }),
    );
    return files.flat();
  });

export const redactedValueReachesTheCommand = Task.make(
  "Task.exec passes a Redacted env value to the command as its real value, next to plain values",
  Effect.gen(function* () {
    const output = yield* Task.exec("sh", ["-c", 'printf "%s|%s" "$PLAIN" "$SECRET"'], {
      env: { PLAIN: "plain", SECRET: Redacted.make(FAKE_SECRET) },
      secret: true,
    });
    expect(Redacted.value(output)).toBe(`plain|${FAKE_SECRET}`);
  }),
);

export const redactedValueWithNulIsRefusedWithoutShowingIt = Task.make(
  "Task.exec refuses a Redacted env value holding a NUL character, naming the variable but not the value",
  Effect.gen(function* () {
    const failure = yield* Task.exec("sh", ["-c", "true"], {
      env: { SECRET: Redacted.make("fake-env-sec\0ret-5d1e") },
    }).pipe(failureAsValue);
    expect(failure._tag).toBe("CommandNotStarted");
    expect(failure.message).toBe(
      "could not start sh: task's process service refused the environment variable SECRET for sh, a Redacted value holding a NUL character",
    );
    for (const shownSecret of SHOWN_SECRETS) {
      expect(String(failure)).not.toContain(shownSecret);
      expect(Cause.pretty(Cause.fail(failure))).not.toContain(shownSecret);
    }
  }),
);

export const redactedValueStaysOutOfOutputTracesAndErrors = Task.make(
  "a Redacted env value reaches commands in a run but is in neither its output, its errors nor any file in the state directory",
  Effect.scoped(
    Effect.gen(function* () {
      yield* typeCheckSource(CONSUMER_SOURCE);
      const project = yield* makeProject({ "main.ts": CONSUMER_SOURCE });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(ran.standardError).toContain(
        `[seen] the command saw ${FAKE_SECRET.length} characters\n`,
      );
      expect(ran.standardError).toContain(
        `[seen] error CommandFailed: sh exited with code ${SECRET_SEEN_EXIT_CODE}\n`,
      );
      expect(ran.standardError).toContain(
        "[nul] error CommandNotStarted: could not start sh: task's process service refused the environment variable SECRET for sh, a Redacted value holding a NUL character\n",
      );
      const stateFiles = yield* readStateDirectoryTexts(project.home);
      expect(stateFiles.some((file) => file.path.startsWith("traces/"))).toBe(true);
      for (const shownSecret of SHOWN_SECRETS) {
        expect(ran.standardOutput).not.toContain(shownSecret);
        expect(ran.standardError).not.toContain(shownSecret);
        expect(
          stateFiles.filter((file) => file.text.includes(shownSecret)).map((file) => file.path),
        ).toEqual([]);
      }
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const returnedRedactedValueReachesOtherTasks = Task.make(
  "a task returns the Redacted output of a secret command, alone or nested, and other tasks yield it typed as Redacted and pass it to commands, while it stays out of output, traces and state files",
  Effect.scoped(
    Effect.gen(function* () {
      yield* typeCheckSource(RETURNING_SOURCE);
      const project = yield* makeProject({
        "main.ts": RETURNING_SOURCE,
        "secret.txt": RETURNED_SECRET,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain(
        "[main] direct <redacted>, nested <redacted>, listed 1\n",
      );
      expect(yield* read(`${project.directory}/received.txt`)).toBe(
        `${RETURNED_SECRET}|${RETURNED_SECRET}|${RETURNED_SECRET}`,
      );
      const stateFiles = yield* readStateDirectoryTexts(project.home);
      expect(stateFiles.some((file) => file.path.startsWith("traces/"))).toBe(true);
      expect(ran.standardOutput).not.toContain(RETURNED_SECRET);
      expect(ran.standardError).not.toContain(RETURNED_SECRET);
      expect(
        stateFiles.filter((file) => file.text.includes(RETURNED_SECRET)).map((file) => file.path),
      ).toEqual([]);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);
