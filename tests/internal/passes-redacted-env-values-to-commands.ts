import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Cause, Effect, Redacted } from "effect";

import { read } from "../shared/access-files.ts";
import { failureAsValue } from "../shared/failure-as-value.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { readStateDirectoryTexts } from "../shared/read-state-directory-texts.ts";
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
export const seen = Task.make({ name: "seen", run: Effect.gen(function* () {
  const length = yield* Task.exec("sh", ["-c", 'printf "%s" "$SECRET" | wc -c'], {
    env: { PLAIN: "plain", SECRET: secret },
  });
  yield* Effect.log(\`the command saw \${length.trim()} characters\`);
  yield* Task.exec("sh", ["-c", '[ "$SECRET" = "$EXPECTED" ] && exit ${SECRET_SEEN_EXIT_CODE}; exit 1'], {
    env: { EXPECTED: Redacted.value(secret), SECRET: secret },
  });
}) });
export const nul = Task.make({ name: "nul", run: Effect.asVoid(Task.exec("sh", ["-c", "true"], {
  env: { SECRET: Redacted.make(${SECRET_NUL_SOURCE}) },
})) });
export default Task.make({ name: "main", run: Task.allSettled([seen, nul]) });
`;

const NON_STRING_SECRET_PART = "fake-listed-secret-9b3f";

const NUMERIC_SECRET = 731_904_266;

const UNTYPED_SOURCE = `import { Redacted } from "effect";
import { Task } from "@sshkeda/task";
export const listed = Task.make({ name: "listed", run: Task.exec("sh", ["-c", "true"], {
  secret: true,
  env: { TOKEN: Redacted.make(["${NON_STRING_SECRET_PART}", "x\\0"]) },
}) });
export const numeric = Task.make({ name: "numeric", run: Task.exec("sh", ["-c", "true"], {
  env: { PLAIN: "plain", TOKEN: Redacted.make(${NUMERIC_SECRET}) },
}) });
export const argument = Task.make({ name: "argument", run: Task.exec("echo", ["plain", Redacted.make(["${NON_STRING_SECRET_PART}"])]) });
export default Task.make({ name: "main", run: Task.allSettled([listed, numeric, argument]) });
`;

const RETURNED_SECRET = "fake-returned-secret-7c4a";

const RETURNING_SOURCE = `import { Effect, Redacted, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const token = Task.make({ name: "token", output: Schema.Redacted(Schema.String), run: Task.exec("cat", ["secret.txt"], { secret: true }) });
export const tokens = Task.make({ name: "tokens", output: Schema.Struct({ nested: Schema.Struct({ value: Schema.Redacted(Schema.String) }), list: Schema.Array(Schema.Redacted(Schema.String)) }), run: Effect.gen(function* () {
  const value = yield* token;
  return { nested: { value }, list: [value] };
}) });
export default Task.make({ name: "main", output: Schema.Struct({ direct: Schema.Redacted(Schema.String), nested: Schema.Struct({ value: Schema.Redacted(Schema.String) }) }), run: Effect.gen(function* () {
  const direct: Redacted.Redacted<string> = yield* token;
  const { nested, list } = yield* tokens;
  const nestedValue: Redacted.Redacted<string> = nested.value;
  yield* Effect.log(\`direct \${direct}, nested \${nestedValue}, listed \${list.length}\`);
  yield* Task.exec("sh", ["-c", 'printf "%s|%s|%s" "$DIRECT" "$NESTED" "$LISTED" > received.txt'], {
    env: { DIRECT: direct, NESTED: nestedValue, LISTED: list[0] ?? "missing" },
  });
  return { direct, nested };
}) });
`;

export const redactedValueReachesTheCommand = Task.make({
  name: "Task.exec passes a Redacted env value to the command as its real value, next to plain values",
  run: Effect.gen(function* () {
    const output = yield* Task.exec("sh", ["-c", 'printf "%s|%s" "$PLAIN" "$SECRET"'], {
      env: { PLAIN: "plain", SECRET: Redacted.make(FAKE_SECRET) },
      secret: true,
    });
    expect(Redacted.value(output)).toBe(`plain|${FAKE_SECRET}`);
  }),
});

export const redactedValueWithNulIsRefusedWithoutShowingIt = Task.make({
  name: "Task.exec refuses a Redacted env value holding a NUL character, naming the variable but not the value",
  run: Effect.gen(function* () {
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
});

export const redactedValueStaysOutOfOutputTracesAndErrors = makeTest(
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
);

export const nonStringRedactedValueIsRefusedWithoutShowingIt = makeTest(
  "a Redacted command value that plain JavaScript made from a non-string is refused, naming it but leaving its value out of output, errors and state files",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.js": UNTYPED_SOURCE });
      const ran = yield* runTaskProcess(project, "main.js");
      expect(ran.exitCode, ran.standardError).toBe(1);
      for (const [taskName, programName, valueName] of [
        ["listed", "sh", "the environment variable TOKEN"],
        ["numeric", "sh", "the environment variable TOKEN"],
        ["argument", "echo", "argument 2"],
      ]) {
        expect(ran.standardError).toContain(
          `[${taskName}] error CommandNotStarted: could not start ${programName}: task's process service refused ${valueName} for ${programName}, a Redacted value that is not a string\n`,
        );
      }
      const stateFiles = yield* readStateDirectoryTexts(project.home);
      expect(stateFiles.some((file) => file.path.startsWith("traces/"))).toBe(true);
      for (const shownSecret of [NON_STRING_SECRET_PART, String(NUMERIC_SECRET)]) {
        expect(ran.standardOutput).not.toContain(shownSecret);
        expect(ran.standardError).not.toContain(shownSecret);
        expect(
          stateFiles.filter((file) => file.text.includes(shownSecret)).map((file) => file.path),
        ).toEqual([]);
      }
    }),
  ),
);

export const returnedRedactedValueReachesOtherTasks = makeTest(
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
);
