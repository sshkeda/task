import { expect } from "bun:test";

import { Effect } from "effect";

import { read } from "../shared/access-files.ts";
import { failureAsValue } from "../shared/failure-as-value.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { readStateDirectoryTexts } from "../shared/read-state-directory-texts.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";
import { typeCheckSource } from "../shared/type-check-source.ts";

const DECLARED_SOURCE = `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
const built = Task.make({
  name: "built",
  output: Schema.Struct({ at: Schema.Date, sizes: Schema.Array(Schema.Finite) }),
  run: Effect.succeed({ at: new Date(0), sizes: [1, 2] }),
});
const server = Task.make({
  name: "server",
  output: Schema.Struct({ url: Schema.String, startedAt: Schema.Date }),
  run: Effect.gen(function* () {
    return yield* Task.serve({ url: "http://localhost:1", startedAt: new Date(1) });
  }),
});
const quiet = Task.make({ name: "quiet", run: Effect.log("quiet ran") });
export default Task.make({
  name: "main",
  run: Effect.gen(function* () {
    const made = yield* built;
    const served = yield* server;
    const nothing: void = yield* quiet;
    const at: Date = made.at;
    const sizes: readonly number[] = made.sizes;
    const startedAt: Date = served.startedAt;
    yield* Effect.log(\`built \${at instanceof Date} \${at.toISOString()} \${sizes.join(",")}\`);
    yield* Effect.log(\`served \${served.url} \${startedAt.getTime()} \${String(nothing)}\`);
  }),
});
`;

export const declaredOutputs = makeTest(
  "a task made from one object returns or serves the value its output schema declares, and a task that yields it gets that value decoded, such as a Date",
  Effect.scoped(
    Effect.gen(function* () {
      yield* typeCheckSource(DECLARED_SOURCE);
      const project = yield* makeProject({ "main.ts": DECLARED_SOURCE });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("[main] built true 1970-01-01T00:00:00.000Z 1,2\n");
      expect(ran.standardError).toContain("[main] served http://localhost:1 1 undefined\n");
      expect(ran.standardError).toMatch(/^passed: 4 tasks, [\d.]+s$/mu);
    }),
  ),
);

const DROPPED_SECRET = "fake-dropped-secret-91d4";

const DROPPED_SOURCE = `import { Effect, Redacted } from "effect";
import { Task } from "@sshkeda/task";
const typecheck = Task.make({
  name: "typecheck",
  run: Task.exec("sh", ["-c", "printf checked > checked.txt"]),
});
const returns = Task.make({ name: "returns", run: Effect.succeed(1) });
const serves = Task.make({
  name: "serves",
  run: Effect.gen(function* () {
    return yield* Task.serve({ port: 1 });
  }),
});
const returnsSecret = Task.make({
  name: "returnsSecret",
  run: Effect.succeed({ token: Redacted.make("fake-dropped-sec" + "ret-91d4"), plain: "fake-dropped-sec" + "ret-91d4" }),
});
const servesSecret = Task.make({
  name: "servesSecret",
  run: Effect.gen(function* () {
    return yield* Task.serve("fake-dropped-sec" + "ret-91d4");
  }),
});
export default Task.make({
  name: "main",
  run: Effect.gen(function* () {
    const checked: void = yield* typecheck;
    const returned: void = yield* returns;
    const served: void = yield* serves;
    const secretReturned: void = yield* returnsSecret;
    const secretServed: void = yield* servesSecret;
    yield* Effect.log(
      \`dropped \${String(checked)} \${String(returned)} \${String(served)} \${String(secretReturned)} \${String(secretServed)}\`,
    );
  }),
});
`;

export const droppedOutputs = makeTest(
  "a task without an output in a file that is not type-checked drops the value it returns or serves, still serves, and its value reaches no other task, output, trace or state file",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({ "main.ts": DROPPED_SOURCE });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain(
        "[main] dropped undefined undefined undefined undefined undefined\n",
      );
      expect(ran.standardError).toMatch(/^passed: 6 tasks, [\d.]+s$/mu);
      expect(yield* read(`${project.directory}/checked.txt`)).toBe("checked");
      const stateFiles = yield* readStateDirectoryTexts(project.home);
      expect(stateFiles.some((file) => file.path.startsWith("traces/"))).toBe(true);
      expect(ran.standardOutput).not.toContain(DROPPED_SECRET);
      expect(ran.standardError).not.toContain(DROPPED_SECRET);
      expect(
        stateFiles.filter((file) => file.text.includes(DROPPED_SECRET)).map((file) => file.path),
      ).toEqual([]);
    }),
  ),
);

export const mismatchedOutput = makeTest(
  "a task whose value does not match its output schema fails with TaskOutputInvalid, naming where and what was expected without printing the value",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({
  name: "main",
  output: Schema.Struct({ deploy: Schema.Struct({ url: Schema.String }) }),
  run: Effect.succeed({ deploy: { url: 20_260_101 } }),
});
`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(1);
      expect(ran.standardError).toMatch(
        /^\[main\] error TaskOutputInvalid: returned a value that does not match the task's output at \.deploy\.url: Expected string$/mu,
      );
      expect(ran.standardError).not.toContain("20260101");
    }),
  ),
);

const OUTPUT_SECRET = "fake-output-secret-7c2a";

const REDACTED_SOURCE = `import { Effect, Redacted, Schema } from "effect";
import { Task } from "@sshkeda/task";
const token = Task.make({
  name: "token",
  output: Schema.Struct({ token: Schema.Redacted(Schema.String, { label: "API_TOKEN" }) }),
  run: Effect.succeed({ token: Redacted.make("fake-output-sec" + "ret-7c2a", { label: "API_TOKEN" }) }),
});
const password = Task.make({
  name: "password",
  output: Schema.Redacted(Schema.String, { label: "DB_PASSWORD" }),
  run: Effect.gen(function* () {
    return yield* Task.serve(Redacted.make("fake-output-sec" + "ret-7c2a", { label: "DB_PASSWORD" }));
  }),
});
export default Task.make({
  name: "main",
  run: Effect.gen(function* () {
    const returned = yield* token;
    const served: Redacted.Redacted<string> = yield* password;
    yield* Effect.log(\`token \${returned.token}, password \${served}\`);
    yield* Task.exec("sh", ["-c", 'printf "%s|%s" "$TOKEN" "$PASSWORD" > received.txt'], {
      env: { TOKEN: returned.token, PASSWORD: served },
    });
  }),
});
`;

export const redactedOutputs = makeTest(
  "a Redacted value in a task's output reaches other tasks as Redacted with the label its schema declares, passes to commands, and stays out of output, traces and state files",
  Effect.scoped(
    Effect.gen(function* () {
      yield* typeCheckSource(REDACTED_SOURCE);
      const project = yield* makeProject({ "main.ts": REDACTED_SOURCE });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain(
        "[main] token <redacted:API_TOKEN>, password <redacted:DB_PASSWORD>\n",
      );
      expect(yield* read(`${project.directory}/received.txt`)).toBe(
        `${OUTPUT_SECRET}|${OUTPUT_SECRET}`,
      );
      const stateFiles = yield* readStateDirectoryTexts(project.home);
      expect(stateFiles.some((file) => file.path.startsWith("traces/"))).toBe(true);
      expect(ran.standardOutput).not.toContain(OUTPUT_SECRET);
      expect(ran.standardError).not.toContain(OUTPUT_SECRET);
      expect(
        stateFiles.filter((file) => file.text.includes(OUTPUT_SECRET)).map((file) => file.path),
      ).toEqual([]);
    }),
  ),
);

export const undeclaredOutputTypes = makeTest(
  "a run that returns or serves a value without an output does not compile, as an Effect or as a function of flags, and one that returns nothing, ends with Effect.asVoid or declares an output compiles",
  Effect.scoped(
    Effect.gen(function* () {
      const undeclared = yield* failureAsValue(
        typeCheckSource(`import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const returns = Task.make({ name: "returns", run: Effect.succeed(1) });
export const flagged = Task.make({ name: "flagged", flags: Schema.Struct({ stage: Schema.String }), run: ({ stage }) => Effect.succeed(stage) });
export const serves = Task.make({ name: "serves", run: Task.serve({ port: 1 }) });
export default Task.make({ name: "typecheck", run: Task.exec("bunx", ["tsc", "--noEmit"]) });`),
      );
      for (const [position, valueType] of [
        ["3,53", "number"],
        ["4,121", "string"],
        ["5,51", "Served<{ port: number; }>"],
        ["6,47", "string"],
      ]) {
        expect(undeclared.message).toContain(`main.ts(${position}): error TS2375`);
        expect(undeclared.message).toContain(
          `Type '${valueType}' is not assignable to type '"a run that returns a value needs an output schema, or Effect.asVoid"'.`,
        );
      }
      yield* typeCheckSource(`import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const discarded = Task.make({ name: "discarded", run: Effect.asVoid(Task.exec("echo", ["hi"])) });
export const declared = Task.make({ name: "declared", output: Schema.String, run: Task.exec("echo", ["hi"]) });
export const nothing = Task.make({ name: "nothing", run: Effect.gen(function* () { yield* Task.exec("echo", ["hi"]); }) });
export const flagged = Task.make({ name: "flagged", flags: Schema.Struct({ stage: Schema.String }), run: ({ stage }) => Effect.asVoid(Task.exec("echo", [stage])) });
export const flaggedDeclared = Task.make({ name: "flagged-declared", flags: Schema.Struct({ stage: Schema.String }), output: Schema.String, run: ({ stage }) => Effect.succeed(stage) });
export const alive = Task.make({ name: "alive", run: Task.serve(undefined) });
export const waiting = Task.make({ name: "waiting", run: Effect.never });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const nothingRead: void = yield* discarded;
  const text: string = yield* declared;
  yield* Effect.log(nothingRead, text);
}) });`);
      const dropped = yield* failureAsValue(
        typeCheckSource(`import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const returns = Task.make({ name: "returns", run: Effect.void });
export default Task.make({
  name: "main",
  run: Effect.gen(function* () {
    const value: number = yield* returns;
    yield* Effect.log(value);
  }),
});`),
      );
      expect(dropped.message).toContain("main.ts(7,11): error TS2322");
      expect(dropped.message).toContain("Type 'void' is not assignable to type 'number'.");
      const mismatched = yield* failureAsValue(
        typeCheckSource(`import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", output: Schema.String, run: Effect.succeed(1) });`),
      );
      expect(mismatched.message).toContain("main.ts(3,65): error TS2375");
      expect(mismatched.message).toContain("Type 'number' is not assignable to type 'string'.");
    }),
  ),
);
