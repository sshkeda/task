import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import { read } from "../shared/access-files.ts";
import { encodeJson } from "../shared/encode-json.ts";
import { failureAsValue } from "../shared/failure-as-value.ts";
import { REPOSITORY_DIRECTORY } from "../shared/find-repository-directory.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";
import { typeCheckSource } from "../shared/type-check-source.ts";

export const orders = makeTest(
  "a task definition compiles with its output, timeout and cleanup in any order, together or alone",
  Effect.scoped(
    typeCheckSource(`import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const a = Task.make({ name: "a", output: Schema.Finite, run: Effect.succeed(1), timeout: "1 minute", cleanup: "5 seconds" });
export const b = Task.make({ cleanup: "5 seconds", timeout: "1 minute", run: Effect.succeed(2), output: Schema.Finite, name: "b" });
export const c = Task.make({ name: "c", run: Effect.void, cleanup: "5 seconds" });
export const d = Task.make({ name: "d", output: Schema.Finite, run: Effect.succeed(4) });
export const e = Task.make({ name: "e", output: Schema.Finite, run: Effect.succeed(5), timeout: 60_000, cleanup: 5_000 });
export const f = Task.make({ name: "f", run: Effect.void, timeout: "1 minute" });
export const server = Task.make({ name: "server", output: Schema.Struct({ port: Schema.Finite }), run: Task.serve({ port: 1 }), timeout: "1 minute", cleanup: "5 seconds" });
export const user = Task.make({ name: "user", output: Schema.Finite, run: Effect.gen(function* () { return (yield* server).port + (yield* d) + (yield* e); }) });
export const cycle: Task.Task<number> = Task.make({ name: "cycle", output: Schema.Finite, run: Effect.gen(function* () { return (yield* user) + 1; }) });
export default Task.make({ name: "main", run: Effect.all([a, b, c, d, e, f, server, user, cycle], { discard: true }) });`),
  ),
);

export const optionRequirements = makeTest(
  "durations do not change what a task's body requires",
  Effect.scoped(
    typeCheckSource(`import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const timed = Task.make({ name: "timed", output: Schema.Finite, run: Effect.succeed(1), timeout: "1 minute", cleanup: "5 seconds" });
export const counted: Task.Task<number, never> = timed;
export default timed;`),
  ),
);

export const undefinedOptions = makeTest(
  "options that may be undefined compile under exactOptionalPropertyTypes",
  Effect.scoped(
    typeCheckSource(`import { Effect, Schema } from "effect";
import type { Duration } from "effect";
import { Task } from "@sshkeda/task";
const settings: { readonly timeout?: Duration.Input; readonly directory?: string; readonly concurrency?: number } = {};
export const listed = Task.make({ name: "listed", output: Schema.String, run: Effect.gen(function* () {
  const text: string = yield* Task.exec("ls", [], { cwd: settings.directory, env: undefined, stdin: undefined });
  return text;
}), timeout: settings.timeout, cleanup: undefined });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Task.all([listed], { concurrency: settings.concurrency });
  yield* Task.allSettled([listed], { concurrency: settings.concurrency });
}) });`),
  ),
);

export const mistypedDefinitions = makeTest(
  "a task definition whose name is not a string, that has no run, or whose run is not an Effect does not compile",
  Effect.scoped(
    Effect.gen(function* () {
      const failed = yield* failureAsValue(
        typeCheckSource(`import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const numbered = Task.make({ name: 42, run: Effect.void });
export const unrun = Task.make({ name: "unrun" });
export const called = Task.make({ name: "called", run: () => Effect.void });
export default numbered;`),
      );
      expect(failed.message).toContain(
        "main.ts(3,37): error TS2322: Type 'number' is not assignable to type 'string'.",
      );
      expect(failed.message).toContain("main.ts(4,32): error TS2741");
      expect(failed.message).toContain("Property 'run' is missing in type '{ name: string; }'");
      expect(failed.message).toContain("main.ts(5,56): error TS2322");
      expect(failed.message).toContain(
        "Type '() => Effect.Effect<void, never, never>' is not assignable to type 'Effect<void, never, never>'.",
      );
    }),
  ),
);

export const hiddenBody = makeTest(
  "a task has no body property, so using its run without going through the task does not compile and finds nothing when the file runs",
  Effect.scoped(
    Effect.gen(function* () {
      const failed = yield* failureAsValue(
        typeCheckSource(`import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const build = Task.make({ name: "build", run: Effect.void });
export default Task.make({ name: "main", run: build.body });`),
      );
      expect(failed.message).toContain("error TS2339: Property 'body' does not exist on type");
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const build = Task.make({ name: "build", run: Effect.void });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* build;
  yield* Effect.log(\`has body: \${"body" in build}\`);
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("[main] has body: false\n");
    }),
  ),
);

export const untaggedFailures = makeTest(
  "a body that can fail with an untagged value does not compile, and tagged failures compile",
  Effect.scoped(
    Effect.gen(function* () {
      const failed = yield* failureAsValue(
        typeCheckSource(`import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export const untaggedBody = Task.make({ name: "untagged-body", run: Effect.fail(new Error("x")) });
export const tagged = Task.make({ name: "tagged", run: Effect.fail(new TaskError({ message: "x" })), cleanup: "5 seconds" });
export default tagged;`),
      );
      expect(failed.message).toContain("main.ts(4,64): error TS2375");
      expect(failed.message).toContain(
        "Property '_tag' is missing in type 'Error' but required in type '{ readonly _tag: string; }'.",
      );
      expect(failed.message).not.toContain("main.ts(5,");
    }),
  ),
);

export const unprovidedServices = makeTest(
  "a body that needs a service task does not provide does not compile",
  Effect.scoped(
    Effect.gen(function* () {
      const failed = yield* failureAsValue(
        typeCheckSource(`import { Context, Effect } from "effect";
import { Task } from "@sshkeda/task";
class Database extends Context.Service<Database, { readonly query: Effect.Effect<string> }>()("Database") {}
export const queried = Task.make({ name: "queried", run: Effect.gen(function* () {
  const database = yield* Database;
  yield* Effect.log(yield* database.query);
}) });
export default queried;`),
      );
      expect(failed.message).toContain("main.ts(4,53): error TS2375");
      expect(failed.message).toContain(
        "Type 'Database' is not assignable to type 'ChildProcessSpawner | Digest | FileSystem | HttpClient | Path | Scope | SecureRandom | Serving | Stdio | Tasks | Terminal'.",
      );
    }),
  ),
);

export const commandsOutsideTasks = makeTest(
  "Task.exec outside a task body does not compile, and inside a body or a helper a body calls it compiles",
  Effect.scoped(
    Effect.gen(function* () {
      const failed = yield* failureAsValue(
        typeCheckSource(`import * as BunServices from "@effect/platform-bun/BunServices";
import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
const listFiles = (directory: string) => Task.exec("ls", [directory]);
export const listed = Task.make({ name: "listed", output: Schema.String, run: Effect.gen(function* () { return yield* listFiles("."); }) });
export const echoed = Task.make({ name: "echoed", run: Effect.asVoid(Task.exec("echo", ["hi"])) });
export default Task.make({ name: "main", run: Task.allSettled([listed, echoed]) });
Effect.runPromise(Task.exec("echo", ["hi"]).pipe(Effect.provide(BunServices.layer)));`),
      );
      expect(failed.message).toContain("main.ts(8,19): error TS2379");
      expect(failed.message).toContain("Type 'Tasks' is not assignable to type 'never'.");
      expect(failed.message).not.toMatch(/main\.ts\([1-7],/u);
    }),
  ),
);

export const modules = makeTest(
  "a module passed to Task.all() with an export that is not a task does not compile",
  Effect.scoped(
    Effect.gen(function* () {
      const failed = yield* failureAsValue(
        typeCheckSource(`import { Effect } from "effect";
import * as taskLibrary from "@sshkeda/task";
export const a = taskLibrary.Task.make({ name: "a", run: Effect.void });
export default taskLibrary.Task.make({ name: "main", run: taskLibrary.Task.all([a, taskLibrary.Task]) });`),
      );
      expect(failed.message).toContain("main.ts(4,84): error TS2322");
      expect(failed.message).toContain("is incompatible with index signature.");
    }),
  ),
);

export const servedTypes = makeTest(
  "a serving task's value has the type its output declares",
  Effect.scoped(
    typeCheckSource(`import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const mock = Task.make({ name: "mock", output: Schema.Struct({ port: Schema.Finite }), run: Effect.gen(function* () { return yield* Task.serve({ port: 4010 }); }) });
export const vm = Task.make({ name: "vm", output: Schema.String, run: Effect.gen(function* () { return yield* Task.serve("water-test"); }), timeout: "5 minutes", cleanup: "1 minute" });
export const raced = Task.make({ name: "raced", output: Schema.String, run: Effect.gen(function* () {
  if (Math.random() > 0.5) { return yield* Task.serve("up"); }
  return "down";
}) });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const port: number = (yield* mock).port;
  const name: string = yield* vm;
  const state: string = yield* raced;
  yield* Effect.log(port, name, state);
}) });`),
  ),
);

export const forgedServed = makeTest(
  "a Served value cannot be constructed outside serve",
  Effect.scoped(
    Effect.gen(function* () {
      const failed = yield* failureAsValue(
        typeCheckSource(`import { Effect } from "effect";
import { Served, Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.succeed(new Served(1)) });`),
      );
      expect(failed.message).toContain("main.ts(3,62): error TS2673");
      expect(failed.message).toContain(
        "Constructor of class 'Served<Success>' is private and only accessible within the class declaration.",
      );
    }),
  ),
);

const expectTypeErrors = (source: string, expectedTexts: readonly string[]) =>
  Effect.gen(function* () {
    const failed = yield* failureAsValue(
      typeCheckSource(`import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
${source}`),
    );
    for (const expectedText of expectedTexts) {
      expect(failed.message).toContain(expectedText);
    }
  });

export const outputMismatches = makeTest(
  "a task whose body returns or serves a value its output does not declare does not compile, even when it serves one value and returns another",
  Effect.scoped(
    Effect.all(
      [
        expectTypeErrors(
          `export default Task.make({ name: "serves-and-returns", output: Schema.String, run: Effect.gen(function* () {
  yield* Task.serve("ready");
  return 123;
}) });`,
          ["main.ts(3,79): error TS2375", "Type 'number' is not assignable to type 'string'."],
        ),
        expectTypeErrors(
          `export default Task.make({ name: "serves-other", output: Schema.Struct({ port: Schema.Finite }), run: Task.serve(new Map([["port", 1234]])) });`,
          [
            "main.ts(3,98): error TS2375",
            "Property 'port' is missing in type 'Served<Map<string, number>>' but required in type '{ readonly port: number; }'.",
          ],
        ),
        expectTypeErrors(
          `export default Task.make({ name: "returns-date", output: Schema.Struct({ at: Schema.String }), run: Effect.succeed({ at: new Date() }) });`,
          ["main.ts(3,96): error TS2375", "Type 'Date' is not assignable to type 'string'."],
        ),
        expectTypeErrors(
          `export default Task.make({ name: "missing", output: Schema.Struct({ found: Schema.Boolean, path: Schema.String }), run: Effect.succeed({ found: true }) });`,
          [
            "main.ts(3,116): error TS2375",
            "Property 'path' is missing in type '{ found: boolean; }' but required in type '{ readonly found: boolean; readonly path: string; }'.",
          ],
        ),
      ],
      { concurrency: "unbounded", discard: true },
    ),
  ),
);

export const nonSchemaOutputs = makeTest(
  "a task whose output is not a schema does not compile",
  Effect.scoped(
    Effect.gen(function* () {
      const failed = yield* failureAsValue(
        typeCheckSource(`import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const plainObject = Task.make({ name: "plain-object", output: { at: Schema.Date }, run: Effect.succeed({ at: new Date() }) });
export const constructor = Task.make({ name: "constructor", output: Date, run: Effect.succeed(new Date()) });
export const text = Task.make({ name: "text", output: "string", run: Effect.succeed("x") });
export default plainObject;`),
      );
      expect(failed.message).toContain("main.ts(3,72): error TS2353");
      expect(failed.message).toContain(
        "Object literal may only specify known properties, and 'at' does not exist in type 'Codec<unknown, unknown, never, never>'.",
      );
      expect(failed.message).toContain("main.ts(4,61): error TS2740");
      expect(failed.message).toContain(
        `Type 'DateConstructor' is missing the following properties from type 'Codec<unknown, unknown, never, never>'`,
      );
      expect(failed.message).toContain("main.ts(5,47): error TS2322");
      expect(failed.message).toContain(
        "Type 'string' is not assignable to type 'Codec<unknown, unknown, never, never>'.",
      );
    }),
  ),
);

export const inferredDeclarations = makeTest(
  "a consumer package emits declarations for unannotated tasks and shared task options",
  Effect.scoped(
    Effect.gen(function* () {
      const repository = yield* REPOSITORY_DIRECTORY;
      const project = yield* makeProject({
        "tsconfig.json": yield* encodeJson({
          extends: `${repository}/tsconfig.json`,
          compilerOptions: {
            noEmit: false,
            declaration: true,
            emitDeclarationOnly: true,
            outDir: "declarations",
            paths: { "@sshkeda/task/errors": [`${repository}/src/lib/errors/index.ts`] },
          },
          include: ["options.ts", "main.ts"],
        }),
        "options.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const sharedOptions = { timeout: "1 minute", cleanup: "5 seconds" } as const;
export const generate = Task.make({ name: "generate", output: Schema.Finite, run: Effect.succeed(1), ...sharedOptions });`,
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
import { generate, sharedOptions } from "./options.ts";
export const lint = Task.make({ name: "lint", run: Effect.asVoid(Task.exec("echo", ["lint"])) });
export const build = Task.make({ name: "build", output: Schema.Finite, run: Effect.gen(function* () { return (yield* generate) + 1; }), ...sharedOptions });
export const server = Task.make({ name: "server", output: Schema.Struct({ port: Schema.Finite }), run: Task.serve({ port: 1 }) });
export const user = Task.make({ name: "user", output: Schema.Finite, run: Effect.gen(function* () { return (yield* server).port + (yield* build); }) });
export const together = Task.all([lint, build]);
export const settled = Task.allSettled([lint, { build }]);
export const served = Task.serve("up");
export const command = Task.exec("echo", ["command"]);
export default Task.make({ name: "main", run: Task.all([lint, build, user]) });`,
      });
      yield* Task.exec(`${repository}/node_modules/.bin/tsc`, [
        "-p",
        project.directory,
        "--singleThreaded",
      ]);
      for (const declarationFileName of ["options.d.ts", "main.d.ts"]) {
        const declarations = yield* read(
          `${project.directory}/declarations/${declarationFileName}`,
        );
        expect(declarations).toMatch(/import\("@sshkeda\/task"\)|from "@sshkeda\/task"/u);
        expect(declarations).not.toMatch(/import\("\.\.\/|from "\.\.\//u);
      }
    }),
  ),
);
