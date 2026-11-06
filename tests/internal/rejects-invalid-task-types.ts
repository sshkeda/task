import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import { read } from "../shared/access-files.ts";
import { encodeJson } from "../shared/encode-json.ts";
import { failureAsValue } from "../shared/failure-as-value.ts";
import { REPOSITORY_DIRECTORY } from "../shared/find-repository-directory.ts";
import { makeProject } from "../shared/make-projects.ts";
import { typeCheckSource } from "../shared/type-check-source.ts";

const TASK_RESULT_TYPE = "Tree<string | number | boolean | Redacted<string> | null>";

const MISTYPED_OPTIONS_SOURCE = `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const misspelled = Task.make("misspelled", Effect.void, { timout: "1 minute" });
export const cached = Task.make("cached", Effect.void, { cache: true });
export const inputs = Task.make("inputs", Effect.void, { inputs: ["src/**"] });
export const outputs = Task.make("outputs", Effect.void, { outputs: ["dist/**"] });
export const env = Task.make("env", Effect.void, { env: ["FOO"] });
if (import.meta.main) Task.main(misspelled);`;

const REMOVED_EXPORTS_SOURCE = `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { run, runCommand, task } from "@sshkeda/task";
export const removed = [Task.files, Task.git, Task.imports, Task.location, Task.untracked, Task.Inputs];
export const main = Task.make("main", Effect.void);
if (import.meta.main) Task.main(main);`;

const positionOf = (source: string, prefix: string, name: string) => {
  const lines = source.split("\n");
  const lineIndex = lines.findIndex((line) => line.includes(`${prefix}${name}`));
  const columnIndex = (lines[lineIndex]?.indexOf(`${prefix}${name}`) ?? 0) + prefix.length;
  return `${lineIndex + 1},${columnIndex + 1}`;
};

export const orders = Task.make(
  "task options compile in any order, together or alone",
  Effect.scoped(
    typeCheckSource(`import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const a = Task.make("a", Effect.succeed(1), { timeout: "1 minute", cleanup: "5 seconds" });
export const b = Task.make("b", Effect.succeed(2), { cleanup: "5 seconds", timeout: "1 minute" });
export const c = Task.make("c", Effect.succeed(3), { cleanup: "5 seconds" });
export const d = Task.make("d", Effect.succeed(4), {});
export const e = Task.make("e", Effect.succeed(5), { timeout: 60_000, cleanup: 5_000 });
export const f = Task.make("f", Effect.succeed(6), { timeout: "1 minute" });
export const g = Task.make("g", Effect.succeed(7));
export const server = Task.make("server", Task.serve({ port: 1 }), { timeout: "1 minute", cleanup: "5 seconds" });
export const user = Task.make("user", Effect.gen(function* () { return (yield* server).port + (yield* d) + (yield* e); }));
export const cycle: Task.Task<number> = Task.make("cycle", Effect.gen(function* () { return (yield* user) + 1; }));
export const main = Task.make("main", Effect.all([a, b, c, d, e, f, g, server, user, cycle]));
if (import.meta.main) Task.main(main);`),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const optionRequirements = Task.make(
  "durations do not change what a task's body requires",
  Effect.scoped(
    typeCheckSource(`import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const timed = Task.make("timed", Effect.succeed(1), { timeout: "1 minute", cleanup: "5 seconds" });
export const counted: Task.Task<number, never> = timed;
if (import.meta.main) Task.main(timed);`),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const mistypedOptions = Task.make(
  "misspelled options and the removed cache, inputs, outputs and env options do not compile",
  Effect.scoped(
    Effect.gen(function* () {
      const failed = yield* failureAsValue(typeCheckSource(MISTYPED_OPTIONS_SOURCE));
      expect(failed.message).toContain(
        "main.ts(3,66): error TS2561: Object literal may only specify known properties, but 'timout' does not exist in type",
      );
      for (const name of ["cache", "inputs", "outputs", "env"]) {
        expect(failed.message).toContain(
          `main.ts(${positionOf(MISTYPED_OPTIONS_SOURCE, "{ ", name)}): error TS2353: Object literal may only specify known properties, and '${name}' does not exist in type`,
        );
      }
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const removedExports = Task.make(
  "the exports removed in 1.0 do not compile",
  Effect.scoped(
    Effect.gen(function* () {
      const failed = yield* failureAsValue(typeCheckSource(REMOVED_EXPORTS_SOURCE));
      expect(failed.message).toContain(
        `main.ts(3,10): error TS2305: Module '"@sshkeda/task"' has no exported member 'run'.`,
      );
      expect(failed.message).toContain(
        `main.ts(3,15): error TS2305: Module '"@sshkeda/task"' has no exported member 'runCommand'.`,
      );
      expect(failed.message).toContain(
        `main.ts(3,27): error TS2724: '"@sshkeda/task"' has no exported member named 'task'.`,
      );
      for (const name of ["files", "git", "imports", "location", "untracked", "Inputs"]) {
        expect(failed.message).toContain(
          `main.ts(${positionOf(REMOVED_EXPORTS_SOURCE, "Task.", name)}): error TS2339: Property '${name}' does not exist on type`,
        );
      }
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const untaggedFailures = Task.make(
  "a body that can fail with an untagged value does not compile, and tagged failures compile",
  Effect.scoped(
    Effect.gen(function* () {
      const failed = yield* failureAsValue(
        typeCheckSource(`import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export const untaggedBody = Task.make("untagged-body", Effect.fail(new Error("x")));
export const tagged = Task.make("tagged", Effect.fail(new TaskError({ message: "x" })), { cleanup: "5 seconds" });
if (import.meta.main) Task.main(tagged);`),
      );
      expect(failed.message).toContain("main.ts(4,56): error TS2379");
      expect(failed.message).toContain(
        "Property '_tag' is missing in type 'Error' but required in type '{ readonly _tag: string; }'.",
      );
      expect(failed.message).not.toContain("main.ts(5,");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const commandsOutsideTasks = Task.make(
  "Task.exec outside a task body does not compile, and inside a body or a helper a body calls it compiles",
  Effect.scoped(
    Effect.gen(function* () {
      const failed = yield* failureAsValue(
        typeCheckSource(`import * as BunServices from "@effect/platform-bun/BunServices";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const listFiles = (directory: string) => Task.exec("ls", [directory]);
export const listed = Task.make("listed", Effect.gen(function* () { return yield* listFiles("."); }));
export const echoed = Task.make("echoed", Task.exec("echo", ["hi"]).pipe(Effect.asVoid));
export const main = Task.make("main", Task.allSettled([listed, echoed]));
Effect.runPromise(Task.exec("echo", ["hi"]).pipe(Effect.provide(BunServices.layer)));
if (import.meta.main) Task.main(main);`),
      );
      expect(failed.message).toContain("main.ts(8,19): error TS2379");
      expect(failed.message).toContain("Type 'Tasks' is not assignable to type 'never'.");
      expect(failed.message).not.toMatch(/main\.ts\((?:[1-7]|9),/u);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const modules = Task.make(
  "a module passed to Task.all() with an export that is not a task does not compile",
  Effect.scoped(
    Effect.gen(function* () {
      const failed = yield* failureAsValue(
        typeCheckSource(`import { Effect } from "effect";
import * as taskLibrary from "@sshkeda/task";
export const a = taskLibrary.Task.make("a", Effect.void);
export const main = taskLibrary.Task.make("main", taskLibrary.Task.all([a, taskLibrary.Task]));
if (import.meta.main) taskLibrary.Task.main(main);`),
      );
      expect(failed.message).toContain("main.ts(4,76): error TS2322");
      expect(failed.message).toContain("is incompatible with index signature.");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const servedTypes = Task.make(
  "a serving task's value has the type of what it serves",
  Effect.scoped(
    typeCheckSource(`import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const mock = Task.make("mock", Effect.gen(function* () { return yield* Task.serve({ port: 4010 }); }));
export const vm = Task.make("vm", Effect.gen(function* () { return yield* Task.serve("water-test"); }), { timeout: "5 minutes", cleanup: "1 minute" });
export const raced = Task.make("raced", Effect.raceFirst(Task.serve("up"), Effect.succeed("down")));
export const main = Task.make("main", Effect.gen(function* () {
  const port: number = (yield* mock).port;
  const name: string = yield* vm;
  const state: string = yield* raced;
  yield* Effect.log(port, name, state);
}));
if (import.meta.main) Task.main(main);`),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const forgedServed = Task.make(
  "a Served value cannot be constructed outside serve",
  Effect.scoped(
    Effect.gen(function* () {
      const failed = yield* failureAsValue(
        typeCheckSource(`import { Effect } from "effect";
import { Served, Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.succeed(new Served(1)));
if (import.meta.main) Task.main(main);`),
      );
      expect(failed.message).toContain("main.ts(3,54): error TS2673");
      expect(failed.message).toContain(
        "Constructor of class 'Served<Success>' is private and only accessible within the class declaration.",
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const mismatched = Task.make(
  "a task that serves one value and returns another does not compile",
  Effect.scoped(
    Effect.gen(function* () {
      const failed = yield* failureAsValue(
        typeCheckSource(`import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.gen(function* () {
  yield* Task.serve("ready");
  return 123;
}));
if (import.meta.main) Task.main(main);`),
      );
      expect(failed.message).toContain("main.ts(3,39): error TS2379");
      expect(failed.message).toContain(
        `Type 'number' is not assignable to type '"serves one value and returns another"'.`,
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const dated = Task.make(
  "a task whose value holds something that is not JSON does not compile",
  Effect.scoped(
    Effect.gen(function* () {
      const failed = yield* failureAsValue(
        typeCheckSource(`import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.succeed({ at: new Date() }));
if (import.meta.main) Task.main(main);`),
      );
      expect(failed.message).toContain("main.ts(3,39): error TS2379");
      expect(failed.message).toContain(
        `Type 'Date' is not assignable to type '${TASK_RESULT_TYPE}'.`,
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const holes = Task.make(
  "a task whose value holds undefined does not compile",
  Effect.scoped(
    Effect.gen(function* () {
      const failed = yield* failureAsValue(
        typeCheckSource(`import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.succeed({ found: true, path: undefined }));
if (import.meta.main) Task.main(main);`),
      );
      expect(failed.message).toContain("main.ts(3,39): error TS2379");
      expect(failed.message).toContain(
        `Type 'undefined' is not assignable to type '${TASK_RESULT_TYPE}'.`,
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const voids = Task.make(
  "a task whose value is a list of what void tasks gave does not compile",
  Effect.scoped(
    Effect.gen(function* () {
      const failed = yield* failureAsValue(
        typeCheckSource(`import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const a = Task.make("a", Effect.void);
export const main = Task.make("main", Effect.all([a, a]));
if (import.meta.main) Task.main(main);`),
      );
      expect(failed.message).toContain("main.ts(4,39): error TS2379");
      expect(failed.message).toContain(
        `Type '[void, void]' is not assignable to type 'string | number | boolean | readonly ${TASK_RESULT_TYPE}[]`,
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const servedMap = Task.make(
  "a task that serves something that is not JSON does not compile",
  Effect.scoped(
    Effect.gen(function* () {
      const failed = yield* failureAsValue(
        typeCheckSource(`import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Task.serve(new Map([["port", 1234]])));
if (import.meta.main) Task.main(main);`),
      );
      expect(failed.message).toContain("main.ts(3,39): error TS2379");
      expect(failed.message).toContain(
        `Type 'Map<string, number>' is not assignable to type 'string | number | boolean | readonly ${TASK_RESULT_TYPE}[]`,
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const inferredDeclarations = Task.make(
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
        "options.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const sharedOptions = { timeout: "1 minute", cleanup: "5 seconds" } as const;
export const generate = Task.make("generate", Effect.succeed(1), sharedOptions);`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { generate, sharedOptions } from "./options.ts";
export const lint = Task.make("lint", Task.exec("echo", ["lint"]).pipe(Effect.asVoid));
export const build = Task.make("build", Effect.gen(function* () { return (yield* generate) + 1; }), sharedOptions);
export const server = Task.make("server", Task.serve({ port: 1 }));
export const user = Task.make("user", Effect.gen(function* () { return (yield* server).port + (yield* build); }));
export const together = Task.all([lint, build]);
export const settled = Task.allSettled([lint, { build }]);
export const served = Task.serve("up");
export const command = Task.exec("echo", ["command"]);
export const buildOptions = { timeout: "1 minute", cleanup: "5 seconds" } satisfies Parameters<typeof Task.make<unknown, never, never>>[2];
export const main = Task.make("main", Task.all([lint, build, user]));`,
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);
