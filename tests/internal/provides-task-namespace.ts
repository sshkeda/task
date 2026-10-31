import { expect } from "bun:test";

import * as TaskPackage from "@sshkeda/task";
import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import { scanProjectModule } from "../../src/lib/main-thread/lib/scan-module-source/internal/scan-module-text.ts";
import { makeProject } from "../shared/make-projects.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";
import { typeCheckSource } from "../shared/type-check-source.ts";

export const exports = Task.make(
  "the package exports the runtime services and the Task namespace, which holds make, exec, main, all, allSettled and serve",
  Effect.sync(() => {
    expect(Object.keys(TaskPackage).toSorted()).toEqual(["Served", "Serving", "Task", "Tasks"]);
    expect(Object.keys(Task).toSorted()).toEqual([
      "Task",
      "all",
      "allSettled",
      "exec",
      "main",
      "make",
      "serve",
    ]);
  }),
);

export const declarations = Task.make(
  "the module scanner finds Task.make through named, aliased, namespace and CommonJS imports",
  Effect.sync(() => {
    for (const [binding, make] of [
      ['import { Task } from "@sshkeda/task";', "Task.make"],
      ['import { Task as T } from "@sshkeda/task";', "T.make"],
      ['import * as library from "@sshkeda/task";', "library.Task.make"],
      ['const { Task: T } = require("@sshkeda/task");', "T.make"],
      ['const { Task } = require("@sshkeda/task");', "Task.make"],
      ['const library = require("@sshkeda/task");', "library.Task.make"],
    ]) {
      const scanned = scanProjectModule(
        "main.ts",
        `${binding}\nexport const main = ${make}("main", Effect.void);`,
      );
      expect(scanned.madeTasks, binding).toEqual([{ name: "main", line: 2 }]);
      expect(scanned.refusals, binding).toEqual([]);
    }
  }),
);

export const oldDeclarations = Task.make(
  "the module scanner no longer takes a task() call as a task declaration",
  Effect.sync(() => {
    for (const source of [
      'import { task } from "@sshkeda/task";\nexport const main = task("main", Effect.void);',
      'import * as library from "@sshkeda/task";\nexport const main = library.task("main", Effect.void);',
      'const { task } = require("@sshkeda/task");\nexport const main = task("main", Effect.void);',
    ]) {
      expect(scanProjectModule("main.ts", source).madeTasks, source).toEqual([]);
    }
  }),
);

export const definitionChecks = Task.make(
  "Task.make keeps the module scanner's definition checks",
  Effect.sync(() => {
    for (const [definition, expectedTag] of [
      ['const make = () => Task.make("inner", Effect.void);', "TaskCalledBelowTopLevel"],
      ["const main = Task.make(name, Effect.void);", "TaskNameComputed"],
      ['const main = Task.make("", Effect.void);', "TaskNameEmpty"],
      ["const main = Task.make();", "TaskNameMissing"],
      ["const main = Task.make(42, Effect.void);", "TaskNameNotString"],
    ] as const) {
      const scanned = scanProjectModule(
        "main.ts",
        `import { Task } from "@sshkeda/task";\n${definition}`,
      );
      expect(
        scanned.refusals.map((refusal) => refusal._tag),
        definition,
      ).toContain(expectedTag);
    }
  }),
);

export const lexicalBindings = Task.make(
  "the module scanner follows the imported bindings of Task, not names that shadow them",
  Effect.sync(() => {
    for (const source of [
      'import { Task } from "@sshkeda/task"; function local(Task) { return Task.make("local", Effect.void); }',
      'import { Task as T } from "@sshkeda/task"; function local(T) { return T.make("local", Effect.void); }',
      'import * as library from "@sshkeda/task"; function local(library) { return library.Task.make("local", Effect.void); }',
      'const { Task } = require("@sshkeda/task"); function local(Task) { return Task.make("local", Effect.void); }',
      'const library = require("@sshkeda/task"); function local(library) { return library.Task.make("local", Effect.void); }',
      'const Task = { make: () => null }; const main = Task.make("local", Effect.void);',
      'import type { Task } from "@sshkeda/task"; function local(Task) { return Task.make("local", Effect.void); }',
    ]) {
      const scanned = scanProjectModule("main.ts", source);
      expect(scanned.madeTasks, source).toEqual([]);
      expect(scanned.refusals, source).toEqual([]);
    }
    const scanned = scanProjectModule(
      "main.ts",
      'import { Task } from "@sshkeda/task"; function local() { return Task.make("nested", Effect.void); }',
    );
    expect(scanned.refusals.map((refusal) => refusal._tag)).toContain("TaskCalledBelowTopLevel");
  }),
);

export const types = Task.make(
  "Task.make keeps result and service types and takes timeout and cleanup options",
  Effect.scoped(
    typeCheckSource(`import { Effect } from "effect";
import { Task, Tasks } from "@sshkeda/task";
export const input = Task.make("input", Effect.succeed(1));
export const plain = Task.make("plain", Effect.void);
export const main: Task.Task<number, Tasks> = Task.make("main", Effect.map(input, (value) => value + 1), {
  timeout: "1 minute", cleanup: "5 seconds",
});
export const command = Task.exec("echo", ["typed"]);
if (import.meta.main) Task.main(main);`),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const execution = Task.make(
  "a task made through an aliased Task import requests another task and runs a command with Task.exec",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task as T } from "@sshkeda/task";
export const dependency = T.make("dependency", Effect.succeed(42));
export const main = T.make("main", Effect.gen(function* () {
  const value = yield* dependency;
  yield* T.exec("echo", [String(value)]);
}));
if (import.meta.main) T.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("[main] 42");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);
