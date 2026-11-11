import { expect } from "bun:test";

import * as TaskPackage from "@sshkeda/task";
import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";
import { typeCheckSource } from "../shared/type-check-source.ts";

export const exports = makeTest(
  "the package exports the runtime services and the Task namespace, which holds make, exec, all, allSettled and serve",
  Effect.sync(() => {
    expect(Object.keys(TaskPackage).toSorted()).toEqual(["Served", "Serving", "Task", "Tasks"]);
    expect(Object.keys(Task).toSorted()).toEqual([
      "Task",
      "all",
      "allSettled",
      "exec",
      "make",
      "serve",
    ]);
  }),
);

export const types = makeTest(
  "Task.make takes a name, a run Effect, an output schema and timeout and cleanup options, and keeps the output and service types",
  Effect.scoped(
    typeCheckSource(`import { Effect, Schema } from "effect";
import { Task, Tasks } from "@sshkeda/task";
export const input = Task.make({ name: "input", output: Schema.Finite, run: Effect.succeed(1) });
export const plain = Task.make({ name: "plain", run: Effect.void });
export const main = Task.make({
  name: "main",
  output: Schema.Finite,
  run: input.pipe(Effect.map((value) => value + 1)),
  timeout: "1 minute",
  cleanup: "5 seconds",
});
export const typed: Task.Task<number, Tasks> = main;
export const nothing: Task.Task<void> = plain;
export const command = Task.exec("echo", ["typed"]);
export default main;`),
  ),
);

export const execution = makeTest(
  "a task made through an aliased Task import requests another task and runs a command with Task.exec",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect, Schema } from "effect";
import { Task as T } from "@sshkeda/task";
const dependency = T.make({ name: "dependency", output: Schema.Finite, run: Effect.succeed(42) });
export default T.make({ name: "main", run: Effect.gen(function* () {
  const value = yield* dependency;
  yield* T.exec("echo", [String(value)]);
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("[main] 42");
    }),
  ),
);
