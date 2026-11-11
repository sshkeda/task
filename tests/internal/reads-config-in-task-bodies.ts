import { expect } from "bun:test";

import { Effect } from "effect";

import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { TASK_ENVIRONMENT_VARIABLE_PREFIX, runTaskProcess } from "../shared/run-task-processes.ts";

export const configEachRun = makeTest(
  "a task reads config freely and runs with each value",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Config, Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () { yield* Effect.log(\`mode \${yield* Config.String("MODE")}\`); }) });`,
      });
      for (const mode of ["a", "b"]) {
        const ran = yield* runTaskProcess(project, "main.ts", { MODE: mode });
        expect(ran.exitCode).toBe(0);
        expect(ran.standardError).toContain(`[main] mode ${mode}`);
      }
    }),
  ),
);

export const emptyConfig = makeTest(
  "an empty environment variable reads as set in the body, apart from an unset one",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Config, Effect, Option } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const version = yield* Config.option(Config.String("TASK_TEST_VERSION"));
  yield* Effect.log(Option.isSome(version) ? "version " + JSON.stringify(version.value) : "version unset");
}) });`,
      });
      expect((yield* runTaskProcess(project, "main.ts")).standardError).toContain(
        "[main] version unset",
      );
      expect(
        (yield* runTaskProcess(project, "main.ts", { TASK_TEST_VERSION: "" })).standardError,
      ).toContain('[main] version ""');
      expect(
        (yield* runTaskProcess(project, "main.ts", { TASK_TEST_VERSION: "1" })).standardError,
      ).toContain('[main] version "1"');
    }),
  ),
);

const rootModuleSettingVersion = (assignment: string) => `import { Config, Effect } from "effect";
import { Task } from "@sshkeda/task";
${assignment}
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* Effect.log("version " + (yield* Config.String("TASK_TEST_VERSION")));
}) });`;

export const rootDefaultedConfig = makeTest(
  "a value the root module gives an unset environment variable while loading is read by the body",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": rootModuleSettingVersion('process.env.TASK_TEST_VERSION ??= "root";'),
      });
      expect((yield* runTaskProcess(project, "main.ts")).standardError).toContain(
        "[main] version root",
      );
      expect(
        (yield* runTaskProcess(project, "main.ts", { TASK_TEST_VERSION: "shell" })).standardError,
      ).toContain("[main] version shell");
    }),
  ),
);

export const rootDefaultedConfigAfterEarlyRead = makeTest(
  "a value the root module gives an unset environment variable after it reads Config while loading is read by the body",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": rootModuleSettingVersion(
          'Effect.runSync(Config.option(Config.String("TASK_TEST_VERSION")));\nprocess.env.TASK_TEST_VERSION ??= "root";',
        ),
      });
      expect((yield* runTaskProcess(project, "main.ts")).standardError).toContain(
        "[main] version root",
      );
    }),
  ),
);

export const rootOverriddenConfig = makeTest(
  "a value the root module writes over the shell's while loading is read by the body",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": rootModuleSettingVersion('process.env.TASK_TEST_VERSION = "root";'),
      });
      expect(
        (yield* runTaskProcess(project, "main.ts", { TASK_TEST_VERSION: "a" })).standardError,
      ).toContain("[main] version root");
    }),
  ),
);

export const configAfterEarlierTaskWrites = makeTest(
  "a value an earlier task writes to process.env is not read by a later task's Config on the same worker",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Config, Effect, Option } from "effect";
import { Task } from "@sshkeda/task";
const writer = Task.make({ name: "writer", run: Effect.sync(() => { process.env.TASK_TEST_WRITTEN = "written"; }) });
export default Task.make({ name: "main", run: Effect.gen(function* () {
  yield* writer;
  const written = yield* Config.option(Config.String("TASK_TEST_WRITTEN"));
  yield* Effect.log(Option.isSome(written) ? "written " + written.value : "written unset");
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", {
        [`${TASK_ENVIRONMENT_VARIABLE_PREFIX}_WORKERS`]: "1",
      });
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain("[main] written unset");
    }),
  ),
);
