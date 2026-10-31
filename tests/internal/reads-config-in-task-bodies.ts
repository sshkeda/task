import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import { makeProject } from "../shared/make-projects.ts";
import { TASK_ENVIRONMENT_VARIABLE_PREFIX, runTaskProcess } from "../shared/run-task-processes.ts";

export const configEachRun = Task.make(
  "a task reads config freely and runs with each value",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Config, Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.gen(function* () { yield* Effect.log(\`mode \${yield* Config.String("MODE")}\`); }));
if (import.meta.main) Task.main(main);`,
      });
      for (const mode of ["a", "b"]) {
        const ran = yield* runTaskProcess(project, "main.ts", { MODE: mode });
        expect(ran.exitCode).toBe(0);
        expect(ran.standardError).toContain(`[main] mode ${mode}`);
      }
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const emptyConfig = Task.make(
  "an empty environment variable reads as set in the body, apart from an unset one",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Config, Effect, Option } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.gen(function* () {
  const version = yield* Config.option(Config.String("TASK_TEST_VERSION"));
  yield* Effect.log(Option.isSome(version) ? "version " + JSON.stringify(version.value) : "version unset");
}));
if (import.meta.main) Task.main(main);`,
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

const rootModuleSettingVersion = (assignment: string) => `import { Config, Effect } from "effect";
import { Task } from "@sshkeda/task";
${assignment}
export const main = Task.make("main", Effect.gen(function* () {
  yield* Effect.log("version " + (yield* Config.String("TASK_TEST_VERSION")));
}));
if (import.meta.main) Task.main(main);`;

export const rootDefaultedConfig = Task.make(
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const rootDefaultedConfigAfterEarlyRead = Task.make(
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const rootOverriddenConfig = Task.make(
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const configAfterEarlierTaskWrites = Task.make(
  "a value an earlier task writes to process.env is not read by a later task's Config on the same worker",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Config, Effect, Option } from "effect";
import { Task } from "@sshkeda/task";
const writer = Task.make("writer", Effect.sync(() => { process.env.TASK_TEST_WRITTEN = "written"; }));
export const main = Task.make("main", Effect.gen(function* () {
  yield* writer;
  const written = yield* Config.option(Config.String("TASK_TEST_WRITTEN"));
  yield* Effect.log(Option.isSome(written) ? "written " + written.value : "written unset");
}));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", {
        [`${TASK_ENVIRONMENT_VARIABLE_PREFIX}_WORKERS`]: "1",
      });
      expect(ran.exitCode).toBe(0);
      expect(ran.standardError).toContain("[main] written unset");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);
