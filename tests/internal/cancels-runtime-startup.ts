import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect, FileSystem } from "effect";

import { edit } from "../shared/access-files.ts";
import { countOccurrences } from "../shared/count-occurrences.ts";
import {
  EXIT_CODE_AFTER_SIGHUP,
  EXIT_CODE_AFTER_SIGINT,
  EXIT_CODE_AFTER_SIGTERM,
} from "../shared/exit-codes-after-stop-signals.ts";
import { REPOSITORY_DIRECTORY } from "../shared/find-repository-directory.ts";
import { makeProjectUsingTaskFrom } from "../shared/make-projects.ts";
import { makeScratchDirectory } from "../shared/make-temporary-directories.ts";
import { runTaskProcess, startTaskProcess } from "../shared/run-task-processes.ts";

const STARTUP_PAUSE_SOURCE = `yield* Effect.addFinalizer(() => Effect.sync(() => { process.stderr.write("startup cleaned up\\n"); }));
yield* Effect.sync(() => { process.stderr.write("startup paused\\n"); });
yield* Effect.never;`;

const copyEngineWithEdits = (
  engineEdits: readonly {
    readonly file: string;
    readonly before: string;
    readonly replacement: string;
  }[],
) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const repository = yield* REPOSITORY_DIRECTORY;
    const engine = yield* makeScratchDirectory;
    yield* fileSystem.copy(`${repository}/src`, `${engine}/src`);
    yield* fileSystem.copyFile(`${repository}/package.json`, `${engine}/package.json`);
    yield* fileSystem.symlink(`${repository}/node_modules`, `${engine}/node_modules`);
    for (const engineEdit of engineEdits) {
      yield* edit(
        `${engine}/src/lib/main-thread/${engineEdit.file}`,
        engineEdit.before,
        engineEdit.replacement,
      );
    }
    return engine;
  });

const cancelsStartup = (
  file: string,
  before: string,
  replacement = `${STARTUP_PAUSE_SOURCE}\n${before}`,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const engine = yield* copyEngineWithEdits([{ file, before, replacement }]);
      for (const [signal, exitCode] of [
        ["INT", EXIT_CODE_AFTER_SIGINT],
        ["TERM", EXIT_CODE_AFTER_SIGTERM],
        ["HUP", EXIT_CODE_AFTER_SIGHUP],
      ] as const) {
        const project = yield* makeProjectUsingTaskFrom({
          taskPackageDirectoryPath: engine,
          files: {
            "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.log("ROOT EXECUTED"));
if (import.meta.main) Task.main(main);`,
          },
        });
        const command = yield* startTaskProcess(project, "main.ts");
        yield* command.waitForLine("startup paused");
        yield* command.sendSignal(signal);
        const result = yield* command.waitForExit;
        expect(result.exitCode).toBe(exitCode);
        expect(result.standardError).toContain(`received SIG${signal}`);
        expect(countOccurrences(result.standardError, "startup cleaned up")).toBe(1);
        expect(result.standardError).not.toContain("ROOT EXECUTED");
        expect(result.standardError).not.toContain("[main] started");
        if (result.standardError.includes("\nwrote the trace to")) {
          expect(result.standardError.indexOf("wrote the trace to")).toBeGreaterThan(
            result.standardError.indexOf("startup cleaned up"),
          );
        }
      }
    }),
  );

export const config = Task.make(
  "SIGINT, SIGTERM and SIGHUP cancel config startup and close its partial scope",
  cancelsStartup(
    "shared/provide-run-root-task-config.ts",
    "  const fileSystem = yield* FileSystem.FileSystem;",
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const trace = Task.make(
  "SIGINT, SIGTERM and SIGHUP cancel trace startup and flush before the final trace report",
  cancelsStartup("internal/trace-the-run.ts", "    return traceFile;"),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const moduleGraph = Task.make(
  "SIGINT, SIGTERM and SIGHUP cancel module graph startup",
  cancelsStartup(
    "lib/find-code-each-task-depends-on/internal/make-module-graph.ts",
    "  const path = yield* Path.Path;",
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const taskRuns = Task.make(
  "SIGINT, SIGTERM and SIGHUP cancel task-run state startup",
  cancelsStartup("lib/run-tasks-on-workers/internal/make-task-runs.ts", "  return {"),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const workers = Task.make(
  "SIGINT, SIGTERM and SIGHUP cancel worker-pool startup",
  cancelsStartup("shared/provide-worker-pool.ts", "  return {\n    events:"),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const processes = Task.make(
  "SIGINT, SIGTERM and SIGHUP cancel process-group startup",
  cancelsStartup(
    "shared/provide-process-groups.ts",
    "Layer.effect(ProcessGroups, makeProcessGroups)",
    `Layer.effect(ProcessGroups, makeProcessGroups.pipe(Effect.tap(() => Effect.gen(function* () { ${STARTUP_PAUSE_SOURCE} }))))`,
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

const ROOT_TASK_SOURCE = `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.log("body ran"));
if (import.meta.main) Task.main(main);`;

const releaseMarkerBefore = (resourceName: string, before: string) =>
  `yield* Effect.addFinalizer(() => Effect.sync(() => { process.stderr.write("released ${resourceName}\\n"); }));\n${before}`;

const CONFIG_ANCHOR = "  const fileSystem = yield* FileSystem.FileSystem;";
const MODULE_GRAPH_ANCHOR = "  const path = yield* Path.Path;";
export const releasesEarlierStartupResources = Task.make(
  "a startup step that fails after the module graph starts releases every earlier startup resource before exit",
  Effect.scoped(
    Effect.gen(function* () {
      const engine = yield* copyEngineWithEdits([
        {
          file: "shared/provide-run-root-task-config.ts",
          before: CONFIG_ANCHOR,
          replacement: releaseMarkerBefore("config", CONFIG_ANCHOR),
        },
        {
          file: "lib/find-code-each-task-depends-on/internal/make-module-graph.ts",
          before: MODULE_GRAPH_ANCHOR,
          replacement: releaseMarkerBefore("module graph", MODULE_GRAPH_ANCHOR),
        },
      ]);
      const project = yield* makeProjectUsingTaskFrom({
        taskPackageDirectoryPath: engine,
        files: {
          "main.ts": `${ROOT_TASK_SOURCE}\nexport const load = () => import("./missing.ts");`,
        },
      });
      const failedRun = yield* runTaskProcess(project, "main.ts");
      expect(failedRun.exitCode).toBe(1);
      const expectedError = 'ImportNotFound: imports "./missing.ts"';
      expect(failedRun.standardError).toContain(expectedError);
      expect(failedRun.standardError).not.toContain("body ran");
      expect(failedRun.standardError).not.toContain("[main] started");
      const finalErrorIndex = failedRun.standardError.lastIndexOf(expectedError);
      for (const resourceName of ["config", "module graph"]) {
        const releasedLines = failedRun.standardError
          .split("\n")
          .filter((line) => line === `released ${resourceName}`);
        expect(releasedLines).toEqual([`released ${resourceName}`]);
        expect(failedRun.standardError.indexOf(`released ${resourceName}`)).toBeLessThan(
          finalErrorIndex,
        );
      }
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);
