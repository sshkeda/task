import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect, FileSystem, Path, Stream } from "effect";
import { ChildProcess } from "effect/process";

import packageJson from "../../package.json" with { type: "json" };
import {
  MAXIMUM_WORKER_COUNT,
  requireSupportedBunVersion,
} from "../../src/shared/read-environment-config.ts";
import { failureAsValue } from "../shared/failure-as-value.ts";
import { findMisprintedErrorLines } from "../shared/find-misprinted-error-lines.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeScratchDirectory } from "../shared/make-temporary-directories.ts";
import { makeTest } from "../shared/make-tests.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

const DIGITS_BEYOND_THE_LARGEST_NUMBER = 401;

export const stateDirectory = makeTest(
  "configured state directories hold the traces and reject blank paths",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.log("computed") });`,
      });
      for (const directory of [`${project.directory}/absolute-state`, "relative-state"]) {
        const location = directory.startsWith("/")
          ? directory
          : `${project.directory}/${directory}`;
        const environment = { SSHKEDA_TASK_STATE_DIRECTORY: directory };
        const ran = yield* runTaskProcess(project, "main.ts", environment);
        expect(ran.exitCode).toBe(0);
        expect(ran.standardError).toContain(`wrote the trace to ${location}/traces/project/`);
        expect(yield* fileSystem.readDirectory(location)).toEqual(["traces"]);
      }
      expect(yield* fileSystem.exists(`${project.home}/.sshkeda-task`)).toBe(false);
      for (const emptyDirectoryPath of ["", "   "]) {
        const invalid = yield* runTaskProcess(project, "main.ts", {
          SSHKEDA_TASK_STATE_DIRECTORY: emptyDirectoryPath,
        });
        expect(invalid.exitCode).toBe(1);
        expect(invalid.standardError).toContain(
          `error StateDirectoryPathBlank: SSHKEDA_TASK_STATE_DIRECTORY is "${emptyDirectoryPath}", which is blank`,
        );
        expect(invalid.standardError).not.toContain("[main] started");
      }
    }),
  ),
);

export const homeDirectoryPath = makeTest(
  "runtime configuration requires HOME only without an explicit state directory",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.void });`,
      });
      for (const configuration of [
        { environment: {}, succeeds: false },
        { environment: { HOME: "" }, succeeds: false },
        { environment: { HOME: "   " }, succeeds: false },
        { environment: { HOME: project.home }, succeeds: true },
        {
          environment: { SSHKEDA_TASK_STATE_DIRECTORY: `${project.directory}/explicit-state` },
          succeeds: true,
        },
      ]) {
        const child = yield* ChildProcess.make(process.execPath, [`${project.directory}/main.ts`], {
          cwd: project.directory,
          env: configuration.environment,
          extendEnv: false,
          stdin: "ignore",
          stdout: "pipe",
          stderr: "pipe",
        });
        const [exitCode, standardError] = yield* Effect.all(
          [
            child.exitCode,
            Stream.mkString(Stream.decodeText(child.stderr)),
            Stream.runDrain(child.stdout),
          ],
          { concurrency: "unbounded" },
        );
        expect(Number(exitCode)).toBe(configuration.succeeds ? 0 : 1);
        if (!configuration.succeeds) {
          expect(standardError).toMatch(
            /^error HomeDirectoryPathMissing: SSHKEDA_TASK_STATE_DIRECTORY (?:and HOME are both unset|is unset and HOME is " *", which is blank)$/mu,
          );
          expect(findMisprintedErrorLines(standardError)).toEqual([]);
          expect(standardError).not.toContain("[main] started");
        }
      }
      expect(yield* fileSystem.exists(`${project.home}/.sshkeda-task/traces`)).toBe(true);
      expect(yield* fileSystem.exists(`${project.directory}/explicit-state/traces`)).toBe(true);
    }),
  ),
);

export const workerCount = makeTest(
  "runtime configuration rejects worker counts that are not whole decimal numbers above 0 or that exceed the maximum",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.void });`,
      });
      for (const configuredWorkerCount of ["", "0", "-1", "1.5", "many", "Infinity", "1e2", " 4"]) {
        const invalid = yield* runTaskProcess(project, "main.ts", {
          SSHKEDA_TASK_WORKERS: configuredWorkerCount,
        });
        expect(invalid.exitCode).toBe(1);
        expect(invalid.standardError).toContain(
          `error WorkerCountInvalid: SSHKEDA_TASK_WORKERS is "${configuredWorkerCount}", not a whole decimal number above 0`,
        );
        expect(invalid.standardError).not.toContain("[main] started");
      }
      const maximum = MAXIMUM_WORKER_COUNT;
      for (const configuredWorkerCount of [
        String(maximum + 1),
        "9007199254740993",
        "9".repeat(DIGITS_BEYOND_THE_LARGEST_NUMBER),
      ]) {
        const tooMany = yield* runTaskProcess(project, "main.ts", {
          SSHKEDA_TASK_WORKERS: configuredWorkerCount,
        });
        expect(tooMany.exitCode).toBe(1);
        expect(tooMany.standardError).toContain(
          `error WorkerCountAboveMaximum: SSHKEDA_TASK_WORKERS is "${configuredWorkerCount}", above the maximum of ${maximum} worker threads`,
        );
        expect(tooMany.standardError).not.toContain("[main] started");
      }
      const atMaximum = yield* runTaskProcess(project, "main.ts", {
        SSHKEDA_TASK_WORKERS: String(maximum),
      });
      expect(atMaximum.exitCode).toBe(0);
      const valid = yield* runTaskProcess(project, "main.ts", { SSHKEDA_TASK_WORKERS: "1" });
      expect(valid.exitCode).toBe(0);
    }),
  ),
);

export const parentEnvironment = makeTest(
  "test children get their own Task config even when the parent environment sets Task variables",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const underParentEnvironment = yield* (yield* Path.Path).fromFileUrl(
        new URL("../shared/run-task-process-under-parent-environment.ts", import.meta.url),
      );
      const scratchDirectory = yield* makeScratchDirectory;
      const leakedStateDirectory = `${scratchDirectory}/leaked-state`;
      yield* Task.exec(process.execPath, [underParentEnvironment], {
        env: {
          SSHKEDA_TASK_STATE_DIRECTORY: leakedStateDirectory,
          SSHKEDA_TASK_UNRELATED_SETTING: "leaked",
        },
      });
      expect(yield* fileSystem.exists(leakedStateDirectory)).toBe(false);
    }),
  ),
);

export const bunVersionRange = makeTest(
  "a Bun older than the package's engines.bun range fails startup naming the package and both versions",
  Effect.gen(function* () {
    const refused = yield* failureAsValue(requireSupportedBunVersion("1.3.14", ">=1.4.2"));
    expect(refused._tag).toBe("BunVersionUnsupported");
    expect(refused.message).toBe(
      `${packageJson.name} ${packageJson.version} needs Bun >=1.4.2, but Bun 1.3.14 is running`,
    );
    for (const supportedVersion of ["1.4.2", "1.5.0", "1.4.3-canary.12+0d9b296af", Bun.version]) {
      yield* requireSupportedBunVersion(supportedVersion, ">=1.4.2");
    }
    yield* requireSupportedBunVersion(Bun.version);
  }),
);

export const setupLimit = makeTest(
  "runtime configuration rejects setup limits that are not durations above 0, and accepts a duration",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.void });`,
      });
      for (const configuredSetupLimit of [
        "",
        "0 seconds",
        "-1 seconds",
        "2s",
        "soon",
        " 1 minute",
        "Infinity",
      ]) {
        const invalid = yield* runTaskProcess(project, "main.ts", {
          SSHKEDA_TASK_SETUP_LIMIT: configuredSetupLimit,
        });
        expect(invalid.exitCode).toBe(1);
        expect(invalid.standardError).toContain(
          `error SetupLimitInvalid: SSHKEDA_TASK_SETUP_LIMIT is "${configuredSetupLimit}", not a duration above 0 such as "90 seconds"`,
        );
        expect(invalid.standardError).not.toContain("[main] started");
      }
      for (const configuredSetupLimit of ["90 seconds", "1.5 minutes", "10 minutes"]) {
        const valid = yield* runTaskProcess(project, "main.ts", {
          SSHKEDA_TASK_SETUP_LIMIT: configuredSetupLimit,
        });
        expect(valid.exitCode, configuredSetupLimit).toBe(0);
      }
    }),
  ),
);
