import { expect } from "bun:test";

import { Effect, FileSystem } from "effect";

import { edit } from "../shared/access-files.ts";
import { findMisprintedErrorLines } from "../shared/find-misprinted-error-lines.ts";
import { REPOSITORY_DIRECTORY } from "../shared/find-repository-directory.ts";
import { makeProjectUsingTaskFrom } from "../shared/make-projects.ts";
import { makeScratchDirectory } from "../shared/make-temporary-directories.ts";
import { makeTest } from "../shared/make-tests.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

const ROOT_TASK_SOURCE = `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.void });`;

const injectedFailure = (input: {
  readonly changes: readonly (readonly [string, string, string])[];
  readonly expectedError: string;
  readonly errorTags: readonly string[];
  readonly source?: string;
}) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const repository = yield* REPOSITORY_DIRECTORY;
      const engine = yield* makeScratchDirectory;
      yield* fileSystem.copy(`${repository}/src`, `${engine}/src`);
      yield* fileSystem.copyFile(`${repository}/package.json`, `${engine}/package.json`);
      yield* fileSystem.symlink(`${repository}/node_modules`, `${engine}/node_modules`);
      for (const [filePath, original, replacement] of input.changes) {
        yield* edit(`${engine}/src/${filePath}`, original, replacement);
      }
      const project = yield* makeProjectUsingTaskFrom({
        taskPackageDirectoryPath: engine,
        files: { "main.ts": input.source ?? ROOT_TASK_SOURCE },
      });
      const result = yield* runTaskProcess(project, "main.ts");
      expect(result.exitCode).toBe(1);
      expect(result.standardError).toContain(input.expectedError);
      expect(
        input.errorTags.some((errorTag) => result.standardError.includes(`error ${errorTag}: `)),
      ).toBe(true);
      expect(findMisprintedErrorLines(result.standardError)).toEqual([]);
      expect(result.standardError).not.toContain("timed out");
    }),
  );

export const moduleImport = makeTest(
  "discovery import failures name the module and the cause",
  injectedFailure({
    changes: [
      [
        "lib/main-thread/shared/find-tasks-made-by-root-task-file.ts",
        "try: () => import(rootTaskFilePath)",
        "try: () => Promise.reject(new Error('injected import failure'))",
      ],
    ],
    expectedError:
      "main.ts: error TaskModuleImportFailed: could not import task module because injected import failure",
    errorTags: ["TaskModuleImportFailed"],
  }),
);

export const workerImport = makeTest(
  "worker import failures report the task instead of hanging its result",
  injectedFailure({
    changes: [
      [
        "lib/worker-thread/internal/run-task-body.ts",
        "try: () => import(filePath)",
        "try: () => Promise.reject(new Error('injected worker import failure'))",
      ],
    ],
    expectedError:
      "[main] main.ts: error TaskModuleImportFailed: could not import task module because injected worker import failure",
    errorTags: ["TaskModuleImportFailed"],
  }),
);

export const workerExport = makeTest(
  "a task missing from the worker's registry reports its task",
  injectedFailure({
    changes: [
      [
        "lib/worker-thread/internal/run-task-body.ts",
        "findTasksMadeWithName(startMessage.taskName)",
        "findTasksMadeWithName('missing-task')",
      ],
    ],
    expectedError: "error TaskNotRegisteredInWorker: importing",
    errorTags: ["TaskNotRegisteredInWorker"],
  }),
);

export const processInspection = makeTest(
  "process listing failures report a cleanup error and end the run",
  injectedFailure({
    changes: [
      [
        "lib/kill-leftover-processes/internal/list-linux-processes-from-proc.ts",
        'const PROC_DIRECTORY_PATH = "/proc";',
        'const PROC_DIRECTORY_PATH = "/proc-missing";',
      ],
      [
        "lib/kill-leftover-processes/internal/list-macos-processes-with-sysctl.ts",
        "const KERN_PROC_ALL = 0;",
        "const KERN_PROC_ALL = -1;",
      ],
    ],
    expectedError: "to find leftover processes",
    errorTags: ["ProcDirectoryUnlistable", "ProcessTableNotRead"],
  }),
);

export const processSignal = makeTest(
  "unexpected process signal failures report the owning task and end the run",
  injectedFailure({
    changes: [
      [
        "lib/kill-leftover-processes/internal/send-signal.ts",
        "process.kill(processId, signal)",
        "(() => { throw new Error('injected signal failure'); })()",
      ],
    ],
    source: `import { Effect } from "effect";
import { ChildProcess } from "effect/process";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const child = yield* ChildProcess.make("bun", ["-e", "process.exit(0)"], { stdin: "ignore", stdout: "ignore", stderr: "ignore" });
  yield* child.exitCode;
}) });`,
    expectedError: "[main] error ProcessNotSignaled: could not send",
    errorTags: ["ProcessNotSignaled"],
  }),
);
