import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect, FileSystem } from "effect";

import { edit } from "../shared/access-files.ts";
import { findMisprintedErrorLines } from "../shared/find-misprinted-error-lines.ts";
import { REPOSITORY_DIRECTORY } from "../shared/find-repository-directory.ts";
import { makeProjectUsingTaskFrom } from "../shared/make-projects.ts";
import { makeScratchDirectory } from "../shared/make-temporary-directories.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

const ROOT_TASK_SOURCE = `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.void);
if (import.meta.main) Task.main(main);`;

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

export const moduleRead = Task.make(
  "module read failures name the file and the cause",
  injectedFailure({
    changes: [
      [
        "lib/main-thread/lib/find-code-each-task-depends-on/internal/follow-imports-from-entry-files.ts",
        ".readFileString(moduleFilePath)",
        ".readFileString(moduleFilePath).pipe(Effect.andThen(Effect.fail(new Error('injected module read failure'))))",
      ],
    ],
    expectedError:
      "main.ts: error CodeFileUnreadable: could not read it because injected module read failure",
    errorTags: ["CodeFileUnreadable"],
  }),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const moduleImport = Task.make(
  "discovery import failures name the module and the cause",
  injectedFailure({
    changes: [
      [
        "lib/main-thread/shared/find-tasks-reachable-from-root-task-file.ts",
        "try: () => import(taskFilePath)",
        "try: () => Promise.reject(new Error('injected import failure'))",
      ],
    ],
    expectedError:
      "main.ts: error TaskModuleImportFailed: could not import task module because injected import failure",
    errorTags: ["TaskModuleImportFailed"],
  }),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const workerImport = Task.make(
  "worker import failures report the task instead of hanging its result",
  injectedFailure({
    changes: [
      [
        "lib/worker-thread/internal/run-task-body.ts",
        "try: () => import(startMessage.taskFilePath)",
        "try: () => Promise.reject(new Error('injected worker import failure'))",
      ],
    ],
    expectedError:
      "[main] main.ts: error TaskModuleImportFailed: could not import task module because injected worker import failure",
    errorTags: ["TaskModuleImportFailed"],
  }),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const workerExport = Task.make(
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const rewrittenModuleRead = Task.make(
  "worker import preparation failures are attributed to the assigned task",
  injectedFailure({
    changes: [
      [
        "lib/main-thread/lib/find-code-each-task-depends-on/internal/rewrite-worker-imports.ts",
        "if (!followsPackageOutsideNodeModules) {",
        "if (followsPackageOutsideNodeModules && !followsPackageOutsideNodeModules) {",
      ],
      [
        "lib/main-thread/lib/find-code-each-task-depends-on/internal/follow-imports-from-entry-files.ts",
        ".readFileString(moduleFilePath)",
        ".readFileString(moduleFilePath).pipe(Effect.tap(() => moduleFilePath.includes('/worker-thread/') ? Effect.fail(new Error('injected rewrite read failure')) : Effect.void))",
      ],
    ],
    expectedError:
      "error CodeFileUnreadable: could not read it because injected rewrite read failure",
    errorTags: ["CodeFileUnreadable"],
  }),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const processInspection = Task.make(
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const processSignal = Task.make(
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
export const main = Task.make("main", Effect.gen(function* () {
  const child = yield* ChildProcess.make("bun", ["-e", "process.exit(0)"], { stdin: "ignore", stdout: "ignore", stderr: "ignore" });
  yield* child.exitCode;
}));
if (import.meta.main) Task.main(main);`,
    expectedError: "[main] error ProcessNotSignaled: could not send",
    errorTags: ["ProcessNotSignaled"],
  }),
  { timeout: "1 minute", cleanup: "5 seconds" },
);
