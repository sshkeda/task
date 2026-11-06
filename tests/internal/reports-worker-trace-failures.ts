import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect, FileSystem } from "effect";

import { edit } from "../shared/access-files.ts";
import { countOccurrences } from "../shared/count-occurrences.ts";
import { encodeJson } from "../shared/encode-json.ts";
import { REPOSITORY_DIRECTORY } from "../shared/find-repository-directory.ts";
import { makeProjectUsingTaskFrom } from "../shared/make-projects.ts";
import { makeScratchDirectory } from "../shared/make-temporary-directories.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

export const workerTraceFailure = Task.make(
  "worker trace failures warn once even when the main thread can write its trace",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const repository = yield* REPOSITORY_DIRECTORY;
      const engine = yield* makeScratchDirectory;
      yield* fileSystem.copy(`${repository}/src`, `${engine}/src`);
      yield* fileSystem.copyFile(`${repository}/package.json`, `${engine}/package.json`);
      yield* fileSystem.symlink(`${repository}/node_modules`, `${engine}/node_modules`);
      yield* fileSystem.makeDirectory(`${engine}/blocked`);
      yield* edit(
        `${engine}/src/lib/main-thread/shared/provide-worker-pool.ts`,
        "traceFilePath: traceFile.filePath,",
        `traceFilePath: ${yield* encodeJson(`${engine}/blocked`)},`,
      );
      const project = yield* makeProjectUsingTaskFrom({
        taskPackageDirectoryPath: engine,
        files: {
          "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.gen(function* () {
  yield* Effect.log("first worker log");
  yield* Effect.log("second worker log");
}));
if (import.meta.main) Task.main(main);`,
        },
      });
      const result = yield* runTaskProcess(project, "main.ts");
      expect(result.exitCode).toBe(0);
      expect(result.standardError).toContain(
        "the trace file is incomplete because worker 0 reported that",
      );
      expect(countOccurrences(result.standardError, "the trace file is incomplete")).toBe(1);
      expect(result.standardError).not.toContain("\nwrote the trace to");
      const traceDirectory = `${project.home}/.sshkeda-task/traces/project`;
      const traceNames = yield* fileSystem.readDirectory(traceDirectory);
      expect(traceNames).toHaveLength(1);
      const trace = yield* fileSystem.readFileString(`${traceDirectory}/${traceNames[0]}`);
      expect(trace).toContain('"stringValue":"main-thread"');
      expect(trace).toContain('"stringValue":"second worker log"');
      expect(
        trace
          .split("\n")
          .filter((line) => line.includes('"resourceSpans"'))
          .join("\n"),
      ).toContain('"stringValue":"worker"');
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);
