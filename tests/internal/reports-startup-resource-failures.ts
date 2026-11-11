import { expect } from "bun:test";

import { Effect } from "effect";

import { findMisprintedErrorLines } from "../shared/find-misprinted-error-lines.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

export const removedRootFile = makeTest(
  "a root task file removed before run fails startup with the original filename",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { unlinkSync } from "node:fs";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
unlinkSync(import.meta.filename);
export default Task.make({ name: "main", run: Effect.log("body ran") });`,
      });
      const failedRun = yield* runTaskProcess(project, "main.ts");
      expect(failedRun.exitCode).toBe(1);
      expect(failedRun.standardError).toContain(
        "main.ts: error RootTaskFileUnavailable: could not resolve the root task file because",
      );
      expect(failedRun.standardError).not.toContain("FileSystem.");
      expect(findMisprintedErrorLines(failedRun.standardError)).toEqual([]);
      expect(failedRun.standardError).not.toContain("body ran");
      expect(failedRun.standardError).not.toContain("PlatformError");
    }),
  ),
);

export const inaccessibleWorkingDirectory = makeTest(
  "a chdir failure after task discovery reports the package directory before any body starts",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const changeWorkingDirectory = process.chdir;
process.chdir = (directory) => changeWorkingDirectory(directory + "/missing-directory");
export default Task.make({ name: "main", run: Effect.log("body ran") });`,
      });
      const failedRun = yield* runTaskProcess(project, "main.ts");
      expect(failedRun.exitCode).toBe(1);
      expect(failedRun.standardError).toContain(
        `error RootTaskWorkingDirectoryUnavailable: could not enter root task directory "${project.directory}" because ENOENT: no such file or directory, chdir '${project.directory}' -> '${project.directory}/missing-directory'\n`,
      );
      expect(findMisprintedErrorLines(failedRun.standardError)).toEqual([]);
      expect(failedRun.standardError).not.toContain("body ran");
    }),
  ),
);

export const packageSearch = makeTest(
  "a package manifest symlink loop fails the package search with its path and cause",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { unlinkSync, symlinkSync } from "node:fs";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const packageJsonFilePath = import.meta.dir + "/package.json";
unlinkSync(packageJsonFilePath);
symlinkSync(packageJsonFilePath, packageJsonFilePath);
export default Task.make({ name: "main", run: Effect.log("body ran") });`,
      });
      const failedRun = yield* runTaskProcess(project, "main.ts");
      expect(failedRun.exitCode).toBe(1);
      expect(failedRun.standardError).toContain(
        "package.json: error PackageJsonSearchFailed: could not check for package.json because",
      );
      expect(failedRun.standardError).not.toContain("FileSystem.");
      expect(findMisprintedErrorLines(failedRun.standardError)).toEqual([]);
      expect(failedRun.standardError).not.toContain("body ran");
      expect(failedRun.standardError).not.toContain("PlatformError");
    }),
  ),
);
