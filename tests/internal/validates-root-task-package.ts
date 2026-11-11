import { expect } from "bun:test";

import { Effect, FileSystem, Path } from "effect";

import { encodeJson } from "../shared/encode-json.ts";
import { findMisprintedErrorLines } from "../shared/find-misprinted-error-lines.ts";
import { makeProject, makeStandaloneDirectory } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

export const missingPackageJson = makeTest(
  "a root task without a package.json above it is refused with its path",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeStandaloneDirectory({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.log("body ran") });`,
      });
      const result = yield* runTaskProcess(project, "main.ts");
      expect(result.exitCode).toBe(1);
      expect(result.standardError).toContain(
        "main.ts: error MissingPackageJson: no package.json found in its directory or any parent directory",
      );
      expect(findMisprintedErrorLines(result.standardError)).toEqual([]);
      expect(result.standardError).not.toContain("body ran");
      expect(result.standardError).not.toContain("[main] started");
    }),
  ),
);

export const sibling = makeTest(
  "a task in a named package requests a task in a sibling file",
  Effect.scoped(
    Effect.gen(function* () {
      const directory = yield* makeStandaloneDirectory({
        "package.json": yield* encodeJson({ name: "project", type: "module" }),
        "base.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const base = Task.make({ name: "base", output: Schema.String, run: Effect.succeed("from base") });`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { base } from "./base.ts";
export default Task.make({ name: "main", run: Effect.gen(function* () { yield* Effect.log(yield* base); }) });`,
      });
      const result = yield* runTaskProcess(directory, "main.ts");
      expect(result.standardError).toContain("[main] from base");
      expect(result.standardError).toMatch(/^passed: 2 tasks, [\d.]+s$/mu);
      expect(result.exitCode).toBe(0);
    }),
  ),
);

export const withoutLockfile = makeTest(
  "a project with a package.json but no bun.lock runs from a task file in a subdirectory",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "tasks/main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.log(process.cwd()) });`,
      });
      yield* FileSystem.FileSystem.use((fileSystem) =>
        fileSystem.remove(`${project.directory}/bun.lock`),
      );
      const result = yield* runTaskProcess(project, "tasks/main.ts");
      expect(result.standardError).toContain(`[main] ${project.directory}`);
      expect(result.exitCode).toBe(0);
    }),
  ),
);

export const namelessRootPackage = makeTest(
  "a root package without a name runs and writes its trace under the package's folder name",
  Effect.scoped(
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const project = yield* makeProject({
        "package.json": yield* encodeJson({ private: true, type: "module" }),
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.log("body ran") });`,
      });
      const result = yield* runTaskProcess(project, "main.ts");
      expect(result.exitCode, result.standardError).toBe(0);
      expect(result.standardError).toContain("[main] body ran");
      expect(result.standardError).toContain(
        `wrote the trace to ${project.home}/.sshkeda-task/traces/${path.basename(project.directory)}/`,
      );
    }),
  ),
);

export const invalidPackageName = makeTest(
  "a root package with an invalid name is refused with its manifest path and the decoding error",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      for (const [manifest, expectedLine] of [
        [
          { name: "" },
          'package.json: error PackageJsonNameBlank: has the "name" "", which is blank',
        ],
        [
          { name: "   " },
          'package.json: error PackageJsonNameBlank: has the "name" "   ", which is blank',
        ],
        [
          { name: 123 },
          'package.json: error PackageJsonNameNotString: has the "name" 123, which is not a string',
        ],
        ...["../escaped-traces", "@scope/../escaped-traces", ".escaped-traces", "a/b"].map(
          (name) =>
            [
              { name },
              `package.json: error PackageJsonNameInvalid: has the "name" "${name}", which is not a package name: "name" or "@scope/name" with no part starting with "."`,
            ] as const,
        ),
      ] as const) {
        const project = yield* makeProject({
          "package.json": yield* encodeJson(manifest),
          "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.log("body ran") });`,
        });
        const result = yield* runTaskProcess(project, "main.ts");
        expect(result.exitCode).toBe(1);
        expect(result.standardError).toMatch(new RegExp(`^${RegExp.escape(expectedLine)}$`, "mu"));
        expect(result.standardError).not.toContain("SchemaError");
        expect(result.standardError).not.toContain("[main] started");
        expect(yield* fileSystem.exists(`${project.home}/.sshkeda-task/escaped-traces`)).toBe(
          false,
        );
      }
      const scoped = yield* makeProject({
        "package.json": yield* encodeJson({ name: "@scope/project", type: "module" }),
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "main", run: Effect.log("body ran") });`,
      });
      const scopedResult = yield* runTaskProcess(scoped, "main.ts");
      expect(scopedResult.exitCode, scopedResult.standardError).toBe(0);
      expect(scopedResult.standardError).toContain(
        `wrote the trace to ${scoped.home}/.sshkeda-task/traces/@scope/project/`,
      );
    }),
  ),
);

export const missingImportedPackageJson = makeTest(
  "a named root refuses an imported task file without an enclosing package.json",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeStandaloneDirectory({
        "app/package.json": yield* encodeJson({ name: "project", type: "module" }),
        "loose.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const loose = Task.make({ name: "loose", run: Effect.log("LOOSE BODY RAN") });`,
        "app/main.ts": `import { Task } from "@sshkeda/task";
import { loose } from "../loose.ts";
export default Task.make({ name: "main", run: loose });`,
      });
      const result = yield* runTaskProcess(project, "app/main.ts");
      expect(result.exitCode).toBe(1);
      expect(result.standardError).toContain(
        "loose.ts: error MissingPackageJson: no package.json found in its directory or any parent directory",
      );
      expect(findMisprintedErrorLines(result.standardError)).toEqual([]);
      expect(result.standardError).not.toContain("LOOSE BODY RAN");
      expect(result.standardError).not.toContain("[main] started");
    }),
  ),
);

export const importedParentPackageJson = makeTest(
  "an imported task file accepts a package.json in its own ancestor directory",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeStandaloneDirectory({
        "app/package.json": yield* encodeJson({ name: "project", type: "module" }),
        "shared/package.json": yield* encodeJson({ name: "shared", type: "module" }),
        "shared/tasks/leaf.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const leaf = Task.make({ name: "leaf", output: Schema.String, run: Effect.succeed("shared value") });`,
        "app/main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { leaf } from "../shared/tasks/leaf.ts";
export default Task.make({ name: "main", run: Effect.gen(function* () { yield* Effect.log(yield* leaf); }) });`,
      });
      const result = yield* runTaskProcess(project, "app/main.ts");
      expect(result.exitCode).toBe(0);
      expect(result.standardError).toContain("[main] shared value");
    }),
  ),
);
