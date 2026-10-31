import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect, FileSystem } from "effect";

import packageJson from "../../package.json" with { type: "json" };
import { encodeJson } from "../shared/encode-json.ts";
import { findMisprintedErrorLines } from "../shared/find-misprinted-error-lines.ts";
import { REPOSITORY_DIRECTORY } from "../shared/find-repository-directory.ts";
import { makeProject, makeStandaloneDirectory } from "../shared/make-projects.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

export const missingPackageJson = Task.make(
  "a root task without a package.json above it is refused with its path",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeStandaloneDirectory({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.log("body ran"));
if (import.meta.main) Task.main(main);`,
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const sibling = Task.make(
  "a task in a named package requests a task in a sibling file",
  Effect.scoped(
    Effect.gen(function* () {
      const directory = yield* makeStandaloneDirectory({
        "package.json": yield* encodeJson({ name: "project", type: "module" }),
        "base.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const base = Task.make("base", Effect.succeed("from base"));`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { base } from "./base.ts";
export const main = Task.make("main", Effect.gen(function* () { yield* Effect.log(yield* base); }));
if (import.meta.main) Task.main(main);`,
      });
      const result = yield* runTaskProcess(directory, "main.ts");
      expect(result.standardError).toContain("[main] from base");
      expect(result.standardError).toMatch(/^passed: 2 tasks, [\d.]+s$/mu);
      expect(result.exitCode).toBe(0);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const withoutLockfile = Task.make(
  "a project with a package.json but no bun.lock runs from a task file in a subdirectory",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "tasks/main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.log(process.cwd()));
if (import.meta.main) Task.main(main);`,
      });
      yield* FileSystem.FileSystem.use((fileSystem) =>
        fileSystem.remove(`${project.directory}/bun.lock`),
      );
      const result = yield* runTaskProcess(project, "tasks/main.ts");
      expect(result.standardError).toContain(`[main] ${project.directory}`);
      expect(result.exitCode).toBe(0);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const invalidPackageName = Task.make(
  "a root package with a missing or invalid name is refused with its manifest path and the decoding error",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      for (const [manifest, expectedLine] of [
        [{}, 'package.json: error PackageJsonWithoutName: has no "name"'],
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
export const main = Task.make("main", Effect.log("body ran"));
if (import.meta.main) Task.main(main);`,
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
export const main = Task.make("main", Effect.log("body ran"));
if (import.meta.main) Task.main(main);`,
      });
      const scopedResult = yield* runTaskProcess(scoped, "main.ts");
      expect(scopedResult.exitCode, scopedResult.standardError).toBe(0);
      expect(scopedResult.standardError).toContain(
        `wrote the trace to ${scoped.home}/.sshkeda-task/traces/@scope/project/`,
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const missingImportedPackageJson = Task.make(
  "a named root refuses an imported task file without an enclosing package.json",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeStandaloneDirectory({
        "app/package.json": yield* encodeJson({ name: "project", type: "module" }),
        "loose.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const loose = Task.make("loose", Effect.log("LOOSE BODY RAN"));`,
        "app/main.ts": `import { Task } from "@sshkeda/task";
import { loose } from "../loose.ts";
export const main = Task.make("main", loose);
if (import.meta.main) Task.main(main);`,
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
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const importedParentPackageJson = Task.make(
  "an imported task file accepts a package.json in its own ancestor directory",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeStandaloneDirectory({
        "app/package.json": yield* encodeJson({ name: "project", type: "module" }),
        "shared/package.json": yield* encodeJson({ name: "shared", type: "module" }),
        "shared/tasks/leaf.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const leaf = Task.make("leaf", Effect.succeed("shared value"));`,
        "app/main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { leaf } from "../shared/tasks/leaf.ts";
export const main = Task.make("main", Effect.gen(function* () { yield* Effect.log(yield* leaf); }));
if (import.meta.main) Task.main(main);`,
      });
      const result = yield* runTaskProcess(project, "app/main.ts");
      expect(result.exitCode).toBe(0);
      expect(result.standardError).toContain("[main] shared value");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const invalidLinkedPackageJson = Task.make(
  "a bare import from a file outside any package, linked outside node_modules whose package.json is not JSON is refused with that manifest's path",
  Effect.scoped(
    Effect.gen(function* () {
      const repository = yield* REPOSITORY_DIRECTORY;
      const project = yield* makeStandaloneDirectory({
        "tsconfig.json": yield* encodeJson({
          compilerOptions: {
            paths: {
              [packageJson.name]: [`${repository}/src/index.ts`],
              effect: [`${repository}/node_modules/effect/dist/index.js`],
              "effect/*": [`${repository}/node_modules/effect/dist/*.js`],
              linked: ["./linked/source/index.ts"],
            },
          },
        }),
        "linked/package.json": "{ not json",
        "linked/source/index.ts": 'export const greeting = "linked";\n',
        "helper.ts": 'export { greeting } from "linked";\n',
        "app/package.json": yield* encodeJson({ name: "project", type: "module" }),
        "app/main.ts": `import { Effect } from "effect";
import { greeting } from "../helper.ts";
import { Task } from "@sshkeda/task";
export const main = Task.make("main", Effect.log(greeting));
if (import.meta.main) Task.main(main);`,
      });
      const result = yield* runTaskProcess(project, "app/main.ts");
      expect(result.exitCode, result.standardError).toBe(1);
      expect(result.standardError).toContain(
        'linked/package.json: error PackageJsonNotJson: is not JSON because JSONC Parse error: Expected string but found "not"',
      );
      expect(findMisprintedErrorLines(result.standardError)).toEqual([]);
      expect(result.standardError).not.toContain("[main] linked");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);
