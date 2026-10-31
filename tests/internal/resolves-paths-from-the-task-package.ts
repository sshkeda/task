import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import packageJson from "../../package.json" with { type: "json" };
import { read, write } from "../shared/access-files.ts";
import { encodeJson } from "../shared/encode-json.ts";
import { makeProject } from "../shared/make-projects.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

const makeWorkspace = (packageTaskFile: string) =>
  Effect.gen(function* () {
    const project = yield* makeProject({
      "package.json": yield* encodeJson({
        name: "workspace",
        private: true,
        type: "module",
        workspaces: ["packages/*"],
      }),
      "packages/a/package.json": yield* encodeJson({ name: "a", type: "module" }),
      "packages/a/src/x.txt": "s1\n",
      "packages/a/build.ts": packageTaskFile,
      "ci.ts": `import { Task } from "@sshkeda/task";
import { buildA } from "./packages/a/build.ts";
export const ci = Task.make("ci", buildA);
if (import.meta.main) Task.main(ci);`,
    });
    return { project, packageDirectory: `${project.directory}/packages/a` };
  });

const STARTS = [
  { file: "ci.ts", workingDirectory: "." },
  { file: "packages/a/build.ts", workingDirectory: "packages/a" },
] as const;

const expectRunsInPackage = (packageTaskFile: string, start: (typeof STARTS)[number]) =>
  Effect.scoped(
    Effect.gen(function* () {
      const { project, packageDirectory } = yield* makeWorkspace(packageTaskFile);
      const runFromStart = runTaskProcess(
        project,
        start.file,
        {},
        `${project.directory}/${start.workingDirectory}`,
      );
      const first = yield* runFromStart;
      expect(first.standardError).toContain("[build-a] started");
      expect(first.exitCode, first.standardError).toBe(0);
      expect(yield* read(`${packageDirectory}/dist/x.txt`)).toBe("s1\n");
      yield* write(`${packageDirectory}/src/x.txt`, "s3\n");
      const edited = yield* runFromStart;
      expect(edited.exitCode, edited.standardError).toBe(0);
      expect(yield* read(`${packageDirectory}/dist/x.txt`)).toBe("s3\n");
    }),
  );

const COPY_SOURCE_TO_DIST = `"sh", ["-c", "mkdir -p dist && cp src/x.txt dist/x.txt"]`;

export const commandWorkingDirectory = Task.make(
  "a command with no working directory or a relative one runs in its workspace package from the package and from the workspace root",
  Effect.forEach(
    [COPY_SOURCE_TO_DIST, `${COPY_SOURCE_TO_DIST}, { cwd: "." }`],
    (command) =>
      Effect.forEach(STARTS, (start) =>
        expectRunsInPackage(
          `import { Task } from "@sshkeda/task";
export const buildA = Task.make("build-a", Task.exec(${command}));
if (import.meta.main) Task.main(buildA);`,
          start,
        ),
      ),
    { discard: true },
  ),
  { timeout: "2 minutes", cleanup: "5 seconds" },
);

export const fileSystemPaths = Task.make(
  "a body's relative file system paths resolve from its workspace package from the package and from the workspace root",
  Effect.forEach(
    STARTS,
    (start) =>
      expectRunsInPackage(
        `import { Effect, FileSystem, Path } from "effect";
import { Task } from "@sshkeda/task";
export const buildA = Task.make("build-a", Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  yield* fileSystem.makeDirectory("dist", { recursive: true });
  yield* fileSystem.writeFileString(path.join("dist", "x.txt"), yield* fileSystem.readFileString("src/x.txt"));
}));
if (import.meta.main) Task.main(buildA);`,
        start,
      ),
    { discard: true },
  ),
  { timeout: "2 minutes", cleanup: "5 seconds" },
);

export const installedPackageTask = Task.make(
  "a task from an installed package resolves its commands and paths from the root task's package",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "node_modules/tool/package.json": yield* encodeJson({
          name: "tool",
          version: "1.0.0",
          type: "module",
          exports: "./copy.js",
          dependencies: { [packageJson.name]: packageJson.version },
        }),
        "node_modules/tool/copy.js": `import { Effect, FileSystem } from "effect";
import { Task } from "@sshkeda/task";
export const copy = Task.make("copy", Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  yield* fileSystem.makeDirectory("dist", { recursive: true });
  yield* fileSystem.writeFileString("dist/x.txt", yield* fileSystem.readFileString("src/x.txt"));
  yield* Task.exec("cp", ["src/x.txt", "dist/y.txt"]);
}));`,
        "src/x.txt": "s1\n",
        "main.ts": `import { Task } from "@sshkeda/task";
import { copy } from "tool";
export const main = Task.make("main", copy);
if (import.meta.main) Task.main(main);`,
      });
      const first = yield* runTaskProcess(project, "main.ts");
      expect(first.exitCode, first.standardError).toBe(0);
      expect(yield* read(`${project.directory}/dist/x.txt`)).toBe("s1\n");
      expect(yield* read(`${project.directory}/dist/y.txt`)).toBe("s1\n");
      yield* write(`${project.directory}/src/x.txt`, "s2\n");
      const edited = yield* runTaskProcess(project, "main.ts");
      expect(edited.exitCode, edited.standardError).toBe(0);
      expect(yield* read(`${project.directory}/dist/x.txt`)).toBe("s2\n");
      expect(yield* read(`${project.directory}/dist/y.txt`)).toBe("s2\n");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const requestedOutputsFromAnotherPackage = Task.make(
  "a task may read the outputs of a task it requests from another workspace package by a path relative to its own package",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "package.json": yield* encodeJson({
          name: "workspace",
          private: true,
          type: "module",
          workspaces: ["packages/*"],
        }),
        "packages/a/package.json": yield* encodeJson({ name: "a", type: "module" }),
        "packages/a/src/x.txt": "s1\n",
        "packages/a/build.ts": `import { Task } from "@sshkeda/task";
export const buildA = Task.make("build-a", Task.exec("sh", ["-c", "mkdir -p dist && cp src/x.txt dist/x.txt"]));`,
        "packages/b/package.json": yield* encodeJson({ name: "b", type: "module" }),
        "packages/b/use.ts": `import { Effect, FileSystem } from "effect";
import { Task } from "@sshkeda/task";
import { buildA } from "../a/build.ts";
export const useA = Task.make("use-a", Effect.gen(function* () {
  yield* buildA;
  yield* Effect.log(\`read \${(yield* (yield* FileSystem.FileSystem).readFileString("../a/dist/x.txt")).trim()}\`);
}));
if (import.meta.main) Task.main(useA);`,
      });
      const first = yield* runTaskProcess(project, "packages/b/use.ts");
      expect(first.exitCode, first.standardError).toBe(0);
      expect(first.standardError).toContain("[use-a] read s1");
      yield* write(`${project.directory}/packages/a/src/x.txt`, "s2\n");
      const edited = yield* runTaskProcess(project, "packages/b/use.ts");
      expect(edited.exitCode, edited.standardError).toBe(0);
      expect(edited.standardError).toContain("[use-a] read s2");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);
