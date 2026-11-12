import { expect } from "bun:test";

import { Effect, FileSystem } from "effect";

import { read } from "../shared/access-files.ts";
import { encodeJson } from "../shared/encode-json.ts";
import { REPOSITORY_DIRECTORY } from "../shared/find-repository-directory.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

export const installed = makeTest(
  "a task from an installed package runs with paths resolved from the root task's package",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "node_modules/tools/package.json": yield* encodeJson({
          name: "tools",
          version: "1.0.0",
          type: "module",
          exports: { "./tasks": "./tasks/index.ts" },
          peerDependencies: { "@sshkeda/task": "*" },
        }),
        "node_modules/tools/tasks/index.ts": 'export { image } from "./image.ts";\n',
        "node_modules/tools/tasks/base.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const base = Task.make({ name: "base", output: Schema.String, run: Effect.succeed("base") });`,
        "node_modules/tools/tasks/image.ts": `import { Effect, FileSystem, Schema } from "effect";
import { Task } from "@sshkeda/task";
import { base } from "./base.ts";
export const image = Task.make({ name: "image", output: Schema.String, run: Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  yield* Effect.log("building");
  const data = yield* fileSystem.readFileString("data.txt");
  yield* fileSystem.makeDirectory("out", { recursive: true });
  yield* fileSystem.writeFileString("out/image.txt", data);
  return \`image of \${yield* base}\`;
}), timeout: "1 minute", cleanup: "5 seconds" });`,
        "data.txt": "1\n",
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { image } from "tools/tasks";
export default Task.make({ name: "main", run: Effect.gen(function* () { yield* Effect.log(yield* image); }) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("[image] building");
      expect(ran.standardError).toContain("[main] image of base");
      expect(yield* read(`${project.directory}/out/image.txt`)).toBe("1\n");
    }),
  ),
);

export const listedByAnotherCopy = makeTest(
  "Task.all and Task.allSettled from a package's own copy of the task package run tasks listed in an array",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const repository = yield* REPOSITORY_DIRECTORY;
      const project = yield* makeProject({
        "library/package.json": yield* encodeJson({
          name: "library",
          type: "module",
          dependencies: { "@sshkeda/task": "*" },
        }),
        "library/lists.ts": `import { Task } from "@sshkeda/task";
export const runAll = (tasks: Parameters<typeof Task.all>[0]) => Task.all(tasks);
export const checkAll = (tasks: Parameters<typeof Task.allSettled>[0]) => Task.allSettled(tasks);`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { checkAll, runAll } from "./library/lists.ts";
export const lint = Task.make({ name: "lint", run: Effect.log("linting") });
export const test = Task.make({ name: "test", run: Effect.log("testing") });
export const broken = Task.make({ name: "broken", run: Effect.fail({ _tag: "Broken" }) });
export default Task.make({ name: "main", run: Effect.andThen(runAll([lint]), checkAll([test, broken])) });`,
      });
      const secondCopy = `${project.directory}/library/node_modules/@sshkeda/task`;
      yield* fileSystem.makeDirectory(secondCopy, { recursive: true });
      yield* fileSystem.copy(`${repository}/src`, `${secondCopy}/src`);
      yield* fileSystem.copyFile(`${repository}/package.json`, `${secondCopy}/package.json`);
      yield* fileSystem.symlink(`${repository}/node_modules`, `${secondCopy}/node_modules`);
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain("[lint] linting");
      expect(ran.standardError).toContain("[test] testing");
      expect(ran.standardError).toContain(
        "[main] error SettledTasksFailed: 1 of 2 tasks failed: broken\n",
      );
    }),
  ),
);

const runWithALibraryCopy = (mainFile: string) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const repository = yield* REPOSITORY_DIRECTORY;
    const project = yield* makeProject({
      "library/package.json": yield* encodeJson({
        name: "library",
        type: "module",
        dependencies: { "@sshkeda/task": "*" },
      }),
      "library/tasks.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const other = Task.make({ name: "other", run: Effect.log("other ran") });`,
      "main.ts": mainFile,
    });
    const secondCopy = `${project.directory}/library/node_modules/@sshkeda/task`;
    yield* fileSystem.makeDirectory(secondCopy, { recursive: true });
    yield* fileSystem.copy(`${repository}/src`, `${secondCopy}/src`);
    yield* fileSystem.copyFile(`${repository}/package.json`, `${secondCopy}/package.json`);
    yield* fileSystem.symlink(`${repository}/node_modules`, `${secondCopy}/node_modules`);
    return yield* runTaskProcess(project, "main.ts");
  });

export const exportsATaskOfAnotherCopy = makeTest(
  "a file whose default export is a task runs it when it also exports a task made with a library's own copy of the task package",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runWithALibraryCopy(`import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export { other } from "./library/tasks.ts";
export default Task.make({ name: "main", run: Effect.log("main ran") });`);
      expect(ran.standardError).toContain("[main] main ran");
      expect(ran.standardError).not.toContain("DefaultExportNotRunnable");
      expect(ran.exitCode, ran.standardError).toBe(0);
    }),
  ),
);

export const listsBesideAnotherCopy = makeTest(
  "a file whose default export is a Task.all() list starts one run when a library it imports makes tasks with its own copy of the task package",
  Effect.scoped(
    Effect.gen(function* () {
      const ran = yield* runWithALibraryCopy(`import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import "./library/tasks.ts";
const build = Task.make({ name: "build", run: Effect.log("build ran") });
export default Task.all([build]);`);
      expect(
        ran.standardError.split("\n").filter((line) => line === "[build] build ran"),
      ).toHaveLength(1);
      expect(ran.exitCode, ran.standardError).toBe(0);
    }),
  ),
);

export const installedFilesNeverLoaded = makeTest(
  "a task runs when an installed package it imports holds a file that does not parse and is never loaded",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "node_modules/dep/package.json": yield* encodeJson({
          name: "dep",
          version: "1.0.0",
          type: "commonjs",
          main: "index.js",
        }),
        "node_modules/dep/index.js":
          'if (process.platform === "win32") require("./win-only.js");\nexports.greeting = "hello";\n',
        "node_modules/dep/win-only.js": "this is not javascript (\n",
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { greeting } from "dep";
export default Task.make({ name: "main", run: Effect.log(greeting) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("[main] hello");
    }),
  ),
);

export const devDependencies = makeTest(
  "a package that has @sshkeda/task only in devDependencies still has its tasks found",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "node_modules/tools/package.json": yield* encodeJson({
          name: "tools",
          version: "1.0.0",
          type: "module",
          exports: { "./tasks": "./tasks.ts" },
          devDependencies: { "@sshkeda/task": "*" },
        }),
        "node_modules/tools/tasks.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
export const tool = Task.make({ name: "tool", output: Schema.String, run: Effect.succeed("tool 1") });`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { tool } from "tools/tasks";
export default Task.make({ name: "main", run: Effect.gen(function* () { yield* Effect.log(yield* tool); }) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("[tool] started");
      expect(ran.standardError).toContain("[main] tool 1");
    }),
  ),
);

export const jsxInJavaScript = makeTest(
  "JSX in a .js file is scanned the way Bun loads it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "tsconfig.json": yield* encodeJson({ compilerOptions: { jsx: "react", jsxFactory: "h" } }),
        "view.js": `const h = (tag) => tag;
export const view = <b />;`,
        "main.ts": `import { Effect, Schema } from "effect";
import { Task } from "@sshkeda/task";
import { view } from "./view.js";
export const build = Task.make({ name: "build", output: Schema.String, run: Effect.succeed(view) });
export default Task.make({ name: "report", run: Effect.gen(function* () { yield* Effect.log(\`value=\${yield* build}\`); }) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("[report] value=b");
    }),
  ),
);

const runsProjectAt = (projectPath: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        [`${projectPath}/package.json`]: '{ "name": "app", "type": "module" }',
        [`${projectPath}/tasks.ts`]: `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const build = Task.make({ name: "build", run: Effect.log("built one") });`,
        [`${projectPath}/main.ts`]: `import { build } from "./tasks.ts";
export default build;`,
      });
      const appDirectory = `${project.directory}/${projectPath}`;
      const ran = yield* runTaskProcess(project, `${projectPath}/main.ts`, {}, appDirectory);
      expect(ran.standardError).toContain("[build] built one");
      expect(ran.exitCode, ran.standardError).toBe(0);
    }),
  );

export const projectInsideNodeModules = makeTest(
  "a project inside a directory named node_modules runs tasks made in its own files",
  runsProjectAt("node_modules/app"),
);

export const projectNamedLikeScopeInsideNodeModules = makeTest(
  "a project whose directory name starts with @ inside node_modules runs tasks made in its own files",
  runsProjectAt("node_modules/@app"),
);

export const projectThatIsNodeModulesDirectory = makeTest(
  "a project whose own directory is named node_modules runs tasks made in its own files",
  runsProjectAt("nested/node_modules"),
);

const expectLocationsInsideAndOutsideNodeModules = (
  mainFile: string,
  expectedLines: readonly string[],
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const projectPath = "node_modules/app";
      const project = yield* makeProject({
        [`${projectPath}/package.json`]: '{ "name": "app", "type": "module" }',
        [`${projectPath}/main.ts`]: mainFile,
        "outside/package.json": '{ "name": "outside", "type": "module" }',
        "outside/main.ts": mainFile,
      });
      for (const directory of [projectPath, "outside"]) {
        const ran = yield* runTaskProcess(
          project,
          `${directory}/main.ts`,
          {},
          `${project.directory}/${directory}`,
        );
        expect(ran.exitCode, directory).toBe(1);
        for (const expectedLine of expectedLines) {
          expect(ran.standardError, directory).toContain(expectedLine);
        }
      }
    }),
  );

export const projectInsideNodeModulesPrintsLocations = makeTest(
  "a project inside a directory named node_modules prints the user location of the error a task body fails with",
  expectLocationsInsideAndOutsideNodeModules(
    `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { TaskError } from "@sshkeda/task/errors";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  return yield* new TaskError({ message: "broke" });
}) });`,
    ["[main] main.ts:5:21: error TaskError: broke\n"],
  ),
);

export const projectInsideNodeModulesNamesBothMakeCalls = makeTest(
  "a project inside a directory named node_modules names both Task.make() calls when two of them make one task name",
  expectLocationsInsideAndOutsideNodeModules(
    `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "same", run: Effect.void });
export const again = Task.make({ name: "same", run: Effect.void });`,
    [
      'main.ts:3:21: error TaskMadeMoreThanOnce: makes the task "same", which 2 Task.make() calls make\n',
      'main.ts:4:27: error TaskMadeMoreThanOnce: makes the task "same", which 2 Task.make() calls make\n',
    ],
  ),
);
