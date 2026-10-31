import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import { read } from "../shared/access-files.ts";
import { encodeJson } from "../shared/encode-json.ts";
import { makeProject } from "../shared/make-projects.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

export const installed = Task.make(
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
        "node_modules/tools/tasks/base.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const base = Task.make("base", Effect.succeed("base"));`,
        "node_modules/tools/tasks/image.ts": `import { Effect, FileSystem } from "effect";
import { Task } from "@sshkeda/task";
import { base } from "./base.ts";
export const image = Task.make("image", Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  yield* Effect.log("building");
  const data = yield* fileSystem.readFileString("data.txt");
  yield* fileSystem.makeDirectory("out", { recursive: true });
  yield* fileSystem.writeFileString("out/image.txt", data);
  return \`image of \${yield* base}\`;
}), { timeout: "1 minute", cleanup: "5 seconds" });`,
        "data.txt": "1\n",
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { image } from "tools/tasks";
export const main = Task.make("main", Effect.gen(function* () { yield* Effect.log(yield* image); }));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("[image] building");
      expect(ran.standardError).toContain("[main] image of base");
      expect(yield* read(`${project.directory}/out/image.txt`)).toBe("1\n");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const installedFilesNeverLoaded = Task.make(
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
export const main = Task.make("main", Effect.log(greeting));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("[main] hello");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const devDependencies = Task.make(
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
        "node_modules/tools/tasks.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const tool = Task.make("tool", Effect.succeed("tool 1"));`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { tool } from "tools/tasks";
export const main = Task.make("main", Effect.gen(function* () { yield* Effect.log(yield* tool); }));
if (import.meta.main) Task.main(main);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("[tool] started");
      expect(ran.standardError).toContain("[main] tool 1");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const jsxInJavaScript = Task.make(
  "JSX in a .js file is scanned the way Bun loads it",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "tsconfig.json": yield* encodeJson({ compilerOptions: { jsx: "react", jsxFactory: "h" } }),
        "view.js": `const h = (tag) => tag;
export const view = <b />;`,
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { view } from "./view.js";
export const build = Task.make("build", Effect.succeed(view));
export const report = Task.make("report", Effect.gen(function* () { yield* Effect.log(\`value=\${yield* build}\`); }));
if (import.meta.main) Task.main(report);`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain("[report] value=b");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

const runsProjectAt = (projectPath: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        [`${projectPath}/package.json`]: '{ "name": "app", "type": "module" }',
        [`${projectPath}/tasks.ts`]: `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const build = Task.make("build", Effect.log("built one"));`,
        [`${projectPath}/main.ts`]: `import { Task } from "@sshkeda/task";
import { build } from "./tasks.ts";
Task.main(build);`,
      });
      const appDirectory = `${project.directory}/${projectPath}`;
      const ran = yield* runTaskProcess(project, `${projectPath}/main.ts`, {}, appDirectory);
      expect(ran.standardError).toContain("[build] built one");
      expect(ran.exitCode, ran.standardError).toBe(0);
    }),
  );

export const projectInsideNodeModules = Task.make(
  "a project inside a directory named node_modules runs tasks made in its own files",
  runsProjectAt("node_modules/app"),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const projectNamedLikeScopeInsideNodeModules = Task.make(
  "a project whose directory name starts with @ inside node_modules runs tasks made in its own files",
  runsProjectAt("node_modules/@app"),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const projectThatIsNodeModulesDirectory = Task.make(
  "a project whose own directory is named node_modules runs tasks made in its own files",
  runsProjectAt("nested/node_modules"),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const projectInsideNodeModulesPrintsLocations = Task.make(
  "a project inside a directory named node_modules prints the user location of a Task.main() call in a task body",
  Effect.scoped(
    Effect.gen(function* () {
      const projectPath = "node_modules/app";
      const mainFile = `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const nested = Task.make("nested", Effect.log("nested ran"));
export const main = Task.make("main", Effect.gen(function* () {
  Task.main(nested);
}));
if (import.meta.main) Task.main(main);`;
      const project = yield* makeProject({
        [`${projectPath}/package.json`]: '{ "name": "app", "type": "module" }',
        [`${projectPath}/main.ts`]: mainFile,
        "outside/package.json": '{ "name": "outside", "type": "module" }',
        "outside/main.ts": mainFile,
      });
      const inside = yield* runTaskProcess(
        project,
        `${projectPath}/main.ts`,
        {},
        `${project.directory}/${projectPath}`,
      );
      const outside = yield* runTaskProcess(
        project,
        "outside/main.ts",
        {},
        `${project.directory}/outside`,
      );
      expect(outside.exitCode).toBe(1);
      expect(outside.standardError).toContain("[main] main.ts:5:8: error RunCalledInTaskBody");
      expect(inside.exitCode).toBe(1);
      expect(inside.standardError).toContain("[main] main.ts:5:8: error RunCalledInTaskBody");
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const projectInsideNodeModulesNamesBothRunCalls = Task.make(
  "a project inside a directory named node_modules names both Task.main() calls when Task.main() is called twice",
  Effect.scoped(
    Effect.gen(function* () {
      const projectPath = "node_modules/app";
      const project = yield* makeProject({
        [`${projectPath}/package.json`]: '{ "name": "app", "type": "module" }',
        [`${projectPath}/main.ts`]: `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export const first = Task.make("first", Effect.log("first ran"));
export const second = Task.make("second", Effect.log("second ran"));
if (import.meta.main) {
  Task.main(first);
  Task.main(second);
}`,
      });
      const ran = yield* runTaskProcess(
        project,
        `${projectPath}/main.ts`,
        {},
        `${project.directory}/${projectPath}`,
      );
      expect(ran.exitCode).toBe(1);
      expect(ran.standardError).toContain(
        'main.ts:7:8: error RunCalledTwice: Task.main() was called again with "second" while it runs "first", started at main.ts:6:8',
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);
