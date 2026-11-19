import { expect } from "bun:test";

import { Effect, FileSystem } from "effect";

import packageJson from "../../package.json" with { type: "json" };
import { read, write } from "../shared/access-files.ts";
import { encodeJson } from "../shared/encode-json.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { findRunDirectoryPath } from "../shared/name-output-files.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

const WHERE = `const where = (options) => Effect.flatMap(Task.exec("pwd", [], options), (directory) => Effect.log(\`ran in \${directory.trim()}\`));`;

const TERMINAL_COMMANDS = `const inTerminal = (label, options) => Task.exec("sh", ["-c", \`echo "\${label} $(pwd)" >> \${import.meta.dir}/terminal.txt\`], { ...options, stdio: "inherit" });`;

const makeWorkspace = Effect.gen(function* () {
  const project = yield* makeProject({
    "package.json": yield* encodeJson({
      name: "workspace",
      private: true,
      type: "module",
      workspaces: ["packages/*"],
    }),
    "packages/app/package.json": yield* encodeJson({ name: "app", type: "module" }),
    "packages/app/sub/.keep": "",
    "packages/shared/package.json": yield* encodeJson({ name: "shared", type: "module" }),
    "packages/shared/sub/.keep": "",
    "packages/shared/tasks.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
${WHERE}
${TERMINAL_COMMANDS}
export const sibling = Task.make({ name: "sibling", run: where() });
export const relative = Task.make({ name: "relative", run: where({ cwd: "sub" }) });
export const ownFolder = Task.make({ name: "own-folder", run: where({ cwd: import.meta.dir }) });
export const makeDirect = (name) => Task.make({ name, run: where() });
export const makeChecked = (name) => {
  const made = Task.make({ name, run: where() });
  if (made.name !== name) throw new Error("the helper made another task");
  return made;
};
export const terminal = Task.make({ name: "terminal", run: Effect.gen(function* () {
  yield* inTerminal("default");
  yield* inTerminal("relative", { cwd: "sub" });
  yield* inTerminal("own-folder", { cwd: import.meta.dir });
}) });`,
    "packages/linked/package.json": yield* encodeJson({
      name: "linked",
      type: "module",
      exports: "./index.js",
    }),
    "packages/linked/index.js": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
${WHERE}
export const linked = Task.make({ name: "linked", run: where() });`,
    "node_modules/copied/package.json": yield* encodeJson({
      name: "copied",
      version: "1.0.0",
      type: "module",
      exports: "./index.js",
      dependencies: { [packageJson.name]: packageJson.version },
    }),
    "node_modules/copied/index.js": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
${WHERE}
export const copied = Task.make({ name: "copied", run: where() });`,
    "packages/app/scripts/ci.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { copied } from "copied";
import { linked } from "linked";
import { makeChecked, makeDirect, ownFolder, relative, sibling, terminal } from "../../shared/tasks.ts";
${WHERE}
export { terminal };
export const project = Task.make({ name: "project", run: where() });
export const direct = makeDirect("direct");
export const checked = makeChecked("checked");
export default Task.make({ name: "ci", run: Effect.gen(function* () {
  yield* Task.all([project, sibling, relative, ownFolder, direct, checked, linked, copied]);
  yield* makeDirect("direct-in-body");
  yield* makeChecked("checked-in-body");
}) });`,
  });
  yield* (yield* FileSystem.FileSystem).symlink(
    "../packages/linked",
    `${project.directory}/node_modules/linked`,
  );
  return project;
});

const RAN_IN_LINE = /^\[(?<taskName>[\w-]+)\] ran in (?<directory>\S+)$/gmu;

const findDirectoriesRunIn = (standardError: string, projectDirectory: string) =>
  Object.fromEntries(
    [...standardError.matchAll(RAN_IN_LINE)].map((match) => {
      const directory = match.groups?.["directory"] ?? "";
      return [
        match.groups?.["taskName"] ?? "",
        directory === projectDirectory ? "." : directory.replace(`${projectDirectory}/`, ""),
      ];
    }),
  );

const EVERY_TASK_RUNS_IN_THE_APP = {
  project: "packages/app",
  sibling: "packages/app",
  direct: "packages/app",
  checked: "packages/app",
  "direct-in-body": "packages/app",
  "checked-in-body": "packages/app",
  linked: "packages/app",
  copied: "packages/app",
  relative: "packages/app/sub",
  "own-folder": "packages/shared",
} as const;

export const commandsRunInTheDirectoryOfTheFileRun = makeTest(
  "every task's commands run in the folder of the package.json nearest the file run, wherever the task was made, and a relative cwd starts there",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeWorkspace;
      const ran = yield* runTaskProcess(
        project,
        "packages/app/scripts/ci.ts",
        {},
        `${project.directory}/packages/app`,
      );
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(findDirectoriesRunIn(ran.standardError, project.directory)).toEqual(
        EVERY_TASK_RUNS_IN_THE_APP,
      );
    }),
  ),
  "2 minutes",
);

export const commandsRunInTheDirectoryOfTheFileRunFromAnotherDirectory = makeTest(
  "a run started from another folder still runs its commands in the folder of the package.json nearest the file run",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeWorkspace;
      const ran = yield* runTaskProcess(
        project,
        "packages/app/scripts/ci.ts",
        {},
        `${project.directory}/packages/shared`,
      );
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(findDirectoriesRunIn(ran.standardError, project.directory)).toEqual(
        EVERY_TASK_RUNS_IN_THE_APP,
      );
      expect(findRunDirectoryPath(ran.standardError)).toContain("/runs/shared/");
    }),
  ),
  "2 minutes",
);

export const commandsOfATaskRunByName = makeTest(
  "a run started at a task by name runs that task's commands in the folder of the package.json nearest the file run",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeWorkspace;
      const ran = yield* runTaskProcess(
        project,
        "packages/app/scripts/ci.ts",
        {},
        project.directory,
        ["sibling"],
      );
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(findDirectoriesRunIn(ran.standardError, project.directory)).toEqual({
        sibling: "packages/app",
      });
    }),
  ),
);

export const terminalCommandsRunInTheDirectoryOfTheFileRun = makeTest(
  "a terminal command of a task made in a sibling package runs in the folder of the package.json nearest the file run, and a relative cwd starts there",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeWorkspace;
      const ran = yield* runTaskProcess(
        project,
        "packages/app/scripts/ci.ts",
        {},
        project.directory,
        ["terminal"],
      );
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(yield* read(`${project.directory}/packages/shared/terminal.txt`)).toBe(
        [
          `default ${project.directory}/packages/app`,
          `relative ${project.directory}/packages/app/sub`,
          `own-folder ${project.directory}/packages/shared`,
          "",
        ].join("\n"),
      );
    }),
  ),
);

const makeOnePackageWorkspace = (packageTaskFile: string) =>
  Effect.gen(function* () {
    const project = yield* makeProject({
      "package.json": yield* encodeJson({
        name: "workspace",
        private: true,
        type: "module",
        workspaces: ["packages/*"],
      }),
      "packages/a/package.json": yield* encodeJson({ name: "a", type: "module" }),
      "packages/a/build.ts": packageTaskFile,
    });
    return { project, packageDirectory: `${project.directory}/packages/a` };
  });

export const runDirectoryOfANestedPackage = makeTest(
  "a run started from the workspace root with a workspace package's file is named after the workspace root, while its commands still run in that package",
  Effect.scoped(
    Effect.gen(function* () {
      const { project, packageDirectory } =
        yield* makeOnePackageWorkspace(`import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "where", run: Effect.flatMap(Task.exec("pwd"), (directory) => Effect.log(\`ran in \${directory.trim()}\`)) });`);
      const ran = yield* runTaskProcess(project, "packages/a/build.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(findRunDirectoryPath(ran.standardError)).toContain("/runs/workspace/");
      expect(ran.standardError).toContain(`[where] ran in ${packageDirectory}\n`);
    }),
  ),
);

export const fileSystemPaths = makeTest(
  "a body's relative file system paths resolve from the folder of the package.json nearest the file run, like its commands, wherever the task was made",
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
        "packages/a/build.ts": `import { Effect, FileSystem, Path } from "effect";
import { Task } from "@sshkeda/task";
export const buildA = Task.make({ name: "build-a", run: Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  yield* fileSystem.makeDirectory("dist", { recursive: true });
  yield* fileSystem.writeFileString(path.join("dist", "x.txt"), yield* fileSystem.readFileString("src/x.txt"));
  yield* Effect.log(\`resolved \${path.resolve("dist")}\`);
}) });`,
        "packages/app/package.json": yield* encodeJson({ name: "app", type: "module" }),
        "packages/app/src/x.txt": "s1\n",
        "packages/app/ci.ts": `import { Task } from "@sshkeda/task";
import { buildA } from "../a/build.ts";
export default Task.make({ name: "ci", run: buildA });`,
      });
      const ran = yield* runTaskProcess(project, "packages/app/ci.ts");
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain(
        `[build-a] resolved ${project.directory}/packages/app/dist\n`,
      );
      expect(yield* read(`${project.directory}/packages/app/dist/x.txt`)).toBe("s1\n");
    }),
  ),
);

export const installedPackageTask = makeTest(
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
export const copy = Task.make({ name: "copy", run: Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  yield* fileSystem.makeDirectory("dist", { recursive: true });
  yield* fileSystem.writeFileString("dist/x.txt", yield* fileSystem.readFileString("src/x.txt"));
  yield* Task.exec("cp", ["src/x.txt", "dist/y.txt"]);
}) });`,
        "src/x.txt": "s1\n",
        "main.ts": `import { Task } from "@sshkeda/task";
import { copy } from "tool";
export default Task.make({ name: "main", run: copy });`,
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
);

export const requestedOutputsFromAnotherPackage = makeTest(
  "a task may read the outputs of a task it requests from another workspace package, which builds in its own folder with cwd: import.meta.dir, by a path relative to the folder of the file run",
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
export const buildA = Task.make({ name: "build-a", run: Task.exec("sh", ["-c", "mkdir -p dist && cp src/x.txt dist/x.txt"], { cwd: import.meta.dir }) });`,
        "packages/b/package.json": yield* encodeJson({ name: "b", type: "module" }),
        "packages/b/use.ts": `import { Effect, FileSystem } from "effect";
import { Task } from "@sshkeda/task";
import { buildA } from "../a/build.ts";
export default Task.make({ name: "use-a", run: Effect.gen(function* () {
  yield* buildA;
  yield* Effect.log(\`read \${(yield* (yield* FileSystem.FileSystem).readFileString("../a/dist/x.txt")).trim()}\`);
}) });`,
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
);

export const nestedRunUsesItsOwnFile = makeTest(
  "a Task program that a command runs is its own run, so its commands, and those of a task it imports from another package, run in the folder of the package.json nearest its own file",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "scripts/ci.ts": `import { Task } from "@sshkeda/task";
export default Task.make({ name: "outer", run: Task.exec("bun", ["tools/inner/scripts/inner.ts"], { print: true }) });`,
        "tools/inner/package.json": yield* encodeJson({ name: "inner", type: "module" }),
        "tools/other/package.json": yield* encodeJson({ name: "other", type: "module" }),
        "tools/other/other.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
${WHERE}
export const other = Task.make({ name: "other", run: where() });`,
        "tools/inner/scripts/inner.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { other } from "../../other/other.ts";
${WHERE}
export default Task.make({ name: "inner", run: Effect.andThen(where(), other) });`,
      });
      const ran = yield* runTaskProcess(project, "scripts/ci.ts", {}, `${project.directory}/tools`);
      expect(ran.exitCode, ran.standardError).toBe(0);
      expect(ran.standardError).toContain(
        `[outer] [inner] ran in ${project.directory}/tools/inner\n`,
      );
      expect(ran.standardError).toContain(
        `[outer] [other] ran in ${project.directory}/tools/inner\n`,
      );
    }),
  ),
);
