import { Effect, FileSystem, Path } from "effect";
import { ChildProcess } from "effect/process";

import packageJson from "../../package.json" with { type: "json" };
import { write } from "./access-files.ts";
import { encodeJson } from "./encode-json.ts";
import { REPOSITORY_DIRECTORY } from "./find-repository-directory.ts";
import { makeScratchDirectory, makeTemporaryDirectory } from "./make-temporary-directories.ts";
import { GitRepositoryNotCreated, PackageJsonAbove } from "./test-errors.ts";

export const makeProjectUsingTaskFrom = (input: {
  readonly files: Readonly<Record<string, string>>;
  readonly taskPackageDirectoryPath: string;
}) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const repository = yield* REPOSITORY_DIRECTORY;
    const directory = yield* makeScratchDirectory;
    const home = yield* makeScratchDirectory;

    for (const [name, text] of Object.entries({
      "package.json": yield* encodeJson({ name: "project", type: "module" }),
      "bun.lock": "{}",
      ".gitignore": "node_modules\n",
      ...input.files,
    })) {
      yield* write(`${directory}/${name}`, text);
    }

    yield* fileSystem.makeDirectory(`${directory}/node_modules/${packageJson.name.split("/")[0]}`, {
      recursive: true,
    });
    yield* fileSystem.symlink(
      input.taskPackageDirectoryPath,
      `${directory}/node_modules/${packageJson.name}`,
    );
    yield* fileSystem.symlink(
      `${repository}/node_modules/effect`,
      `${directory}/node_modules/effect`,
    );
    const gitExitCode = yield* Effect.scoped(
      ChildProcess.make("git", ["init", "-q"], { cwd: directory }).pipe(
        Effect.flatMap((child) => child.exitCode),
      ),
    );
    if (gitExitCode !== 0) {
      return yield* new GitRepositoryNotCreated({ directory, exitCode: gitExitCode });
    }

    return { directory, home, scope: yield* Effect.scope };
  });

export const makeStandaloneDirectory = (files: Readonly<Record<string, string>>) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const repository = yield* REPOSITORY_DIRECTORY;
    const directory = yield* makeTemporaryDirectory();
    const home = yield* makeScratchDirectory;
    const directoriesBelowRoot = (from: string): readonly string[] =>
      path.dirname(from) === from ? [] : [from, ...directoriesBelowRoot(path.dirname(from))];
    for (const above of directoriesBelowRoot(directory)) {
      if (yield* fileSystem.exists(`${above}/package.json`)) {
        return yield* new PackageJsonAbove({ file: `${above}/package.json` });
      }
    }

    const tsconfig = {
      compilerOptions: {
        paths: {
          [packageJson.name]: [`${repository}/src/index.ts`],
          effect: [`${repository}/node_modules/effect/dist/index.js`],
          "effect/*": [`${repository}/node_modules/effect/dist/*.js`],
        },
      },
    };
    for (const [name, text] of Object.entries({
      "tsconfig.json": yield* encodeJson(tsconfig),
      ...files,
    })) {
      yield* write(`${directory}/${name}`, text);
    }
    return { directory, home, scope: yield* Effect.scope };
  });

export const makeProject = (files: Readonly<Record<string, string>>) =>
  Effect.flatMap(REPOSITORY_DIRECTORY, (taskPackageDirectoryPath) =>
    makeProjectUsingTaskFrom({ files, taskPackageDirectoryPath }),
  );
