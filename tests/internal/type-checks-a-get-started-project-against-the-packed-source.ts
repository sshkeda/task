import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect, FileSystem, Path } from "effect";

import packageJson from "../../package.json" with { type: "json" };
import { encodeJson } from "../shared/encode-json.ts";
import { REPOSITORY_DIRECTORY } from "../shared/find-repository-directory.ts";
import { makeTest } from "../shared/make-tests.ts";
import { packPackageTarballInto } from "../shared/pack-package-tarball.ts";

const GET_STARTED_TSCONFIG = {
  compilerOptions: {
    strict: true,
    module: "Preserve",
    moduleResolution: "bundler",
    noEmit: true,
    skipLibCheck: true,
    allowImportingTsExtensions: true,
    types: ["bun"],
  },
} as const;

const GET_STARTED_TASK_FILE = `import { Task } from "@sshkeda/task";
import { Effect } from "effect";

export default Task.make({
  name: "typecheck",
  run: Effect.asVoid(Task.exec("bunx", ["tsc", "--noEmit"])),
});
`;

export const getStartedProjectTypeChecks = makeTest(
  "a project set up as README's Get started says, with plain strict and no target, type-checks the packed TypeScript source with TypeScript 7 without errors",
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const repository = yield* REPOSITORY_DIRECTORY;
      const tarballDirectory = yield* fileSystem.makeTempDirectoryScoped();
      const project = yield* fileSystem.realPath(yield* fileSystem.makeTempDirectoryScoped());
      const tarball = yield* packPackageTarballInto(tarballDirectory);
      const projectPackageJson = {
        name: "get-started-project",
        type: "module",
        dependencies: {
          [packageJson.name]: `file:${tarball}`,
          effect: packageJson.devDependencies.effect,
        },
        devDependencies: { "@types/bun": packageJson.devDependencies["@types/bun"] },
      };
      yield* fileSystem.writeFileString(
        path.join(project, "package.json"),
        yield* encodeJson(projectPackageJson),
      );
      yield* fileSystem.writeFileString(
        path.join(project, "tsconfig.json"),
        yield* encodeJson(GET_STARTED_TSCONFIG),
      );
      yield* fileSystem.makeDirectory(path.join(project, "scripts"));
      yield* fileSystem.writeFileString(
        path.join(project, "scripts/typecheck.ts"),
        GET_STARTED_TASK_FILE,
      );
      yield* Task.exec("bun", ["install"], { cwd: project });
      const typeCheckOutput = yield* Task.exec(
        path.join(repository, "node_modules/.bin/tsc"),
        ["--noEmit"],
        { cwd: project },
      );
      expect(typeCheckOutput).toBe("");
    }),
  ),
  "2 minutes",
);
