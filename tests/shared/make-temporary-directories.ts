import { Effect, Exit, FileSystem } from "effect";

import { failTestCleanup } from "./fail-test-cleanup.ts";
import { REPOSITORY_DIRECTORY } from "./find-repository-directory.ts";

export const makeTemporaryDirectory = (directory?: string) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    return yield* Effect.acquireRelease(
      fileSystem
        .makeTempDirectory(directory === undefined ? {} : { directory })
        .pipe(Effect.flatMap((directoryPath) => fileSystem.realPath(directoryPath))),
      (directoryPath, scopeExit) =>
        Exit.isSuccess(scopeExit)
          ? fileSystem
              .remove(directoryPath, { recursive: true })
              .pipe(failTestCleanup(`remove scratch directory ${directoryPath}`))
          : Effect.log(`kept failure artifacts: ${directoryPath}`),
    );
  });

export const makeScratchDirectory = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const repository = yield* REPOSITORY_DIRECTORY;
  yield* fileSystem.makeDirectory(`${repository}/.scratch`, { recursive: true });
  return yield* makeTemporaryDirectory(`${repository}/.scratch`);
});
