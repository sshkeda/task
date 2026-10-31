import { Effect, FileSystem, Path } from "effect";

import { listDirectoryPathsUpToRoot } from "./list-directory-paths-up-to-root.ts";
import { MissingPackageJson } from "./main-thread-errors.ts";
import { PackageJsonSearchFailed } from "./package-json-errors.ts";

export const findPackageJsonDirectoryPath = Effect.fn("findPackageJsonDirectoryPath")(function* (
  start: string,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  for (const searchDirectoryPath of listDirectoryPathsUpToRoot(path, start)) {
    const packageJsonFilePath = `${searchDirectoryPath}/package.json`;
    const packageJsonExists = yield* fileSystem
      .exists(packageJsonFilePath)
      .pipe(
        Effect.mapError(
          (cause) => new PackageJsonSearchFailed({ filePath: packageJsonFilePath, cause }),
        ),
      );
    if (packageJsonExists) {
      return searchDirectoryPath;
    }
  }
  return null;
});

export const requirePackageJsonDirectoryPath = (filePath: string) =>
  Path.Path.use((path) => findPackageJsonDirectoryPath(path.dirname(filePath))).pipe(
    Effect.filterOrFail(
      (packageJsonDirectoryPath) => packageJsonDirectoryPath !== null,
      () => new MissingPackageJson({ filePath }),
    ),
  );
