import type { Path } from "effect";

export const listDirectoryPathsUpToRoot = function* (
  path: Path.Path,
  directoryPath: string,
): Generator<string, void, undefined> {
  yield directoryPath;
  const parentDirectoryPath = path.dirname(directoryPath);
  if (parentDirectoryPath === directoryPath) {
    return;
  }
  yield* listDirectoryPathsUpToRoot(path, parentDirectoryPath);
};
