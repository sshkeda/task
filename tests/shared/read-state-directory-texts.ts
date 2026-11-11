import { Effect, FileSystem } from "effect";

import { TASK_STATE_DIRECTORY_NAME } from "../../src/shared/names-from-package-json.ts";

export const readStateDirectoryTexts = (home: string) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const directory = `${home}/${TASK_STATE_DIRECTORY_NAME}`;
    const paths = yield* fileSystem.readDirectory(directory, { recursive: true });
    const files = yield* Effect.forEach(paths, (path) =>
      Effect.gen(function* () {
        if ((yield* fileSystem.stat(`${directory}/${path}`)).type !== "File") {
          return [];
        }
        return [{ path, text: yield* fileSystem.readFileString(`${directory}/${path}`) }];
      }),
    );
    return files.flat();
  });
