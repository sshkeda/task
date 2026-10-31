import { Effect, FileSystem } from "effect";

import { RootTaskFileNotChecked } from "../../../shared/task-errors.ts";

export const isRootTaskFileThere = FileSystem.FileSystem.pipe(
  Effect.flatMap((fileSystem) => fileSystem.exists(Bun.main)),
  Effect.mapError((cause) => new RootTaskFileNotChecked({ filePath: Bun.main, cause })),
);
