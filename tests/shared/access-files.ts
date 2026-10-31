import { Effect, FileSystem, Path } from "effect";

import { MissingText } from "./test-errors.ts";

export const write = (file: string, text: string) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;

    yield* fileSystem.makeDirectory(path.dirname(file), { recursive: true });
    yield* fileSystem.writeFileString(file, text);
  });

export const read = (file: string) =>
  FileSystem.FileSystem.use((fileSystem) => fileSystem.readFileString(file));

export const exists = (file: string) =>
  FileSystem.FileSystem.use((fileSystem) => fileSystem.exists(file));

export const edit = (file: string, before: string, after: string) =>
  Effect.gen(function* () {
    const text = yield* read(file);
    if (!text.includes(before)) {
      return yield* new MissingText({ file, text: before });
    }

    yield* write(file, text.replace(before, after));
  });
