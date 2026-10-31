import { Task } from "@sshkeda/task";
import { Effect, FileSystem, Path } from "effect";

import packageJson from "../../package.json" with { type: "json" };
import { REPOSITORY_DIRECTORY } from "./find-repository-directory.ts";

const PACKED_FILES = [...packageJson.files, "package.json", "README.md", "LICENSE"];

export const packPackageTarballInto = (destination: string) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const repository = yield* REPOSITORY_DIRECTORY;
    const stagedPackage = yield* fileSystem.makeTempDirectoryScoped();
    yield* Effect.forEach(
      PACKED_FILES,
      (file) => fileSystem.copy(path.join(repository, file), path.join(stagedPackage, file)),
      { discard: true },
    );
    const packedTarball = yield* Task.exec(
      "bun",
      ["pm", "pack", "--quiet", "--destination", destination],
      { cwd: stagedPackage },
    );
    return path.resolve(destination, path.basename(packedTarball.trim()));
  }).pipe(Effect.scoped);
