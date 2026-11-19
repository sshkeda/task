import { Effect, FileSystem } from "effect";

const MACOS_SHELL_SELECTION_PATH = "/private/var/select/sh";

export const readShellExecutableName = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const shellPath =
    process.platform === "darwin"
      ? MACOS_SHELL_SELECTION_PATH
      : yield* Effect.fromNullishOr(Bun.which("sh"));
  const executablePath = yield* fileSystem.realPath(shellPath);
  return executablePath.slice(executablePath.lastIndexOf("/") + 1);
});
