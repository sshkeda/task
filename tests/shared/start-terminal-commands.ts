import { Clock, Duration, Effect, FileSystem, Option, Schedule } from "effect";

import { failTestCleanup } from "./fail-test-cleanup.ts";
import { REPOSITORY_DIRECTORY } from "./find-repository-directory.ts";
import { SpawnLockNeverFreed } from "./test-errors.ts";

const STALE_AFTER = "10 seconds";

const WAIT_AT_MOST = "30 seconds";

const removeIfStale = (fileSystem: FileSystem.FileSystem, lock: string) =>
  Effect.gen(function* () {
    const { mtime } = yield* fileSystem.stat(lock);
    const now = yield* Clock.currentTimeMillis;
    if (Option.exists(mtime, (made) => now - made.getTime() > Duration.toMillis(STALE_AFTER))) {
      yield* fileSystem.remove(lock);
    }
  }).pipe(Effect.catchReason("PlatformError", "NotFound", () => Effect.void));

const holdSpawnLock = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const scratch = `${yield* REPOSITORY_DIRECTORY}/.scratch`;
  const lock = `${scratch}/terminal-spawn.lock`;
  yield* fileSystem.makeDirectory(scratch, { recursive: true });
  yield* Effect.acquireRelease(
    fileSystem.writeFileString(lock, String(process.pid), { flag: "wx" }).pipe(
      Effect.as(true),
      Effect.catchReason("PlatformError", "AlreadyExists", () =>
        removeIfStale(fileSystem, lock).pipe(Effect.as(false)),
      ),
      Effect.repeat({ until: (taken) => taken, schedule: Schedule.spaced("5 millis") }),
      Effect.timeoutOrElse({
        duration: WAIT_AT_MOST,
        orElse: () => new SpawnLockNeverFreed({ lock, waited: WAIT_AT_MOST }),
      }),
    ),
    () => fileSystem.remove(lock).pipe(failTestCleanup(`remove ${lock}`)),
  );
});

export const startTerminalCommand = (
  command: readonly string[],
  {
    workingDirectory,
    environment,
    terminal,
  }: {
    readonly workingDirectory: string;
    readonly environment: Readonly<Record<string, string | undefined>>;
    readonly terminal: Bun.TerminalOptions;
  },
) =>
  Effect.acquireRelease(
    holdSpawnLock.pipe(
      Effect.andThen(
        Effect.sync(() =>
          Bun.spawn([...command], { cwd: workingDirectory, env: environment, terminal }),
        ),
      ),
      Effect.scoped,
    ),
    (subprocess) =>
      Effect.promise(() => {
        subprocess.kill("SIGKILL");
        subprocess.terminal?.close();
        return subprocess.exited;
      }),
  );
