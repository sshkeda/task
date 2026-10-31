import { Clock, Context, Effect, FileSystem, Layer } from "effect";

import { setProjectDirectoryPath } from "../../../shared/find-user-locations-in-stack-traces.ts";
import {
  readSetupLimit,
  passStateDirectoryToCommands,
  readStateDirectory,
  readWorkerCount,
  requireSupportedBunVersion,
  inheritedRunMarkers,
  workingDirectoryPath,
} from "../../../shared/read-environment-config.ts";
import { TASK_PACKAGE } from "../../../shared/task-package.ts";
import { createRunMarker } from "../../kill-leftover-processes/index.ts";
import { readSupervisorRunMarker } from "../../supervise-the-run/index.ts";
import type { Task } from "../../task/index.ts";
import { requirePackageJsonDirectoryPath } from "./find-package-directories.ts";
import { RootTaskFileUnavailable } from "./main-thread-errors.ts";
import { readPackageJson } from "./read-package-json.ts";

const readRunRootTaskConfig = Effect.fn("readRunRootTaskConfig")(function* (
  rootTask: Task,
  supervisorProcessId: number | null,
) {
  yield* requireSupportedBunVersion(Bun.version);
  const fileSystem = yield* FileSystem.FileSystem;
  const runStartedAtMilliseconds = yield* Clock.currentTimeMillis;
  const rootTaskFilePath = yield* fileSystem
    .realPath(Bun.main)
    .pipe(Effect.mapError((cause) => new RootTaskFileUnavailable({ filePath: Bun.main, cause })));
  const rootTaskPackageJsonDirectoryPath = yield* requirePackageJsonDirectoryPath(rootTaskFilePath);
  yield* setProjectDirectoryPath(rootTaskPackageJsonDirectoryPath);
  const rootTaskPackageJsonName = (yield* readPackageJson(rootTaskPackageJsonDirectoryPath)).name;

  const { stateDirectoryPath, configuredStateDirectoryPath } = yield* readStateDirectory();
  yield* passStateDirectoryToCommands(configuredStateDirectoryPath);
  const workerCount = yield* readWorkerCount();
  const setupLimit = yield* readSetupLimit();
  const runMarker =
    supervisorProcessId === null
      ? yield* createRunMarker(yield* inheritedRunMarkers)
      : yield* readSupervisorRunMarker;
  return {
    rootTask,
    workerCount,
    setupLimit,
    stateDirectoryPath,
    configuredStateDirectoryPath,
    rootTaskFilePath,
    rootTaskPackageJsonDirectoryPath,
    rootTaskPackageJsonName,
    workingDirectoryPathAtStart: yield* workingDirectoryPath,
    runStartedAtMilliseconds,
    runProcessId: supervisorProcessId ?? process.pid,
    ownsSession: supervisorProcessId !== null,
    runMarker,
  };
});

export class RunRootTaskConfig extends Context.Service<
  RunRootTaskConfig,
  Effect.Success<ReturnType<typeof readRunRootTaskConfig>>
>()(`${TASK_PACKAGE.name}/RunRootTaskConfig`) {}

export const RunRootTaskConfigLive = (rootTask: Task, supervisorProcessId: number | null) =>
  Layer.effect(RunRootTaskConfig, readRunRootTaskConfig(rootTask, supervisorProcessId));
