import { Clock, Context, Effect, FileSystem, Layer, Path } from "effect";

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
import type { takeRunArgument } from "../../../shared/read-environment-config.ts";
import { setStartDirectoryPath } from "../../../shared/start-directory-path.ts";
import { TASK_PACKAGE } from "../../../shared/task-package.ts";
import { createRunMarker } from "../../kill-leftover-processes/index.ts";
import { readSupervisorRunMarker } from "../../supervise-the-run/index.ts";
import {
  findPackageJsonDirectoryPath,
  requirePackageJsonDirectoryPath,
} from "./find-package-directories.ts";
import { RootTaskFileUnavailable } from "./main-thread-errors.ts";
import { readPackageJson } from "./read-package-json.ts";

const findProjectDirectory = Effect.fn(function* (rootTaskPackageJsonDirectoryPath: string) {
  const startPackageJsonDirectoryPath = yield* findPackageJsonDirectoryPath(
    yield* workingDirectoryPath,
  );
  const startsInsideTheRootTaskPackage =
    startPackageJsonDirectoryPath === null ||
    startPackageJsonDirectoryPath === rootTaskPackageJsonDirectoryPath ||
    startPackageJsonDirectoryPath.startsWith(`${rootTaskPackageJsonDirectoryPath}/`);
  return startsInsideTheRootTaskPackage
    ? rootTaskPackageJsonDirectoryPath
    : startPackageJsonDirectoryPath;
});

const readRunRootTaskConfig = Effect.fn(function* (
  runArgument: Effect.Success<typeof takeRunArgument>,
  supervisorProcessId: number | null,
) {
  const { rootTaskName, rootTaskGroup, checksForgottenTasks, flags, scriptFilePath, log } =
    runArgument;
  yield* requireSupportedBunVersion(Bun.version);
  const fileSystem = yield* FileSystem.FileSystem;
  const runStartedAtMilliseconds = yield* Clock.currentTimeMillis;
  const rootTaskFilePath = yield* fileSystem
    .realPath(scriptFilePath)
    .pipe(
      Effect.mapError((cause) => new RootTaskFileUnavailable({ filePath: scriptFilePath, cause })),
    );
  const rootTaskPackageJsonDirectoryPath = yield* requirePackageJsonDirectoryPath(rootTaskFilePath);
  const projectDirectoryPath = yield* findProjectDirectory(rootTaskPackageJsonDirectoryPath);
  yield* setProjectDirectoryPath(projectDirectoryPath);
  const workingDirectoryPathAtStart = yield* workingDirectoryPath;
  yield* setStartDirectoryPath(workingDirectoryPathAtStart);
  const projectRunsDirectoryName =
    (yield* readPackageJson(projectDirectoryPath)).name ??
    (yield* Path.Path).basename(projectDirectoryPath);

  const { stateDirectoryPath, configuredStateDirectoryPath } = yield* readStateDirectory();
  yield* passStateDirectoryToCommands(configuredStateDirectoryPath);
  const workerCount = yield* readWorkerCount();
  const setupLimit = yield* readSetupLimit();
  const runMarker =
    supervisorProcessId === null
      ? yield* createRunMarker(yield* inheritedRunMarkers)
      : yield* readSupervisorRunMarker;
  return {
    rootTaskName,
    checksForgottenTasks,
    rootTaskGroup,
    flags,
    log,
    workerCount,
    setupLimit,
    stateDirectoryPath,
    configuredStateDirectoryPath,
    rootTaskFilePath,
    rootTaskPackageJsonDirectoryPath,
    projectDirectoryPath,
    projectRunsDirectoryName,
    workingDirectoryPathAtStart,
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

export const RunRootTaskConfigLive = (...runRoot: Parameters<typeof readRunRootTaskConfig>) =>
  Layer.effect(RunRootTaskConfig, readRunRootTaskConfig(...runRoot));
