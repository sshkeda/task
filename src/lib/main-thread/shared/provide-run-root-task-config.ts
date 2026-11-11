import { Clock, Context, Effect, FileSystem, Layer, Path } from "effect";

import { makeNodeModulesPackageFinder } from "../../../shared/find-node-modules-package-directory.ts";
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
import { TASK_PACKAGE } from "../../../shared/task-package.ts";
import { createRunMarker } from "../../kill-leftover-processes/index.ts";
import { readSupervisorRunMarker } from "../../supervise-the-run/index.ts";
import {
  findPackageJsonDirectoryPath,
  requirePackageJsonDirectoryPath,
} from "./find-package-directories.ts";
import { RootTaskFileUnavailable } from "./main-thread-errors.ts";
import { readPackageJson } from "./read-package-json.ts";

const readTraceDirectoryName = Effect.fn("readTraceDirectoryName")(function* (
  rootTaskFilePath: string,
  rootTaskPackageJsonDirectoryPath: string,
) {
  const path = yield* Path.Path;
  const startPackageJsonDirectoryPath = yield* findPackageJsonDirectoryPath(
    yield* workingDirectoryPath,
  ).pipe(Effect.catchTag("PackageJsonSearchFailed", () => Effect.succeed(null)));
  const startsInsideAnInstalledPackage =
    startPackageJsonDirectoryPath !== null &&
    makeNodeModulesPackageFinder(startPackageJsonDirectoryPath)(rootTaskFilePath) !== null;
  if (startsInsideAnInstalledPackage) {
    const startDirectoryName = path.basename(startPackageJsonDirectoryPath);
    return yield* readPackageJson(startPackageJsonDirectoryPath).pipe(
      Effect.map((packageJson) => packageJson.name ?? startDirectoryName),
      Effect.catchTag(
        [
          "PackageJsonUnreadable",
          "PackageJsonNotJson",
          "PackageJsonNotObject",
          "PackageJsonNameNotString",
          "PackageJsonNameBlank",
          "PackageJsonNameInvalid",
          "PackageJsonFieldInvalid",
        ],
        () => Effect.succeed(startDirectoryName),
      ),
    );
  }
  return (
    (yield* readPackageJson(rootTaskPackageJsonDirectoryPath)).name ??
    path.basename(rootTaskPackageJsonDirectoryPath)
  );
});

const readRunRootTaskConfig = Effect.fn("readRunRootTaskConfig")(function* (
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
  yield* setProjectDirectoryPath(rootTaskPackageJsonDirectoryPath);
  const traceDirectoryName = yield* readTraceDirectoryName(
    rootTaskFilePath,
    rootTaskPackageJsonDirectoryPath,
  );

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
    traceDirectoryName,
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

export const RunRootTaskConfigLive = (...runRoot: Parameters<typeof readRunRootTaskConfig>) =>
  Layer.effect(RunRootTaskConfig, readRunRootTaskConfig(...runRoot));
