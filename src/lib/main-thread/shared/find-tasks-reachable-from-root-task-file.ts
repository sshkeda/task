import { inspect } from "node:util";

import { Array as Arrays, Duration, Effect, Option, Path } from "effect";

import { makeNodeModulesPackageFinder } from "../../../shared/find-node-modules-package-directory.ts";
import { InternalInvariantBroken } from "../../../shared/invariant-errors.ts";
import { TASK_TRACE_LABEL_PREFIX } from "../../../shared/names-from-package-json.ts";
import { findTasksMadeWithName } from "../../../shared/record-tasks-made.ts";
import { STOP_GRACE_PERIOD } from "../../../shared/stop-signals.ts";
import { TaskModuleImportFailed, TaskSetupTimedOut } from "../../../shared/task-errors.ts";
import { ModuleGraph } from "../lib/find-code-each-task-depends-on/index.ts";
import { requirePackageJsonDirectoryPath } from "./find-package-directories.ts";
import {
  BrokenTasksFound,
  RootTaskNotMadeAtTopLevel,
  TaskCleanupInvalid,
  TaskFileImportsItself,
  TaskMadeAgainInThread,
  TaskMadeMoreThanOnce,
  TaskNotMadeByImport,
  TaskOptionsUnknown,
  TaskTimeoutInvalid,
} from "./main-thread-errors.ts";
import type { TaskDefinitionViolation } from "./main-thread-errors.ts";
import { RunRootTaskConfig } from "./provide-run-root-task-config.ts";

const readTimeoutDuration = (input: Duration.Input) =>
  Option.filter(Duration.fromInput(input), Duration.isPositive);

const readCleanupDuration = (input: Duration.Input) =>
  Option.filter(
    Duration.fromInput(input),
    (duration) => Duration.isZero(duration) || Duration.isPositive(duration),
  );

const findImportCycleThrough = (
  reachable: Effect.Success<ReturnType<ModuleGraph["Service"]["findReachableModules"]>>,
  taskFilePath: string,
) => {
  const seenFilePaths = new Set([taskFilePath]);
  const importPaths = [Arrays.of(taskFilePath)];
  for (const importPath of importPaths) {
    const importingFilePath = Arrays.lastNonEmpty(importPath);
    const importingModule = reachable.get(importingFilePath);
    if (importingModule === undefined) {
      throw new InternalInvariantBroken({
        area: "module graph",
        invariant: `${importingFilePath} is among the modules reachable from the root task file`,
      });
    }
    for (const importedModule of importingModule.imports) {
      if (importedModule.filePath === taskFilePath) {
        return Arrays.append(importPath, taskFilePath);
      }
      if (!seenFilePaths.has(importedModule.filePath)) {
        seenFilePaths.add(importedModule.filePath);
        importPaths.push(Arrays.append(importPath, importedModule.filePath));
      }
    }
  }
  return null;
};

const findTaskFileImportCycles = (
  reachable: Effect.Success<ReturnType<ModuleGraph["Service"]["findReachableModules"]>>,
) => {
  const cycles: Arrays.NonEmptyArray<string>[] = [];
  const filePathsInFoundCycles = new Set<string>();
  for (const [filePath, module] of reachable) {
    const isUncheckedTaskFile =
      module.madeTasks.length > 0 && !filePathsInFoundCycles.has(filePath);
    const cycle = isUncheckedTaskFile ? findImportCycleThrough(reachable, filePath) : null;
    if (cycle !== null) {
      cycles.push(cycle);
      for (const filePathInCycle of cycle) {
        filePathsInFoundCycles.add(filePathInCycle);
      }
    }
  }
  return cycles;
};

export const findTasksReachableFromRootTaskFile = Effect.fn("findTasksReachableFromRootTaskFile")(
  function* () {
    const path = yield* Path.Path;
    const runRootTaskConfig = yield* RunRootTaskConfig;
    const rootTask = runRootTaskConfig.rootTask;
    const findNodeModulesPackageDirectoryPath = makeNodeModulesPackageFinder(
      runRootTaskConfig.rootTaskPackageJsonDirectoryPath,
    );
    const moduleGraph = yield* ModuleGraph;
    const reachable = yield* moduleGraph.findReachableModules(runRootTaskConfig.rootTaskFilePath);
    const sitesMakingTask = new Map<
      string,
      Arrays.NonEmptyArray<{
        readonly filePath: string;
        readonly location: string;
      }>
    >();
    for (const [taskFilePath, module] of reachable) {
      for (const madeTask of module.madeTasks) {
        sitesMakingTask.set(
          madeTask.name,
          Arrays.append(sitesMakingTask.get(madeTask.name) ?? [], {
            filePath: taskFilePath,
            location: `${taskFilePath}:${madeTask.line}`,
          }),
        );
      }
    }

    const importCycles = findTaskFileImportCycles(reachable).map(
      (cycle) =>
        new TaskFileImportsItself({
          filePath: Arrays.headNonEmpty(cycle),
          cycle: cycle.map((filePath) =>
            path.relative(runRootTaskConfig.rootTaskPackageJsonDirectoryPath, filePath),
          ),
        }),
    );
    const violations: (typeof TaskDefinitionViolation.Type)[] = [...reachable.values()].flatMap(
      (module) => module.refusals,
    );
    if (Arrays.isArrayNonEmpty(importCycles)) {
      return yield* new BrokenTasksFound({
        violations: Arrays.appendAll(violations, importCycles),
      });
    }
    for (const [taskFilePath, taskName] of new Map(
      Array.from(sitesMakingTask, ([name, sites]) => [Arrays.headNonEmpty(sites).filePath, name]),
    )) {
      yield* Effect.tryPromise({
        try: () => import(taskFilePath),
        catch: (cause) => new TaskModuleImportFailed({ filePath: taskFilePath, cause }),
      }).pipe(
        Effect.timeoutOrElse({
          duration: runRootTaskConfig.setupLimit,
          orElse: () =>
            Effect.fail(
              new TaskSetupTimedOut({
                taskName,
                limitMilliseconds: Duration.toMillis(runRootTaskConfig.setupLimit),
                unfinishedStep: `loading ${taskFilePath}`,
              }),
            ),
        }),
        Effect.withSpan("import task file", {
          attributes: {
            [`${TASK_TRACE_LABEL_PREFIX}.file`]: path.relative(
              runRootTaskConfig.rootTaskPackageJsonDirectoryPath,
              taskFilePath,
            ),
          },
        }),
      );
    }

    const discoveredTaskEntries = [];
    for (const [taskName, sites] of sitesMakingTask) {
      const [task, ...otherTasks] = findTasksMadeWithName(taskName);
      const { filePath: taskFilePath, location } = Arrays.headNonEmpty(sites);
      if (sites.length > 1) {
        violations.push(
          ...sites.map(
            (site) =>
              new TaskMadeMoreThanOnce({
                taskName,
                location: site.location,
                madeCount: sites.length,
              }),
          ),
        );
      } else if (task === undefined) {
        violations.push(new TaskNotMadeByImport({ taskName, filePath: taskFilePath }));
      } else if (otherTasks.length > 0) {
        violations.push(
          new TaskMadeAgainInThread({
            taskName,
            filePath: taskFilePath,
            madeCount: otherTasks.length + 1,
          }),
        );
      } else {
        const timeoutDuration =
          task.options.timeout === null
            ? Option.some(null)
            : readTimeoutDuration(task.options.timeout);
        const cleanupDuration =
          task.options.cleanup === null
            ? Option.some(STOP_GRACE_PERIOD)
            : readCleanupDuration(task.options.cleanup);
        if (Option.isNone(timeoutDuration)) {
          violations.push(
            new TaskTimeoutInvalid({ taskName, timeout: inspect(task.options.timeout) }),
          );
        }
        if (Option.isNone(cleanupDuration)) {
          violations.push(
            new TaskCleanupInvalid({ taskName, cleanup: inspect(task.options.cleanup) }),
          );
        }
        if (Arrays.isReadonlyArrayNonEmpty(task.options.unknownOptionNames)) {
          violations.push(
            new TaskOptionsUnknown({
              taskName,
              location,
              optionNames: task.options.unknownOptionNames,
            }),
          );
        }
        const isMissingDuration = Option.isNone(timeoutDuration) || Option.isNone(cleanupDuration);
        if (isMissingDuration) {
          continue;
        }
        const taskPackageJsonDirectoryPath =
          findNodeModulesPackageDirectoryPath(taskFilePath) === null
            ? yield* requirePackageJsonDirectoryPath(taskFilePath)
            : runRootTaskConfig.rootTaskPackageJsonDirectoryPath;
        discoveredTaskEntries.push([
          taskName,
          {
            definition: task,
            filePath: taskFilePath,
            packageJsonDirectoryPath: taskPackageJsonDirectoryPath,
            timeoutDuration: timeoutDuration.value,
            cleanupDuration: cleanupDuration.value,
          },
        ] as const);
      }
    }
    const discoveredTasksByName = new Map(discoveredTaskEntries);
    const isRootTaskNotMadeAtTopLevel =
      !sitesMakingTask.has(rootTask.name) ||
      !findTasksMadeWithName(rootTask.name).includes(rootTask);
    if (isRootTaskNotMadeAtTopLevel) {
      violations.push(
        new RootTaskNotMadeAtTopLevel({ rootTaskFilePath: runRootTaskConfig.rootTaskFilePath }),
      );
    }
    if (Arrays.isArrayNonEmpty(violations)) {
      return yield* new BrokenTasksFound({ violations });
    }

    return discoveredTasksByName;
  },
);
