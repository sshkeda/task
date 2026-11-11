import { inspect } from "node:util";

import { Array as Arrays, Duration, Effect, Path, Predicate } from "effect";

import { makeNodeModulesPackageFinder } from "../../../shared/find-node-modules-package-directory.ts";
import { makeRootTaskGroup } from "../../../shared/make-root-task-group.ts";
import { TASK_TRACE_LABEL_PREFIX } from "../../../shared/names-from-package-json.ts";
import {
  findDefinitionNotGiven,
  filePathOfLocation,
  findTasksMadeWithName,
  listTasksMade,
  listTasksMadeWithName,
} from "../../../shared/record-tasks-made.ts";
import { readTaskOptions, TaskDurationOption } from "../../../shared/resolve-task-options.ts";
import type { TaskOptionsRead } from "../../../shared/resolve-task-options.ts";
import { STOP_GRACE_PERIOD } from "../../../shared/stop-signals.ts";
import {
  TaskDefinitionNotGiven,
  TaskModuleImportFailed,
  TaskNameNotString,
  TaskSetupTimedOut,
} from "../../../shared/task-errors.ts";
import { requirePackageJsonDirectoryPath } from "./find-package-directories.ts";
import {
  BrokenTasksFound,
  RootTaskNotMadeWhileLoading,
  TaskCleanupInvalid,
  TaskIsolatedInvalid,
  TaskMadeMoreThanOnce,
  TaskNameBlank,
  TaskNameEmpty,
  TaskNameMultiline,
  TaskOptionsUnknown,
  TaskTimeoutInvalid,
} from "./main-thread-errors.ts";
import type { TaskDefinitionViolation } from "./main-thread-errors.ts";
import { RunRootTaskConfig } from "./provide-run-root-task-config.ts";

const LINE_BREAK = /[\n\r]/u;

export const checkTasksMadeWithName = Effect.fn("checkTasksMadeWithName")(function* (
  taskName: string,
  madeTasks: readonly {
    readonly location: string | undefined;
    readonly options: typeof TaskOptionsRead.Type;
    readonly definitionNotGiven?: ReturnType<typeof findDefinitionNotGiven>;
  }[],
  madeOnWorkerId: number | null,
) {
  const { rootTaskFilePath, rootTaskPackageJsonDirectoryPath } = yield* RunRootTaskConfig;
  const violations: (typeof TaskDefinitionViolation.Type)[] = [];
  const [madeTask, ...otherMadeTasks] = madeTasks;
  if (madeTask === undefined) {
    return { violations, discovered: null };
  }
  const location = madeTask.location ?? rootTaskFilePath;
  const taskFilePath = filePathOfLocation(location);
  const definitionsNotGiven = madeTasks.flatMap((made) => made.definitionNotGiven ?? []);
  if (Arrays.isArrayNonEmpty(definitionsNotGiven)) {
    violations.push(
      ...definitionsNotGiven.map(
        (notGiven) =>
          new TaskDefinitionNotGiven({
            given: notGiven.given,
            location: notGiven.location ?? rootTaskFilePath,
          }),
      ),
    );
    return { violations, discovered: null };
  }
  const name: unknown = taskName;
  if (typeof name !== "string") {
    violations.push(new TaskNameNotString({ location, nameText: inspect(name) }));
    return { violations, discovered: null };
  }
  if (name === "") {
    violations.push(new TaskNameEmpty({ location }));
    return { violations, discovered: null };
  }
  if (name.trim() === "") {
    violations.push(new TaskNameBlank({ location }));
    return { violations, discovered: null };
  }
  if (LINE_BREAK.test(name)) {
    violations.push(
      new TaskNameMultiline({
        location,
        nameText: `"${name.replaceAll("\n", String.raw`\n`).replaceAll("\r", String.raw`\r`)}"`,
      }),
    );
    return { violations, discovered: null };
  }
  if (otherMadeTasks.length > 0) {
    violations.push(
      ...madeTasks.map(
        (made) =>
          new TaskMadeMoreThanOnce({
            taskName,
            location: made.location ?? rootTaskFilePath,
            madeCount: madeTasks.length,
          }),
      ),
    );
    return { violations, discovered: null };
  }
  const { timeout, cleanup, isolated, unknownOptionNames } = madeTask.options;
  if (TaskDurationOption.guards.Invalid(timeout)) {
    violations.push(new TaskTimeoutInvalid({ taskName, timeout: timeout.text }));
  }
  if (TaskDurationOption.guards.Invalid(cleanup)) {
    violations.push(new TaskCleanupInvalid({ taskName, cleanup: cleanup.text }));
  }
  if (Predicate.isObject(isolated)) {
    violations.push(new TaskIsolatedInvalid({ taskName, isolated: isolated.invalidText }));
  }
  if (Arrays.isReadonlyArrayNonEmpty(unknownOptionNames)) {
    violations.push(
      new TaskOptionsUnknown({
        taskName,
        location,
        optionNames: unknownOptionNames,
      }),
    );
  }
  const isMissingDuration =
    TaskDurationOption.guards.Invalid(timeout) || TaskDurationOption.guards.Invalid(cleanup);
  if (isMissingDuration) {
    return { violations, discovered: null };
  }
  const taskPackageJsonDirectoryPath =
    makeNodeModulesPackageFinder(rootTaskPackageJsonDirectoryPath)(taskFilePath) === null
      ? yield* requirePackageJsonDirectoryPath(taskFilePath)
      : rootTaskPackageJsonDirectoryPath;
  return {
    violations,
    discovered: {
      location,
      filePath: taskFilePath,
      packageJsonDirectoryPath: taskPackageJsonDirectoryPath,
      timeoutDuration: timeout.duration,
      cleanupDuration: cleanup.duration ?? STOP_GRACE_PERIOD,
      isolated: isolated === true,
      madeOnWorkerId,
    },
  };
});

const readTasksMade = (madeTasks: ReturnType<typeof listTasksMadeWithName>) =>
  madeTasks.map((made) => ({
    location: made.location,
    options: readTaskOptions(made.task.options),
    definitionNotGiven: findDefinitionNotGiven(made.task),
  }));

export const findTaskMadeLate = Effect.fn("findTaskMadeLate")(function* (
  taskName: string,
  taskFilePath: string,
) {
  yield* Effect.promise(() => import(taskFilePath));
  return yield* checkTasksMadeWithName(
    taskName,
    readTasksMade(listTasksMadeWithName(taskName)),
    null,
  );
});

export const findTasksMadeByRootTaskFile = Effect.fn("findTasksMadeByRootTaskFile")(function* () {
  const path = yield* Path.Path;
  const runRootTaskConfig = yield* RunRootTaskConfig;
  const { rootTaskName, rootTaskGroup, rootTaskFilePath, rootTaskPackageJsonDirectoryPath } =
    runRootTaskConfig;
  yield* Effect.tryPromise({
    try: () => import(rootTaskFilePath),
    catch: (cause) => new TaskModuleImportFailed({ filePath: rootTaskFilePath, cause }),
  }).pipe(
    Effect.timeoutOrElse({
      duration: runRootTaskConfig.setupLimit,
      orElse: () =>
        Effect.fail(
          new TaskSetupTimedOut({
            taskName: rootTaskName,
            limitMilliseconds: Duration.toMillis(runRootTaskConfig.setupLimit),
            unfinishedStep: `loading ${rootTaskFilePath}`,
          }),
        ),
    }),
    Effect.withSpan("import task file", {
      attributes: {
        [`${TASK_TRACE_LABEL_PREFIX}.file`]: path.relative(
          rootTaskPackageJsonDirectoryPath,
          rootTaskFilePath,
        ),
      },
    }),
  );
  yield* makeRootTaskGroup(rootTaskGroup);

  const violations: (typeof TaskDefinitionViolation.Type)[] = [];
  const discoveredTaskEntries = [];
  for (const [taskName, madeTasks] of listTasksMade()) {
    const checked = yield* checkTasksMadeWithName(taskName, readTasksMade(madeTasks), null);
    violations.push(...checked.violations);
    if (checked.discovered !== null) {
      discoveredTaskEntries.push([taskName, checked.discovered] as const);
    }
  }
  if (findTasksMadeWithName(rootTaskName).length === 0) {
    violations.push(new RootTaskNotMadeWhileLoading({ rootTaskFilePath }));
  }
  if (Arrays.isArrayNonEmpty(violations)) {
    return yield* new BrokenTasksFound({ violations });
  }
  return new Map(discoveredTaskEntries);
});

export const findUnexportedRootFileTaskNames = Effect.fn("findUnexportedRootFileTaskNames")(
  function* () {
    const { rootTaskFilePath } = yield* RunRootTaskConfig;
    const rootTaskFile = yield* Effect.promise((): Promise<unknown> => import(rootTaskFilePath));
    const exportedValues = new Set(
      Predicate.isObject(rootTaskFile) ? Object.values(rootTaskFile) : [],
    );
    return listTasksMade().flatMap(([taskName, madeTasks]) =>
      madeTasks.some((madeTask) => {
        const isMadeByTheRootFile =
          madeTask.location !== undefined &&
          filePathOfLocation(madeTask.location) === rootTaskFilePath;
        const isLeftOut = !madeTask.madeInTaskBody && !exportedValues.has(madeTask.task);
        return isMadeByTheRootFile && isLeftOut;
      })
        ? [taskName]
        : [],
    );
  },
);
