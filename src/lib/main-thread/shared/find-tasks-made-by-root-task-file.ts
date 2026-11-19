import { inspect } from "node:util";

import { Array as Arrays, Duration, Effect, Formatter, Path, Predicate } from "effect";

import { filePathOfLocation } from "../../../shared/find-user-locations-in-stack-traces.ts";
import { nameTask } from "../../../shared/label-lines.ts";
import { makeRootTaskGroup } from "../../../shared/make-root-task-group.ts";
import { TASK_TRACE_LABEL_PREFIX } from "../../../shared/names-from-package-json.ts";
import {
  findDefinitionNotGiven,
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
import { withTaskSpan } from "../../../shared/task-spans.ts";
import {
  BrokenTasksFound,
  RootTaskNotMadeWhileLoading,
  TaskCleanupInvalid,
  TaskIsolatedInvalid,
  TaskMadeAtUnknownLocation,
  TaskMadeMoreThanOnce,
  TaskNameBlank,
  TaskNameEmpty,
  TaskOptionsUnknown,
  TaskTimeoutInvalid,
} from "./main-thread-errors.ts";
import type { TaskDefinitionViolation } from "./main-thread-errors.ts";
import { RunRootTaskConfig } from "./provide-run-root-task-config.ts";

export const checkTasksMadeWithName = (
  taskName: string,
  madeTasks: readonly {
    readonly location: string | undefined;
    readonly options: typeof TaskOptionsRead.Type;
    readonly definitionNotGiven?: ReturnType<typeof findDefinitionNotGiven>;
  }[],
  madeOnWorkerId: number | null,
) => {
  const violations: (typeof TaskDefinitionViolation.Type)[] = [];
  const [madeTask, ...otherMadeTasks] = madeTasks;
  if (madeTask === undefined) {
    return { violations, discovered: null };
  }
  const definitionsNotGiven = madeTasks.flatMap((made) => made.definitionNotGiven ?? []);
  if (Arrays.isArrayNonEmpty(definitionsNotGiven)) {
    violations.push(
      ...definitionsNotGiven.map(
        (notGiven) =>
          new TaskDefinitionNotGiven({
            given: notGiven.given,
            location: notGiven.location,
          }),
      ),
    );
    return { violations, discovered: null };
  }
  const name: unknown = taskName;
  const locations = madeTasks.flatMap((made) => made.location ?? []);
  const location = madeTask.location;
  const isAnyLocationUnknown = location === undefined || locations.length < madeTasks.length;
  if (isAnyLocationUnknown) {
    violations.push(
      new TaskMadeAtUnknownLocation({
        nameText: Predicate.isString(name) ? nameTask(name) : Formatter.format(name),
      }),
    );
    return { violations, discovered: null };
  }
  const taskFilePath = filePathOfLocation(location);
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
  if (otherMadeTasks.length > 0) {
    violations.push(
      ...locations.map(
        (madeLocation) =>
          new TaskMadeMoreThanOnce({
            taskName,
            location: madeLocation,
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
  return {
    violations,
    discovered: {
      location,
      filePath: taskFilePath,
      timeoutDuration: timeout.duration,
      cleanupDuration: cleanup.duration ?? STOP_GRACE_PERIOD,
      isolated: isolated === true,
      madeOnWorkerId,
    },
  };
};

const readTasksMade = (madeTasks: ReturnType<typeof listTasksMadeWithName>) =>
  madeTasks.map((made) => ({
    location: made.location,
    options: readTaskOptions(made.task.options),
    definitionNotGiven: findDefinitionNotGiven(made.task),
  }));

export const findTaskMadeLate = Effect.fn(function* (taskName: string, taskFilePath: string) {
  yield* Effect.promise(() => import(taskFilePath));
  return checkTasksMadeWithName(taskName, readTasksMade(listTasksMadeWithName(taskName)), null);
});

export const findTasksMadeByRootTaskFile = Effect.fn(function* () {
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
    withTaskSpan("import", "import task file", {
      [`${TASK_TRACE_LABEL_PREFIX}.file`]: path.relative(
        rootTaskPackageJsonDirectoryPath,
        rootTaskFilePath,
      ),
    }),
  );
  yield* makeRootTaskGroup(rootTaskGroup, rootTaskFilePath);

  const violations: (typeof TaskDefinitionViolation.Type)[] = [];
  const discoveredTaskEntries = [];
  for (const [taskName, madeTasks] of listTasksMade()) {
    const checked = checkTasksMadeWithName(taskName, readTasksMade(madeTasks), null);
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

export const findRootFileTaskNames = Effect.fn(function* () {
  const { rootTaskFilePath } = yield* RunRootTaskConfig;
  const rootTaskFile = yield* Effect.promise((): Promise<unknown> => import(rootTaskFilePath));
  const exportedValues = new Set(
    Predicate.isObject(rootTaskFile) ? Object.values(rootTaskFile) : [],
  );
  const tasksMadeByTheRootFile = listTasksMade().map(([taskName, madeTasks]) => ({
    taskName,
    madeTasks: madeTasks.filter(
      (madeTask) =>
        madeTask.location !== undefined &&
        filePathOfLocation(madeTask.location) === rootTaskFilePath,
    ),
  }));
  return {
    made: tasksMadeByTheRootFile
      .filter(({ madeTasks }) => Arrays.isArrayNonEmpty(madeTasks))
      .map(({ taskName }) => taskName),
    unexported: tasksMadeByTheRootFile
      .filter(({ madeTasks }) =>
        madeTasks.some(
          (madeTask) => !madeTask.madeInTaskBody && !exportedValues.has(madeTask.task),
        ),
      )
      .map(({ taskName }) => taskName),
  };
});
