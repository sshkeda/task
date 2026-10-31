import { Array as Arrays, Duration, Schema } from "effect";

import { TASK_ERROR_FIELDS, UNATTRIBUTED_FAILURE_FIELDS } from "../../../shared/error-fields.ts";
import { ImportResolutionThrew } from "../../../shared/module-graph-errors.ts";
import { describeCauseMessage, reportError } from "../../../shared/report-errors.ts";
import { TASK_OPTION_NAMES } from "../../../shared/resolve-task-options.ts";
import { exitCodeAfterStopSignal, STOP_REASONS } from "../../../shared/stop-signals.ts";
import { TASK_PACKAGE } from "../../../shared/task-package.ts";
import type { PrintableError } from "../../errors/index.ts";

export class ImportNotFound extends Schema.TaggedError<ImportNotFound>()("ImportNotFound", {
  ...TASK_ERROR_FIELDS,
  importingFilePath: Schema.String,
  specifier: Schema.String,
}) {
  public get location() {
    return this.importingFilePath;
  }
  public override get message() {
    return `imports "${this.specifier}", which the resolver reported as not found`;
  }
}

export class MissingPackageJson extends Schema.TaggedError<MissingPackageJson>()(
  "MissingPackageJson",
  { ...TASK_ERROR_FIELDS, filePath: Schema.String },
) {
  public get location() {
    return this.filePath;
  }
  public override get message() {
    return "no package.json found in its directory or any parent directory";
  }
}

export class RootTaskFileUnavailable extends Schema.TaggedError<RootTaskFileUnavailable>()(
  "RootTaskFileUnavailable",
  { ...TASK_ERROR_FIELDS, filePath: Schema.String, cause: Schema.Defect() },
) {
  public get location() {
    return this.filePath;
  }
  public override get message() {
    return `could not resolve the root task file because ${describeCauseMessage(this.cause)}`;
  }
}

export class RootTaskWorkingDirectoryUnavailable extends Schema.TaggedError<RootTaskWorkingDirectoryUnavailable>()(
  "RootTaskWorkingDirectoryUnavailable",
  { ...TASK_ERROR_FIELDS, packageJsonDirectoryPath: Schema.String, cause: Schema.Defect() },
) {
  public override get message() {
    return `could not enter root task directory "${this.packageJsonDirectoryPath}" because ${describeCauseMessage(this.cause)}`;
  }
}

const SCANNED_LINE_FIELDS = { line: Schema.NullOr(Schema.Int) };

export class ModuleSyntaxInvalid extends Schema.TaggedError<ModuleSyntaxInvalid>()(
  "ModuleSyntaxInvalid",
  { ...TASK_ERROR_FIELDS, ...SCANNED_LINE_FIELDS, parserMessage: Schema.String },
) {
  public override get message() {
    return `has the syntax error ${this.parserMessage}`;
  }
}

export class TaskPackageImportedWithoutTask extends Schema.TaggedError<TaskPackageImportedWithoutTask>()(
  "TaskPackageImportedWithoutTask",
  { ...SCANNED_LINE_FIELDS, ...TASK_ERROR_FIELDS },
) {
  public override get message() {
    return `imports ${TASK_PACKAGE.name} without naming Task`;
  }
}

export class ModuleLoaderRefused extends Schema.TaggedError<ModuleLoaderRefused>()(
  "ModuleLoaderRefused",
  { ...TASK_ERROR_FIELDS, ...SCANNED_LINE_FIELDS, loader: Schema.String },
) {
  public override get message() {
    return `uses the module loader ${this.loader}`;
  }
}

export class ModuleLoadedByComputedPath extends Schema.TaggedError<ModuleLoadedByComputedPath>()(
  "ModuleLoadedByComputedPath",
  {
    ...TASK_ERROR_FIELDS,
    ...SCANNED_LINE_FIELDS,
    loader: Schema.String,
    argumentText: Schema.String,
  },
) {
  public override get message() {
    return `calls ${this.loader}(${this.argumentText}), which has no literal path`;
  }
}

export class TaskNameMissing extends Schema.TaggedError<TaskNameMissing>()("TaskNameMissing", {
  ...SCANNED_LINE_FIELDS,
  ...TASK_ERROR_FIELDS,
}) {
  public override get message() {
    return "calls Task.make() without a name";
  }
}

export class TaskNameNotString extends Schema.TaggedError<TaskNameNotString>()(
  "TaskNameNotString",
  { ...SCANNED_LINE_FIELDS, ...TASK_ERROR_FIELDS, nameText: Schema.String },
) {
  public override get message() {
    return `calls Task.make() with the name ${this.nameText}, which is not a string`;
  }
}

export class TaskNameComputed extends Schema.TaggedError<TaskNameComputed>()("TaskNameComputed", {
  ...SCANNED_LINE_FIELDS,
  ...TASK_ERROR_FIELDS,
  nameText: Schema.String,
}) {
  public override get message() {
    return `calls Task.make() with the name ${this.nameText}, which is computed`;
  }
}

export class TaskNameEmpty extends Schema.TaggedError<TaskNameEmpty>()("TaskNameEmpty", {
  ...SCANNED_LINE_FIELDS,
  ...TASK_ERROR_FIELDS,
}) {
  public override get message() {
    return "calls Task.make() with an empty name";
  }
}

export class TaskCalledBelowTopLevel extends Schema.TaggedError<TaskCalledBelowTopLevel>()(
  "TaskCalledBelowTopLevel",
  { ...SCANNED_LINE_FIELDS, ...TASK_ERROR_FIELDS },
) {
  public override get message() {
    return "calls Task.make() below the top level of its module";
  }
}

export const ModuleScanRefusal = Schema.Union([
  ModuleSyntaxInvalid,
  TaskPackageImportedWithoutTask,
  ModuleLoaderRefused,
  ModuleLoadedByComputedPath,
  TaskNameMissing,
  TaskNameNotString,
  TaskNameComputed,
  TaskNameEmpty,
  TaskCalledBelowTopLevel,
]);

const ScannedModuleRefusal = Schema.Struct({
  filePath: Schema.String,
  refusal: ModuleScanRefusal,
});

export const ModuleRefusal = Schema.Union([
  ScannedModuleRefusal,
  ImportNotFound,
  ImportResolutionThrew,
]);

export class TaskFileImportsItself extends Schema.TaggedError<TaskFileImportsItself>()(
  "TaskFileImportsItself",
  { ...TASK_ERROR_FIELDS, filePath: Schema.String, cycle: Schema.Array(Schema.String) },
) {
  public override get message() {
    return `imports ${this.cycle.slice(1).join(", which imports ")}; a worker imports only the file of the task it runs, so a task file in an import cycle would load in a different order there than when Task.main() loaded it`;
  }
}

export class TaskMadeMoreThanOnce extends Schema.TaggedError<TaskMadeMoreThanOnce>()(
  "TaskMadeMoreThanOnce",
  { ...TASK_ERROR_FIELDS, taskName: Schema.String, location: Schema.String, madeCount: Schema.Int },
) {
  public override get message() {
    return `makes the task "${this.taskName}", which ${this.madeCount} Task.make() calls make`;
  }
}

export class TaskNotMadeByImport extends Schema.TaggedError<TaskNotMadeByImport>()(
  "TaskNotMadeByImport",
  { ...TASK_ERROR_FIELDS, taskName: Schema.String, filePath: Schema.String },
) {
  public override get message() {
    return `importing this file did not make task "${this.taskName}" in the copy of ${TASK_PACKAGE.name} that calls Task.main()`;
  }
}

export class TaskMadeAgainInThread extends Schema.TaggedError<TaskMadeAgainInThread>()(
  "TaskMadeAgainInThread",
  {
    ...TASK_ERROR_FIELDS,
    taskName: Schema.String,
    filePath: Schema.String,
    madeCount: Schema.Int,
  },
) {
  public override get message() {
    return `Task.make() made "${this.taskName}" ${this.madeCount} times in this thread, but this file has its only top-level Task.make() call`;
  }
}

export class TaskTimeoutInvalid extends Schema.TaggedError<TaskTimeoutInvalid>()(
  "TaskTimeoutInvalid",
  { ...TASK_ERROR_FIELDS, taskName: Schema.String, timeout: Schema.String },
) {
  public override get message() {
    return `has the timeout ${this.timeout}, which is not a duration above 0`;
  }
}

export class TaskCleanupInvalid extends Schema.TaggedError<TaskCleanupInvalid>()(
  "TaskCleanupInvalid",
  { ...TASK_ERROR_FIELDS, taskName: Schema.String, cleanup: Schema.String },
) {
  public override get message() {
    return `has the cleanup ${this.cleanup}, which is not a duration of 0 or more`;
  }
}

export class TaskOptionsUnknown extends Schema.TaggedError<TaskOptionsUnknown>()(
  "TaskOptionsUnknown",
  {
    ...TASK_ERROR_FIELDS,
    taskName: Schema.String,
    location: Schema.String,
    optionNames: Schema.NonEmptyArray(Schema.String),
  },
) {
  public override get message() {
    return `task "${this.taskName}" has the ${this.optionNames.length === 1 ? "option" : "options"} ${this.optionNames.join(", ")} outside the task options (${TASK_OPTION_NAMES.join(", ")})`;
  }
}

export class RootTaskNotMadeAtTopLevel extends Schema.TaggedError<RootTaskNotMadeAtTopLevel>()(
  "RootTaskNotMadeAtTopLevel",
  { ...TASK_ERROR_FIELDS, rootTaskFilePath: Schema.String },
) {
  public override get message() {
    return `${this.rootTaskFilePath} runs a task that is not made by a top-level Task.make() call`;
  }
}

export const TaskDefinitionViolation = Schema.Union([
  ModuleRefusal,
  TaskFileImportsItself,
  TaskMadeMoreThanOnce,
  TaskNotMadeByImport,
  TaskMadeAgainInThread,
  TaskTimeoutInvalid,
  TaskCleanupInvalid,
  TaskOptionsUnknown,
  RootTaskNotMadeAtTopLevel,
]);

const isScannedModuleRefusal = Schema.is(ScannedModuleRefusal);

export const reportTaskDefinitionViolation = (
  violation: typeof TaskDefinitionViolation.Type,
): PrintableError =>
  isScannedModuleRefusal(violation)
    ? {
        tag: violation.refusal._tag,
        location: `${violation.filePath}${violation.refusal.line === null ? "" : `:${violation.refusal.line}`}`,
        message: violation.refusal.message,
      }
    : reportError(violation);

export class BrokenTasksFound extends Schema.TaggedError<BrokenTasksFound>()("BrokenTasksFound", {
  ...TASK_ERROR_FIELDS,
  ...UNATTRIBUTED_FAILURE_FIELDS,
  violations: Schema.NonEmptyArray(TaskDefinitionViolation),
}) {
  public get reportedErrors() {
    return Arrays.map(this.violations, reportTaskDefinitionViolation);
  }
  public override get message() {
    return `found ${this.violations.length} task definition ${this.violations.length === 1 ? "violation" : "violations"}`;
  }
}

export class StopRequested extends Schema.TaggedError<StopRequested>()("StopRequested", {
  ...TASK_ERROR_FIELDS,
  ...UNATTRIBUTED_FAILURE_FIELDS,
  signal: Schema.Literals(STOP_REASONS),
}) {
  public override get message() {
    return this.signal === "SIGPIPE"
      ? "standard error closed, so task stopped the run"
      : `received ${this.signal}`;
  }
  public get exitCode() {
    return exitCodeAfterStopSignal(this.signal);
  }
}

export class ServingTaskRequestsItself extends Schema.TaggedError<ServingTaskRequestsItself>()(
  "ServingTaskRequestsItself",
  { ...TASK_ERROR_FIELDS, taskName: Schema.String, cycle: Schema.Array(Schema.String) },
) {
  public override get message() {
    return `requests itself while serving through ${this.cycle.join(" -> ")}, so task cannot stop each of them after the tasks that requested it`;
  }
}

export class TaskCleanupTimedOut extends Schema.TaggedError<TaskCleanupTimedOut>()(
  "TaskCleanupTimedOut",
  { ...TASK_ERROR_FIELDS, taskName: Schema.String, cleanupMilliseconds: Schema.Finite },
) {
  public override get message() {
    return `its cleanup did not finish within ${Duration.format(Duration.millis(this.cleanupMilliseconds))}`;
  }
}

export class TaskCleanupNotStarted extends Schema.TaggedError<TaskCleanupNotStarted>()(
  "TaskCleanupNotStarted",
  { ...TASK_ERROR_FIELDS, taskName: Schema.String, cleanupMilliseconds: Schema.Finite },
) {
  public override get message() {
    return `its worker did not start its cleanup within ${Duration.format(Duration.millis(this.cleanupMilliseconds))}, so task terminated that worker`;
  }
}

export class TaskCleanupFrozeWorker extends Schema.TaggedError<TaskCleanupFrozeWorker>()(
  "TaskCleanupFrozeWorker",
  { ...TASK_ERROR_FIELDS, taskName: Schema.String, cleanupMilliseconds: Schema.Finite },
) {
  public override get message() {
    const cleanupLimit = Duration.format(Duration.millis(this.cleanupMilliseconds));
    return `its cleanup ran past its ${cleanupLimit} limit and then stopped its worker from answering, so task terminated that worker`;
  }
}

export class UnawaitedTasksFailed extends Schema.TaggedError<UnawaitedTasksFailed>()(
  "UnawaitedTasksFailed",
  {
    ...TASK_ERROR_FIELDS,
    ...UNATTRIBUTED_FAILURE_FIELDS,
    failedTaskNames: Schema.NonEmptyArray(Schema.String),
  },
) {
  public override get message() {
    return `no task awaited the failure of ${this.failedTaskNames.join(", ")}`;
  }
}

export class TraceFileIncomplete extends Schema.TaggedError<TraceFileIncomplete>()(
  "TraceFileIncomplete",
  { ...TASK_ERROR_FIELDS, traceFilePath: Schema.String, reason: Schema.String },
) {
  public get location() {
    return this.traceFilePath;
  }
  public override get message() {
    return `the trace file is incomplete because ${this.reason}`;
  }
}

export class FailureReasonNotFormatted extends Schema.TaggedError<FailureReasonNotFormatted>()(
  "FailureReasonNotFormatted",
  { ...TASK_ERROR_FIELDS, cause: Schema.Defect() },
) {
  public override get message() {
    return "the reason the run failed could not be formatted";
  }
}
