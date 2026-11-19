import { Array as Arrays, Duration, Schema } from "effect";

import { TASK_ERROR_FIELDS, UNATTRIBUTED_FAILURE_FIELDS } from "../../../shared/error-fields.ts";
import { joinMessages } from "../../../shared/join-messages.ts";
import { nameTask } from "../../../shared/label-lines.ts";
import { reportTaskText, reportWrappedCause } from "../../../shared/report-cause.ts";
import { reportError } from "../../../shared/report-errors.ts";
import { TASK_OPTION_NAMES } from "../../../shared/resolve-task-options.ts";
import { exitCodeAfterStopSignal, STOP_REASONS } from "../../../shared/stop-signals.ts";
import { TaskDefinitionNotGiven, TaskNameNotString } from "../../../shared/task-errors.ts";

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
  public get reportedErrors() {
    return reportWrappedCause(this, "could not resolve the root task file");
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
  }
}

export class RootTaskWorkingDirectoryUnavailable extends Schema.TaggedError<RootTaskWorkingDirectoryUnavailable>()(
  "RootTaskWorkingDirectoryUnavailable",
  {
    ...TASK_ERROR_FIELDS,
    ...UNATTRIBUTED_FAILURE_FIELDS,
    packageJsonDirectoryPath: Schema.String,
    cause: Schema.Defect(),
  },
) {
  public get reportedErrors() {
    return reportWrappedCause(
      this,
      `could not enter root task directory "${this.packageJsonDirectoryPath}"`,
    );
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
  }
}

export class TaskNameEmpty extends Schema.TaggedError<TaskNameEmpty>()("TaskNameEmpty", {
  ...TASK_ERROR_FIELDS,
  location: Schema.String,
}) {
  public override get message() {
    return "calls Task.make() with an empty name";
  }
}

export class TaskNameBlank extends Schema.TaggedError<TaskNameBlank>()("TaskNameBlank", {
  ...TASK_ERROR_FIELDS,
  location: Schema.String,
}) {
  public override get message() {
    return "calls Task.make() with a name of only whitespace";
  }
}

export class TaskMadeMoreThanOnce extends Schema.TaggedError<TaskMadeMoreThanOnce>()(
  "TaskMadeMoreThanOnce",
  { ...TASK_ERROR_FIELDS, taskName: Schema.String, location: Schema.String, madeCount: Schema.Int },
) {
  public override get message() {
    return `makes the task ${nameTask(this.taskName)}, which ${this.madeCount} Task.make() calls make`;
  }
}

export class TaskMadeAtUnknownLocation extends Schema.TaggedError<TaskMadeAtUnknownLocation>()(
  "TaskMadeAtUnknownLocation",
  { ...TASK_ERROR_FIELDS, nameText: Schema.String },
) {
  public override get message() {
    return `makes the task ${this.nameText}, but no source file is named for its Task.make() call in the call stack task can read, so task cannot tell which package the task belongs to`;
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

export class TaskIsolatedInvalid extends Schema.TaggedError<TaskIsolatedInvalid>()(
  "TaskIsolatedInvalid",
  {
    ...TASK_ERROR_FIELDS,
    taskName: Schema.String,
    isolated: Schema.String,
  },
) {
  public override get message() {
    return `sets isolated to ${this.isolated}, which is not true or false`;
  }
}

export class TaskIsolatedInBody extends Schema.TaggedError<TaskIsolatedInBody>()(
  "TaskIsolatedInBody",
  { ...TASK_ERROR_FIELDS, taskName: Schema.String, madeInTaskName: Schema.String },
) {
  public override get message() {
    return `is made inside the body of ${nameTask(this.madeInTaskName)}, so it cannot run on a worker of its own`;
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
    return `task ${nameTask(this.taskName)} has the ${this.optionNames.length === 1 ? "option" : "options"} ${this.optionNames.join(", ")} outside the task options (${TASK_OPTION_NAMES.join(", ")})`;
  }
}

export class RootTaskNotMadeWhileLoading extends Schema.TaggedError<RootTaskNotMadeWhileLoading>()(
  "RootTaskNotMadeWhileLoading",
  { ...TASK_ERROR_FIELDS, rootTaskFilePath: Schema.String },
) {
  public override get message() {
    return `${this.rootTaskFilePath} runs a task that no Task.make() call made while ${this.rootTaskFilePath} loaded`;
  }
}

export const TaskDefinitionViolation = Schema.Union([
  TaskDefinitionNotGiven,
  TaskNameNotString,
  TaskNameEmpty,
  TaskNameBlank,
  TaskMadeAtUnknownLocation,
  TaskMadeMoreThanOnce,
  TaskTimeoutInvalid,
  TaskCleanupInvalid,
  TaskIsolatedInvalid,
  TaskIsolatedInBody,
  TaskOptionsUnknown,
  RootTaskNotMadeWhileLoading,
]);

export class BrokenTasksFound extends Schema.TaggedError<BrokenTasksFound>()("BrokenTasksFound", {
  ...TASK_ERROR_FIELDS,
  ...UNATTRIBUTED_FAILURE_FIELDS,
  violations: Schema.NonEmptyArray(TaskDefinitionViolation),
}) {
  public get reportedErrors() {
    return Arrays.map(this.violations, reportError);
  }
  public override get message() {
    return `found ${this.violations.length} task definition ${this.violations.length === 1 ? "violation" : "violations"}`;
  }
}

export class WorkerOutputNotRead extends Schema.TaggedError<WorkerOutputNotRead>()(
  "WorkerOutputNotRead",
  {
    ...TASK_ERROR_FIELDS,
    ...UNATTRIBUTED_FAILURE_FIELDS,
    workerId: Schema.Int,
    cause: Schema.Defect(),
  },
) {
  public get reportedErrors() {
    return reportWrappedCause(
      this,
      `could not read the output of worker ${this.workerId}, so the rest of it was lost`,
    );
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
  }
}

export class WorkerTerminationFailed extends Schema.TaggedError<WorkerTerminationFailed>()(
  "WorkerTerminationFailed",
  {
    ...TASK_ERROR_FIELDS,
    ...UNATTRIBUTED_FAILURE_FIELDS,
    workerId: Schema.Int,
    reason: Schema.String,
  },
) {
  public override get message() {
    return `could not terminate worker ${this.workerId}: ${this.reason}`;
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
    return `requests itself while serving through ${this.cycle.map(nameTask).join(" -> ")}, so task cannot stop each of them after the tasks that requested it`;
  }
}

export class TaskCleanupTimedOut extends Schema.TaggedError<TaskCleanupTimedOut>()(
  "TaskCleanupTimedOut",
  { ...TASK_ERROR_FIELDS, taskName: Schema.String, cleanupMilliseconds: Schema.Finite },
) {
  public override get message() {
    return `its cleanup did not finish within ${Duration.format(Duration.millis(this.cleanupMilliseconds))}, so the run stopped waiting for it`;
  }
}

export class TaskCleanupCutOff extends Schema.TaggedError<TaskCleanupCutOff>()(
  "TaskCleanupCutOff",
  { ...TASK_ERROR_FIELDS, taskName: Schema.String },
) {
  public override get message() {
    return "its cleanup was cut off because the stop was forced";
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

export class WorkerTerminatedForAnotherTask extends Schema.TaggedError<WorkerTerminatedForAnotherTask>()(
  "WorkerTerminatedForAnotherTask",
  { ...TASK_ERROR_FIELDS, taskName: Schema.String, terminatingTaskName: Schema.String },
) {
  public override get message() {
    return `its worker was terminated because ${nameTask(this.terminatingTaskName)} ran past its cleanup limit on it`;
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
    return `no task awaited the failure of ${this.failedTaskNames.map(nameTask).join(", ")}`;
  }
}

export class TasksNeverRan extends Schema.TaggedError<TasksNeverRan>()("TasksNeverRan", {
  ...TASK_ERROR_FIELDS,
  ...UNATTRIBUTED_FAILURE_FIELDS,
  taskNames: Schema.NonEmptyArray(Schema.String),
  filePath: Schema.String,
}) {
  public override get message() {
    const isOneTask = this.taskNames.length === 1;
    return `makes ${this.taskNames.map(nameTask).join(", ")} but nothing ran ${isOneTask ? "it" : "them"}`;
  }
}

export class NoTaskStarted extends Schema.TaggedError<NoTaskStarted>()("NoTaskStarted", {
  ...TASK_ERROR_FIELDS,
  ...UNATTRIBUTED_FAILURE_FIELDS,
  madeTaskNames: Schema.NonEmptyArray(Schema.String),
  filePath: Schema.String,
}) {
  public override get message() {
    return `makes ${this.madeTaskNames.map(nameTask).join(", ")} but the run started no task`;
  }
}

const INCOMPLETE_FILES_FIELDS = {
  failedStep: Schema.String,
  cause: Schema.NullOr(Schema.Defect()),
};

const reportIncompleteFiles = (
  incomplete: Parameters<typeof reportWrappedCause>[0] & { readonly failedStep: string },
  whatIsIncomplete: string,
) =>
  incomplete.cause === null
    ? reportTaskText(incomplete, incomplete.failedStep, whatIsIncomplete)
    : reportWrappedCause(incomplete, `${incomplete.failedStep}, so ${whatIsIncomplete}`);

export class TraceFileIncomplete extends Schema.TaggedError<TraceFileIncomplete>()(
  "TraceFileIncomplete",
  { ...TASK_ERROR_FIELDS, traceFilePath: Schema.String, ...INCOMPLETE_FILES_FIELDS },
) {
  public get location() {
    return this.traceFilePath;
  }
  public get reportedErrors() {
    return reportIncompleteFiles(this, "the trace file is incomplete");
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
  }
}

export class TaskOutputFilesIncomplete extends Schema.TaggedError<TaskOutputFilesIncomplete>()(
  "TaskOutputFilesIncomplete",
  { ...TASK_ERROR_FIELDS, runDirectoryPath: Schema.String, ...INCOMPLETE_FILES_FIELDS },
) {
  public get location() {
    return this.runDirectoryPath;
  }
  public get reportedErrors() {
    return reportIncompleteFiles(this, "the task output files are incomplete");
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
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
