import { Duration, Effect, Option, Runtime, Schema } from "effect";

import {
  AwaitedTaskFailed,
  REPORTED_FAILURE_FIELDS,
  SettledTasksFailed,
  TaskMadeByAnotherCopy,
  TaskNotMadeWhileLoading,
  TaskWaitsOnItself,
} from "../lib/errors/index.ts";
import { attributeFailures, joinMessages } from "./attribute-reported-failures.ts";
import { TASK_ERROR_FIELDS, UNATTRIBUTED_FAILURE_FIELDS } from "./error-fields.ts";
import {
  findConstructingLocation,
  findLocationInFile,
  findUserLocationInStack,
} from "./find-user-locations-in-stack-traces.ts";
import { ErrorStack, describeCauseMessage } from "./report-errors.ts";
import { TASK_PACKAGE } from "./task-package.ts";

export class TaskModuleImportFailed extends Schema.TaggedError<TaskModuleImportFailed>()(
  "TaskModuleImportFailed",
  { ...TASK_ERROR_FIELDS, filePath: Schema.String, cause: Schema.Defect() },
) {
  public get location() {
    return Option.match(Schema.decodeUnknownOption(ErrorStack)(this.cause), {
      onSome: (withStack) =>
        findLocationInFile(withStack.stack, this.filePath) ??
        findUserLocationInStack(withStack.stack) ??
        this.filePath,
      onNone: () => this.filePath,
    });
  }
  public override get message() {
    return `could not import task module because ${describeCauseMessage(this.cause)}`;
  }
}

export class TaskTimedOut extends Schema.TaggedError<TaskTimedOut>()("TaskTimedOut", {
  ...TASK_ERROR_FIELDS,
  taskName: Schema.String,
  timeoutMilliseconds: Schema.Finite,
}) {
  public override get message() {
    return `timed out after ${Duration.format(Duration.millis(this.timeoutMilliseconds))}`;
  }
}

export class TaskSetupTimedOut extends Schema.TaggedError<TaskSetupTimedOut>()(
  "TaskSetupTimedOut",
  {
    ...TASK_ERROR_FIELDS,
    taskName: Schema.String,
    limitMilliseconds: Schema.Finite,
    unfinishedStep: Schema.String,
  },
) {
  public override get message() {
    return `setup timed out after ${Duration.format(Duration.millis(this.limitMilliseconds))} while ${this.unfinishedStep}`;
  }
}

export class StandardInputReadOutsideRootTask extends Schema.TaggedError<StandardInputReadOutsideRootTask>()(
  "StandardInputReadOutsideRootTask",
  { ...TASK_ERROR_FIELDS, taskName: Schema.String, rootTaskName: Schema.String },
) {
  public override get message() {
    return `read standard input, which only the root task "${this.rootTaskName}" can read`;
  }
}

export class ConfigKeyNotSet extends Schema.TaggedError<ConfigKeyNotSet>()("ConfigKeyNotSet", {
  ...TASK_ERROR_FIELDS,
  configKey: Schema.String,
  environmentConfigVariable: Schema.NullOr(Schema.String),
}) {
  public override get message() {
    return `configuration key ${this.configKey} is not set${this.environmentConfigVariable === null ? "" : `: the environment has no ${this.environmentConfigVariable}`}`;
  }
}

export class TaskBodyFailed extends Schema.TaggedError<TaskBodyFailed>()("TaskBodyFailed", {
  ...REPORTED_FAILURE_FIELDS,
  ...TASK_ERROR_FIELDS,
  failedCommandExitCode: Schema.optional(Schema.Int),
}) {
  public get reportedErrors() {
    return attributeFailures(this, (failureMessage) => `its body failed with ${failureMessage}`);
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
  }
}

export class ServedTaskFailed extends Schema.TaggedError<ServedTaskFailed>()("ServedTaskFailed", {
  ...REPORTED_FAILURE_FIELDS,
  ...TASK_ERROR_FIELDS,
}) {
  public get reportedErrors() {
    return attributeFailures(
      this,
      (failureMessage) => `its body failed after serving with ${failureMessage}`,
    );
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
  }
}

export class Exited extends Schema.TaggedError<Exited>()("Exited", { code: Schema.Int }) {
  public override get [Runtime.errorExitCode]() {
    return this.code;
  }

  public override get [Runtime.errorReported]() {
    return false;
  }
}

export class DefaultExportNotRunnable extends Schema.TaggedError<DefaultExportNotRunnable>()(
  "DefaultExportNotRunnable",
  {
    ...TASK_ERROR_FIELDS,
    ...UNATTRIBUTED_FAILURE_FIELDS,
    location: Schema.String,
    defaultExport: Schema.Literals(["missing", "not a task", "unreadable list"]),
    tasksInFile: Schema.NullOr(Schema.Literals(["makes", "exports"])),
  },
) {
  public override get message() {
    if (this.defaultExport === "unreadable list") {
      return "its default export is a Task.all() or Task.allSettled() list whose options can't be read";
    }
    return this.defaultExport === "not a task"
      ? `${this.tasksInFile} tasks, but its default export is neither a task nor a Task.all() or Task.allSettled() list`
      : `${this.tasksInFile} tasks, but has no default export to run`;
  }
}

export class RunArgumentsNotAccepted extends Schema.TaggedError<RunArgumentsNotAccepted>()(
  "RunArgumentsNotAccepted",
  {
    ...TASK_ERROR_FIELDS,
    ...UNATTRIBUTED_FAILURE_FIELDS,
    givenArguments: Schema.NonEmptyArray(Schema.String),
  },
) {
  public override get message() {
    const isOneArgument = this.givenArguments.length === 1;
    return `the ${isOneArgument ? "argument" : "arguments"} ${this.givenArguments.map((givenArgument) => `"${givenArgument}"`).join(" ")} ${isOneArgument ? "is" : "are"} not the name of one task to start`;
  }
}

export class StartTaskNotMade extends Schema.TaggedError<StartTaskNotMade>()("StartTaskNotMade", {
  ...TASK_ERROR_FIELDS,
  ...UNATTRIBUTED_FAILURE_FIELDS,
  startTaskName: Schema.String,
}) {
  public override get message() {
    return `the argument "${this.startTaskName}" is not the name of a task made while the task files loaded`;
  }
}

export class TaskNameNotString extends Schema.TaggedError<TaskNameNotString>()(
  "TaskNameNotString",
  { ...TASK_ERROR_FIELDS, location: Schema.String, nameText: Schema.String },
) {
  public override get message() {
    return `calls Task.make() with the name ${this.nameText}, which is not a string`;
  }
}

export class TaskDefinitionNotGiven extends Schema.TaggedError<TaskDefinitionNotGiven>()(
  "TaskDefinitionNotGiven",
  { ...TASK_ERROR_FIELDS, given: Schema.String, location: Schema.String },
) {
  public override get message() {
    return `calls Task.make() with ${this.given}, which is not a task definition`;
  }
}

export class TaskListNotGiven extends Schema.TaggedError<TaskListNotGiven>()("TaskListNotGiven", {
  ...TASK_ERROR_FIELDS,
  calledFunction: Schema.Literals(["Task.all", "Task.allSettled"]),
  given: Schema.String,
  location: Schema.UndefinedOr(Schema.String).pipe(
    Schema.withConstructorDefault(Effect.sync(findConstructingLocation)),
  ),
}) {
  public override get message() {
    return `${this.calledFunction}() was called with ${this.given}, which is not a list or record of tasks`;
  }
}

export const TaskRequestRefusal = Schema.Union([
  TaskWaitsOnItself,
  TaskNotMadeWhileLoading,
  TaskMadeByAnotherCopy,
]);

export const TaskRequestFailure = Schema.Union([AwaitedTaskFailed, TaskRequestRefusal]);

export class TaskCleanupFailed extends Schema.TaggedError<TaskCleanupFailed>()(
  "TaskCleanupFailed",
  { ...REPORTED_FAILURE_FIELDS, ...TASK_ERROR_FIELDS },
) {
  public get reportedErrors() {
    return attributeFailures(this, (failureMessage) => `its cleanup failed with ${failureMessage}`);
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
  }
}

export const isAwaitedTaskFailed = Schema.is(AwaitedTaskFailed);

export const AwaitedTasksFailure = Schema.Union([AwaitedTaskFailed, SettledTasksFailed]);

export const isAwaitedTasksFailure = Schema.is(AwaitedTasksFailure);

export class RunCalledInStandaloneExecutable extends Schema.TaggedError<RunCalledInStandaloneExecutable>()(
  "RunCalledInStandaloneExecutable",
  { ...TASK_ERROR_FIELDS, ...UNATTRIBUTED_FAILURE_FIELDS, rootTaskName: Schema.String },
) {
  public override get message() {
    return `${TASK_PACKAGE.name} can't run "${this.rootTaskName}" inside a bun build --compile binary, which holds only its bundle, not the task source files it imports again to run tasks`;
  }
}

export class RunCalledInBunWatchMode extends Schema.TaggedError<RunCalledInBunWatchMode>()(
  "RunCalledInBunWatchMode",
  { ...TASK_ERROR_FIELDS, ...UNATTRIBUTED_FAILURE_FIELDS, flag: Schema.String },
) {
  public override get message() {
    return `${TASK_PACKAGE.name} doesn't run under bun ${this.flag}`;
  }
}

export class RunCalledInBunEvaluation extends Schema.TaggedError<RunCalledInBunEvaluation>()(
  "RunCalledInBunEvaluation",
  { ...TASK_ERROR_FIELDS, ...UNATTRIBUTED_FAILURE_FIELDS, flag: Schema.String },
) {
  public override get message() {
    return `${TASK_PACKAGE.name} doesn't run under bun ${this.flag}`;
  }
}
