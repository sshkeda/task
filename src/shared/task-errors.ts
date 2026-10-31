import { Duration, Effect, Option, Runtime, Schema } from "effect";

import {
  AwaitedTaskFailed,
  REPORTED_FAILURE_FIELDS,
  SettledTasksFailed,
  TaskNotMadeAtTopLevel,
  TaskWaitsOnItself,
} from "../lib/errors/index.ts";
import { attributeFailures, joinMessages } from "./attribute-reported-failures.ts";
import { TASK_ERROR_FIELDS, UNATTRIBUTED_FAILURE_FIELDS } from "./error-fields.ts";
import {
  findConstructingLocation,
  findLocationInFile,
  findUserLocationInStack,
} from "./find-user-locations-in-stack-traces.ts";
import { relativeToWorkingDirectory } from "./format-error-lines.ts";
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

export class RunCalledTwice extends Schema.TaggedError<RunCalledTwice>()("RunCalledTwice", {
  ...TASK_ERROR_FIELDS,
  ...UNATTRIBUTED_FAILURE_FIELDS,
  runningRootTaskName: Schema.String,
  runningRootTaskLocation: Schema.UndefinedOr(Schema.String),
  laterRootTaskName: Schema.String,
  location: Schema.UndefinedOr(Schema.String).pipe(
    Schema.withConstructorDefault(Effect.sync(findConstructingLocation)),
  ),
}) {
  public override get message() {
    const startedAt =
      this.runningRootTaskLocation === undefined
        ? ""
        : `, started at ${relativeToWorkingDirectory(this.runningRootTaskLocation)}`;
    return `Task.main() was called again with "${this.laterRootTaskName}" while it runs "${this.runningRootTaskName}"${startedAt}`;
  }
}

export class RunCalledDuringImport extends Schema.TaggedError<RunCalledDuringImport>()(
  "RunCalledDuringImport",
  {
    ...TASK_ERROR_FIELDS,
    ...UNATTRIBUTED_FAILURE_FIELDS,
    location: Schema.UndefinedOr(Schema.String).pipe(
      Schema.withConstructorDefault(Effect.sync(findConstructingLocation)),
    ),
  },
) {
  public override get message() {
    return "Task.main() ran while Task imported this file";
  }
}

export class RunCalledInTaskBody extends Schema.TaggedError<RunCalledInTaskBody>()(
  "RunCalledInTaskBody",
  {
    ...TASK_ERROR_FIELDS,
    rootTaskName: Schema.String,
    location: Schema.UndefinedOr(Schema.String).pipe(
      Schema.withConstructorDefault(Effect.sync(findConstructingLocation)),
    ),
  },
) {
  public override get message() {
    return `Task.main() was called with "${this.rootTaskName}" inside a task body`;
  }
}

export const TaskRequestRefusal = Schema.Union([TaskWaitsOnItself, TaskNotMadeAtTopLevel]);

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

export class RootTaskFileNotChecked extends Schema.TaggedError<RootTaskFileNotChecked>()(
  "RootTaskFileNotChecked",
  {
    ...TASK_ERROR_FIELDS,
    ...UNATTRIBUTED_FAILURE_FIELDS,
    filePath: Schema.String,
    cause: Schema.Defect(),
  },
) {
  public get location() {
    return this.filePath;
  }
  public override get message() {
    return `could not check whether the root task file exists because ${describeCauseMessage(this.cause)}`;
  }
}

export class RunCalledInStandaloneExecutable extends Schema.TaggedError<RunCalledInStandaloneExecutable>()(
  "RunCalledInStandaloneExecutable",
  { ...TASK_ERROR_FIELDS, ...UNATTRIBUTED_FAILURE_FIELDS, rootTaskName: Schema.String },
) {
  public override get message() {
    return `Task.main() was called with "${this.rootTaskName}" inside a bun build --compile binary, which holds only its bundle, not the task source files ${TASK_PACKAGE.name} scans and re-imports in worker threads`;
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
