import { Effect, Schema } from "effect";

import { TASK_ERROR_FIELDS, UNATTRIBUTED_FAILURE_FIELDS } from "../../../shared/error-fields.ts";
import { findConstructingLocation } from "../../../shared/find-user-locations-in-stack-traces.ts";
import { CauseWithoutFailure, describeCauseMessage } from "../../../shared/report-errors.ts";

const isCauseWithoutFailure = Schema.is(CauseWithoutFailure);

export class PrintableError extends Schema.Opaque<PrintableError>()(
  Schema.Struct({
    tag: Schema.String,
    location: Schema.optional(Schema.String),
    message: Schema.String,
  }),
) {}

export class ReportedErrors extends Schema.TaggedError<ReportedErrors>()("ReportedErrors", {
  ...TASK_ERROR_FIELDS,
  reportedErrors: Schema.NonEmptyArray(PrintableError),
}) {
  public override get message() {
    return this.reportedErrors.map((printableError) => printableError.message).join("\n");
  }
}

export const REPORTED_FAILURE_FIELDS = {
  taskName: Schema.String,
  failures: Schema.NonEmptyArray(PrintableError),
  details: Schema.optional(Schema.String),
};

export class TaskResultNotJson extends Schema.TaggedError<TaskResultNotJson>()(
  "TaskResultNotJson",
  {
    ...TASK_ERROR_FIELDS,
    taskName: Schema.String,
    handedOver: Schema.Literals(["returned", "served"]),
    valuePath: Schema.String,
    valueDescription: Schema.String,
  },
) {
  public override get message() {
    return `${this.handedOver} ${this.valueDescription}${this.valuePath === "" ? "" : ` at ${this.valuePath}`}, which is not JSON`;
  }
}

export class TaskError extends Schema.TaggedError<TaskError>()("TaskError", {
  ...TASK_ERROR_FIELDS,
  message: Schema.String,
  location: Schema.UndefinedOr(Schema.String).pipe(
    Schema.withConstructorDefault(Effect.sync(findConstructingLocation)),
  ),
}) {}

export class TaskNotMadeAtTopLevel extends Schema.TaggedError<TaskNotMadeAtTopLevel>()(
  "TaskNotMadeAtTopLevel",
  { ...TASK_ERROR_FIELDS, taskName: Schema.String, requestedTaskName: Schema.String },
) {
  public override get message() {
    return `the module scan found no top-level Task.make("${this.requestedTaskName}") call`;
  }
}

export class TaskWaitsOnItself extends Schema.TaggedError<TaskWaitsOnItself>()(
  "TaskWaitsOnItself",
  { ...TASK_ERROR_FIELDS, taskName: Schema.String, cycle: Schema.Array(Schema.String) },
) {
  public override get message() {
    return `waits on itself through ${this.cycle.join(" -> ")}`;
  }
}

export class AwaitedTaskFailed extends Schema.TaggedError<AwaitedTaskFailed>()(
  "AwaitedTaskFailed",
  { ...TASK_ERROR_FIELDS, ...UNATTRIBUTED_FAILURE_FIELDS, awaitedTaskName: Schema.String },
) {
  public override get message() {
    return `${this.awaitedTaskName} failed`;
  }
}

export class SettledTasksFailed extends Schema.TaggedError<SettledTasksFailed>()(
  "SettledTasksFailed",
  {
    ...TASK_ERROR_FIELDS,
    ...UNATTRIBUTED_FAILURE_FIELDS,
    settledTaskCount: Schema.Int,
    failedTaskNames: Schema.Array(Schema.String),
  },
) {
  public override get message() {
    return `${this.failedTaskNames.length} of ${this.settledTaskCount} ${this.settledTaskCount === 1 ? "task" : "tasks"} failed: ${this.failedTaskNames.join(", ")}`;
  }
}

export class CommandFailed extends Schema.TaggedError<CommandFailed>()("CommandFailed", {
  ...TASK_ERROR_FIELDS,
  command: Schema.String,
  code: Schema.Int,
  standardOutput: Schema.String,
  standardError: Schema.String,
}) {
  public override get message() {
    return [
      `${this.command} exited with code ${this.code}`,
      this.standardOutput.trim(),
      this.standardError.trim(),
    ]
      .filter((line) => line !== "")
      .join("\n");
  }
}

export class CapturedCommandFailed extends Schema.TaggedError<CapturedCommandFailed>()(
  "CapturedCommandFailed",
  {
    ...TASK_ERROR_FIELDS,
    command: Schema.String,
    code: Schema.Int,
    standardOutput: Schema.Redacted(Schema.String),
    standardError: Schema.Redacted(Schema.String),
  },
) {
  public override get message() {
    return `${this.command} exited with code ${this.code}, and its captured output is not shown`;
  }
}

export class CommandNotStarted extends Schema.TaggedError<CommandNotStarted>()(
  "CommandNotStarted",
  { ...TASK_ERROR_FIELDS, command: Schema.String, cause: Schema.Defect() },
) {
  public override get message() {
    return `could not start ${this.command}: ${describeCauseMessage(this.cause)}`;
  }
}

export class CommandStandardInputFailed extends Schema.TaggedError<CommandStandardInputFailed>()(
  "CommandStandardInputFailed",
  { ...TASK_ERROR_FIELDS, command: Schema.String, cause: Schema.Defect() },
) {
  public override get message() {
    return isCauseWithoutFailure(this.cause)
      ? `the standard input stream of ${this.command} ${this.cause.message}`
      : `the standard input stream of ${this.command} failed with ${describeCauseMessage(this.cause)}`;
  }
}
