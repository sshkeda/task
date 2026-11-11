import { Effect, Schema } from "effect";

import { TASK_ERROR_FIELDS, UNATTRIBUTED_FAILURE_FIELDS } from "../../../shared/error-fields.ts";
import { findConstructingLocation } from "../../../shared/find-user-locations-in-stack-traces.ts";
import { CauseWithoutFailure, describeCauseMessage } from "../../../shared/report-errors.ts";
import { TASK_PACKAGE } from "../../../shared/task-package.ts";

const isCauseWithoutFailure = Schema.is(CauseWithoutFailure);

const COMMAND_OUTPUT_LINES_SHOWN = 20;

const newlineBeforeLastLines = (text: string, lineCount: number, newlineAfter: number): number => {
  const newlineBefore = text.lastIndexOf("\n", newlineAfter - 1);
  return lineCount === 1
    ? newlineBefore
    : newlineBeforeLastLines(text, lineCount - 1, newlineBefore);
};

const lastLinesOf = (output: string, outputName: string) => {
  const trimmed = output.trim();
  const newlineBeforeShownLines = newlineBeforeLastLines(
    trimmed,
    COMMAND_OUTPUT_LINES_SHOWN,
    trimmed.length,
  );
  if (newlineBeforeShownLines === -1) {
    return trimmed;
  }
  const leftOutLines = (trimmed.slice(0, newlineBeforeShownLines + 1).match(/\n/gu) ?? []).length;
  return [
    `(${leftOutLines} earlier ${leftOutLines === 1 ? "line" : "lines"} of ${outputName} not shown)`,
    trimmed.slice(newlineBeforeShownLines + 1),
  ].join("\n");
};

export class PrintableError extends Schema.Opaque<PrintableError>()(
  Schema.Struct({
    tag: Schema.String,
    location: Schema.optional(Schema.String),
    message: Schema.String,
    messageBeforeOutput: Schema.optional(Schema.String),
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

export class TaskOutputInvalid extends Schema.TaggedError<TaskOutputInvalid>()(
  "TaskOutputInvalid",
  {
    ...TASK_ERROR_FIELDS,
    taskName: Schema.String,
    handedOver: Schema.Literals(["returned", "served"]),
    valuePath: Schema.String,
    expectation: Schema.String,
  },
) {
  public override get message() {
    return `${this.handedOver} a value that does not match the task's output${this.valuePath === "" ? "" : ` at ${this.valuePath}`}: ${this.expectation}`;
  }
}

export class TaskError extends Schema.TaggedError<TaskError>()("TaskError", {
  ...TASK_ERROR_FIELDS,
  message: Schema.String,
  location: Schema.UndefinedOr(Schema.String).pipe(
    Schema.withConstructorDefault(Effect.sync(findConstructingLocation)),
  ),
}) {}

export class TaskNotMadeWhileLoading extends Schema.TaggedError<TaskNotMadeWhileLoading>()(
  "TaskNotMadeWhileLoading",
  { ...TASK_ERROR_FIELDS, taskName: Schema.String, requestedTaskName: Schema.String },
) {
  public override get message() {
    return `the task "${this.requestedTaskName}" was not made while the task files loaded, so no worker thread can run it`;
  }
}

export class TaskMadeByAnotherCopy extends Schema.TaggedError<TaskMadeByAnotherCopy>()(
  "TaskMadeByAnotherCopy",
  { ...TASK_ERROR_FIELDS, taskName: Schema.String, requestedTaskName: Schema.String },
) {
  public override get message() {
    return `the task "${this.requestedTaskName}" was made by another copy of ${TASK_PACKAGE.name}, so this copy can't run it`;
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

const COMMAND_END_FIELDS = {
  code: Schema.NullOr(Schema.Int),
  signal: Schema.NullOr(Schema.String).pipe(Schema.withConstructorDefault(Effect.succeed(null))),
};

const endsWithCodeOrSignal = Schema.makeFilter(
  ({ code, signal }: { readonly code: number | null; readonly signal: string | null }) => {
    const hasCodeOrSignal = (code === null) !== (signal === null);
    return hasCodeOrSignal || "has either an exit code or a signal, not both or neither";
  },
);

const describeCommandEnd = (command: string, code: number | null, signal: string | null) =>
  code === null
    ? `${command} was killed by signal ${signal}`
    : `${command} exited with code ${code}`;

export class CommandFailed extends Schema.TaggedError<CommandFailed>()(
  "CommandFailed",
  Schema.Struct({
    ...TASK_ERROR_FIELDS,
    command: Schema.String,
    ...COMMAND_END_FIELDS,
    standardOutput: Schema.String,
    standardError: Schema.String,
  }).check(endsWithCodeOrSignal),
) {
  public override get message() {
    return [
      describeCommandEnd(this.command, this.code, this.signal),
      lastLinesOf(this.standardOutput, "standard output"),
      lastLinesOf(this.standardError, "standard error"),
    ]
      .filter((line) => line !== "")
      .join("\n");
  }

  public get messageBeforeOutput() {
    return describeCommandEnd(this.command, this.code, this.signal);
  }
}

export class CapturedCommandFailed extends Schema.TaggedError<CapturedCommandFailed>()(
  "CapturedCommandFailed",
  Schema.Struct({
    ...TASK_ERROR_FIELDS,
    command: Schema.String,
    ...COMMAND_END_FIELDS,
    standardOutput: Schema.Redacted(Schema.String),
    standardError: Schema.Redacted(Schema.String),
  }).check(endsWithCodeOrSignal),
) {
  public override get message() {
    return `${describeCommandEnd(this.command, this.code, this.signal)}, and its captured output is not shown`;
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
