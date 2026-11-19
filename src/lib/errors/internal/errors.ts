import { Effect, Schema } from "effect";

import {
  PRINTABLE_ERROR_FIELDS,
  TASK_ERROR_FIELDS,
  UNATTRIBUTED_FAILURE_FIELDS,
} from "../../../shared/error-fields.ts";
import { findConstructingLocation } from "../../../shared/find-user-locations-in-stack-traces.ts";
import { joinMessages } from "../../../shared/join-messages.ts";
import { nameTask } from "../../../shared/label-lines.ts";
import { readNestedRecapLines } from "../../../shared/nested-recap-lines.ts";
import { reportWrappedCause } from "../../../shared/report-cause.ts";
import { TASK_PACKAGE } from "../../../shared/task-package.ts";
import { selectRecapOutputLines } from "./recap-output-lines.ts";

export class PrintableError extends Schema.Opaque<PrintableError>()(
  Schema.Struct(PRINTABLE_ERROR_FIELDS),
) {}

export class ReportedErrors extends Schema.TaggedError<ReportedErrors>()("ReportedErrors", {
  ...TASK_ERROR_FIELDS,
  reportedErrors: Schema.NonEmptyArray(PrintableError),
}) {
  public override get message() {
    return joinMessages(this.reportedErrors);
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
    return `the task ${nameTask(this.requestedTaskName)} was not made while the task files loaded, so no worker thread can run it`;
  }
}

export class TaskMadeByAnotherCopy extends Schema.TaggedError<TaskMadeByAnotherCopy>()(
  "TaskMadeByAnotherCopy",
  { ...TASK_ERROR_FIELDS, taskName: Schema.String, requestedTaskName: Schema.String },
) {
  public override get message() {
    return `the task ${nameTask(this.requestedTaskName)} was made by another copy of ${TASK_PACKAGE.name}, so this copy can't run it`;
  }
}

export class TaskWaitsOnItself extends Schema.TaggedError<TaskWaitsOnItself>()(
  "TaskWaitsOnItself",
  { ...TASK_ERROR_FIELDS, taskName: Schema.String, cycle: Schema.Array(Schema.String) },
) {
  public override get message() {
    return `waits on itself through ${this.cycle.map(nameTask).join(" -> ")}`;
  }
}

export class AwaitedTaskFailed extends Schema.TaggedError<AwaitedTaskFailed>()(
  "AwaitedTaskFailed",
  { ...TASK_ERROR_FIELDS, ...UNATTRIBUTED_FAILURE_FIELDS, awaitedTaskName: Schema.String },
) {
  public override get message() {
    return `${nameTask(this.awaitedTaskName)} failed`;
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
    return `${this.failedTaskNames.length} of ${this.settledTaskCount} ${this.settledTaskCount === 1 ? "task" : "tasks"} failed: ${this.failedTaskNames.map(nameTask).join(", ")}`;
  }
}

const COMMAND_CALL_FIELDS = {
  location: Schema.optionalKey(Schema.String),
};

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

const OUTPUT_LINES_IN_MESSAGE = 20;

const OUTPUT_CHARACTERS_IN_MESSAGE = 4096;

const startOfLastLines = (text: string, lineCount: number, end: number): number => {
  const lineBreak = text.lastIndexOf("\n", end - 1);
  const keepsNoEarlierLine = lineBreak === -1 || lineCount === 1;
  return keepsNoEarlierLine ? lineBreak + 1 : startOfLastLines(text, lineCount - 1, lineBreak);
};

const lastLinesOf = (text: string) =>
  text.slice(startOfLastLines(text, OUTPUT_LINES_IN_MESSAGE, text.length));

const endOfOutput = (output: string) => {
  const trimmed = output.trimEnd();
  if (trimmed.length <= OUTPUT_CHARACTERS_IN_MESSAGE) {
    return lastLinesOf(trimmed);
  }
  const cutAt = trimmed.length - OUTPUT_CHARACTERS_IN_MESSAGE;
  const cut = trimmed.slice(cutAt);
  const tail = cut.isWellFormed() ? cut : cut.slice(1);
  if (trimmed[cutAt - 1] === "\n") {
    return lastLinesOf(tail);
  }
  const firstLineBreak = tail.indexOf("\n");
  return firstLineBreak === -1 ? `…${tail}` : lastLinesOf(tail.slice(firstLineBreak + 1));
};

const describeOutputEnd = (outputName: string, output: string) => {
  const shownOutput = endOfOutput(output);
  return shownOutput === "" ? [] : [`${outputName}:`, shownOutput];
};

export class CommandFailed extends Schema.TaggedError<CommandFailed>()(
  "CommandFailed",
  Schema.Struct({
    ...TASK_ERROR_FIELDS,
    command: Schema.String,
    ...COMMAND_END_FIELDS,
    standardOutput: Schema.String,
    standardError: Schema.String,
    stdio: Schema.optionalKey(Schema.Literal("inherit")),
    ...COMMAND_CALL_FIELDS,
  }).check(endsWithCodeOrSignal),
) {
  public override get message() {
    return [
      this.printedMessage,
      ...describeOutputEnd("standard output", this.standardOutput),
      ...describeOutputEnd("standard error", this.standardError),
    ].join("\n");
  }

  public get printedMessage() {
    return describeCommandEnd(this.command, this.code, this.signal);
  }

  public get recapOutputLines() {
    const [firstLine, ...laterLines] = selectRecapOutputLines([
      this.standardOutput,
      this.standardError,
    ]);
    const nestedRecapLines = readNestedRecapLines(this);
    return firstLine === undefined
      ? [...nestedRecapLines]
      : [firstLine, ...nestedRecapLines, ...laterLines];
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
    ...COMMAND_CALL_FIELDS,
  }).check(endsWithCodeOrSignal),
) {
  public override get message() {
    return `${describeCommandEnd(this.command, this.code, this.signal)}, and its captured output is not shown`;
  }
}

export class CommandNotStarted extends Schema.TaggedError<CommandNotStarted>()(
  "CommandNotStarted",
  { ...TASK_ERROR_FIELDS, command: Schema.String, cause: Schema.Defect(), ...COMMAND_CALL_FIELDS },
) {
  public get reportedErrors() {
    return reportWrappedCause(this, `could not start ${this.command}`);
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
  }
}

export class CommandStandardInputFailed extends Schema.TaggedError<CommandStandardInputFailed>()(
  "CommandStandardInputFailed",
  { ...TASK_ERROR_FIELDS, command: Schema.String, cause: Schema.Defect(), ...COMMAND_CALL_FIELDS },
) {
  public get reportedErrors() {
    return reportWrappedCause(this, `the standard input stream of ${this.command} failed`);
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
  }
}
