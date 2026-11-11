import { Schema } from "effect";

import { TASK_ERROR_FIELDS } from "./error-fields.ts";

export class StateDirectoryPathBlank extends Schema.TaggedError<StateDirectoryPathBlank>()(
  "StateDirectoryPathBlank",
  { ...TASK_ERROR_FIELDS, variableName: Schema.String, value: Schema.String },
) {
  public override get message() {
    return `${this.variableName} is "${this.value}", which is blank`;
  }
}

export class LogLevelInvalid extends Schema.TaggedError<LogLevelInvalid>()("LogLevelInvalid", {
  ...TASK_ERROR_FIELDS,
  variableName: Schema.String,
  value: Schema.String,
  logLevelNames: Schema.Array(Schema.String),
}) {
  public override get message() {
    return `${this.variableName} is "${this.value}", not ${this.logLevelNames.join(" or ")}`;
  }
}

export class HomeDirectoryPathMissing extends Schema.TaggedError<HomeDirectoryPathMissing>()(
  "HomeDirectoryPathMissing",
  {
    ...TASK_ERROR_FIELDS,
    stateDirectoryVariableName: Schema.String,
    homeValue: Schema.NullOr(Schema.String),
  },
) {
  public override get message() {
    return this.homeValue === null
      ? `${this.stateDirectoryVariableName} and HOME are both unset`
      : `${this.stateDirectoryVariableName} is unset and HOME is "${this.homeValue}", which is blank`;
  }
}

export class WorkerCountInvalid extends Schema.TaggedError<WorkerCountInvalid>()(
  "WorkerCountInvalid",
  { ...TASK_ERROR_FIELDS, variableName: Schema.String, value: Schema.String },
) {
  public override get message() {
    return `${this.variableName} is "${this.value}", not a whole decimal number above 0`;
  }
}

export class SetupLimitInvalid extends Schema.TaggedError<SetupLimitInvalid>()(
  "SetupLimitInvalid",
  { ...TASK_ERROR_FIELDS, variableName: Schema.String, value: Schema.String },
) {
  public override get message() {
    return `${this.variableName} is "${this.value}", not a duration above 0 such as "90 seconds"`;
  }
}

export class WorkerCountAboveMaximum extends Schema.TaggedError<WorkerCountAboveMaximum>()(
  "WorkerCountAboveMaximum",
  {
    ...TASK_ERROR_FIELDS,
    variableName: Schema.String,
    value: Schema.String,
    maximum: Schema.Int,
  },
) {
  public override get message() {
    return `${this.variableName} is "${this.value}", above the maximum of ${this.maximum} worker threads`;
  }
}

export class BunVersionUnsupported extends Schema.TaggedError<BunVersionUnsupported>()(
  "BunVersionUnsupported",
  {
    ...TASK_ERROR_FIELDS,
    bunVersion: Schema.String,
    supportedRange: Schema.String,
    taskPackageName: Schema.String,
    taskVersion: Schema.String,
  },
) {
  public override get message() {
    return `${this.taskPackageName} ${this.taskVersion} needs Bun ${this.supportedRange}, but Bun ${this.bunVersion} is running`;
  }
}
