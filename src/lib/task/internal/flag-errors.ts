import { Schema } from "effect";

import { TASK_ERROR_FIELDS } from "../../../shared/error-fields.ts";

export class FlagNotRead extends Schema.TaggedError<FlagNotRead>()("FlagNotRead", {
  ...TASK_ERROR_FIELDS,
  flagName: Schema.String,
}) {
  public override get message() {
    return `the flag --${this.flagName} is not read by any task`;
  }
}

export class FlagGivenTwice extends Schema.TaggedError<FlagGivenTwice>()("FlagGivenTwice", {
  ...TASK_ERROR_FIELDS,
  flagName: Schema.String,
}) {
  public override get message() {
    return `the flag --${this.flagName} is given more than once`;
  }
}

export class FlagValueMissing extends Schema.TaggedError<FlagValueMissing>()("FlagValueMissing", {
  ...TASK_ERROR_FIELDS,
  flagName: Schema.String,
}) {
  public override get message() {
    return `the flag --${this.flagName} is given without a value`;
  }
}

export class FlagMissing extends Schema.TaggedError<FlagMissing>()("FlagMissing", {
  ...TASK_ERROR_FIELDS,
  readingTaskName: Schema.String,
  flagName: Schema.String,
}) {
  public override get message() {
    return `the task "${this.readingTaskName}" reads the flag --${this.flagName}, which was not given`;
  }
}

export class FlagInvalid extends Schema.TaggedError<FlagInvalid>()("FlagInvalid", {
  ...TASK_ERROR_FIELDS,
  readingTaskName: Schema.String,
  flagName: Schema.String,
  value: Schema.String,
  reason: Schema.String,
}) {
  public override get message() {
    return `the flag --${this.flagName} is "${this.value}", which the task "${this.readingTaskName}" does not accept: ${this.reason}`;
  }
}

export class FlagReadTwoWays extends Schema.TaggedError<FlagReadTwoWays>()("FlagReadTwoWays", {
  ...TASK_ERROR_FIELDS,
  switchTaskName: Schema.String,
  valueTaskName: Schema.String,
  flagName: Schema.String,
}) {
  public override get message() {
    return `the task "${this.switchTaskName}" reads the flag --${this.flagName} as a switch without a value, but the task "${this.valueTaskName}" reads it with a value`;
  }
}

export class FlagNotFlat extends Schema.TaggedError<FlagNotFlat>()("FlagNotFlat", {
  ...TASK_ERROR_FIELDS,
  readingTaskName: Schema.String,
  flagName: Schema.String,
}) {
  public override get message() {
    return `the task "${this.readingTaskName}" reads the flag --${this.flagName} as a value a command line cannot carry`;
  }
}

export class FlagReserved extends Schema.TaggedError<FlagReserved>()("FlagReserved", {
  ...TASK_ERROR_FIELDS,
  readingTaskName: Schema.String,
  flagName: Schema.String,
}) {
  public override get message() {
    return `the task "${this.readingTaskName}" reads the flag --${this.flagName}, which is kept for printing what the file accepts`;
  }
}
