import { Schema } from "effect";

import { TASK_ERROR_FIELDS } from "./error-fields.ts";

export class CommandPipelineRefused extends Schema.TaggedError<CommandPipelineRefused>()(
  "CommandPipelineRefused",
  { ...TASK_ERROR_FIELDS, programNames: Schema.String },
) {
  public override get message() {
    return `task's process service refused the pipeline ${this.programNames}`;
  }
}

export class CommandOptionsRefused extends Schema.TaggedError<CommandOptionsRefused>()(
  "CommandOptionsRefused",
  {
    ...TASK_ERROR_FIELDS,
    programName: Schema.String,
    optionNames: Schema.NonEmptyArray(Schema.String),
  },
) {
  public override get message() {
    return `task's process service refused ${this.optionNames.join(", ")} for ${this.programName}`;
  }
}

export class RedactedValueWithNulRefused extends Schema.TaggedError<RedactedValueWithNulRefused>()(
  "RedactedValueWithNulRefused",
  {
    ...TASK_ERROR_FIELDS,
    programName: Schema.String,
    valueNames: Schema.NonEmptyArray(Schema.String),
  },
) {
  public override get message() {
    return `task's process service refused ${this.valueNames.join(", ")} for ${this.programName}, a Redacted value holding a NUL character`;
  }
}

export const CommandRefusal = Schema.Union([
  CommandPipelineRefused,
  CommandOptionsRefused,
  RedactedValueWithNulRefused,
]);
