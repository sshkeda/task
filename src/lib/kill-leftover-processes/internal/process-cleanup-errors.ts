import { Schema } from "effect";

import { TASK_ERROR_FIELDS } from "../../../shared/error-fields.ts";

export class ProcessNotKillable extends Schema.TaggedError<ProcessNotKillable>()(
  "ProcessNotKillable",
  {
    ...TASK_ERROR_FIELDS,
    processId: Schema.Int,
    command: Schema.String,
    ownerTaskName: Schema.NullOr(Schema.String),
  },
) {
  public override get message() {
    return `could not kill ${this.processId} (${this.command}), started by ${this.ownerTaskName === null ? "no known task" : `"${this.ownerTaskName}"`}, because the signal returned EPERM`;
  }
}
