import { Schema } from "effect";

import { TASK_ERROR_FIELDS } from "../../../shared/error-fields.ts";
import { joinMessages } from "../../../shared/join-messages.ts";
import { nameTask } from "../../../shared/label-lines.ts";
import { reportWrappedCause } from "../../../shared/report-cause.ts";

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
    return `could not kill ${this.processId} (${this.command}), started by ${this.ownerTaskName === null ? "no known task" : nameTask(this.ownerTaskName)}, because the signal returned EPERM`;
  }
}

export class ProcessOwnershipUnknown extends Schema.TaggedError<ProcessOwnershipUnknown>()(
  "ProcessOwnershipUnknown",
  {
    ...TASK_ERROR_FIELDS,
    processId: Schema.Int,
    startTime: Schema.String,
    command: Schema.String,
    cause: Schema.Defect(),
  },
) {
  public get reportedErrors() {
    return reportWrappedCause(
      this,
      `could not tell whether ${this.processId} (${this.command}) is the run's, so it was left running`,
    );
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
  }
}
