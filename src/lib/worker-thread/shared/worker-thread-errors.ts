import { Schema } from "effect";

import { TASK_ERROR_FIELDS } from "../../../shared/error-fields.ts";
import { joinMessages } from "../../../shared/join-messages.ts";
import { nameTask } from "../../../shared/label-lines.ts";
import { reportWrappedCause } from "../../../shared/report-cause.ts";

export class ProcessGroupNotSignaledByWorker extends Schema.TaggedError<ProcessGroupNotSignaledByWorker>()(
  "ProcessGroupNotSignaledByWorker",
  {
    ...TASK_ERROR_FIELDS,
    processGroupId: Schema.Int,
    programName: Schema.String,
    cause: Schema.Defect(),
  },
) {
  public get reportedErrors() {
    return reportWrappedCause(
      this,
      `worker could not signal process group ${this.processGroupId} of ${this.programName}`,
    );
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
  }
}

export class TaskNotRegisteredInWorker extends Schema.TaggedError<TaskNotRegisteredInWorker>()(
  "TaskNotRegisteredInWorker",
  { ...TASK_ERROR_FIELDS, taskName: Schema.String, filePath: Schema.String },
) {
  public override get message() {
    return `importing this file in a worker made no task named ${nameTask(this.taskName)}`;
  }
}

export class TaskRegisteredAgainInWorker extends Schema.TaggedError<TaskRegisteredAgainInWorker>()(
  "TaskRegisteredAgainInWorker",
  {
    ...TASK_ERROR_FIELDS,
    taskName: Schema.String,
    filePath: Schema.String,
    madeCount: Schema.Int,
  },
) {
  public override get message() {
    return `importing this file in a worker made ${this.madeCount} tasks named ${nameTask(this.taskName)}`;
  }
}
