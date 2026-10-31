import { Schema } from "effect";

import { TASK_ERROR_FIELDS, UNATTRIBUTED_FAILURE_FIELDS } from "./error-fields.ts";
import { describeCauseMessage } from "./report-errors.ts";

export class ProcDirectoryUnlistable extends Schema.TaggedError<ProcDirectoryUnlistable>()(
  "ProcDirectoryUnlistable",
  {
    ...TASK_ERROR_FIELDS,
    taskName: Schema.NullOr(Schema.String),
    directoryPath: Schema.String,
    cause: Schema.Defect(),
  },
) {
  public override get message() {
    return `could not list ${this.directoryPath} to find leftover processes because ${describeCauseMessage(this.cause)}`;
  }
}

export class ProcStatUnreadable extends Schema.TaggedError<ProcStatUnreadable>()(
  "ProcStatUnreadable",
  {
    ...TASK_ERROR_FIELDS,
    taskName: Schema.NullOr(Schema.String),
    filePath: Schema.String,
    cause: Schema.Defect(),
  },
) {
  public override get message() {
    return `could not be read to find leftover processes because ${describeCauseMessage(this.cause)}`;
  }
}

export class ProcStatMalformed extends Schema.TaggedError<ProcStatMalformed>()(
  "ProcStatMalformed",
  {
    ...TASK_ERROR_FIELDS,
    taskName: Schema.NullOr(Schema.String),
    filePath: Schema.String,
    cause: Schema.Defect(),
  },
) {
  public override get message() {
    return `has no readable state, parent, group and start time because ${describeCauseMessage(this.cause)}`;
  }
}

export class ProcCommandLineUnreadable extends Schema.TaggedError<ProcCommandLineUnreadable>()(
  "ProcCommandLineUnreadable",
  {
    ...TASK_ERROR_FIELDS,
    taskName: Schema.NullOr(Schema.String),
    filePath: Schema.String,
    cause: Schema.Defect(),
  },
) {
  public override get message() {
    return `could not be read because ${describeCauseMessage(this.cause)}`;
  }
}

export class ProcRunMarkerUnreadable extends Schema.TaggedError<ProcRunMarkerUnreadable>()(
  "ProcRunMarkerUnreadable",
  {
    ...TASK_ERROR_FIELDS,
    taskName: Schema.NullOr(Schema.String),
    filePath: Schema.String,
    cause: Schema.Defect(),
  },
) {
  public override get message() {
    return `could not be read because ${describeCauseMessage(this.cause)}`;
  }
}

export class ProcessNotSignaled extends Schema.TaggedError<ProcessNotSignaled>()(
  "ProcessNotSignaled",
  {
    ...TASK_ERROR_FIELDS,
    taskName: Schema.NullOr(Schema.String),
    processId: Schema.Int,
    signal: Schema.Literals(["SIGKILL", "SIGSTOP", "SIGCONT", 0]),
    cause: Schema.Defect(),
  },
) {
  public override get message() {
    return `could not send ${this.signal === 0 ? "a liveness check" : this.signal} to ${this.processId < 0 ? `process group ${-this.processId}` : `process ${this.processId}`} because ${describeCauseMessage(this.cause)}`;
  }
}

export class SystemLibraryNotLoaded extends Schema.TaggedError<SystemLibraryNotLoaded>()(
  "SystemLibraryNotLoaded",
  {
    ...TASK_ERROR_FIELDS,
    taskName: Schema.NullOr(Schema.String),
    libraryPath: Schema.String,
    cause: Schema.Defect(),
  },
) {
  public override get message() {
    return `could not load ${this.libraryPath} to list processes because ${describeCauseMessage(this.cause)}`;
  }
}

export class ProcessArgumentsMalformed extends Schema.TaggedError<ProcessArgumentsMalformed>()(
  "ProcessArgumentsMalformed",
  {
    ...TASK_ERROR_FIELDS,
    processId: Schema.Int,
    argumentCount: Schema.Int,
    argumentsFound: Schema.Int,
  },
) {
  public override get message() {
    return `kern.procargs2 for process ${this.processId} declares ${this.argumentCount} arguments but holds ${this.argumentsFound} NUL-terminated strings after its executable path`;
  }
}

export class SystemCallFailed extends Schema.TaggedError<SystemCallFailed>()("SystemCallFailed", {
  ...TASK_ERROR_FIELDS,
  call: Schema.String,
  processId: Schema.NullOr(Schema.Int),
  errorNumber: Schema.Int,
  errorName: Schema.String,
}) {
  public override get message() {
    return `${this.call}${this.processId === null ? "" : ` for process ${this.processId}`} failed with ${this.errorName} (errno ${this.errorNumber})`;
  }
}

export class ProcessCommandLineNotRead extends Schema.TaggedError<ProcessCommandLineNotRead>()(
  "ProcessCommandLineNotRead",
  {
    ...TASK_ERROR_FIELDS,
    taskName: Schema.NullOr(Schema.String),
    processId: Schema.Int,
    cause: Schema.Union([SystemCallFailed, ProcessArgumentsMalformed]),
  },
) {
  public override get message() {
    return `could not read the command line of leftover process ${this.processId} because ${this.cause.message}`;
  }
}

export class ProcessTableNotRead extends Schema.TaggedError<ProcessTableNotRead>()(
  "ProcessTableNotRead",
  {
    ...TASK_ERROR_FIELDS,
    taskName: Schema.NullOr(Schema.String),
    cause: Schema.Union([SystemCallFailed, ProcessArgumentsMalformed]),
  },
) {
  public override get message() {
    return `could not read the process table to find leftover processes because ${this.cause.message}`;
  }
}

export const UncaughtWorkerError = Schema.Struct({
  message: Schema.String,
  location: Schema.NullOr(Schema.String),
});

export class WorkerExited extends Schema.TaggedError<WorkerExited>()("WorkerExited", {
  ...TASK_ERROR_FIELDS,
  taskName: Schema.NullOr(Schema.String),
  workerId: Schema.Int,
  exitCode: Schema.Int,
  uncaughtError: Schema.NullOr(UncaughtWorkerError),
}) {
  public get location() {
    return this.uncaughtError?.location ?? undefined;
  }
  public override get message() {
    const howItStopped =
      this.uncaughtError === null
        ? `exited on its own with code ${this.exitCode}`
        : `stopped on the uncaught error ${this.uncaughtError.message}`;
    return this.taskName === null
      ? `worker ${this.workerId} ${howItStopped}`
      : `its worker ${howItStopped}`;
  }
}

export class SupervisorSocketNotOpened extends Schema.TaggedError<SupervisorSocketNotOpened>()(
  "SupervisorSocketNotOpened",
  { ...TASK_ERROR_FIELDS, ...UNATTRIBUTED_FAILURE_FIELDS, cause: Schema.Defect() },
) {
  public override get message() {
    return `could not open the socket the run reports to its supervisor on because ${describeCauseMessage(this.cause)}`;
  }
}
