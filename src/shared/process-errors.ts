import { Array as Arrays, Schema } from "effect";

import { PrintableError } from "../lib/errors/index.ts";
import { TASK_ERROR_FIELDS, UNATTRIBUTED_FAILURE_FIELDS } from "./error-fields.ts";
import { joinMessages } from "./join-messages.ts";
import { addContext, reportTaskText, reportWrappedCause } from "./report-cause.ts";

export class ProcDirectoryUnlistable extends Schema.TaggedError<ProcDirectoryUnlistable>()(
  "ProcDirectoryUnlistable",
  {
    ...TASK_ERROR_FIELDS,
    taskName: Schema.NullOr(Schema.String),
    directoryPath: Schema.String,
    cause: Schema.Defect(),
  },
) {
  public get reportedErrors() {
    return reportWrappedCause(
      this,
      `could not list ${this.directoryPath} to find leftover processes`,
    );
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
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
  public get reportedErrors() {
    return reportWrappedCause(this, `could not read ${this.filePath} to find leftover processes`);
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
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
  public get reportedErrors() {
    return reportWrappedCause(
      this,
      `${this.filePath} has no readable state, parent, group and start time`,
    );
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
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
  public get reportedErrors() {
    return reportWrappedCause(this, `could not read ${this.filePath}`);
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
  }
}

export class ProcRunMarkerUnreadable extends Schema.TaggedError<ProcRunMarkerUnreadable>()(
  "ProcRunMarkerUnreadable",
  {
    ...TASK_ERROR_FIELDS,
    taskName: Schema.NullOr(Schema.String),
    filePath: Schema.String,
    whatIsWrong: Schema.NullOr(Schema.String),
    cause: Schema.NullOr(Schema.Defect()),
  },
) {
  public get reportedErrors() {
    const context = `could not read the run marker ${this.filePath}`;
    return this.whatIsWrong === null
      ? reportWrappedCause(this, context)
      : reportTaskText(this, this.whatIsWrong, context);
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
  }
}

export class RunMarkerNotCreated extends Schema.TaggedError<RunMarkerNotCreated>()(
  "RunMarkerNotCreated",
  {
    ...TASK_ERROR_FIELDS,
    taskName: Schema.NullOr(Schema.String),
    cause: Schema.Defect(),
  },
) {
  public get reportedErrors() {
    return reportWrappedCause(
      this,
      "could not create the pipe that marks the run's processes as its own",
    );
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
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
  public get reportedErrors() {
    return reportWrappedCause(
      this,
      `could not send ${this.signal === 0 ? "a liveness check" : this.signal} to ${this.processId < 0 ? `process group ${-this.processId}` : `process ${this.processId}`}`,
    );
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
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
  public get reportedErrors() {
    return reportWrappedCause(this, `could not load ${this.libraryPath} to list processes`);
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
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
  public get reportedErrors() {
    return reportWrappedCause(
      this,
      `could not read the command line of leftover process ${this.processId}`,
    );
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
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
  public get reportedErrors() {
    return reportWrappedCause(this, "could not read the process table to find leftover processes");
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
  }
}

export class WorkerExited extends Schema.TaggedError<WorkerExited>()("WorkerExited", {
  ...TASK_ERROR_FIELDS,
  taskName: Schema.NullOr(Schema.String),
  workerId: Schema.Int,
  exitCode: Schema.Int,
  uncaughtError: Schema.NullOr(PrintableError),
}) {
  public get reportedErrors(): Arrays.NonEmptyReadonlyArray<PrintableError> {
    const worker = this.taskName === null ? `worker ${this.workerId}` : "its worker";
    const taskName = this.taskName === null ? {} : { taskName: this.taskName };
    if (this.uncaughtError === null) {
      return Arrays.of({
        tag: this._tag,
        message: `${worker} exited on its own with code ${this.exitCode}`,
        ...taskName,
      });
    }
    const uncaught = addContext(this.uncaughtError, `${worker} stopped on this uncaught error`);
    const { location, ...described } = uncaught;
    return Arrays.of(
      location === undefined ? { ...described, ...taskName } : { ...described, location },
    );
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
  }
}

export class SupervisorSocketNotOpened extends Schema.TaggedError<SupervisorSocketNotOpened>()(
  "SupervisorSocketNotOpened",
  { ...TASK_ERROR_FIELDS, ...UNATTRIBUTED_FAILURE_FIELDS, cause: Schema.Defect() },
) {
  public get reportedErrors() {
    return reportWrappedCause(
      this,
      "could not open the socket the run reports to its supervisor on",
    );
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
  }
}
