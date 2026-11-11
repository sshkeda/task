import { LogLevel, Schema } from "effect";
import { Rpc, RpcGroup } from "effect/rpc";
import { Transferable } from "effect/workers";

import { PrintableError } from "../lib/errors/index.ts";
import { ProcessIdentity } from "./claim-processes-to-report.ts";
import { TaskOptionsRead } from "./resolve-task-options.ts";
import { RootTaskGroup } from "./root-task-group.ts";
import { TerminalCommand, TerminalCommandEnded, TerminalCommandId } from "./run-in-terminal.ts";
import { RunMarker } from "./run-marker.ts";
import { AwaitedTasksFailure, TaskRequestFailure } from "./task-errors.ts";

export const StandardInputRead = Schema.TaggedUnion({
  Chunks: { chunks: Schema.NonEmptyArray(Schema.Uint8Array) },
  Ended: {},
  Failed: { reason: Schema.String },
});

export const StartTaskFiber = Schema.Struct({
  taskName: Schema.String,
  rootTaskFilePath: Schema.String,
  taskFilePath: Schema.String,
  taskPackageDirectoryPath: Schema.String,
  cleanup: Schema.DurationFromMillis,
  traceId: Schema.String,
  spanId: Schema.String,
  standardInput: Schema.Struct({ isTerminal: Schema.Boolean, rootTaskName: Schema.String }),
});

export const TaskMadeInBody = Schema.Struct({
  locations: Schema.Array(Schema.NullOr(Schema.String)),
  options: TaskOptionsRead,
});

export const TaskFiberResult = Schema.TaggedUnion({
  Returned: {
    value: Schema.optional(Schema.Json),
    elapsedMilliseconds: Schema.Finite,
  },
  Failed: {
    failures: Schema.NonEmptyArray(PrintableError),
    details: Schema.optional(Schema.String),
    failedCommandExitCode: Schema.optional(Schema.Int),
  },
  AwaitedTasksFailed: { failure: AwaitedTasksFailure },
  CleanupFailed: {
    failures: Schema.NonEmptyArray(PrintableError),
    details: Schema.optional(Schema.String),
  },
  Interrupted: {},
});

const TaskFiberFailure = Schema.Union([
  TaskFiberResult.cases.Failed,
  TaskFiberResult.cases.AwaitedTasksFailed,
]);

export const StandardErrorLine = Schema.Struct({
  logLevel: Schema.Literals(LogLevel.values),
  text: Schema.String,
});

export const TASK_EVENT_FIELDS = {
  TraceLogRecordRequested: { traceLogRecordJson: Schema.String },
  StandardErrorLineRequested: {
    ...StandardErrorLine.fields,
    traceLogRecordJson: Schema.optional(Schema.String),
  },
  TaskFiberLoading: { taskName: Schema.String },
  TaskFiberStarted: { taskName: Schema.String },
  TaskFiberReceivedAwaitedFailure: { taskName: Schema.String, awaitedTaskName: Schema.String },
  TaskFiberFailedBeforeCleanup: { taskName: Schema.String, failure: TaskFiberFailure },
  TaskFiberProcessLeaderExited: {
    taskName: Schema.String,
    processId: Schema.Int,
    latestMemberStartTime: Schema.NullOr(Schema.String),
  },
  TaskFiberServed: { taskName: Schema.String, value: Schema.optional(Schema.Json) },
  TaskFiberStartedCleanUp: { taskName: Schema.String },
  TaskFiberEnded: {
    taskName: Schema.String,
    result: TaskFiberResult,
  },
};

export const TaskEvent = Schema.TaggedUnion(TASK_EVENT_FIELDS);

export const StartedProcess = Schema.Struct({
  taskName: Schema.String,
  processId: Schema.Int,
  startTime: Schema.NullOr(Schema.String),
});

export const ProcessReportsToClaim = Schema.Struct({
  taskName: Schema.String,
  processes: Schema.Array(ProcessIdentity),
});

export const TaskWorkerRpcs = RpcGroup.make(
  Rpc.make("StartTaskFiber", { payload: StartTaskFiber }),
  Rpc.make("StopTaskFiber", { payload: { taskName: Schema.String } }),
  Rpc.make("AnswerIfResponsive"),
  Rpc.make("ExitWorker"),
);

export const MainThreadRpcs = RpcGroup.make(
  Rpc.make("ReportTaskEvent", { payload: TaskEvent }),
  Rpc.make("RequestTask", {
    payload: {
      taskName: Schema.String,
      dependencyTaskName: Schema.String,
      dependencyTaskFilePath: Schema.NullOr(Schema.String),
      dependencyTaskMadeInBody: Schema.NullOr(TaskMadeInBody),
    },
    success: Schema.Struct({ value: Schema.optionalKey(Schema.Json) }),
    error: TaskRequestFailure,
  }),
  Rpc.make("ReadStandardInput", {
    payload: { taskName: Schema.String },
    success: StandardInputRead,
  }),
  Rpc.make("RunInTerminal", { payload: TerminalCommand, success: TerminalCommandEnded }),
  Rpc.make("AwaitTerminalCommandExit", { payload: TerminalCommandId }),
  Rpc.make("RecordStartedProcess", { payload: StartedProcess }),
  Rpc.make("RecordTraceSpan", { payload: { spanJson: Schema.String } }),
  Rpc.make("ClaimProcessReports", {
    payload: ProcessReportsToClaim,
    success: Schema.Array(ProcessIdentity),
  }),
);

export const WorkerInitialMessage = Schema.Struct({ mainThreadPort: Transferable.MessagePort });

const RequestIdentifier = Schema.Union([Schema.String, Schema.Finite]);

export const MainThreadRpcRequest = Schema.TaggedStruct("Request", {
  id: RequestIdentifier,
  tag: Schema.String,
  payload: Schema.Unknown,
  headers: Schema.Array(Schema.mutable(Schema.Tuple([Schema.String, Schema.String]))),
  isNotification: Schema.optionalKey(Schema.Literal(true)),
});

export const MessageFromMainThreadRpcClient = Schema.Union([
  MainThreadRpcRequest,
  Schema.TaggedStruct("Ack", { requestId: RequestIdentifier }),
  Schema.TaggedStruct("Interrupt", { requestId: RequestIdentifier }),
  Schema.TaggedStruct("Eof", {}),
]);

export const MessageFromMainThreadRpcServer = Schema.Union([
  Schema.TaggedStruct("Exit", {
    requestId: RequestIdentifier,
    exit: Schema.Union([
      Schema.TaggedStruct("Success", { value: Schema.Unknown }),
      Schema.TaggedStruct("Failure", {
        cause: Schema.Array(
          Schema.Union([
            Schema.TaggedStruct("Fail", { error: Schema.Unknown }),
            Schema.TaggedStruct("Die", { defect: Schema.Unknown }),
            Schema.TaggedStruct("Interrupt", {
              fiberId: Schema.UndefinedOr(Schema.NullOr(Schema.Finite)),
            }),
          ]),
        ),
      }),
    ]),
  }),
  Schema.TaggedStruct("Defect", { defect: Schema.Unknown }),
]);

export const WorkerData = Schema.Struct({
  workerId: Schema.Int,
  projectDirectoryPath: Schema.String,
  taskLogHold: Schema.instanceOf(Int32Array<SharedArrayBuffer>),
  postedBodyStartCount: Schema.instanceOf(Int32Array<SharedArrayBuffer>),
  runMarker: Schema.NullOr(RunMarker),
  rootTaskGroup: Schema.NullOr(RootTaskGroup),
  flags: Schema.Record(Schema.String, Schema.String),
});
