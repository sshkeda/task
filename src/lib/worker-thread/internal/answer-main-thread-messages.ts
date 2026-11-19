import { Cause, Clock, Deferred, Effect, Exit, Logger, Result, Schema, Tracer } from "effect";
import { RpcServer } from "effect/rpc";

import { TASK_ERROR_FIELDS } from "../../../shared/error-fields.ts";
import { joinMessages } from "../../../shared/join-messages.ts";
import { describeStartedTaskSpan, makeTraceTracer } from "../../../shared/make-trace-tracer.ts";
import { reportCause, reportWrappedCause } from "../../../shared/report-cause.ts";
import { THROWN_VALUE_WITHOUT_MESSAGE } from "../../../shared/report-errors.ts";
import {
  TaskEvent,
  TaskFiberResult,
  TaskWorkerRpcs,
  WorkerTraceRecord,
} from "../../../shared/rpc.ts";
import type { StartTaskFiber } from "../../../shared/rpc.ts";
import { TaskSpanAnnouncements } from "../../../shared/task-spans.ts";
import {
  forgetEndedProcessGroups,
  killTaskProcessGroups,
} from "../../kill-leftover-processes/index.ts";
import { makeTaskBodyRecord } from "../shared/define-task-body-record.ts";
import { makeTraceOnlyLogger } from "../shared/make-trace-log-record.ts";
import { WorkerState, reportToMainThread } from "../shared/provide-worker-state.ts";
import { describeCleanupFailure, describeTaskBodyResult } from "./describe-task-body-result.ts";
import { runTaskBody } from "./run-task-body.ts";

class TaskResultNotHandedOver extends Schema.TaggedError<TaskResultNotHandedOver>()(
  "TaskResultNotHandedOver",
  { ...TASK_ERROR_FIELDS, taskName: Schema.String, cause: Schema.Defect() },
) {
  public get reportedErrors() {
    return reportWrappedCause(this, "could not hand its result to the main thread");
  }
  public override get message() {
    return joinMessages(this.reportedErrors);
  }
}

const reportResultNotHandedOver = (taskName: string, cause: TaskResultNotHandedOver["cause"]) =>
  TaskFiberResult.cases.Failed.make(
    reportCause(Cause.fail(new TaskResultNotHandedOver({ taskName, cause }))),
  );

const describeResultNotHandedOver = (taskName: string, cause: TaskResultNotHandedOver["cause"]) =>
  Result.getOrElse(
    Result.try(() => reportResultNotHandedOver(taskName, cause)),
    () => reportResultNotHandedOver(taskName, THROWN_VALUE_WITHOUT_MESSAGE),
  );

const endTaskBody = Effect.fn(function* (
  startMessage: typeof StartTaskFiber.Type,
  body: ReturnType<typeof makeTaskBodyRecord>,
  taskExit: Exit.Exit<unknown, unknown>,
) {
  const elapsedMilliseconds = (yield* Clock.currentTimeMillis) - body.startedAtMilliseconds;
  const workerState = yield* WorkerState;
  const cleanupExit = yield* Effect.exit(
    killTaskProcessGroups(
      body.startedProcessGroups,
      startMessage.taskName,
      (processes) => workerState.claimProcessesToReport(startMessage.taskName, processes),
      workerState.runMarker,
    ).pipe(Effect.flatMap(forgetEndedProcessGroups(body.startedProcessGroups))),
  );
  const sendTaskFiberEnded = (result: typeof TaskFiberResult.Type) =>
    Effect.sync(() => {
      body.hasEnded = true;
    }).pipe(
      Effect.andThen(
        reportToMainThread(
          TaskEvent.cases.TaskFiberEnded.make({
            taskName: startMessage.taskName,
            result,
          }),
        ),
      ),
    );
  yield* Effect.suspend((): Effect.Effect<typeof TaskFiberResult.Type> => {
    const cleanupFailure = describeCleanupFailure(body, cleanupExit, taskExit);
    return cleanupFailure === null
      ? describeTaskBodyResult(
          startMessage,
          body,
          Exit.isFailure(cleanupExit) ? cleanupExit : taskExit,
          elapsedMilliseconds,
        )
      : Effect.succeed(cleanupFailure);
  }).pipe(
    Effect.flatMap(sendTaskFiberEnded),
    Effect.matchCauseEffect({
      onFailure: (failureCause) => {
        const defect = Cause.findDefect(failureCause);
        return Result.isFailure(defect)
          ? Effect.failCause(defect.failure)
          : Effect.sync(() =>
              describeResultNotHandedOver(startMessage.taskName, defect.success),
            ).pipe(Effect.flatMap(sendTaskFiberEnded));
      },
      onSuccess: (success) => Effect.succeed(success),
    }),
  );
});

const startTaskFiber = Effect.fn(function* (start: typeof StartTaskFiber.Type) {
  const workerState = yield* WorkerState;
  const body = makeTaskBodyRecord();
  yield* workerState.startTaskBodyFiber(
    start.taskName,
    runTaskBody(start, body).pipe(
      Effect.onExit((taskExit) => endTaskBody(start, body, taskExit)),
      Effect.withParentSpan(Tracer.externalSpan({ traceId: start.traceId, spanId: start.spanId })),
    ),
  );
});

const serveUntilExitRequested = Effect.gen(function* () {
  const workerState = yield* WorkerState;
  const exitRequested = yield* Deferred.make<void>();
  const handlers = yield* TaskWorkerRpcs.toHandlers({
    StartTaskFiber: startTaskFiber,
    StopTaskFiber: (stop) => workerState.requestTaskBodyInterruption(stop.taskName),
    AnswerIfResponsive: () => Effect.void,
    ExitWorker: () => Deferred.done(exitRequested, Exit.void).pipe(Effect.asVoid),
  });
  yield* RpcServer.make(TaskWorkerRpcs, { disableTracing: true }).pipe(
    Effect.provideContext(handlers),
    Effect.interruptible,
    Effect.forkScoped,
  );
  yield* Deferred.await(exitRequested);
});

export const serveTheMainThread = Effect.gen(function* () {
  const workerState = yield* WorkerState;
  const tracer = makeTraceTracer((recorded) => {
    workerState.recordTraceNow(WorkerTraceRecord.cases.SpanExported.make(recorded));
  });
  const logger = makeTraceOnlyLogger(workerState.reportToMainThreadNow);
  yield* serveUntilExitRequested.pipe(
    Effect.provideService(Tracer.Tracer, tracer),
    Effect.provideService(Logger.CurrentLoggers, new Set([logger])),
    Effect.provideService(TaskSpanAnnouncements, {
      started: (span) => {
        workerState.recordTraceNow(
          WorkerTraceRecord.cases.TaskSpanStarted.make({ span: describeStartedTaskSpan(span) }),
        );
      },
      updated: (span) => {
        workerState.recordTraceNow(
          WorkerTraceRecord.cases.TaskSpanUpdated.make({ span: describeStartedTaskSpan(span) }),
        );
      },
      ended: (span) => {
        workerState.recordTraceNow(
          WorkerTraceRecord.cases.TaskSpanEnded.make({ spanId: span.spanId }),
        );
      },
    }),
  );
});
