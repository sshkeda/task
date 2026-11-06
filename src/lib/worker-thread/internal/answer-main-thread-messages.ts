import { Cause, Clock, Deferred, Effect, Exit, Logger, Result, Schema, Tracer } from "effect";
import { RpcServer } from "effect/rpc";

import { TASK_ERROR_FIELDS } from "../../../shared/error-fields.ts";
import { causeWithPrintableErrors, reportCause } from "../../../shared/report-cause.ts";
import {
  describeCauseMessage,
  THROWN_VALUE_WITHOUT_MESSAGE,
} from "../../../shared/report-errors.ts";
import { TaskEvent, TaskFiberResult, TaskWorkerRpcs } from "../../../shared/rpc.ts";
import type { StartTaskFiber } from "../../../shared/rpc.ts";
import { makeTraceFile } from "../../../shared/write-trace-file.ts";
import {
  forgetEndedProcessGroups,
  killTaskProcessGroups,
} from "../../kill-leftover-processes/index.ts";
import { makeTaskBodyRecord } from "../shared/define-task-body-record.ts";
import { WorkerState, reportToMainThread } from "../shared/provide-worker-state.ts";
import { describeCleanupFailure, describeTaskBodyResult } from "./describe-task-body-result.ts";
import { makeTraceSpanJson } from "./make-trace-span-json.ts";
import { runTaskBody } from "./run-task-body.ts";

class TaskResultNotHandedOver extends Schema.TaggedError<TaskResultNotHandedOver>()(
  "TaskResultNotHandedOver",
  { ...TASK_ERROR_FIELDS, taskName: Schema.String, cause: Schema.Defect() },
) {
  public override get message() {
    return `could not hand the result of "${this.taskName}" to the main thread because ${describeCauseMessage(this.cause)}`;
  }
}

const describeResultNotHandedOver = (notHandedOver: TaskResultNotHandedOver) =>
  TaskFiberResult.cases.Failed.make(reportCause(Cause.fail(notHandedOver)));

const endTaskBody = Effect.fn("endTaskBody")(function* (
  startMessage: typeof StartTaskFiber.Type,
  body: ReturnType<typeof makeTaskBodyRecord>,
  taskExit: Exit.Exit<unknown, unknown>,
) {
  const elapsedMilliseconds = (yield* Clock.currentTimeMillis) - body.startedAtMilliseconds;
  const workerState = yield* WorkerState;
  const cleanupExit = yield* Effect.exit(
    killTaskProcessGroups(body.startedProcessGroups, startMessage.taskName, (processes) =>
      workerState.claimProcessesToReport(startMessage.taskName, processes),
    ).pipe(Effect.flatMap(forgetEndedProcessGroups(body.startedProcessGroups))),
  );
  const sendTaskFiberEnded = (result: typeof TaskFiberResult.Type) =>
    reportToMainThread(
      TaskEvent.cases.TaskFiberEnded.make({
        taskName: startMessage.taskName,
        result,
        logs: body.logs,
      }),
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
          : Effect.try(() =>
              describeResultNotHandedOver(
                new TaskResultNotHandedOver({
                  taskName: startMessage.taskName,
                  cause: defect.success,
                }),
              ),
            ).pipe(
              Effect.catchTag("UnknownError", () =>
                Effect.sync(() =>
                  describeResultNotHandedOver(
                    new TaskResultNotHandedOver({
                      taskName: startMessage.taskName,
                      cause: THROWN_VALUE_WITHOUT_MESSAGE,
                    }),
                  ),
                ),
              ),
              Effect.flatMap(sendTaskFiberEnded),
            );
      },
      onSuccess: (success) => Effect.succeed(success),
    }),
  );
});

const startTaskFiber = Effect.fn("startTaskFiber")(function* (start: typeof StartTaskFiber.Type) {
  const workerState = yield* WorkerState;
  const body = makeTaskBodyRecord();
  yield* workerState.startTaskBodyFiber(
    start.taskName,
    runTaskBody(start, body).pipe(
      Effect.withParentSpan(Tracer.externalSpan({ traceId: start.traceId, spanId: start.spanId })),
      Effect.onExit((taskExit) => endTaskBody(start, body, taskExit)),
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
  const traceFile = yield* makeTraceFile({
    traceFilePath: workerState.traceFilePath,
    thread: "worker",
    maxBatchSize: 1,
    onIncomplete: (reason) =>
      Effect.sync(() => {
        workerState.reportToMainThreadNow(TaskEvent.cases.TraceWriteFailed.make({ reason }));
      }),
  });
  const tracer = Tracer.make({
    span: (options) => {
      const span = new Tracer.NativeSpan(options);
      const end = span.end.bind(span);
      const ending = { hasEnded: false };
      span.end = (endTime, exit) => {
        const isExported = span.sampled && !ending.hasEnded;
        ending.hasEnded = true;
        end(endTime, exit);
        const spanJson = isExported
          ? makeTraceSpanJson(
              span,
              endTime,
              Exit.isFailure(exit) ? Exit.failCause(causeWithPrintableErrors(exit.cause)) : exit,
            )
          : undefined;
        if (spanJson !== undefined) {
          workerState.recordTraceSpanNow(spanJson);
        }
      };
      return span;
    },
  });
  yield* serveUntilExitRequested.pipe(
    Effect.provideService(Tracer.Tracer, tracer),
    Effect.provideService(Logger.CurrentLoggers, new Set([traceFile.logger])),
  );
});
