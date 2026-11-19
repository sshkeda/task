import { Worker } from "node:worker_threads";

import * as BunStream from "@effect/platform-bun/BunStream";
import {
  Cause,
  Clock,
  Context,
  Deferred,
  Duration,
  Effect,
  Exit,
  FiberSet,
  Layer,
  Option,
  Queue,
  Ref,
  Result,
  Schema,
  Scope,
  Semaphore,
  Stream,
  SubscriptionRef,
  Tracer,
} from "effect";
import { RpcClient, RpcSerialization, RpcServer, RpcWorker } from "effect/rpc";
import type { RpcClientError, RpcMessage } from "effect/rpc";
import { Worker as EffectWorker } from "effect/workers";

import type { ProcessIdentity } from "../../../shared/claim-processes-to-report.ts";
import { dieOnInvariantFailure } from "../../../shared/invariant-errors.ts";
import { joinMessages } from "../../../shared/join-messages.ts";
import { decodeOutputText, OutputToJoin, splitOutputPieces } from "../../../shared/label-output.ts";
import { TASK_TRACE_LABEL_PREFIX } from "../../../shared/names-from-package-json.ts";
import {
  offerOutputPiece,
  standardErrorWriterStoppedDeferred,
} from "../../../shared/print-to-standard-error.ts";
import { bunArguments, commandLineArguments } from "../../../shared/read-environment-config.ts";
import { addContext, reportUnderlyingErrors } from "../../../shared/report-cause.ts";
import {
  describeCauseMessage,
  THROWN_VALUE_WITHOUT_MESSAGE,
  UNTAGGED_FAILURE_TAG,
} from "../../../shared/report-errors.ts";
import {
  MainThreadRpcs,
  MessageFromWorkerToMainThread,
  TASK_EVENT_FIELDS,
  ConsoleLineWritten,
  TaskMadeInBody,
  TaskWorkerRpcs,
  WorkerInitialMessage,
  WorkerTraceRecord,
} from "../../../shared/rpc.ts";
import type { StandardInputRead, StartTaskFiber, WorkerData } from "../../../shared/rpc.ts";
import { awaitTerminalCommandExit, runInTerminal } from "../../../shared/run-in-terminal.ts";
import type { TaskRequestFailure } from "../../../shared/task-errors.ts";
import {
  liftTaskOutputBudget,
  makeTaskOutputBudget,
  releaseTaskOutputBytes,
} from "../../../shared/task-output-budget.ts";
import { TASK_PACKAGE } from "../../../shared/task-package.ts";
import { PrintableError } from "../../errors/index.ts";
import {
  awaitTheSupervisorTerminalCommandExit,
  runInTheSupervisorTerminal,
} from "../../supervise-the-run/index.ts";
import {
  isUsable,
  moveWorkerStage,
  WorkerEnding,
  WorkerStage,
  WorkerStageEvent,
} from "./define-worker-stage.ts";
import { WorkerOutputNotRead, WorkerTerminationFailed } from "./main-thread-errors.ts";
import { DiscoveredTasks } from "./provide-discovered-tasks.ts";
import { RunRootTaskConfig } from "./provide-run-root-task-config.ts";
import { StopState } from "./provide-stop-state.ts";
import { whileACommandHasTheTerminal } from "./track-terminal-commands.ts";
import { TraceFile } from "./write-trace-file.ts";

const WORKER_EXIT_GRACE_SECONDS = 2;

const WORKER_EXIT_GRACE = Duration.seconds(WORKER_EXIT_GRACE_SECONDS);

const OUTPUT_MEBIBYTES_WAITING_TO_BE_WRITTEN = 16;

const BYTES_PER_MEBIBYTE = 1_048_576;

const OUTPUT_BYTES_WAITING_TO_BE_WRITTEN =
  OUTPUT_MEBIBYTES_WAITING_TO_BE_WRITTEN * BYTES_PER_MEBIBYTE;

const FORCED_STOP_REASON = "without waiting for it to exit because the stop was forced";

const readOutputPieces = (readable: Worker["stdout"]) =>
  splitOutputPieces(decodeOutputText(BunStream.fromReadable({ evaluate: () => readable })));

const copyOutput = (
  source: string,
  readable: Worker["stdout"],
  budgetLifted: Deferred.Deferred<void>,
) =>
  Effect.gen(function* () {
    const budget = yield* Semaphore.make(OUTPUT_BYTES_WAITING_TO_BE_WRITTEN);
    yield* readOutputPieces(readable).pipe(
      Stream.runForEach((piece) => {
        const permits = Math.min(Buffer.byteLength(piece.text), OUTPUT_BYTES_WAITING_TO_BE_WRITTEN);
        return budget.take(permits).pipe(
          Effect.as(Effect.asVoid(budget.release(permits))),
          Effect.raceFirst(Deferred.await(budgetLifted).pipe(Effect.as(Effect.void))),
          Effect.flatMap((onWritten) =>
            offerOutputPiece(
              OutputToJoin.cases.Piece.make({ source, taskName: null, piece }),
              onWritten,
            ),
          ),
        );
      }),
    );
  });

const isWorkerTraceRecord = Schema.is(WorkerTraceRecord);

const describeWorkerEndingForTrace = (
  workerId: number,
  exitCode: number,
  ending: typeof WorkerEnding.Type,
  uncaughtError: PrintableError | null,
) => {
  if (WorkerEnding.guards.Terminated(ending)) {
    return {
      errorType: "WorkerTerminated",
      message: `worker ${workerId} was terminated before this span ended`,
    };
  }
  return uncaughtError === null
    ? {
        errorType: "WorkerExited",
        message: `worker ${workerId} exited with code ${exitCode} before this span ended`,
      }
    : {
        errorType: uncaughtError.tag,
        message: joinMessages([
          addContext(
            uncaughtError,
            `worker ${workerId} stopped on this uncaught error before this span ended`,
          ),
        ]),
      };
};

export const FromWorker = Schema.TaggedUnion({
  ...TASK_EVENT_FIELDS,
  TaskFiberRequestedTask: {
    dependencyRequestId: Schema.Int,
    taskName: Schema.String,
    dependencyTaskName: Schema.String,
    dependencyTaskFilePath: Schema.NullOr(Schema.String),
    dependencyTaskMadeInBody: Schema.NullOr(TaskMadeInBody),
  },
  TaskFiberCancelledRequest: { taskName: Schema.String, dependencyRequestId: Schema.Int },
  TaskFiberRequestedStandardInput: { taskName: Schema.String, readId: Schema.Int },
  TaskFiberStartedProcess: {
    taskName: Schema.String,
    recordId: Schema.Int,
    processId: Schema.Int,
    startTime: Schema.NullOr(Schema.String),
  },
  TaskFiberClaimingProcessReports: {
    taskName: Schema.String,
    claimId: Schema.Int,
    processes: Schema.Array(Schema.Struct({ processId: Schema.Int, startTime: Schema.String })),
  },
});

const decodeMessageFromWorkerToMainThread = Schema.decodeUnknownExit(MessageFromWorkerToMainThread);

const isConsoleLineWritten = Schema.is(ConsoleLineWritten);

const holdMessagesUntilListened = (thread: Worker) => {
  const inbox = {
    heldMessages: new Array<EffectWorker.PlatformMessage>(),
    deliver: (message: EffectWorker.PlatformMessage) => {
      inbox.heldMessages.push(message);
    },
  };
  thread.on("message", (message: EffectWorker.PlatformMessage) => {
    inbox.deliver(message);
  });
  return {
    postMessage: (...posting: Parameters<Worker["postMessage"]>) => {
      thread.postMessage(...posting);
    },
    listen: (emit: (message: EffectWorker.PlatformMessage) => void) => {
      inbox.deliver = emit;
      for (const message of inbox.heldMessages.splice(0)) {
        emit(message);
      }
    },
  };
};

const workerThreadPlatform = EffectWorker.makePlatform<
  ReturnType<typeof holdMessagesUntilListened>
>()({
  setup: ({ worker }) => Effect.succeed(worker),
  listen: ({ port, emit }) =>
    Effect.sync(() => {
      port.listen(emit);
    }),
});

export const WorkerEvent = Schema.TaggedUnion({
  ConsoleLineWritten: ConsoleLineWritten.fields,
  MessageReceived: { workerId: Schema.Int, message: FromWorker },
  MessageUnreadable: { workerId: Schema.Int, reason: Schema.String },
  ExitedWithCode: {
    workerId: Schema.Int,
    exitCode: Schema.Int,
    ending: WorkerEnding,
    uncaughtError: Schema.NullOr(PrintableError),
  },
});

const answerPendingRequest = <Success, Failure>(
  pendingAnswers: Map<number, Deferred.Deferred<Success, Failure>>,
  answerId: number,
  exit: Exit.Exit<Success, Failure>,
) =>
  Effect.sync(() => {
    const pendingAnswer = pendingAnswers.get(answerId);
    if (pendingAnswer !== undefined) {
      Deferred.doneUnsafe(pendingAnswer, exit);
    }
  });

const makeWorkerPool = Effect.gen(function* () {
  const shutdown = yield* StopState;
  const runRootTaskConfig = yield* RunRootTaskConfig;
  const traceFile = yield* TraceFile;
  const parentSpan = yield* Tracer.ParentSpan;
  const { tasksByName } = yield* DiscoveredTasks;
  const workerLimit = Math.min(runRootTaskConfig.workerCount, tasksByName?.size ?? 0);
  const workersById = new Map<
    number,
    {
      readonly thread: Worker;
      readonly client: RpcClient.FromGroup<typeof TaskWorkerRpcs, RpcClientError.RpcClientError>;
      readonly stage: Ref.Ref<typeof WorkerStage.Type>;
      readonly exitedDeferred: Deferred.Deferred<void>;
      readonly outputCopyEndedDeferred: Deferred.Deferred<void>;
      readonly terminationFailedDeferred: Deferred.Deferred<void>;
      readonly postedBodyStartCount: Int32Array<SharedArrayBuffer>;
      readonly taskOutputBudget: Int32Array<SharedArrayBuffer>;
      readonly handledBodyStartCount: SubscriptionRef.SubscriptionRef<number>;
      readonly isIsolated: boolean;
      uncaughtError: Option.Option<PrintableError>;
    }
  >();
  const workerSelectionSemaphore = yield* Semaphore.make(1);
  const workerEvents = yield* Queue.unbounded<typeof WorkerEvent.Type, Cause.Done>();
  const workerConnectionsScope = yield* Scope.fork(yield* Effect.scope);
  const callsToWorkers = yield* FiberSet.make().pipe(Scope.provide(workerConnectionsScope));
  const runInWorkerConnectionsScope = yield* FiberSet.makeRuntime<never, void, never>().pipe(
    Scope.provide(workerConnectionsScope),
  );
  const outputBudgetLifted = yield* Deferred.make<void>();
  yield* Effect.raceAll([
    shutdown.awaitForcedStop,
    Deferred.await(standardErrorWriterStoppedDeferred),
    shutdown.awaitStopSignal,
  ]).pipe(
    Effect.andThen(Deferred.done(outputBudgetLifted, Exit.void)),
    Effect.andThen(
      Effect.sync(() => {
        for (const workerThread of workersById.values()) {
          liftTaskOutputBudget(workerThread.taskOutputBudget);
        }
      }),
    ),
    Effect.forkIn(workerConnectionsScope),
  );
  const copyWorkerOutput = (workerId: number, thread: Worker) =>
    Effect.all(
      [
        copyOutput(`${workerId} stdout`, thread.stdout, outputBudgetLifted),
        copyOutput(`${workerId} stderr`, thread.stderr, outputBudgetLifted),
      ],
      { concurrency: "unbounded", discard: true },
    ).pipe(
      Effect.catchTag("UnknownError", (cause) => {
        const lost = new WorkerOutputNotRead({ workerId, cause });
        return shutdown
          .recordRunFailure(lost)
          .pipe(Effect.andThen(shutdown.recordFirstFailure(lost)), Effect.asVoid);
      }),
    );
  const nextAnswerId = yield* Ref.make(0);
  const pendingDependencyResults = new Map<
    number,
    Deferred.Deferred<{ readonly value?: Schema.Json }, typeof TaskRequestFailure.Type>
  >();
  const pendingStandardInputReads = new Map<
    number,
    Deferred.Deferred<typeof StandardInputRead.Type>
  >();
  const pendingProcessRecords = new Map<number, Deferred.Deferred<void>>();
  const pendingReportClaims = new Map<
    number,
    Deferred.Deferred<readonly (typeof ProcessIdentity.Type)[]>
  >();
  const receiveMessage = (workerId: number, message: typeof FromWorker.Type) => {
    Queue.offerUnsafe(workerEvents, WorkerEvent.cases.MessageReceived.make({ workerId, message }));
  };
  const reportAndAwaitAnswer = <Success, Failure>(
    workerId: number,
    pendingAnswers: Map<number, Deferred.Deferred<Success, Failure>>,
    makeMessage: (answerId: number) => typeof FromWorker.Type,
    makeCancelledMessage: ((answerId: number) => typeof FromWorker.Type) | null,
  ) =>
    Effect.gen(function* () {
      const answerId = yield* Ref.getAndUpdate(nextAnswerId, (id) => id + 1);
      const answer = Deferred.makeUnsafe<Success, Failure>();
      pendingAnswers.set(answerId, answer);
      receiveMessage(workerId, makeMessage(answerId));
      return yield* Deferred.await(answer).pipe(
        Effect.onInterrupt(() =>
          Effect.sync(() => {
            if (makeCancelledMessage !== null) {
              receiveMessage(workerId, makeCancelledMessage(answerId));
            }
          }),
        ),
        Effect.ensuring(
          Effect.sync(() => {
            pendingAnswers.delete(answerId);
          }),
        ),
      );
    });
  const recordWorkerTrace = (workerId: number, record: typeof WorkerTraceRecord.Type) => {
    WorkerTraceRecord.match(record, {
      SpanExported: (exported) => {
        traceFile.writeWorkerSpan(exported);
      },
      TaskSpanStarted: ({ span }) => {
        traceFile.recordWorkerTaskSpanStart(workerId, span);
      },
      TaskSpanUpdated: ({ span }) => {
        traceFile.recordWorkerTaskSpanUpdate(span);
      },
      TaskSpanEnded: ({ spanId }) => {
        traceFile.recordWorkerTaskSpanEnd(spanId);
      },
    });
  };
  const makeWorkerRpcServerProtocol = (workerId: number, workerPort: MessagePort) =>
    RpcServer.Protocol.make((writeRequest) =>
      Effect.gen(function* () {
        const runInPoolScope = yield* FiberSet.makeRuntime<never, void, never>();
        const onMessage = (event: { readonly data: unknown }) => {
          if (isConsoleLineWritten(event.data)) {
            Queue.offerUnsafe(
              workerEvents,
              event.data.workerId === workerId
                ? event.data
                : WorkerEvent.cases.MessageUnreadable.make({
                    workerId,
                    reason: `a console line names worker ${event.data.workerId}`,
                  }),
            );
            return;
          }
          Exit.match(decodeMessageFromWorkerToMainThread(event.data), {
            onSuccess: (message) => {
              if (isWorkerTraceRecord(message)) {
                recordWorkerTrace(workerId, message);
                return;
              }
              runInPoolScope(writeRequest(0, message));
            },
            onFailure: (cause) => {
              Queue.offerUnsafe(
                workerEvents,
                WorkerEvent.cases.MessageUnreadable.make({ workerId, reason: Cause.pretty(cause) }),
              );
            },
          });
        };
        yield* Effect.acquireRelease(
          Effect.sync(() => {
            workerPort.addEventListener("message", onMessage);
            workerPort.start();
          }),
          () =>
            Effect.sync(() => {
              workerPort.removeEventListener("message", onMessage);
              workerPort.close();
            }),
        );
        const workerRpcServerProtocol = {
          disconnects: yield* Queue.make<number>(),
          send: (_clientId: number, response: RpcMessage.FromServerEncoded) =>
            Effect.sync(() => {
              workerPort.postMessage(response);
            }),
          end: () => Effect.void,
          clientIds: Effect.succeed(new Set([0])),
          initialMessage: Effect.succeedNone,
          supportsAck: false,
          supportsTransferables: false,
          supportsSpanPropagation: false,
          supportsNotifications: false,
          codecFor: RpcSerialization.json.codecFor,
        };
        return workerRpcServerProtocol;
      }),
    );
  const serveWorker = (workerId: number, workerPort: MessagePort) =>
    Effect.gen(function* () {
      const handlers = yield* MainThreadRpcs.toHandlers({
        ReportTaskEvent: (event) =>
          Effect.sync(() => {
            receiveMessage(workerId, event);
          }),
        RequestTask: (requested) =>
          reportAndAwaitAnswer(
            workerId,
            pendingDependencyResults,
            (dependencyRequestId) =>
              FromWorker.cases.TaskFiberRequestedTask.make({ ...requested, dependencyRequestId }),
            (dependencyRequestId) =>
              FromWorker.cases.TaskFiberCancelledRequest.make({
                taskName: requested.taskName,
                dependencyRequestId,
              }),
          ),
        ReadStandardInput: (requested) =>
          reportAndAwaitAnswer(
            workerId,
            pendingStandardInputReads,
            (readId) =>
              FromWorker.cases.TaskFiberRequestedStandardInput.make({
                taskName: requested.taskName,
                readId,
              }),
            null,
          ),
        RunInTerminal: (command) =>
          whileACommandHasTheTerminal(
            runRootTaskConfig.ownsSession
              ? runInTheSupervisorTerminal(command)
              : runInTerminal(
                  Effect.asVoid(shutdown.awaitFailure),
                  runRootTaskConfig.runMarker === null
                    ? null
                    : runRootTaskConfig.runMarker.fileDescriptor,
                )(command),
          ),
        AwaitTerminalCommandExit: (command) =>
          whileACommandHasTheTerminal(
            runRootTaskConfig.ownsSession
              ? awaitTheSupervisorTerminalCommandExit(command)
              : awaitTerminalCommandExit(command),
          ),
        RecordStartedProcess: (started) =>
          reportAndAwaitAnswer(
            workerId,
            pendingProcessRecords,
            (recordId) => FromWorker.cases.TaskFiberStartedProcess.make({ ...started, recordId }),
            null,
          ),
        ClaimProcessReports: (claiming) =>
          reportAndAwaitAnswer(
            workerId,
            pendingReportClaims,
            (claimId) =>
              FromWorker.cases.TaskFiberClaimingProcessReports.make({ ...claiming, claimId }),
            null,
          ),
      });
      yield* RpcServer.make(MainThreadRpcs, { disableTracing: true }).pipe(
        Effect.provideContext(handlers),
        Effect.provideServiceEffect(
          RpcServer.Protocol,
          makeWorkerRpcServerProtocol(workerId, workerPort),
        ),
        Effect.interruptible,
        Effect.forkScoped,
        Scope.provide(workerConnectionsScope),
      );
    });
  const connectToWorker = (
    thread: ReturnType<typeof holdMessagesUntilListened>,
    mainThreadPort: MessagePort,
  ) =>
    RpcClient.make(TaskWorkerRpcs, { disableTracing: true }).pipe(
      Effect.provideServiceEffect(
        RpcClient.Protocol,
        RpcClient.makeProtocolWorker({ size: 1, concurrency: Number.POSITIVE_INFINITY }),
      ),
      Effect.provideService(EffectWorker.WorkerPlatform, workerThreadPlatform),
      Effect.provideService(EffectWorker.Spawner, () => thread),
      Effect.provideService(
        RpcWorker.InitialMessage,
        RpcWorker.makeInitialMessage(WorkerInitialMessage, Effect.succeed({ mainThreadPort })).pipe(
          dieOnInvariantFailure("worker startup", "the main thread's RPC port can be handed over"),
        ),
      ),
      Scope.provide(workerConnectionsScope),
      dieOnInvariantFailure("worker startup", "a started worker can be called over RPC"),
    );

  const moveWorker = (
    workerThread: Parameters<typeof workersById.set>[1],
    event: typeof WorkerStageEvent.Type,
  ) =>
    Ref.modify(workerThread.stage, (current) => {
      const moved = moveWorkerStage(current, event);
      return [moved, moved[0]];
    });
  const recordExit = (
    workerId: number,
    workerThread: Parameters<typeof workersById.set>[1],
    exitCode: number,
  ) =>
    Effect.gen(function* () {
      const uncaughtError = Option.getOrNull(workerThread.uncaughtError);
      const [stage, action] = yield* moveWorker(
        workerThread,
        WorkerStageEvent.cases.Exit.make({ exitCode, uncaughtError }),
      );
      const isNewlyExited = action === "reportExit" && WorkerStage.guards.Exited(stage);
      if (isNewlyExited) {
        traceFile.recordWorkerEnded(
          workerId,
          yield* Clock.currentTimeNanos,
          describeWorkerEndingForTrace(workerId, exitCode, stage.ending, uncaughtError),
        );
        yield* Queue.offer(
          workerEvents,
          WorkerEvent.cases.ExitedWithCode.make({
            workerId,
            exitCode,
            ending: stage.ending,
            uncaughtError,
          }),
        );
      }
      yield* Deferred.done(workerThread.exitedDeferred, Exit.void);
    }).pipe(Effect.uninterruptible);
  const recordOutputCopyEnded = (workerThread: Parameters<typeof workersById.set>[1]) =>
    Deferred.await(workerThread.exitedDeferred).pipe(
      Effect.andThen(moveWorker(workerThread, WorkerStageEvent.cases.DrainOutput.make({}))),
      Effect.andThen(Deferred.done(workerThread.outputCopyEndedDeferred, Exit.void)),
    );
  const startWorker = (workerId: number, isIsolated: boolean) =>
    Effect.gen(function* () {
      const postedBodyStartCount = new Int32Array(
        new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT),
      );
      const taskOutputBudget = makeTaskOutputBudget();
      const workerData = {
        workerId,
        projectDirectoryPath: runRootTaskConfig.projectDirectoryPath,
        startDirectoryPath: runRootTaskConfig.workingDirectoryPathAtStart,
        taskLogHold: shutdown.taskLogHold,
        postedBodyStartCount,
        taskOutputBudget,
        runMarker: runRootTaskConfig.runMarker,
        rootTaskGroup: runRootTaskConfig.rootTaskGroup,
        flags: runRootTaskConfig.flags,
      } satisfies typeof WorkerData.Type;
      const rpcChannel = new MessageChannel();
      yield* serveWorker(workerId, rpcChannel.port1);
      const thread = new Worker(new URL("../../worker-thread/index.ts", import.meta.url), {
        workerData,
        stdout: true,
        stderr: true,
        argv: yield* commandLineArguments,
        execArgv: yield* bunArguments,
      });
      const threadMessages = holdMessagesUntilListened(thread);
      const workerThread = {
        thread,
        client: yield* connectToWorker(threadMessages, rpcChannel.port2),
        stage: yield* Ref.make<typeof WorkerStage.Type>(WorkerStage.cases.Idle.make({})),
        exitedDeferred: yield* Deferred.make<void>(),
        outputCopyEndedDeferred: yield* Deferred.make<void>(),
        terminationFailedDeferred: yield* Deferred.make<void>(),
        postedBodyStartCount,
        taskOutputBudget,
        handledBodyStartCount: yield* SubscriptionRef.make(0),
        isIsolated,
        uncaughtError: Option.none(),
      } satisfies Parameters<typeof workersById.set>[1];
      if (Deferred.isDoneUnsafe(outputBudgetLifted)) {
        liftTaskOutputBudget(taskOutputBudget);
      }
      workersById.set(workerId, workerThread);
      yield* copyWorkerOutput(workerId, thread).pipe(
        Effect.andThen(recordOutputCopyEnded(workerThread)),
        Effect.forkIn(workerConnectionsScope),
      );
      workerThread.thread.on("error", (error) => {
        workerThread.uncaughtError = Option.some(
          reportUnderlyingErrors({ cause: error }, UNTAGGED_FAILURE_TAG)[0],
        );
      });
      workerThread.thread.on("exit", (exitCode) => {
        runInWorkerConnectionsScope(recordExit(workerId, workerThread, exitCode));
      });
    });
  const startNextWorker = (isIsolated: boolean) =>
    Effect.gen(function* () {
      const workerId = workersById.size;
      if (yield* shutdown.isStopping) {
        return yield* Effect.interrupt;
      }
      yield* startWorker(workerId, isIsolated);
      return workerId;
    });
  const terminateWorker = (
    workerId: number,
    workerThread: Parameters<typeof workersById.set>[1],
    reason: string,
    terminatingTaskName: string | null,
  ) =>
    Effect.gen(function* () {
      const [, action] = yield* moveWorker(
        workerThread,
        WorkerStageEvent.cases.Terminate.make({ terminatingTaskName }),
      );
      if (action !== "terminate") {
        return;
      }
      yield* Effect.logInfo(`worker terminated ${reason}`).pipe(
        Effect.annotateLogs(`${TASK_TRACE_LABEL_PREFIX}.worker`, workerId),
      );
      const termination = workerThread.thread.terminate();
      runInWorkerConnectionsScope(
        Effect.promise(() => termination).pipe(
          Effect.asVoid,
          Effect.catchCause((cause) => {
            const failed = new WorkerTerminationFailed({
              workerId,
              reason: Result.getOrElse(
                Result.try(() => describeCauseMessage(Cause.squash(cause))),
                () => THROWN_VALUE_WITHOUT_MESSAGE,
              ),
            });
            return shutdown
              .recordRunFailure(failed)
              .pipe(
                Effect.andThen(shutdown.recordFirstFailure(failed)),
                Effect.asVoid,
                Effect.ensuring(Deferred.done(workerThread.terminationFailedDeferred, Exit.void)),
              );
          }),
        ),
      );
    }).pipe(Effect.uninterruptible);
  const requestWorkerExit = (workerThread: Parameters<typeof workersById.set>[1]) =>
    Effect.gen(function* () {
      const [, action] = yield* moveWorker(
        workerThread,
        WorkerStageEvent.cases.RequestExit.make({}),
      );
      if (action !== "sendExit") {
        return;
      }
      yield* FiberSet.run(
        callsToWorkers,
        workerThread.client.ExitWorker().pipe(Effect.catchTag("RpcClientError", () => Effect.void)),
      );
    }).pipe(Effect.uninterruptible);
  const requestExitOfAllWorkers = Effect.forEach(workersById.values(), requestWorkerExit, {
    discard: true,
  });
  const terminateAllWorkers = Effect.forEach(
    workersById,
    ([workerId, workerThread]) => terminateWorker(workerId, workerThread, FORCED_STOP_REASON, null),
    { discard: true },
  );
  const awaitWorkerShutdown = Effect.forEach(
    workersById,
    ([workerId, workerThread]) =>
      Effect.gen(function* () {
        const exited = yield* Deferred.await(workerThread.exitedDeferred).pipe(
          Effect.timeoutOption(WORKER_EXIT_GRACE),
          Effect.raceFirst(shutdown.awaitForcedStop.pipe(Effect.as(Option.none()))),
        );
        if (Option.isSome(exited)) {
          return yield* Deferred.await(workerThread.outputCopyEndedDeferred);
        }
        yield* terminateWorker(
          workerId,
          workerThread,
          (yield* shutdown.isForced)
            ? FORCED_STOP_REASON
            : `because it did not exit within ${Duration.format(WORKER_EXIT_GRACE)} of being asked to`,
          null,
        );
        yield* Deferred.await(workerThread.exitedDeferred).pipe(
          Effect.raceFirst(Deferred.await(workerThread.terminationFailedDeferred)),
        );
      }),
    { concurrency: "unbounded", discard: true },
  ).pipe(Effect.andThen(Queue.end(workerEvents)));
  const findStartedWorker = (workerId: number, invariant: string) =>
    Effect.fromNullishOr(workersById.get(workerId)).pipe(
      dieOnInvariantFailure("worker pool", invariant),
    );
  const findReachableWorker = (workerId: number, rpcTag: string) =>
    Effect.gen(function* () {
      const workerThread = yield* findStartedWorker(workerId, "messages go to a started worker");
      if (!isUsable(Ref.getUnsafe(workerThread.stage))) {
        yield* Effect.logDebug(
          "message not sent because its worker is gone, and that worker's exit ends its tasks",
        ).pipe(
          Effect.annotateLogs({
            [`${TASK_TRACE_LABEL_PREFIX}.worker`]: workerId,
            [`${TASK_TRACE_LABEL_PREFIX}.message`]: rpcTag,
          }),
        );
        return null;
      }
      return workerThread;
    });
  const callWorker = (
    workerId: number,
    rpcTag: string,
    call: (
      client: RpcClient.FromGroup<typeof TaskWorkerRpcs, RpcClientError.RpcClientError>,
    ) => Effect.Effect<void, RpcClientError.RpcClientError>,
  ) =>
    Effect.gen(function* () {
      const workerThread = yield* findReachableWorker(workerId, rpcTag);
      if (workerThread === null) {
        return false;
      }
      yield* FiberSet.run(
        callsToWorkers,
        call(workerThread.client).pipe(Effect.catchTag("RpcClientError", () => Effect.void)),
      );
      return true;
    });
  const selectWorkerIdForTask = (
    readWorkerLoad: (workerId: number) => {
      readonly occupyingTaskCount: number;
      readonly servingTaskCount: number;
    },
    recordAssignment: (workerId: number) => Effect.Effect<void>,
    requiredWorkerId: number | null,
    isIsolated: boolean,
    awaitNextWorkerLoadChange: () => Effect.Effect<void>,
  ): Effect.Effect<number> =>
    Effect.gen(function* () {
      const workerLoadChanged = awaitNextWorkerLoadChange();
      const selectedWorkerId = yield* Effect.gen(function* () {
        if (yield* shutdown.isStopping) {
          return yield* Effect.interrupt;
        }
        const usableWorkers = [...workersById].filter(([, workerThread]) =>
          isUsable(Ref.getUnsafe(workerThread.stage)),
        );
        const usableWorkerIds = usableWorkers.map(([workerId]) => workerId);
        const sharedWorkerLoads = usableWorkers
          .filter(([, workerThread]) => !workerThread.isIsolated)
          .map(([workerId]) => [workerId, readWorkerLoad(workerId)] as const);
        const busyWorkerCount = usableWorkerIds.filter(
          (workerId) => readWorkerLoad(workerId).occupyingTaskCount > 0,
        ).length;
        const startsIsolatedWorker = isIsolated && requiredWorkerId === null;
        const awaitsFreeWorkerSlot = startsIsolatedWorker && busyWorkerCount >= workerLimit;
        if (awaitsFreeWorkerSlot) {
          return null;
        }
        const idleWorkerId = sharedWorkerLoads.find(
          ([, load]) => load.occupyingTaskCount === 0 && load.servingTaskCount === 0,
        )?.[0];
        const startsSharedWorker =
          idleWorkerId === undefined &&
          [...workersById.values()].filter((workerThread) => !workerThread.isIsolated).length <
            workerLimit;
        const selectSharedWorkerId = startsSharedWorker
          ? startNextWorker(false)
          : Effect.succeed(
              idleWorkerId ??
                sharedWorkerLoads.toSorted(
                  ([, left], [, right]) =>
                    left.occupyingTaskCount - right.occupyingTaskCount ||
                    left.servingTaskCount - right.servingTaskCount,
                )[0]?.[0],
            );
        const workerId =
          requiredWorkerId === null
            ? yield* startsIsolatedWorker ? startNextWorker(true) : selectSharedWorkerId
            : usableWorkerIds.find((usableWorkerId) => usableWorkerId === requiredWorkerId);
        if (workerId === undefined) {
          yield* shutdown.awaitFailure;
          return yield* Effect.interrupt;
        }
        yield* recordAssignment(workerId);
        yield* findStartedWorker(workerId, "a worker a task is assigned to was started").pipe(
          Effect.flatMap((workerThread) =>
            moveWorker(workerThread, WorkerStageEvent.cases.AssignTask.make({})),
          ),
        );
        return workerId;
      }).pipe(workerSelectionSemaphore.withPermits(1));
      if (selectedWorkerId !== null) {
        return selectedWorkerId;
      }
      yield* Effect.raceFirst(workerLoadChanged, Effect.asVoid(shutdown.awaitFailure));
      return yield* selectWorkerIdForTask(
        readWorkerLoad,
        recordAssignment,
        requiredWorkerId,
        isIsolated,
        awaitNextWorkerLoadChange,
      );
    });
  yield* Effect.addFinalizer(() => Effect.andThen(requestExitOfAllWorkers, awaitWorkerShutdown));
  return {
    events: Stream.fromQueue(workerEvents).pipe(
      Stream.provideService(Tracer.ParentSpan, parentSpan),
    ),
    selectWorkerIdForTask,
    recordTaskEnded: (workerId: number) =>
      Effect.gen(function* () {
        const workerThread = yield* findStartedWorker(
          workerId,
          "a worker whose task ended was started",
        );
        const [stage] = yield* moveWorker(workerThread, WorkerStageEvent.cases.EndTask.make({}));
        const isDiscardable = workerThread.isIsolated && WorkerStage.guards.Idle(stage);
        if (isDiscardable) {
          yield* requestWorkerExit(workerThread);
        }
      }),
    startTaskFiber: (workerId: number, start: typeof StartTaskFiber.Type) =>
      callWorker(workerId, "StartTaskFiber", (client) => client.StartTaskFiber(start)),
    stopTaskFiber: (workerId: number, taskName: string) =>
      callWorker(workerId, "StopTaskFiber", (client) => client.StopTaskFiber({ taskName })),
    answerDependencyRequest: (
      dependencyRequestId: number,
      exit: Exit.Exit<Schema.Json | undefined, typeof TaskRequestFailure.Type>,
    ) =>
      answerPendingRequest(
        pendingDependencyResults,
        dependencyRequestId,
        Exit.map(exit, (value) => (value === undefined ? {} : { value })),
      ),
    answerStandardInputRead: (readId: number, read: typeof StandardInputRead.Type) =>
      answerPendingRequest(pendingStandardInputReads, readId, Exit.succeed(read)),
    acknowledgeProcessRecorded: (recordId: number) =>
      answerPendingRequest(pendingProcessRecords, recordId, Exit.void),
    answerReportClaim: (claimId: number, claimed: readonly (typeof ProcessIdentity.Type)[]) =>
      answerPendingRequest(pendingReportClaims, claimId, Exit.succeed(claimed)),
    awaitAnswer: (workerId: number) =>
      Effect.gen(function* () {
        const reachableWorker = yield* findReachableWorker(workerId, "AnswerIfResponsive");
        if (reachableWorker === null) {
          return "gone" as const;
        }
        return yield* reachableWorker.client.AnswerIfResponsive().pipe(
          Effect.as("answered" as const),
          Effect.catchTag("RpcClientError", () => Effect.succeed("gone" as const)),
          Effect.raceFirst(
            Deferred.await(reachableWorker.exitedDeferred).pipe(Effect.as("gone" as const)),
          ),
        );
      }),
    requestWorkerTermination: (workerId: number, reason: string, terminatingTaskName: string) =>
      findStartedWorker(workerId, "a worker whose termination is requested was started").pipe(
        Effect.flatMap((workerThread) =>
          terminateWorker(workerId, workerThread, reason, terminatingTaskName),
        ),
      ),
    requestExitOfAllWorkers,
    terminateAllWorkers,
    awaitWorkerShutdown,
    releaseTaskOutputBytes: (workerId: number, bytes: number) =>
      Effect.sync(() => {
        const workerThread = workersById.get(workerId);
        if (workerThread !== undefined) {
          releaseTaskOutputBytes(workerThread.taskOutputBudget, bytes);
        }
      }),
    recordBodyStartHandled: (workerId: number) =>
      findStartedWorker(workerId, "a body start comes from a started worker").pipe(
        Effect.flatMap((workerThread) =>
          SubscriptionRef.update(workerThread.handledBodyStartCount, (count) => count + 1),
        ),
      ),
    awaitPostedBodyStartsHandled: Effect.forEach(
      workersById.values(),
      (workerThread) => {
        const postedCount = Atomics.load(workerThread.postedBodyStartCount, 0);
        return SubscriptionRef.changes(workerThread.handledBodyStartCount).pipe(
          Stream.takeUntil((handledCount) => handledCount >= postedCount),
          Stream.runDrain,
        );
      },
      { concurrency: "unbounded", discard: true },
    ),
    startedWorkerCount: Effect.sync(() => workersById.size),
  };
});

export class WorkerPool extends Context.Service<
  WorkerPool,
  Effect.Success<typeof makeWorkerPool>
>()(`${TASK_PACKAGE.name}/WorkerPool`) {}
export const WorkerPoolLive = Layer.effect(WorkerPool, makeWorkerPool);
