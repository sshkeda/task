import { fileURLToPath } from "node:url";
import { Worker } from "node:worker_threads";

import {
  Cause,
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
  Schema,
  Scope,
  Semaphore,
  Stream,
  Tracer,
} from "effect";
import { RpcClient, RpcSerialization, RpcServer, RpcWorker } from "effect/rpc";
import type { RpcClientError, RpcMessage } from "effect/rpc";
import { Worker as EffectWorker } from "effect/workers";

import type { ProcessIdentity } from "../../../shared/claim-processes-to-report.ts";
import { findUserLocationInStack } from "../../../shared/find-user-locations-in-stack-traces.ts";
import { dieOnInvariantFailure } from "../../../shared/invariant-errors.ts";
import { TASK_TRACE_LABEL_PREFIX } from "../../../shared/names-from-package-json.ts";
import { UncaughtWorkerError } from "../../../shared/process-errors.ts";
import { bunArguments, commandLineArguments } from "../../../shared/read-environment-config.ts";
import { describeCauseMessage, ErrorStack } from "../../../shared/report-errors.ts";
import {
  MainThreadRpcs,
  MessageFromMainThreadRpcClient,
  TASK_EVENT_FIELDS,
  TaskWorkerRpcs,
  WorkerInitialMessage,
} from "../../../shared/rpc.ts";
import type { StandardInputRead, StartTaskFiber, WorkerData } from "../../../shared/rpc.ts";
import type { TaskRequestFailure } from "../../../shared/task-errors.ts";
import { TASK_PACKAGE } from "../../../shared/task-package.ts";
import { TraceFile } from "../../../shared/write-trace-file.ts";
import { ModuleGraph } from "../lib/find-code-each-task-depends-on/index.ts";
import { DiscoveredTasks } from "./provide-discovered-tasks.ts";
import { RunRootTaskConfig } from "./provide-run-root-task-config.ts";
import { StopState } from "./provide-stop-state.ts";

const WORKER_EXIT_GRACE_SECONDS = 2;

const WORKER_EXIT_GRACE = Duration.seconds(WORKER_EXIT_GRACE_SECONDS);

const FORCED_STOP_REASON = "without waiting for it to exit because the stop was forced";

export const FromWorker = Schema.TaggedUnion({
  ...TASK_EVENT_FIELDS,
  TaskFiberRequestedTask: {
    dependencyRequestId: Schema.Int,
    taskName: Schema.String,
    dependencyTaskName: Schema.String,
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

const decodeMessageFromMainThreadRpcClient = Schema.decodeUnknownExit(
  MessageFromMainThreadRpcClient,
);

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
  MessageReceived: { workerId: Schema.Int, message: FromWorker },
  MessageUnreadable: { workerId: Schema.Int, reason: Schema.String },
  ExitedWithCode: {
    workerId: Schema.Int,
    exitCode: Schema.Int,
    terminationRequested: Schema.Boolean,
    uncaughtError: Schema.NullOr(UncaughtWorkerError),
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
  const moduleGraph = yield* ModuleGraph;
  const traceFile = yield* TraceFile;
  const parentSpan = yield* Tracer.ParentSpan;
  const { tasksByName } = yield* DiscoveredTasks;
  const taskFilePaths =
    tasksByName === null
      ? []
      : Array.from(tasksByName.values(), (discoveredTask) => discoveredTask.filePath);
  const workerLimit = Math.min(runRootTaskConfig.workerCount, taskFilePaths.length);
  const workersById = new Map<
    number,
    {
      readonly thread: Worker;
      readonly client: RpcClient.FromGroup<typeof TaskWorkerRpcs, RpcClientError.RpcClientError>;
      readonly exitDeferred: Deferred.Deferred<void>;
      terminationRequested: boolean;
      terminateCalled: boolean;
      uncaughtError: Option.Option<typeof UncaughtWorkerError.Type>;
    }
  >();
  const workerSelectionSemaphore = yield* Semaphore.make(1);
  const workerEvents = yield* Queue.unbounded<typeof WorkerEvent.Type, Cause.Done>();
  const workerConnectionsScope = yield* Scope.fork(yield* Effect.scope);
  const callsToWorkers = yield* FiberSet.make().pipe(Scope.provide(workerConnectionsScope));
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
  const makeWorkerRpcServerProtocol = (workerId: number, workerPort: MessagePort) =>
    RpcServer.Protocol.make((writeRequest) =>
      Effect.gen(function* () {
        const runInPoolScope = yield* FiberSet.makeRuntime<never, void, never>();
        const onMessage = (event: { readonly data: unknown }) => {
          Exit.match(decodeMessageFromMainThreadRpcClient(event.data), {
            onSuccess: (request) => {
              runInPoolScope(writeRequest(0, request));
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

  const startWorker = (
    workerId: number,
    rewrittenFiles: (typeof WorkerData.Type)["rewrittenFiles"],
  ) =>
    Effect.gen(function* () {
      const workerData = {
        workerId,
        traceFilePath: traceFile.filePath,
        projectDirectoryPath: runRootTaskConfig.rootTaskPackageJsonDirectoryPath,
        rewrittenFiles,
        taskLogHold: shutdown.taskLogHold,
        runMarker: runRootTaskConfig.runMarker,
      } satisfies typeof WorkerData.Type;
      const rpcChannel = new MessageChannel();
      yield* serveWorker(workerId, rpcChannel.port1);
      const thread = new Worker(new URL("../../worker-thread/index.ts", import.meta.url), {
        workerData,
        argv: yield* commandLineArguments,
        execArgv: yield* bunArguments,
        ...(Object.keys(rewrittenFiles).length === 0
          ? {}
          : {
              preload: [
                fileURLToPath(
                  new URL(
                    "../../preload-rewritten-imports-into-each-worker/index.ts",
                    import.meta.url,
                  ),
                ),
              ],
            }),
      });
      const threadMessages = holdMessagesUntilListened(thread);
      const workerThread = {
        thread,
        client: yield* connectToWorker(threadMessages, rpcChannel.port2),
        exitDeferred: yield* Deferred.make<void>(),
        terminationRequested: false,
        terminateCalled: false,
        uncaughtError: Option.none(),
      } satisfies Parameters<typeof workersById.set>[1];
      workersById.set(workerId, workerThread);
      workerThread.thread.on("error", (error) => {
        workerThread.uncaughtError = Option.some({
          message: describeCauseMessage(error),
          location: Option.getOrNull(
            Option.flatMap(Schema.decodeUnknownOption(ErrorStack)(error), (withStack) =>
              Option.fromUndefinedOr(findUserLocationInStack(withStack.stack)),
            ),
          ),
        });
      });
      workerThread.thread.on("exit", (exitCode) => {
        Deferred.doneUnsafe(workerThread.exitDeferred, Effect.void);
        Queue.offerUnsafe(
          workerEvents,
          WorkerEvent.cases.ExitedWithCode.make({
            workerId,
            exitCode,
            terminationRequested: workerThread.terminationRequested,
            uncaughtError: Option.getOrNull(workerThread.uncaughtError),
          }),
        );
      });
    });
  const readRewrittenWorkerImports = yield* Effect.cached(
    moduleGraph.rewriteBareImports([
      fileURLToPath(new URL("../../worker-thread/index.ts", import.meta.url)),
      ...taskFilePaths,
    ]),
  );
  const startNextWorker = Effect.gen(function* () {
    const workerId = workersById.size;
    const rewrittenWorkerImports = yield* readRewrittenWorkerImports;
    if (yield* shutdown.isStopping) {
      return yield* Effect.interrupt;
    }
    yield* startWorker(workerId, rewrittenWorkerImports);
    return workerId;
  });
  const terminateWorker = (
    workerId: number,
    workerThread: Parameters<typeof workersById.set>[1],
    reason: string,
  ) =>
    Effect.gen(function* () {
      const isWorkerEnding =
        workerThread.terminateCalled || Deferred.isDoneUnsafe(workerThread.exitDeferred);
      if (isWorkerEnding) {
        return;
      }
      workerThread.terminateCalled = true;
      workerThread.terminationRequested = true;
      yield* Effect.logInfo(`worker terminated ${reason}`).pipe(
        Effect.annotateLogs(`${TASK_TRACE_LABEL_PREFIX}.worker`, workerId),
      );
      void workerThread.thread.terminate();
    }).pipe(Effect.uninterruptible);
  const requestExitOfAllWorkers = Effect.forEach(
    workersById.values(),
    (workerThread) =>
      Effect.gen(function* () {
        const isWorkerLeaving =
          workerThread.terminationRequested || Deferred.isDoneUnsafe(workerThread.exitDeferred);
        if (isWorkerLeaving) {
          return;
        }
        workerThread.terminationRequested = true;
        yield* FiberSet.run(
          callsToWorkers,
          workerThread.client
            .ExitWorker()
            .pipe(Effect.catchTag("RpcClientError", () => Effect.void)),
        );
      }),
    { discard: true },
  );
  const terminateAllWorkers = Effect.forEach(
    workersById,
    ([workerId, workerThread]) => terminateWorker(workerId, workerThread, FORCED_STOP_REASON),
    { discard: true },
  );
  const awaitWorkerShutdown = Effect.forEach(
    workersById,
    ([workerId, workerThread]) =>
      Effect.gen(function* () {
        const exited = yield* Deferred.await(workerThread.exitDeferred).pipe(
          Effect.timeoutOption(WORKER_EXIT_GRACE),
          Effect.raceFirst(shutdown.awaitForcedStop.pipe(Effect.as(Option.none()))),
        );
        if (Option.isSome(exited)) {
          return;
        }
        yield* terminateWorker(
          workerId,
          workerThread,
          (yield* shutdown.isForced)
            ? FORCED_STOP_REASON
            : `because it did not exit within ${Duration.format(WORKER_EXIT_GRACE)} of being asked to`,
        );
        yield* Deferred.await(workerThread.exitDeferred);
      }),
    { concurrency: "unbounded", discard: true },
  ).pipe(Effect.andThen(Queue.end(workerEvents)));
  const findReachableWorker = (workerId: number, rpcTag: string) =>
    Effect.gen(function* () {
      const workerThread = yield* Effect.fromNullishOr(workersById.get(workerId)).pipe(
        dieOnInvariantFailure("worker pool", "messages go to a started worker"),
      );
      const isWorkerGone =
        workerThread.terminationRequested || Deferred.isDoneUnsafe(workerThread.exitDeferred);
      if (isWorkerGone) {
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
  yield* Effect.addFinalizer(() => Effect.andThen(requestExitOfAllWorkers, awaitWorkerShutdown));
  return {
    events: Stream.fromQueue(workerEvents).pipe(
      Stream.provideService(Tracer.ParentSpan, parentSpan),
    ),
    selectWorkerIdForTask: (
      readWorkerLoad: (workerId: number) => {
        readonly occupyingTaskCount: number;
        readonly servingTaskCount: number;
      },
      recordAssignment: (workerId: number) => Effect.Effect<void>,
    ) =>
      Effect.gen(function* () {
        if (yield* shutdown.isStopping) {
          return yield* Effect.interrupt;
        }
        const usableWorkerIds = [...workersById]
          .filter(
            ([, workerThread]) =>
              !workerThread.terminationRequested &&
              !Deferred.isDoneUnsafe(workerThread.exitDeferred),
          )
          .map(([workerId]) => workerId);
        const usableWorkerLoads = usableWorkerIds.map(
          (usableWorkerId) => [usableWorkerId, readWorkerLoad(usableWorkerId)] as const,
        );
        const idleWorkerId = usableWorkerLoads.find(
          ([, load]) => load.occupyingTaskCount === 0 && load.servingTaskCount === 0,
        )?.[0];
        const workerId =
          idleWorkerId ??
          (workersById.size < workerLimit
            ? yield* startNextWorker
            : usableWorkerLoads.toSorted(
                ([, left], [, right]) =>
                  left.occupyingTaskCount - right.occupyingTaskCount ||
                  left.servingTaskCount - right.servingTaskCount,
              )[0]?.[0]);
        if (workerId === undefined) {
          yield* shutdown.awaitFailure;
          return yield* Effect.interrupt;
        }
        yield* recordAssignment(workerId);
        return workerId;
      }).pipe(workerSelectionSemaphore.withPermits(1)),
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
            Deferred.await(reachableWorker.exitDeferred).pipe(Effect.as("gone" as const)),
          ),
        );
      }),
    requestWorkerTermination: (workerId: number, reason: string) =>
      Effect.fromNullishOr(workersById.get(workerId)).pipe(
        dieOnInvariantFailure("worker pool", "a worker whose termination is requested was started"),
        Effect.flatMap((workerThread) => terminateWorker(workerId, workerThread, reason)),
      ),
    requestExitOfAllWorkers,
    terminateAllWorkers,
    awaitWorkerShutdown,
    startedWorkerCount: Effect.sync(() => workersById.size),
  };
});

export class WorkerPool extends Context.Service<
  WorkerPool,
  Effect.Success<typeof makeWorkerPool>
>()(`${TASK_PACKAGE.name}/WorkerPool`) {}
export const WorkerPoolLive = Layer.effect(WorkerPool, makeWorkerPool);
