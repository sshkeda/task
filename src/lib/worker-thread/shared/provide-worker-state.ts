import { workerData } from "node:worker_threads";

import type * as BunServices from "@effect/platform-bun/BunServices";
import {
  Context,
  Deferred,
  Effect,
  Layer,
  Schema,
  Fiber,
  FiberMap,
  FiberSet,
  Option,
} from "effect";
import { constVoid } from "effect/Function";
import type { HttpClient } from "effect/http";
import { RpcClient, RpcSerialization, RpcWorker } from "effect/rpc";
import type { RpcMessage } from "effect/rpc";

import { setProjectDirectoryPath } from "../../../shared/find-user-locations-in-stack-traces.ts";
import { dieOnInvariantFailure } from "../../../shared/invariant-errors.ts";
import { standardErrorLinesToTheMainThreadDeferred } from "../../../shared/print-to-standard-error.ts";
import {
  MainThreadRpcs,
  MainThreadRpcRequest,
  MessageFromMainThreadRpcServer,
  TaskEvent,
  WorkerData,
  WorkerInitialMessage,
} from "../../../shared/rpc.ts";
import { TASK_PACKAGE } from "../../../shared/task-package.ts";
import { makeMainThreadAnswers } from "./await-main-thread-answers.ts";

const decodeMessageFromMainThreadRpcServer = Schema.decodeUnknownEffect(
  MessageFromMainThreadRpcServer,
);

const encodeTaskEvent = Schema.encodeEffect(TaskEvent);

const makeMainThreadRpcProtocol = (mainThreadPort: MessagePort) =>
  RpcClient.Protocol.make((writeResponse, clientIds) =>
    Effect.gen(function* () {
      const runInWorkerScope = yield* FiberSet.makeRuntime<never, void, never>();
      const onMessage = (event: { readonly data: unknown }) => {
        runInWorkerScope(
          decodeMessageFromMainThreadRpcServer(event.data).pipe(
            dieOnInvariantFailure("worker protocol", "main-thread answers match the RPC protocol"),
            Effect.flatMap((response) =>
              Effect.forEach(clientIds, (clientId) => writeResponse(clientId, response), {
                discard: true,
              }),
            ),
          ),
        );
      };
      yield* Effect.acquireRelease(
        Effect.sync(() => {
          mainThreadPort.addEventListener("message", onMessage);
          mainThreadPort.start();
        }),
        () =>
          Effect.sync(() => {
            mainThreadPort.removeEventListener("message", onMessage);
            mainThreadPort.close();
          }),
      );
      return {
        send: (_clientId: number, request: RpcMessage.FromClientEncoded) =>
          Effect.sync(() => {
            mainThreadPort.postMessage(request);
          }),
        supportsAck: false,
        supportsTransferables: false,
        codecFor: RpcSerialization.json.codecFor,
      };
    }),
  );

const makeTaskBodyFiberTracker = (
  reportToMainThread: (event: typeof TaskEvent.Type) => Effect.Effect<void>,
) =>
  Effect.gen(function* () {
    const taskBodyFibersByTaskName = yield* FiberMap.make<string>();
    const taskNamesAskedToStop = new Set<string>();
    const taskNamesWithStartedBody = new Set<string>();
    const lifetimeScope = yield* Effect.scope;
    return {
      startTaskBodyFiber: <Success, Failure, Services>(
        taskName: string,
        effect: Effect.Effect<Success, Failure, Services>,
      ) => FiberMap.run(taskBodyFibersByTaskName, taskName, effect).pipe(Effect.asVoid),
      requestTaskBodyInterruption: (taskName: string) =>
        Effect.gen(function* () {
          taskNamesAskedToStop.add(taskName);
          yield* reportToMainThread(
            taskNamesWithStartedBody.has(taskName)
              ? TaskEvent.cases.TaskFiberStartedCleanUp.make({ taskName })
              : TaskEvent.cases.TaskFiberStoppedBeforeBody.make({ taskName }),
          );
          const bodyFiber = FiberMap.getUnsafe(taskBodyFibersByTaskName, taskName);
          if (Option.isSome(bodyFiber)) {
            yield* Effect.forkIn(Fiber.interrupt(bodyFiber.value), lifetimeScope);
          }
        }),
      wasAskedToStop: (taskName: string) => Effect.sync(() => taskNamesAskedToStop.has(taskName)),
      recordBodyStarted: (taskName: string) =>
        Effect.sync(() => {
          taskNamesWithStartedBody.add(taskName);
        }),
      forkInWorkerScope: <Success, Failure, Services>(
        effect: Effect.Effect<Success, Failure, Services>,
      ) => Effect.forkIn(effect, lifetimeScope),
    };
  });

const connectToMainThread = Effect.gen(function* () {
  const decodedWorkerData = yield* Schema.decodeUnknownEffect(WorkerData)(workerData).pipe(
    dieOnInvariantFailure("worker startup", "worker data matches the worker protocol"),
  );
  yield* setProjectDirectoryPath(decodedWorkerData.projectDirectoryPath);
  const { mainThreadPort } = yield* RpcWorker.initialMessage(WorkerInitialMessage).pipe(
    dieOnInvariantFailure("worker startup", "the main thread hands over its RPC port first"),
  );
  const mainThread = yield* RpcClient.make(MainThreadRpcs, { disableTracing: true }).pipe(
    Effect.provideServiceEffect(RpcClient.Protocol, makeMainThreadRpcProtocol(mainThreadPort)),
  );
  const notifyMainThread = (request: Pick<typeof MainThreadRpcRequest.Type, "payload" | "tag">) => {
    mainThreadPort.postMessage(
      MainThreadRpcRequest.make({ ...request, id: "", headers: [], isNotification: true }),
    );
  };
  const reportToMainThreadThen = (event: typeof TaskEvent.Type, afterPosting: () => void) =>
    encodeTaskEvent(event).pipe(
      dieOnInvariantFailure("worker protocol", "reports reach the main thread"),
      Effect.flatMap((payload) =>
        Effect.sync(() => {
          notifyMainThread({ tag: "ReportTaskEvent", payload });
          afterPosting();
        }),
      ),
    );
  const reportToMainThread = (event: typeof TaskEvent.Type) =>
    reportToMainThreadThen(event, constVoid);
  const reportBodyStarted = (taskName: string) =>
    reportToMainThreadThen(TaskEvent.cases.TaskFiberStarted.make({ taskName }), () => {
      Atomics.add(decodedWorkerData.postedBodyStartCount, 0, 1);
    });
  const runInWorkerScope = yield* FiberSet.makeRuntime<never, void, never>();
  const reportToMainThreadNow = (event: typeof TaskEvent.Type) => {
    runInWorkerScope(reportToMainThread(event));
  };
  const recordTraceSpanNow = (spanJson: string) => {
    notifyMainThread({ tag: "RecordTraceSpan", payload: { spanJson } });
  };
  yield* Deferred.succeed(standardErrorLinesToTheMainThreadDeferred, (logLevel, text) =>
    reportToMainThread(TaskEvent.cases.StandardErrorLineRequested.make({ logLevel, text })),
  );
  return {
    ...decodedWorkerData,
    mainThread,
    reportToMainThread,
    reportBodyStarted,
    reportToMainThreadNow,
    recordTraceSpanNow,
  };
});

const makeWorkerState = Effect.gen(function* () {
  const mainThreadConnection = yield* connectToMainThread;
  const taskBodyFibers = yield* makeTaskBodyFiberTracker(mainThreadConnection.reportToMainThread);
  const runtimeServices = yield* Effect.context<BunServices.BunServices | HttpClient.HttpClient>();
  const mainThreadAnswers = yield* makeMainThreadAnswers(
    mainThreadConnection.mainThread,
    mainThreadConnection.reportToMainThread,
    taskBodyFibers.forkInWorkerScope,
  );
  return {
    ...mainThreadConnection,
    provideRuntimeServices: <Success, Failure, Services>(
      effect: Effect.Effect<Success, Failure, Services>,
    ) => Effect.provide(effect, runtimeServices),
    ...taskBodyFibers,
    ...mainThreadAnswers,
  };
});

export class WorkerState extends Context.Service<
  WorkerState,
  Effect.Success<typeof makeWorkerState>
>()(`${TASK_PACKAGE.name}/WorkerState`) {}

export const WorkerStateLive = Layer.effect(WorkerState, makeWorkerState);

export const reportToMainThread = (event: typeof TaskEvent.Type) =>
  WorkerState.use((workerState) => workerState.reportToMainThread(event));
