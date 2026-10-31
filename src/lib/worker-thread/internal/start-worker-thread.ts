import { parentPort } from "node:worker_threads";

import * as BunCrypto from "@effect/platform-bun/BunCrypto";
import * as BunServices from "@effect/platform-bun/BunServices";
import { Layer, Effect, Exit } from "effect";
import { FetchHttpClient } from "effect/http";
import { OtlpExporter, OtlpSerialization } from "effect/observability";
import { RpcServer } from "effect/rpc";

import { runtimeConfigProviderLayer } from "../../../shared/read-environment-config.ts";
import { WorkerStateLive } from "../shared/provide-worker-state.ts";
import { serveTheMainThread } from "./answer-main-thread-messages.ts";
import { ParentPortWorkerRunnerLive } from "./serve-on-the-parent-port.ts";

const workerThreadLayer = WorkerStateLive.pipe(
  Layer.provideMerge(RpcServer.layerProtocolWorkerRunner),
  Layer.provide(ParentPortWorkerRunnerLive),
  Layer.provideMerge(
    Layer.mergeAll(
      BunServices.layer,
      BunCrypto.layerDigest,
      BunCrypto.layerSecureRandom,
      FetchHttpClient.layer,
      runtimeConfigProviderLayer,
      OtlpExporter.layerFlusher,
      OtlpSerialization.layerJson,
    ),
  ),
);

export const startWorkerThread = () => {
  if (parentPort !== null) {
    Effect.runFork(
      serveTheMainThread.pipe(
        Effect.provide(workerThreadLayer),
        Effect.scoped,
        Effect.onExit((exit) => Effect.sync(() => process.exit(Exit.isSuccess(exit) ? 0 : 1))),
      ),
    );
  }
};
