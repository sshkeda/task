import { workerData } from "node:worker_threads";

import { Effect, Schema } from "effect";

import { RewrittenModuleUnloadable } from "../../../shared/module-graph-errors.ts";
import { WorkerData } from "../../../shared/rpc.ts";

export const loadRewrittenImports = Effect.gen(function* () {
  const rewrittenFiles = new Map(
    Object.entries((yield* Schema.decodeUnknownEffect(WorkerData)(workerData)).rewrittenFiles),
  );

  Bun.plugin({
    name: "task rewritten imports",
    target: "bun",
    setup: (build) => {
      build.onLoad(
        {
          filter: new RegExp(
            `^(?:${[...rewrittenFiles.keys()].map((path) => RegExp.escape(path)).join("|")})$`,
            "u",
          ),
        },
        (loaded) => {
          const rewrittenFile = rewrittenFiles.get(loaded.path);
          if (rewrittenFile === undefined) {
            throw new RewrittenModuleUnloadable({ filePath: loaded.path });
          }
          return rewrittenFile;
        },
      );
    },
  });
});
