import { Effect, FileSystem, Logger, Path } from "effect";
import { ChildProcessSpawner } from "effect/process";

import { TASK_TRACE_LABEL_PREFIX } from "../../../../../shared/names-from-package-json.ts";
import { TaskEvent } from "../../../../../shared/rpc.ts";
import type { StartTaskFiber } from "../../../../../shared/rpc.ts";
import { Serving, Tasks } from "../../../../../shared/task-services.ts";
import { decodeTaskResultJson } from "../../../shared/decode-task-result-json.ts";
import type { OptionalJson } from "../../../shared/decode-task-result-json.ts";
import type { makeTaskBodyRecord } from "../../../shared/define-task-body-record.ts";
import { reportToMainThread, WorkerState } from "../../../shared/provide-worker-state.ts";
import { makeTaskLogger } from "./make-task-logger.ts";
import { provideTaskStdio } from "./read-standard-input-from-main-thread.ts";
import { resolvePathsFromPackageDirectory } from "./resolve-paths-from-package-directory.ts";
import { runCommandsInPackageDirectory } from "./run-commands-in-package-directory.ts";
import { spawnReportedProcess } from "./spawn-reported-process.ts";

const serve = (startMessage: typeof StartTaskFiber.Type, served: typeof OptionalJson.Type) =>
  reportToMainThread(
    TaskEvent.cases.TaskFiberServed.make({
      taskName: startMessage.taskName,
      ...(served === undefined ? {} : { value: served }),
    }),
  ).pipe(Effect.andThen(Effect.never));

export const provideTaskServices =
  (
    startMessage: typeof StartTaskFiber.Type,
    workerState: WorkerState["Service"],
    body: ReturnType<typeof makeTaskBodyRecord>,
  ) =>
  <Success, Failure, Services>(effect: Effect.Effect<Success, Failure, Services>) =>
    Effect.gen(function* () {
      const { fileSystem, path } = resolvePathsFromPackageDirectory(
        yield* Path.Path,
        yield* FileSystem.FileSystem,
        startMessage.taskPackageDirectoryPath,
      );
      return yield* effect.pipe(
        provideTaskStdio(startMessage, workerState),
        Effect.provideService(Path.Path, path),
        Effect.provideService(FileSystem.FileSystem, fileSystem),
        Effect.provideService(
          Tasks,
          Tasks.of({
            requestResult: (dependencyTaskDefinition) =>
              workerState.requestDependencyTaskResult(
                dependencyTaskDefinition.name,
                startMessage.taskName,
              ),
          }),
        ),
        Effect.provideService(
          Serving,
          Serving.of({
            serve: (servedValue) =>
              decodeTaskResultJson(startMessage.taskName, "served", servedValue).pipe(
                Effect.flatMap((served) => serve(startMessage, served)),
                Effect.provideService(WorkerState, workerState),
              ),
          }),
        ),
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          runCommandsInPackageDirectory(path, startMessage.taskPackageDirectoryPath, (command) =>
            spawnReportedProcess(startMessage, body, command).pipe(
              Effect.provideService(WorkerState, workerState),
            ),
          ),
        ),
        Effect.provideService(
          Logger.CurrentLoggers,
          new Set([
            makeTaskLogger(
              startMessage,
              body,
              workerState.taskLogHold,
              workerState.reportToMainThreadNow,
            ),
          ]),
        ),
        Effect.annotateLogs(`${TASK_TRACE_LABEL_PREFIX}.task`, startMessage.taskName),
        workerState.provideRuntimeServices,
      );
    });
