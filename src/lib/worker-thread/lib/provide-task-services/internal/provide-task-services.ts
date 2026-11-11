import { Effect, FileSystem, Logger, Path } from "effect";
import type { Schema } from "effect";
import { SecureRandom } from "effect/crypto";
import { ChildProcessSpawner } from "effect/process";

import { TASK_TRACE_LABEL_PREFIX } from "../../../../../shared/names-from-package-json.ts";
import { RUN_MARKERS_VARIABLE_NAME } from "../../../../../shared/read-environment-config.ts";
import {
  describeTaskMadeInBody,
  findFileThatMade,
  isMadeByThisCopy,
} from "../../../../../shared/record-tasks-made.ts";
import { TaskEvent } from "../../../../../shared/rpc.ts";
import type { StartTaskFiber } from "../../../../../shared/rpc.ts";
import { failUnlessStarted, TerminalCommands } from "../../../../../shared/run-in-terminal.ts";
import { markCommandOfTask } from "../../../../../shared/run-marker.ts";
import { Serving, Tasks } from "../../../../../shared/task-services.ts";
import { TaskMadeByAnotherCopy } from "../../../../errors/index.ts";
import type { makeTaskBodyRecord } from "../../../shared/define-task-body-record.ts";
import { encodeTaskOutput } from "../../../shared/encode-task-output.ts";
import { reportToMainThread, WorkerState } from "../../../shared/provide-worker-state.ts";
import { makeTaskLogger } from "./make-task-logger.ts";
import {
  provideTaskStdio,
  refuseStandardInputOutsideRootTask,
} from "./read-standard-input-from-main-thread.ts";
import { resolvePathsFromPackageDirectory } from "./resolve-paths-from-package-directory.ts";
import { runCommandsInPackageDirectory } from "./run-commands-in-package-directory.ts";
import { blameMissingWorkingDirectory } from "./spawn-commands-with-bun.ts";
import { spawnReportedProcess } from "./spawn-reported-process.ts";

const serve = (startMessage: typeof StartTaskFiber.Type, served: Schema.Json | undefined) =>
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
              isMadeByThisCopy(dependencyTaskDefinition)
                ? workerState.requestDependencyTaskResult(
                    dependencyTaskDefinition.name,
                    startMessage.taskName,
                    findFileThatMade(dependencyTaskDefinition),
                    describeTaskMadeInBody(dependencyTaskDefinition),
                  )
                : Effect.fail(
                    new TaskMadeByAnotherCopy({
                      taskName: startMessage.taskName,
                      requestedTaskName: dependencyTaskDefinition.name,
                    }),
                  ),
          }),
        ),
        Effect.provideService(TerminalCommands, (command) => {
          const refusal = refuseStandardInputOutsideRootTask(startMessage);
          if (refusal !== null) {
            return Effect.die(refusal);
          }
          const workingDirectory = path.resolve(startMessage.taskPackageDirectoryPath, command.cwd);
          const { runMarker } = workerState;
          return workerState
            .runInTerminal({
              ...command,
              env:
                runMarker === null
                  ? command.env
                  : {
                      ...command.env,
                      [RUN_MARKERS_VARIABLE_NAME]: markCommandOfTask(
                        runMarker,
                        startMessage.taskName,
                      ),
                    },
              id: SecureRandom.uuidV4Unsafe(),
              cwd: workingDirectory,
              cleanup: startMessage.cleanup,
            })
            .pipe(
              Effect.flatMap(failUnlessStarted(command.command)),
              Effect.catchTag("PlatformError", (failure) =>
                blameMissingWorkingDirectory(
                  command.command.slice(command.command.lastIndexOf("/") + 1),
                  startMessage.taskPackageDirectoryPath,
                  workingDirectory,
                  failure,
                ).pipe(workerState.provideRuntimeServices),
              ),
            );
        }),
        Effect.provideService(
          Serving,
          Serving.of({
            serve: (servedValue) =>
              encodeTaskOutput(startMessage.taskName, "served", servedValue).pipe(
                Effect.catchTag("TaskOutputInvalid", (error) => Effect.die(error)),
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
              workerState.taskLogHold,
              workerState.reportToMainThreadNow,
            ),
          ]),
        ),
        Effect.annotateLogs(`${TASK_TRACE_LABEL_PREFIX}.task`, startMessage.taskName),
        workerState.provideRuntimeServices,
      );
    });
