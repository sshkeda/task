import { Effect, FileSystem, Logger, Path } from "effect";
import type { Schema } from "effect";
import { SecureRandom } from "effect/crypto";
import { ChildProcessSpawner } from "effect/process";

import {
  CommandRecapFiles,
  makeCommandRecapPath,
  readCommandRecap,
} from "../../../../../shared/command-recap-file.ts";
import {
  CommandCalledAt,
  CurrentTaskName,
  TaskLines,
} from "../../../../../shared/mark-task-worker-thread-contexts.ts";
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
import { makeTaskLines, makeTaskLogger } from "./make-task-logger.ts";
import {
  provideTaskStdio,
  refuseStandardInputOutsideRootTask,
} from "./read-standard-input-from-main-thread.ts";
import { resolvePathsFromRunDirectory } from "./resolve-paths-from-run-directory.ts";
import { runCommandsInRunDirectory } from "./run-commands-in-run-directory.ts";
import {
  checkWorkingDirectoryBeforeSpawn,
  recheckWorkingDirectoryAfterSpawnFailure,
} from "./spawn-commands-with-bun.ts";
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
      const { fileSystem, path } = resolvePathsFromRunDirectory(
        yield* Path.Path,
        yield* FileSystem.FileSystem,
        startMessage.runDirectoryPath,
      );
      return yield* effect.pipe(
        provideTaskStdio(startMessage, workerState),
        Effect.provideService(Path.Path, path),
        Effect.provideService(FileSystem.FileSystem, fileSystem),
        Effect.provideService(
          CommandRecapFiles,
          CommandRecapFiles.of({
            makeCommandRecapPath: makeCommandRecapPath(
              fileSystem,
              path,
              startMessage.commandRecapDirectoryPath,
            ),
            readCommandRecap: (recapPath) => readCommandRecap(fileSystem, recapPath),
          }),
        ),
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
          const workingDirectory = path.resolve(startMessage.runDirectoryPath, command.cwd);
          const { runMarker } = workerState;
          const programName = command.command.slice(command.command.lastIndexOf("/") + 1);
          const commandId = SecureRandom.uuidV4Unsafe();
          const reportCommandStarted = Effect.gen(function* () {
            yield* workerState.reportToMainThread(
              TaskEvent.cases.TaskFiberStartedTerminalCommand.make({
                taskName: startMessage.taskName,
                commandId,
                programName,
                calledAt: yield* CommandCalledAt,
              }),
            );
          });
          const reportCommandEnded = workerState.reportToMainThread(
            TaskEvent.cases.TaskFiberEndedTerminalCommand.make({
              taskName: startMessage.taskName,
              commandId,
            }),
          );
          return checkWorkingDirectoryBeforeSpawn(
            programName,
            startMessage.runDirectoryPath,
            workingDirectory,
          ).pipe(
            workerState.provideRuntimeServices,
            Effect.andThen(
              reportCommandStarted.pipe(
                Effect.andThen(
                  workerState.runInTerminal({
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
                    id: commandId,
                    cwd: workingDirectory,
                    cleanup: startMessage.cleanup,
                  }),
                ),
                Effect.flatMap(failUnlessStarted(command.command)),
                Effect.catchTag("PlatformError", (spawnFailure) =>
                  recheckWorkingDirectoryAfterSpawnFailure(
                    programName,
                    startMessage.runDirectoryPath,
                    workingDirectory,
                    spawnFailure,
                  ).pipe(workerState.provideRuntimeServices),
                ),
                Effect.ensuring(reportCommandEnded),
              ),
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
          runCommandsInRunDirectory(path, startMessage.runDirectoryPath, (command) =>
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
        Effect.provideService(
          TaskLines,
          makeTaskLines(
            startMessage,
            workerState.taskLogHold,
            workerState.taskOutputBudget,
            workerState.reportToMainThreadNow,
            workerState.postStreamOutputNow,
          ),
        ),
        Effect.provideService(CurrentTaskName, startMessage.taskName),
        workerState.provideRuntimeServices,
      );
    });
