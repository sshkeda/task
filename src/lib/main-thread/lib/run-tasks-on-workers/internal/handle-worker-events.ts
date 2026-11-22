import {
  Cause,
  Clock,
  Deferred,
  Effect,
  Exit,
  Fiber,
  FiberSet,
  Match,
  Option,
  Schema,
} from "effect";

import {
  dieOnInvariantFailure,
  InternalInvariantBroken,
} from "../../../../../shared/invariant-errors.ts";
import { labelLine, labelLines } from "../../../../../shared/label-lines.ts";
import { printLine } from "../../../../../shared/print-to-standard-error.ts";
import { WorkerExited } from "../../../../../shared/process-errors.ts";
import { TaskFiberResult, WorkerLine } from "../../../../../shared/rpc.ts";
import type { TaskMadeInBody } from "../../../../../shared/rpc.ts";
import {
  TaskCleanupFailed,
  ServedTaskFailed,
  TaskBodyFailed,
} from "../../../../../shared/task-errors.ts";
import type { AwaitedTasksFailure, TaskRequestFailure } from "../../../../../shared/task-errors.ts";
import { WorkerEnding } from "../../../shared/define-worker-stage.ts";
import { failRun, failTask } from "../../../shared/fail-run.ts";
import { WorkerTerminatedForAnotherTask } from "../../../shared/main-thread-errors.ts";
import { ProcessGroups } from "../../../shared/provide-process-groups.ts";
import { RunRootTaskConfig } from "../../../shared/provide-run-root-task-config.ts";
import { StopState } from "../../../shared/provide-stop-state.ts";
import { FromWorker, WorkerEvent, WorkerPool } from "../../../shared/provide-worker-pool.ts";
import { reportFailure } from "../../../shared/report-task-failure.ts";
import {
  addStackFrames,
  endClosedTask,
  keepStackFrames,
  printStreamOutput,
  printStreamOutputs,
  printTaskLines,
  printTaskOutputs,
  settleHeldOutput,
} from "../../../shared/write-task-output-files.ts";
import { TraceFile } from "../../../shared/write-trace-file.ts";
import { findAssignedWorkerId, isUnfinishedOnWorker } from "../shared/define-task-run-state.ts";
import type { makeTaskRunsState } from "../shared/define-task-run-state.ts";
import { isEndedByTheRun, TaskRunEvent, TaskRunOutcome } from "../shared/define-task-run-status.ts";
import { TaskStopEvent } from "../shared/define-task-stop-stage.ts";

const sendDependencyResultToWorker = Effect.fn(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  requestDependencyTaskResult: (
    dependencyTaskName: string,
    requestingTaskName: string,
    dependencyTaskFilePath: string | null,
    dependencyTaskMadeInBody: typeof TaskMadeInBody.Type | null,
  ) => Effect.Effect<Schema.Json | undefined, typeof TaskRequestFailure.Type>,
  messageFromWorker: typeof FromWorker.cases.TaskFiberRequestedTask.Type,
) {
  const requestingTaskRun = yield* Effect.fromNullishOr(
    taskRunsState.runsByTaskName.get(messageFromWorker.taskName),
  ).pipe(dieOnInvariantFailure("task requests", "a task that requests another task has a run"));
  const answer = yield* requestDependencyTaskResult(
    messageFromWorker.dependencyTaskName,
    messageFromWorker.taskName,
    messageFromWorker.dependencyTaskFilePath,
    messageFromWorker.dependencyTaskMadeInBody,
  ).pipe(
    Effect.exit,
    Effect.tap((dependencyExit) => {
      const dependencyWasStopped =
        Exit.isFailure(dependencyExit) && Cause.hasInterruptsOnly(dependencyExit.cause);
      return dependencyWasStopped ? requestingTaskRun.stop.awaitCleanupStarted : Effect.void;
    }),
    Effect.raceFirst(
      Deferred.await(requestingTaskRun.bodyFinishedDeferred).pipe(Effect.andThen(Effect.interrupt)),
    ),
  );
  requestingTaskRun.dependencyRequestIdsAwaitingAnswer.delete(
    messageFromWorker.dependencyRequestId,
  );
  const workerId = findAssignedWorkerId(requestingTaskRun.status);
  if (workerId === null) {
    return;
  }

  yield* (yield* WorkerPool).answerDependencyRequest(messageFromWorker.dependencyRequestId, answer);
});

const isTaskBodyFailed = Schema.is(TaskBodyFailed);

const isWorkerExited = Schema.is(WorkerExited);

const isTaskCleanupFailed = Schema.is(TaskCleanupFailed);

const isWorkerTerminatedForAnotherTask = Schema.is(WorkerTerminatedForAnotherTask);

const isCleanupEndingFailure = Schema.is(
  Schema.Union([TaskCleanupFailed, WorkerExited, WorkerTerminatedForAnotherTask]),
);

const isFailedResult = Schema.is(TaskFiberResult.cases.Failed);

const isCleanupFailedResult = Schema.is(TaskFiberResult.cases.CleanupFailed);

const keepStackFramesOf = (taskName: string, result: typeof TaskFiberResult.Type) => {
  if (isCleanupFailedResult(result)) {
    return result.stackFrames === undefined
      ? Effect.void
      : addStackFrames(taskName, result.stackFrames);
  }
  const stackFrames = isFailedResult(result) ? result.stackFrames : undefined;
  return stackFrames === undefined ? Effect.void : keepStackFrames(taskName, stackFrames);
};

const bodyEnding = (taskName: string, result: typeof TaskFiberResult.Type) =>
  TaskFiberResult.match(result, {
    Returned: (
      returned,
    ): Exit.Exit<
      typeof returned,
      | TaskBodyFailed
      | TaskCleanupFailed
      | WorkerExited
      | WorkerTerminatedForAnotherTask
      | typeof AwaitedTasksFailure.Type
    > => Exit.succeed(returned),
    Failed: (failed) =>
      Exit.fail(
        new TaskBodyFailed({
          taskName,
          failures: failed.failures,
          details: failed.details,
          failedCommandExitCode: failed.failedCommandExitCode,
          awaitedTaskNames: failed.awaitedTaskNames,
        }),
      ),
    AwaitedTasksFailed: (awaitedTasksFailed) => Exit.fail(awaitedTasksFailed.failure),
    CleanupFailed: (cleanupFailure) =>
      Exit.fail(
        new TaskCleanupFailed({
          taskName,
          failures: cleanupFailure.failures,
          details: cleanupFailure.details,
        }),
      ),
    Interrupted: () => Exit.interrupt(),
  });

const settleBodyOutcome = Effect.fn(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  taskName: string,
  ending: Exit.Exit<
    Effect.Success<ReturnType<typeof bodyEnding>>,
    Effect.Error<ReturnType<typeof bodyEnding>>
  >,
) {
  const taskRun = taskRunsState.runsByTaskName.get(taskName);
  if (taskRun === undefined) {
    return;
  }
  const failure = Exit.findErrorOption(ending);
  const failedAfterTaskEnded = TaskRunOutcome.guards.Cancelled(taskRun.outcome)
    ? Option.filter(
        failure,
        (bodyFailure) =>
          isTaskBodyFailed(bodyFailure) || isWorkerTerminatedForAnotherTask(bodyFailure),
      )
    : Option.filter(failure, (bodyFailure) =>
        isEndedByTheRun(taskRun.outcome) ? isTaskBodyFailed(bodyFailure) : false,
      );
  Deferred.doneUnsafe(taskRun.bodyOutcomeDeferred, ending);
  const { rootTaskName } = yield* RunRootTaskConfig;
  const runEndingRootTaskBodyFailure = Option.filter(failure, () => taskName === rootTaskName);
  if (Option.isSome(runEndingRootTaskBodyFailure)) {
    yield* (yield* StopState).recordFirstFailure(runEndingRootTaskBodyFailure.value);
  }
  if (Option.isSome(failedAfterTaskEnded)) {
    yield* failTask(failedAfterTaskEnded.value, taskRunsState.markFailed);
  }
});

const reportFailureAfterOutcome = Effect.fn(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  taskName: string,
  ending: Exit.Exit<
    Effect.Success<ReturnType<typeof bodyEnding>>,
    Effect.Error<ReturnType<typeof bodyEnding>>
  >,
  served: boolean,
) {
  const failure = Exit.findErrorOption(ending);
  if (Option.isNone(failure)) {
    return;
  }
  const stopState = yield* StopState;
  const failureEndsRun =
    isTaskCleanupFailed(failure.value) ||
    isWorkerExited(failure.value) ||
    isWorkerTerminatedForAnotherTask(failure.value);
  if (failureEndsRun) {
    yield* stopState.recordFirstFailure(failure.value);
    return yield* failTask(failure.value, taskRunsState.markFailed);
  }
  if (!served) {
    return;
  }

  const servedTaskFailed = new ServedTaskFailed({
    taskName,
    failures: isTaskBodyFailed(failure.value)
      ? failure.value.failures
      : reportFailure(failure.value),
    details: isTaskBodyFailed(failure.value) ? failure.value.details : undefined,
  });
  yield* stopState.recordFirstFailure(servedTaskFailed);
  yield* failTask(servedTaskFailed, taskRunsState.markFailed);
});

const endTask = Effect.fn(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  taskName: string,
  ending: Exit.Exit<
    Effect.Success<ReturnType<typeof bodyEnding>>,
    Effect.Error<ReturnType<typeof bodyEnding>>
  >,
) {
  const action = yield* taskRunsState.moveTaskRun(taskName, TaskRunEvent.cases.End.make({}));
  if (action === "ignored") {
    return;
  }
  const cleanupFailure = Option.filter(Exit.findErrorOption(ending), isCleanupEndingFailure);
  if (Option.isSome(cleanupFailure)) {
    yield* taskRunsState.recordCleanupFailure(taskName, cleanupFailure.value);
  }
  taskRunsState.refreshBodyClocks(taskName, yield* Clock.currentTimeMillis);
  const closes = action === "close" || action === "closeFailed";
  if (closes) {
    yield* printTaskLines(taskName, labelLine("closed", taskName, ""));
  }
  yield* action === "settleOutcome"
    ? settleBodyOutcome(taskRunsState, taskName, ending)
    : reportFailureAfterOutcome(taskRunsState, taskName, ending, action === "close");
  if (closes) {
    yield* endClosedTask(taskName);
  }
  const taskRun = taskRunsState.runsByTaskName.get(taskName);
  const workerId = taskRun === undefined ? null : findAssignedWorkerId(taskRun.status);
  if (workerId !== null) {
    yield* WorkerPool.use((workerPool) => workerPool.recordTaskEnded(workerId));
  }
  yield* taskRunsState.endTaskSpanAfterWorker(taskName);
});

const failTasksOnWorker = Effect.fn(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  taskNames: readonly string[],
  makeFailure: (taskName: string) => WorkerExited | WorkerTerminatedForAnotherTask,
) {
  const processGroups = yield* ProcessGroups;
  yield* Effect.forEach(taskNames, processGroups.killForTask, { discard: true });
  yield* Effect.forEach(
    taskNames,
    (taskName) => endTask(taskRunsState, taskName, Exit.fail(makeFailure(taskName))),
    { discard: true },
  );
});

const endTasksOfExitedWorker = Effect.fn(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  workerEvent: typeof WorkerEvent.cases.ExitedWithCode.Type,
) {
  const unfinishedTaskNames = [...taskRunsState.runsByTaskName]
    .filter(([, taskRun]) => isUnfinishedOnWorker(taskRun, workerEvent.workerId))
    .map(([taskName]) => taskName);
  const workerExit = {
    workerId: workerEvent.workerId,
    exitCode: workerEvent.exitCode,
    uncaughtError: workerEvent.uncaughtError,
  };
  const stopTasks = (taskNames: readonly string[]) =>
    Effect.forEach(taskNames, (taskName) => endTask(taskRunsState, taskName, Exit.interrupt()), {
      discard: true,
    });
  yield* WorkerEnding.match(workerEvent.ending, {
    Crashed: () =>
      unfinishedTaskNames.length === 0
        ? failRun(new WorkerExited({ taskName: null, ...workerExit }), taskRunsState.markFailed)
        : failTasksOnWorker(
            taskRunsState,
            unfinishedTaskNames,
            (taskName) => new WorkerExited({ taskName, ...workerExit }),
          ),
    ExitedOnRequest: () => stopTasks(unfinishedTaskNames),
    Terminated: ({ terminatingTaskName }) =>
      terminatingTaskName === null
        ? stopTasks(unfinishedTaskNames)
        : failTasksOnWorker(
            taskRunsState,
            unfinishedTaskNames.filter((taskName) => taskName !== terminatingTaskName),
            (taskName) => new WorkerTerminatedForAnotherTask({ taskName, terminatingTaskName }),
          ).pipe(
            Effect.andThen(
              stopTasks(unfinishedTaskNames.filter((taskName) => taskName === terminatingTaskName)),
            ),
          ),
  });
});

const onMessageAboutTask = Effect.fn(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  requestDependencyTaskResult: Parameters<typeof sendDependencyResultToWorker>[1],
  messageFromWorker: Exclude<
    typeof FromWorker.Type,
    | typeof FromWorker.cases.StandardErrorLineRequested.Type
    | typeof FromWorker.cases.TraceLogRecordRequested.Type
  >,
) {
  const processGroups = yield* ProcessGroups;
  const clock = yield* Clock.Clock;
  const taskRun = yield* Effect.fromNullishOr(
    taskRunsState.runsByTaskName.get(messageFromWorker.taskName),
  ).pipe(dieOnInvariantFailure("worker protocol", "messages refer to a started task"));
  yield* Match.value(messageFromWorker).pipe(
    Match.tagsExhaustive({
      TaskFiberStartedProcess: (started) =>
        Effect.sync(() => {
          taskRun.runningCommandsById.set(`process ${started.processId}`, {
            programName: started.programName,
            calledAt: started.calledAt,
            startedAtMilliseconds: clock.currentTimeMillisUnsafe(),
          });
        }).pipe(
          Effect.andThen(
            processGroups.record(started.processId, started.taskName, started.startTime),
          ),
          Effect.andThen(
            WorkerPool.use((workerPool) => workerPool.acknowledgeProcessRecorded(started.recordId)),
          ),
        ),
      TaskFiberProcessLeaderExited: (exited) =>
        Effect.sync(() => {
          taskRun.runningCommandsById.delete(`process ${exited.processId}`);
        }).pipe(
          Effect.andThen(
            processGroups.recordLeaderExit(
              exited.processId,
              exited.taskName,
              exited.membersAtLeaderExit,
            ),
          ),
        ),
      TaskFiberStartedTerminalCommand: (started) =>
        Effect.sync(() => {
          taskRun.runningCommandsById.set(`terminal ${started.commandId}`, {
            programName: started.programName,
            calledAt: started.calledAt,
            startedAtMilliseconds: clock.currentTimeMillisUnsafe(),
          });
        }),
      TaskFiberEndedTerminalCommand: (ended) =>
        Effect.sync(() => {
          taskRun.runningCommandsById.delete(`terminal ${ended.commandId}`);
        }),
      TaskFiberClaimingProcessReports: (claiming) =>
        processGroups
          .claimProcessesToReport(claiming.processes)
          .pipe(
            Effect.flatMap((claimed) =>
              WorkerPool.use((workerPool) =>
                workerPool.answerReportClaim(claiming.claimId, claimed),
              ),
            ),
          ),
      TaskFiberLoading: () =>
        taskRunsState.moveTaskRun(messageFromWorker.taskName, TaskRunEvent.cases.Load.make({})),
      TaskFiberStarted: () =>
        taskRunsState.moveTaskRun(
          messageFromWorker.taskName,
          TaskRunEvent.cases.StartBody.make({}),
        ),
      TaskFiberBodyEnded: (ended) =>
        taskRunsState.openCleanupSpan(ended.taskName, ended.endedAtUnixNano),
      TaskFiberStartedCleanUp: () => taskRun.stop.move(TaskStopEvent.cases.StartCleanup.make({})),
      TaskFiberStoppedBeforeBody: () =>
        taskRun.stop.move(TaskStopEvent.cases.StopBeforeBody.make({})),
      TaskFiberRequestedTask: (requested) => {
        taskRun.bodyRequestedTaskNames.add(requested.dependencyTaskName);
        taskRun.dependencyRequestIdsAwaitingAnswer.add(requested.dependencyRequestId);
        return FiberSet.run(
          taskRunsState.taskFibers,
          sendDependencyResultToWorker(taskRunsState, requestDependencyTaskResult, requested),
          { startImmediately: false },
        ).pipe(
          Effect.flatMap((requestFiber) =>
            Effect.sync(() => {
              taskRun.requestFibersByDependencyRequestId.set(
                requested.dependencyRequestId,
                requestFiber,
              );
              requestFiber.addObserver(() => {
                taskRun.requestFibersByDependencyRequestId.delete(requested.dependencyRequestId);
                taskRunsState.refreshBodyClocks(
                  requested.taskName,
                  clock.currentTimeMillisUnsafe(),
                );
              });
            }),
          ),
        );
      },
      TaskFiberRequestedStandardInput: (requested) =>
        taskRunsState.rootTaskStandardInput.answerNextRead(requested.taskName, (read) =>
          WorkerPool.use((workerPool) =>
            workerPool.answerStandardInputRead(requested.readId, read),
          ),
        ),
      TaskFiberReceivedAwaitedFailure: (received) =>
        Effect.sync(() => {
          taskRunsState.recordFailureReachedAwaitingTask(received.awaitedTaskName);
        }),
      TaskFiberCancelledRequest: (cancelled) => {
        taskRun.dependencyRequestIdsAwaitingAnswer.delete(cancelled.dependencyRequestId);
        const requestFiber = taskRun.requestFibersByDependencyRequestId.get(
          cancelled.dependencyRequestId,
        );
        return requestFiber === undefined ? Effect.void : Fiber.interrupt(requestFiber);
      },
      TaskFiberFailedBeforeCleanup: (failed) =>
        Effect.gen(function* () {
          yield* taskRun.stop.move(TaskStopEvent.cases.FailBeforeCleanup.make({}));
          yield* keepStackFramesOf(failed.taskName, failed.failure);
          const action = yield* taskRunsState.moveTaskRun(
            failed.taskName,
            TaskRunEvent.cases.FailBeforeCleanup.make({}),
          );
          if (action === "settleOutcome") {
            const ending = yield* Effect.exit(bodyEnding(failed.taskName, failed.failure));
            yield* settleBodyOutcome(taskRunsState, failed.taskName, ending);
          }
          if (action === "reportServedFailure") {
            const ending = yield* Effect.exit(bodyEnding(failed.taskName, failed.failure));
            yield* reportFailureAfterOutcome(taskRunsState, failed.taskName, ending, true);
          }
        }),
      TaskFiberServed: (served) =>
        Effect.gen(function* () {
          const action = yield* taskRunsState.moveTaskRun(
            served.taskName,
            TaskRunEvent.cases.Serve.make({}),
          );
          if (action === "settleOutcome") {
            yield* taskRunsState.markServed(served.taskName);
            Deferred.doneUnsafe(taskRun.bodyOutcomeDeferred, Exit.succeed(served));
          }
        }),
      TaskFiberEnded: (ended) =>
        keepStackFramesOf(ended.taskName, ended.result).pipe(
          Effect.andThen(Effect.exit(bodyEnding(ended.taskName, ended.result))),
          Effect.flatMap((ending) => endTask(taskRunsState, messageFromWorker.taskName, ending)),
        ),
    }),
  );
  taskRunsState.refreshBodyClocks(messageFromWorker.taskName, yield* Clock.currentTimeMillis);
});

const printWorkerLine = (workerId: number, line: typeof WorkerLine.Type) =>
  WorkerLine.match(line, {
    Logged: ({ taskName, text }) => printTaskOutputs(taskName, [text]),
    StreamWrote: ({ taskName, streamId, pieces, heldBytes, printedTo }) =>
      WorkerPool.use((workerPool) =>
        printStreamOutput(
          taskName,
          streamId,
          pieces,
          workerPool.releaseTaskOutputBytes(workerId, heldBytes),
          printedTo,
        ),
      ),
    HeldOutputSettled: ({ taskName, streamIds, outcome }) =>
      settleHeldOutput(taskName, streamIds, outcome),
    Reported: ({ taskName, label, text }) =>
      printTaskLines(taskName, labelLines(label, taskName, text).join("\n")),
    Printed: ({ taskName, text }) =>
      taskName === null ? printLine(text) : printTaskLines(taskName, text),
  });

const printLoggedLine = (
  workerId: number,
  requested: typeof FromWorker.cases.StandardErrorLineRequested.Type,
) =>
  Effect.gen(function* () {
    const traceFile = yield* TraceFile;
    if (requested.traceLogRecordJson !== undefined) {
      traceFile.writeWorkerLogRecord(requested.traceLogRecordJson);
    }
    return yield* printWorkerLine(workerId, requested.line);
  });

const isMessageReceived = WorkerEvent.isAnyOf(["MessageReceived"]);

const isConsoleLineWrittenEvent = WorkerEvent.isAnyOf(["ConsoleLineWritten"]);

const isExitedWithCode = WorkerEvent.isAnyOf(["ExitedWithCode"]);

const isMessageUnreadable = WorkerEvent.isAnyOf(["MessageUnreadable"]);

const isStandardErrorLineRequested = FromWorker.isAnyOf(["StandardErrorLineRequested"]);

const isStreamWrote = WorkerLine.isAnyOf(["StreamWrote"]);

const isLogged = WorkerLine.isAnyOf(["Logged"]);

const LOGGED_RUN_CHARACTER_LIMIT = 4_194_304;

const loggedLineOf = (workerEvent: typeof WorkerEvent.Type) => {
  if (!isMessageReceived(workerEvent)) {
    return null;
  }
  const { message } = workerEvent;
  const isLoggedLine = isStandardErrorLineRequested(message) && isLogged(message.line);
  return isLoggedLine
    ? {
        taskName: message.line.taskName,
        text: message.line.text,
        traceLogRecordJson: message.traceLogRecordJson,
      }
    : null;
};

const printLoggedRun = (loggedLines: readonly NonNullable<ReturnType<typeof loggedLineOf>>[]) =>
  Effect.gen(function* () {
    const traceFile = yield* TraceFile;
    for (const { traceLogRecordJson } of loggedLines) {
      if (traceLogRecordJson !== undefined) {
        traceFile.writeWorkerLogRecord(traceLogRecordJson);
      }
    }
    const [firstLine] = loggedLines;
    if (firstLine !== undefined) {
      yield* printTaskOutputs(
        firstLine.taskName,
        loggedLines.map(({ text }) => text),
      );
    }
  });

const streamWriteOf = (workerEvent: typeof WorkerEvent.Type) => {
  if (isConsoleLineWrittenEvent(workerEvent)) {
    return {
      ...workerEvent,
      printedTo: "terminal and file" as const,
      traceLogRecordJson: undefined,
      isConsoleLine: true,
    };
  }
  if (!isMessageReceived(workerEvent)) {
    return null;
  }
  const { message, workerId } = workerEvent;
  const isStreamWrite = isStandardErrorLineRequested(message) && isStreamWrote(message.line);
  return isStreamWrite
    ? {
        workerId,
        ...message.line,
        traceId: null,
        spanId: null,
        writtenAtMilliseconds: 0,
        traceLogRecordJson: message.traceLogRecordJson,
        isConsoleLine: false,
      }
    : null;
};

const writeTraceRecordsOf = (
  traceFile: TraceFile["Service"],
  writes: readonly NonNullable<ReturnType<typeof streamWriteOf>>[],
) => {
  const run = { first: Option.none<(typeof writes)[number]>(), texts: new Array<string>() };
  const writeRun = () => {
    if (Option.isSome(run.first)) {
      traceFile.writeConsoleLogRecord(run.first.value, run.texts.join("\n"));
    }
    run.first = Option.none();
    run.texts = [];
  };
  for (const write of writes) {
    if (!write.isConsoleLine) {
      writeRun();
      if (write.traceLogRecordJson !== undefined) {
        traceFile.writeWorkerLogRecord(write.traceLogRecordJson);
      }
      continue;
    }
    const first = Option.getOrUndefined(run.first);
    const continuesRun =
      first?.taskName === write.taskName &&
      first.streamId === write.streamId &&
      first.traceId === write.traceId &&
      first.spanId === write.spanId &&
      first.writtenAtMilliseconds === write.writtenAtMilliseconds;
    if (!continuesRun) {
      writeRun();
      run.first = Option.some(write);
    }
    for (const piece of write.pieces) {
      run.texts.push(piece.text);
    }
  }
  writeRun();
};

const printStreamWrites = (
  workerId: number,
  writes: readonly NonNullable<ReturnType<typeof streamWriteOf>>[],
) =>
  Effect.gen(function* () {
    writeTraceRecordsOf(yield* TraceFile, writes);
    const workerPool = yield* WorkerPool;
    yield* printStreamOutputs(
      writes.map((write) => ({
        taskName: write.taskName,
        streamId: write.streamId,
        pieces: write.pieces,
        printedTo: write.printedTo,
      })),
      workerPool.releaseTaskOutputBytes(
        workerId,
        writes.reduce((heldBytes, write) => heldBytes + write.heldBytes, 0),
      ),
    );
  });

const handleMessageOrExit = Effect.fn(function* (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  requestDependencyTaskResult: Parameters<typeof sendDependencyResultToWorker>[1],
  workerEvent: typeof WorkerEvent.Type,
) {
  if (isConsoleLineWrittenEvent(workerEvent)) {
    return yield* printStreamWrites(workerEvent.workerId, [
      {
        ...workerEvent,
        printedTo: "terminal and file" as const,
        traceLogRecordJson: undefined,
        isConsoleLine: true,
      },
    ]);
  }
  if (isExitedWithCode(workerEvent)) {
    return yield* endTasksOfExitedWorker(taskRunsState, workerEvent);
  }
  if (isMessageUnreadable(workerEvent)) {
    return yield* Effect.die(
      new InternalInvariantBroken({
        area: "worker protocol",
        invariant: "messages match the worker protocol",
        cause: workerEvent.reason,
      }),
    );
  }
  const messageFromWorker = workerEvent.message;
  if (FromWorker.guards.StandardErrorLineRequested(messageFromWorker)) {
    return yield* printLoggedLine(workerEvent.workerId, messageFromWorker);
  }
  if (FromWorker.guards.TraceLogRecordRequested(messageFromWorker)) {
    const traceFile = yield* TraceFile;
    traceFile.writeWorkerLogRecord(messageFromWorker.traceLogRecordJson);
    return;
  }
  yield* onMessageAboutTask(taskRunsState, requestDependencyTaskResult, messageFromWorker);
  if (FromWorker.guards.TaskFiberStarted(messageFromWorker)) {
    yield* WorkerPool.use((workerPool) => workerPool.recordBodyStartHandled(workerEvent.workerId));
  }
});

const handleWorkerEvent = (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  requestDependencyTaskResult: Parameters<typeof sendDependencyResultToWorker>[1],
  workerEvent: typeof WorkerEvent.Type,
) => {
  const isStandardErrorLine =
    isMessageReceived(workerEvent) && isStandardErrorLineRequested(workerEvent.message);
  return isStandardErrorLine
    ? printLoggedLine(workerEvent.workerId, workerEvent.message)
    : handleMessageOrExit(taskRunsState, requestDependencyTaskResult, workerEvent);
};

export const splitWorkerEvents = (
  taskRunsState: Effect.Success<typeof makeTaskRunsState>,
  requestDependencyTaskResult: Parameters<typeof sendDependencyResultToWorker>[1],
  workerEvents: readonly (typeof WorkerEvent.Type)[],
) => {
  const steps: (
    | ReturnType<typeof handleWorkerEvent>
    | ReturnType<typeof printLoggedRun>
    | ReturnType<typeof printStreamWrites>
  )[] = [];
  const waiting = {
    writes: new Array<NonNullable<ReturnType<typeof streamWriteOf>>>(),
    loggedLines: new Array<NonNullable<ReturnType<typeof loggedLineOf>>>(),
    loggedCharacters: 0,
  };
  const printWaitingLoggedLines = () => {
    if (waiting.loggedLines.length > 0) {
      steps.push(printLoggedRun(waiting.loggedLines));
      waiting.loggedLines = [];
      waiting.loggedCharacters = 0;
    }
  };
  const printWaitingWrites = () => {
    const [firstWrite] = waiting.writes;
    if (firstWrite !== undefined) {
      steps.push(printStreamWrites(firstWrite.workerId, waiting.writes));
      waiting.writes = [];
    }
  };
  for (const workerEvent of workerEvents) {
    const loggedLine = loggedLineOf(workerEvent);
    if (loggedLine !== null) {
      printWaitingWrites();
      const endsTheRun =
        waiting.loggedLines[0]?.taskName !== loggedLine.taskName ||
        waiting.loggedCharacters + loggedLine.text.length > LOGGED_RUN_CHARACTER_LIMIT;
      if (endsTheRun) {
        printWaitingLoggedLines();
      }
      waiting.loggedLines.push(loggedLine);
      waiting.loggedCharacters += loggedLine.text.length;
      continue;
    }
    printWaitingLoggedLines();
    const write = streamWriteOf(workerEvent);
    if (write === null) {
      printWaitingWrites();
      steps.push(handleWorkerEvent(taskRunsState, requestDependencyTaskResult, workerEvent));
      continue;
    }
    const isFromAnotherWorker = waiting.writes[0]?.workerId !== write.workerId;
    if (isFromAnotherWorker) {
      printWaitingWrites();
    }
    waiting.writes.push(write);
  }
  printWaitingWrites();
  printWaitingLoggedLines();
  return steps;
};
