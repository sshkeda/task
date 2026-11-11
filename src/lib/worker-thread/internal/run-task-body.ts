import { Effect, Exit, Cause, Clock, Option } from "effect";

import { makeRootTaskGroup } from "../../../shared/make-root-task-group.ts";
import {
  CommandLineFlags,
  InsideTaskBody,
} from "../../../shared/mark-task-worker-thread-contexts.ts";
import { TASK_TRACE_LABEL_PREFIX } from "../../../shared/names-from-package-json.ts";
import { findTasksMadeWithName, listTasksMadeWithName } from "../../../shared/record-tasks-made.ts";
import { CauseWithoutFailure } from "../../../shared/report-errors.ts";
import { TaskEvent } from "../../../shared/rpc.ts";
import type { StartTaskFiber } from "../../../shared/rpc.ts";
import { runInChildFiberAndJoin } from "../../../shared/run-in-child-fiber-and-join.ts";
import { TASK_BODY } from "../../../shared/task-body-key.ts";
import { TaskModuleImportFailed } from "../../../shared/task-errors.ts";
import { provideTaskServices } from "../lib/provide-task-services/index.ts";
import type { makeTaskBodyRecord } from "../shared/define-task-body-record.ts";
import { WorkerState, reportToMainThread } from "../shared/provide-worker-state.ts";
import {
  TaskNotRegisteredInWorker,
  TaskRegisteredAgainInWorker,
} from "../shared/worker-thread-errors.ts";
import { describeBodyFailure } from "./describe-task-body-result.ts";
import { nameConfigKeyNotSet } from "./name-config-key-not-set.ts";

const importTaskModule = (filePath: string) =>
  Effect.tryPromise({
    try: () => import(filePath),
    catch: (cause) => new TaskModuleImportFailed({ filePath, cause }),
  });

export const runTaskBody = Effect.fn("runTaskBody")(
  function* (
    startMessage: typeof StartTaskFiber.Type,
    body: ReturnType<typeof makeTaskBodyRecord>,
  ) {
    yield* reportToMainThread(
      TaskEvent.cases.TaskFiberLoading.make({ taskName: startMessage.taskName }),
    );
    yield* importTaskModule(startMessage.rootTaskFilePath);
    const workerState = yield* WorkerState;
    yield* makeRootTaskGroup(workerState.rootTaskGroup);
    if (listTasksMadeWithName(startMessage.taskName).length === 0) {
      yield* importTaskModule(startMessage.taskFilePath);
    }
    yield* Effect.yieldNow;
    if (yield* workerState.wasAskedToStop(startMessage.taskName)) {
      return yield* Effect.interrupt;
    }
    const tasksMade = findTasksMadeWithName(startMessage.taskName);
    const [task] = tasksMade;
    if (task === undefined) {
      return yield* new TaskNotRegisteredInWorker({
        taskName: startMessage.taskName,
        filePath: startMessage.rootTaskFilePath,
      });
    }
    if (tasksMade.length > 1) {
      return yield* new TaskRegisteredAgainInWorker({
        taskName: startMessage.taskName,
        filePath: startMessage.rootTaskFilePath,
        madeCount: tasksMade.length,
      });
    }
    body.startedAtMilliseconds = yield* Clock.currentTimeMillis;
    yield* workerState.reportBodyStarted(startMessage.taskName);
    return yield* Effect.uninterruptibleMask((restore) =>
      restore(task[TASK_BODY]).pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            body.returnedBeforeCleanup = true;
          }),
        ),
        nameConfigKeyNotSet,
        Effect.provideService(InsideTaskBody, true),
        Effect.provideService(CommandLineFlags, workerState.flags),
        Effect.onExit((bodyExit) =>
          Effect.gen(function* () {
            const endedWithOnlyInterrupts =
              Exit.isFailure(bodyExit) && Cause.hasInterruptsOnly(bodyExit.cause);
            if (!endedWithOnlyInterrupts) {
              return;
            }
            if (yield* workerState.wasAskedToStop(startMessage.taskName)) {
              return;
            }
            const interruptedItself = Cause.fail(
              new CauseWithoutFailure({ details: Cause.pretty(bodyExit.cause) }),
            );
            body.causeReportedBeforeCleanup = Option.some(interruptedItself);
            yield* reportToMainThread(
              TaskEvent.cases.TaskFiberFailedBeforeCleanup.make({
                taskName: startMessage.taskName,
                failure: describeBodyFailure(interruptedItself),
              }),
            );
          }),
        ),
        Effect.tapCause((cause) => {
          if (Cause.hasInterruptsOnly(cause)) {
            return Effect.void;
          }
          body.causeReportedBeforeCleanup = Option.some(cause);
          return reportToMainThread(
            TaskEvent.cases.TaskFiberFailedBeforeCleanup.make({
              taskName: startMessage.taskName,
              failure: describeBodyFailure(cause),
            }),
          );
        }),
      ),
    );
  },
  (effect, startMessage, body) =>
    Effect.gen(function* () {
      const workerState = yield* WorkerState;
      yield* Effect.annotateCurrentSpan({
        [`${TASK_TRACE_LABEL_PREFIX}.task`]: startMessage.taskName,
        [`${TASK_TRACE_LABEL_PREFIX}.worker`]: workerState.workerId,
      });
      return yield* effect.pipe(
        Effect.scoped,
        runInChildFiberAndJoin,
        provideTaskServices(startMessage, workerState, body),
      );
    }),
);
