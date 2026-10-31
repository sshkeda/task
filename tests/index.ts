import { check } from "@sshkeda/check/tasks";
import { Task } from "@sshkeda/task";
import { Effect, Record, identity } from "effect";

import * as startupCancellation from "./internal/cancels-runtime-startup.ts";
import * as ctrlC from "./internal/cleans-up-when-stopped.ts";
import * as stateDirectory from "./internal/configures-runtime-state-directory.ts";
import * as taskLifecycleTypes from "./internal/enforces-task-lifecycle-types.ts";
import * as interruptsFromInside from "./internal/fails-on-interrupts-from-inside-tasks.ts";
import * as notEffects from "./internal/fails-steps-that-are-not-effects.ts";
import * as installedAndNestedPackages from "./internal/finds-tasks-in-installed-and-nested-packages.ts";
import * as workerFailures from "./internal/handles-crashed-workers.ts";
import * as processObservers from "./internal/handles-process-observer-failures.ts";
import * as unresponsiveWorkers from "./internal/handles-unresponsive-workers.ts";
import * as capturedCommandOutput from "./internal/keeps-captured-command-output-out-of-logs.ts";
import * as servingTasks from "./internal/keeps-services-alive-until-cleanup.ts";
import * as escapedProcesses from "./internal/kills-processes-that-leave-their-session.ts";
import * as listConcurrency from "./internal/limits-list-concurrency.ts";
import * as processListing from "./internal/lists-processes.ts";
import * as packageFields from "./internal/matches-its-package-json.ts";
import * as shutdownTransitions from "./internal/owns-shutdown-transitions.ts";
import * as redactedEnvironment from "./internal/passes-redacted-env-values-to-commands.ts";
import * as failureArtifacts from "./internal/preserves-failed-test-artifacts.ts";
import * as errorLogLevel from "./internal/prints-only-errors-at-the-error-log-level.ts";
import * as taskErrors from "./internal/prints-task-errors.ts";
import * as taskLogs from "./internal/prints-task-logs.ts";
import * as taskNamespace from "./internal/provides-task-namespace.ts";
import * as bodyServices from "./internal/provides-task-runtime-services.ts";
import * as bodyConfig from "./internal/reads-config-in-task-bodies.ts";
import * as standardInput from "./internal/reads-standard-input-in-the-root-task.ts";
import * as runTraces from "./internal/records-run-traces.ts";
import * as misplacedRunCalls from "./internal/refuses-misplaced-run-calls.ts";
import * as refusals from "./internal/rejects-invalid-task-definitions.ts";
import * as typeErrors from "./internal/rejects-invalid-task-types.ts";
import * as killedOnce from "./internal/reports-each-killed-process-once.ts";
import * as runtimeBoundaryFailures from "./internal/reports-runtime-boundary-failures.ts";
import * as startupResources from "./internal/reports-startup-resource-failures.ts";
import * as workerTraceFailures from "./internal/reports-worker-trace-failures.ts";
import * as packageRelativePaths from "./internal/resolves-paths-from-the-task-package.ts";
import * as commands from "./internal/runs-commands.ts";
import * as standaloneExecutables from "./internal/runs-inside-standalone-executables.ts";
import * as runningTasks from "./internal/runs-task-dependencies.ts";
import * as workers from "./internal/schedules-tasks-across-workers.ts";
import * as settledGroups from "./internal/settles-every-task-in-all-settled.ts";
import * as supervision from "./internal/supervises-each-run.ts";
import * as leftoverProcesses from "./internal/terminates-leftover-process-groups.ts";
import { TESTS_AT_ONCE } from "./internal/tests-at-once.ts";
import * as packedSource from "./internal/type-checks-a-strict-consumer-against-the-packed-source.ts";
import * as rootTaskPackage from "./internal/validates-root-task-package.ts";

export const runtimeBoundaryFailureSuite = Task.make(
  "runtime-boundary-failures",
  Effect.forEach(Record.values(runtimeBoundaryFailures), identity, { discard: true }),
);

export const runtimeTests = Task.make(
  "runtime-tests",
  Task.allSettled(
    [
      runtimeBoundaryFailureSuite,
      taskLifecycleTypes,
      failureArtifacts,
      shutdownTransitions,
      startupCancellation,
      processObservers,
      notEffects,
      interruptsFromInside,
      workerTraceFailures,
      stateDirectory,
      startupResources,
      commands,
      redactedEnvironment,
      bodyConfig,
      installedAndNestedPackages,
      capturedCommandOutput,
      runningTasks,
      taskLogs,
      taskErrors,
      errorLogLevel,
      settledGroups,
      listConcurrency,
      packedSource,
      standaloneExecutables,
      rootTaskPackage,
      packageFields,
      typeErrors,
      refusals,
      misplacedRunCalls,
      servingTasks,
      runTraces,
      bodyServices,
      taskNamespace,
      packageRelativePaths,
      standardInput,
      leftoverProcesses,
      escapedProcesses,
      processListing,
      killedOnce,
      workerFailures,
      unresponsiveWorkers,
      workers,
      ctrlC,
      supervision,
    ],
    { concurrency: TESTS_AT_ONCE },
  ),
);

export const ci = Task.make("ci", Task.allSettled([check, runtimeTests]));
