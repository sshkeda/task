import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import * as startupCancellation from "./internal/cancels-runtime-startup.ts";
import * as customStackFormatters from "./internal/captures-stacks-under-custom-formatters.ts";
import * as ctrlC from "./internal/cleans-up-when-stopped.ts";
import * as stateDirectory from "./internal/configures-runtime-state-directory.ts";
import * as taskOutputs from "./internal/declares-task-outputs.ts";
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
import * as terminalCommands from "./internal/passes-the-terminal-to-commands.ts";
import * as failureArtifacts from "./internal/preserves-failed-test-artifacts.ts";
import * as errorLogLevel from "./internal/prints-only-errors-at-the-error-log-level.ts";
import * as taskErrors from "./internal/prints-task-errors.ts";
import * as taskLogs from "./internal/prints-task-logs.ts";
import * as taskNamespace from "./internal/provides-task-namespace.ts";
import * as bodyServices from "./internal/provides-task-runtime-services.ts";
import * as commandLineFlags from "./internal/reads-command-line-flags.ts";
import * as bodyConfig from "./internal/reads-config-in-task-bodies.ts";
import * as standardInput from "./internal/reads-standard-input-in-the-root-task.ts";
import * as runTraces from "./internal/records-run-traces.ts";
import * as bunModeRefusals from "./internal/refuses-runs-under-bun-watch-or-evaluation.ts";
import * as refusals from "./internal/rejects-invalid-task-definitions.ts";
import * as typeErrors from "./internal/rejects-invalid-task-types.ts";
import * as killedOnce from "./internal/reports-each-killed-process-once.ts";
import * as runtimeBoundaryFailures from "./internal/reports-runtime-boundary-failures.ts";
import * as startupResources from "./internal/reports-startup-resource-failures.ts";
import * as packageRelativePaths from "./internal/resolves-paths-from-the-task-package.ts";
import * as commands from "./internal/runs-commands.ts";
import * as standaloneExecutables from "./internal/runs-inside-standalone-executables.ts";
import * as isolatedTasks from "./internal/runs-isolated-tasks-on-their-own-workers.ts";
import * as onlyTask from "./internal/runs-one-task-by-name.ts";
import * as runningTasks from "./internal/runs-task-dependencies.ts";
import * as tasksMadeInBodies from "./internal/runs-tasks-made-in-bodies.ts";
import * as workers from "./internal/schedules-tasks-across-workers.ts";
import * as settledGroups from "./internal/settles-every-task-in-all-settled.ts";
import * as autoStart from "./internal/starts-runs-from-files-that-make-tasks.ts";
import * as supervision from "./internal/supervises-each-run.ts";
import * as leftoverProcesses from "./internal/terminates-leftover-process-groups.ts";
import { TESTS_AT_ONCE } from "./internal/tests-at-once.ts";
import * as workerTraces from "./internal/traces-workers-from-the-main-thread.ts";
import * as packedSource from "./internal/type-checks-a-strict-consumer-against-the-packed-source.ts";
import * as rootTaskPackage from "./internal/validates-root-task-package.ts";

export const runtimeBoundaryFailureSuite = Task.make({
  name: "runtime-boundary-failures",
  run: Task.all(runtimeBoundaryFailures, { concurrency: 1 }),
});

export const runtimeTests = Task.make({
  name: "runtime-tests",
  run: Task.allSettled(
    [
      runtimeBoundaryFailureSuite,
      workers,
      ctrlC,
      taskLifecycleTypes,
      customStackFormatters,
      failureArtifacts,
      shutdownTransitions,
      startupCancellation,
      processObservers,
      notEffects,
      interruptsFromInside,
      workerTraces,
      stateDirectory,
      startupResources,
      commands,
      redactedEnvironment,
      bodyConfig,
      installedAndNestedPackages,
      capturedCommandOutput,
      runningTasks,
      tasksMadeInBodies,
      isolatedTasks,
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
      bunModeRefusals,
      servingTasks,
      runTraces,
      bodyServices,
      taskNamespace,
      packageRelativePaths,
      standardInput,
      terminalCommands,
      leftoverProcesses,
      escapedProcesses,
      processListing,
      killedOnce,
      workerFailures,
      unresponsiveWorkers,
      supervision,
      onlyTask,
      taskOutputs,
      commandLineFlags,
      autoStart,
    ],
    { concurrency: TESTS_AT_ONCE },
  ),
});

export const check = Task.make({
  name: "check",
  run: Effect.asVoid(Task.exec("bunx", ["@sshkeda/check"])),
});

export const ci = Task.make({ name: "ci", run: Task.allSettled([check, runtimeTests]) });
