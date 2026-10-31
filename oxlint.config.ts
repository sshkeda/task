import { lintConfig } from "@sshkeda/check";

export default lintConfig({
  "src/lib/task/internal/trust-task-results.ts": {
    allow: [{ trustsUnverifiedTypes: "task results cross threads as JSON" }],
  },
  "src/lib/task/internal/trust-output-mode-names.ts": {
    allow: [
      {
        trustsUnverifiedTypes:
          "exec's return type follows the secret option, which TypeScript can't check inside the function",
      },
    ],
  },
  "src/lib/task/internal/run-command.ts": {
    allow: [
      "usesAbbreviations",
      "spawnsCommands",
      {
        handlesEveryFailure:
          "exec reports how its standard input stream ended next to the command's own exit",
      },
    ],
  },
  "tests/shared/run-task-processes.ts": {
    allow: [
      "readsConfig",
      "spawnsCommands",
      {
        handlesEveryFailure:
          "a test child's output is read in the background while the test drives the child",
      },
    ],
  },
  "tests/internal/supervises-each-run.ts": { allow: ["spawnsCommands", "pollsWithADeadline"] },
  "tests/internal/terminates-leftover-process-groups.ts": {
    allow: ["pollsWithADeadline", "spawnsCommands"],
  },
  "tests/internal/kills-processes-that-leave-their-session.ts": { allow: ["pollsWithADeadline"] },
  "src/lib/task/internal/run-root-task.ts": {
    allow: [
      "exitsTheProcess",
      "importsDynamically",
      "writesToTheTerminal",
      {
        handlesEveryFailure:
          "Task.main runs the root task as the process's main fiber, and the process exit code follows however that fiber ended",
      },
    ],
  },
  "src/shared/task-services.ts": { services: ["Tasks", "Serving"] },
  "src/shared/read-environment-config.ts": { allow: ["readsConfig"] },
  "src/lib/supervise-the-run/internal/supervise-run.ts": {
    allow: [
      "spawnsCommands",
      {
        handlesEveryFailure:
          "the supervisor's RPC server, signal forwarding and stop and continue handling run in the background for the life of the run",
      },
    ],
  },
  "src/shared/mark-task-worker-thread-contexts.ts": { services: ["InsideTaskBody"] },
  "src/shared/write-trace-file.ts": { services: ["TraceFile"] },
  "src/lib/main-thread/shared/find-tasks-reachable-from-root-task-file.ts": {
    allow: ["importsDynamically"],
  },
  "src/lib/main-thread/shared/provide-discovered-tasks.ts": { services: ["DiscoveredTasks"] },
  "src/lib/main-thread/shared/provide-process-groups.ts": { services: ["ProcessGroups"] },
  "src/lib/main-thread/shared/provide-run-root-task-config.ts": { services: ["RunRootTaskConfig"] },
  "src/lib/main-thread/shared/provide-stop-state.ts": { services: ["StopState"] },
  "src/lib/main-thread/lib/find-code-each-task-depends-on/internal/make-module-graph.ts": {
    services: ["ModuleGraph"],
  },
  "src/lib/worker-thread/internal/run-task-body.ts": { allow: ["importsDynamically"] },
  "src/lib/worker-thread/internal/start-worker-thread.ts": {
    allow: [
      "exitsTheProcess",
      {
        handlesEveryFailure:
          "a worker thread runs its whole life as one fiber started from the thread's entry point",
      },
    ],
  },
  "src/lib/worker-thread/lib/provide-task-services/internal/spawn-commands-with-bun.ts": {
    allow: [
      "spawnsCommands",
      {
        handlesEveryFailure:
          "process cleanup runs and logs whatever the process, its exit watch or its standard input ended with",
      },
    ],
  },
  "src/lib/worker-thread/lib/provide-task-services/internal/run-commands-in-package-directory.ts": {
    allow: ["spawnsCommands"],
  },
  "src/lib/worker-thread/lib/provide-task-services/internal/provide-task-services.ts": {
    allow: ["spawnsCommands"],
  },
  "src/lib/worker-thread/lib/provide-task-services/internal/spawn-reported-process.ts": {
    allow: [
      "spawnsCommands",
      { handlesEveryFailure: "a process span ends however the process ended" },
    ],
  },
  "src/lib/main-thread/lib/find-code-each-task-depends-on/internal/resolve-module-specifiers.ts": {
    allow: ["importsDynamically"],
  },
  "src/lib/supervise-the-run/internal/watch-the-supervisor.ts": {
    allow: [
      "writesToTheTerminal",
      {
        handlesEveryFailure:
          "the supervisor watch prints any failure before the process kills itself",
      },
    ],
  },
  "src/lib/worker-thread/lib/provide-task-services/internal/read-standard-input-from-main-thread.ts":
    { allow: ["writesToTheTerminal"] },
  "tests/shared/make-projects.ts": { allow: ["spawnsCommands"] },
  "tests/internal/runs-commands.ts": { allow: ["spawnsCommands"] },
  "tests/internal/lists-processes.ts": { allow: ["spawnsCommands"] },
  "tests/internal/reports-each-killed-process-once.ts": {
    allow: [
      "spawnsCommands",
      {
        handlesEveryFailure: "the test starts a kill pass in the background so it can interrupt it",
      },
    ],
  },
  "tests/internal/configures-runtime-state-directory.ts": { allow: ["spawnsCommands"] },
  "src/shared/invariant-errors.ts": {
    allow: [
      {
        handlesEveryFailure:
          "an invariant that fails in any way is a bug, so every failure becomes a defect",
      },
    ],
  },
  "src/lib/main-thread/shared/fail-run.ts": {
    allow: [{ handlesEveryFailure: "a failure anywhere in the run becomes the run's failure" }],
  },
  "src/lib/main-thread/internal/run-main-thread.ts": {
    allow: [
      {
        handlesEveryFailure:
          "the run prints whatever cause ended it and takes its exit code from it",
      },
    ],
  },
  "src/lib/main-thread/internal/run-root-task-fiber.ts": {
    allow: [
      {
        handlesEveryFailure:
          "shutdown records the run's first failure and waits for worker event handling however it ends",
      },
    ],
  },
  "src/lib/main-thread/lib/find-code-each-task-depends-on/internal/load-all-reporting-first-failure.ts":
    {
      allow: [
        { handlesEveryFailure: "every module is loaded before the first failure is reported" },
      ],
    },
  "src/lib/main-thread/lib/run-tasks-on-workers/internal/handle-worker-events.ts": {
    allow: [
      {
        handlesEveryFailure:
          "task outcomes arrive from workers as exits and are settled whatever they are",
      },
    ],
  },
  "src/lib/main-thread/lib/run-tasks-on-workers/lib/run-requested-task-once/internal/await-within-counted-limit.ts":
    {
      allow: [
        { handlesEveryFailure: "a failure that arrives after the limit counts as running past it" },
      ],
    },
  "src/lib/main-thread/lib/run-tasks-on-workers/lib/run-requested-task-once/internal/execute-task.ts":
    {
      allow: [
        {
          handlesEveryFailure:
            "a task's cleanup is watched past its timeout whatever its body ended with",
        },
      ],
    },
  "src/lib/main-thread/lib/run-tasks-on-workers/lib/run-requested-task-once/internal/make-task-setup-clock.ts":
    {
      allow: [
        {
          handlesEveryFailure:
            "task setup is awaited and interrupted keeping whatever it ended with",
        },
      ],
    },
  "src/lib/main-thread/lib/run-tasks-on-workers/shared/share-standard-input-with-root-task.ts": {
    allow: [{ handlesEveryFailure: "a standard input failure is sent to the worker as a message" }],
  },
  "src/lib/task/internal/run-task-lists.ts": {
    allow: [
      { handlesEveryFailure: "Task.allSettled waits for every task before reporting which failed" },
    ],
  },
  "src/lib/worker-thread/internal/answer-main-thread-messages.ts": {
    allow: [
      {
        handlesEveryFailure:
          "a task's result and cleanup are sent to the main thread however they ended",
      },
    ],
  },
  "src/shared/complete-discarded-calls-once-sent.ts": {
    allow: [{ handlesEveryFailure: "a call that already finished hands back its own result" }],
  },
  "tests/shared/fail-test-cleanup.ts": {
    allow: [{ handlesEveryFailure: "any failure while cleaning up after a test is a defect" }],
  },
  "tests/shared/failure-as-value.ts": {
    allow: [{ handlesEveryFailure: "tests assert how an effect ended" }],
  },
  "src/lib/main-thread/internal/listen-for-stop-requests.ts": {
    allow: [
      {
        handlesEveryFailure:
          "stop-signal, stderr-close and failure listeners run in the background for the whole run and report through StopState",
      },
    ],
  },
  "src/lib/main-thread/internal/trace-the-run.ts": {
    allow: [
      {
        handlesEveryFailure:
          "old traces are swept in the background, ignoring a failed sweep by name, until the run's scope closes",
      },
    ],
  },
  "src/lib/main-thread/lib/run-tasks-on-workers/internal/make-task-runs.ts": {
    services: ["TaskRuns"],
    allow: [
      {
        handlesEveryFailure:
          "a stopping task's worker watch runs in the run's fiber set, and its failure fails the run",
      },
    ],
  },
  "src/lib/main-thread/lib/run-tasks-on-workers/lib/run-requested-task-once/internal/run-requested-tasks.ts":
    {
      allow: [
        {
          handlesEveryFailure:
            "each requested task runs in the background and its exit completes the task's result deferred",
        },
      ],
    },
  "src/lib/main-thread/shared/provide-worker-pool.ts": {
    services: ["WorkerPool"],
    allow: [
      {
        handlesEveryFailure:
          "worker RPC servers, port message handlers and calls to workers run in the pool's background, and their failures reach the pool's own handling",
      },
    ],
  },
  "src/lib/preload-rewritten-imports-into-each-worker/index.ts": {
    allow: [
      {
        handlesEveryFailure:
          "the worker preload loads rewritten imports synchronously before task modules run, and a failure stops the worker",
      },
    ],
  },
  "src/lib/supervise-the-run/internal/adopt-orphans-on-linux.ts": {
    allow: [
      {
        handlesEveryFailure:
          "orphaned processes are reaped in the background until the supervisor's scope closes",
      },
    ],
  },
  "src/lib/supervise-the-run/internal/start-watching-the-supervisor.ts": {
    allow: [
      {
        handlesEveryFailure:
          "the preload starts watching the supervisor in the background, and the watch prints its own failures",
      },
    ],
  },
  "src/lib/worker-thread/internal/serve-on-the-parent-port.ts": {
    allow: [
      {
        handlesEveryFailure:
          "each message from the main thread is handled in a fiber of the worker's scope",
      },
    ],
  },
  "src/lib/worker-thread/shared/provide-worker-state.ts": {
    services: ["WorkerState"],
    allow: [
      {
        handlesEveryFailure:
          "task bodies, body interruptions, main-thread messages and reports run in the worker's background scope and report through the main-thread RPC",
      },
    ],
  },
  "src/shared/run-in-child-fiber-and-join.ts": {
    allow: [
      {
        handlesEveryFailure:
          "runs an effect in a child fiber and joins it, so the caller sees its failure",
      },
    ],
  },
  "tests/shared/wait-until-traced.ts": {
    allow: [
      {
        handlesEveryFailure:
          "this helper script runs as its own process, watching file changes in the background and passing a watch failure to the waiter, so its exit code reports it",
      },
    ],
  },
  "tests/shared/run-task-process-under-parent-environment.ts": {
    allow: [
      {
        handlesEveryFailure:
          "this helper script runs as its own process, and its exit code reports whether the parent config leaked",
      },
    ],
  },
  vendor: { allow: ["isAnUnlistedDirectory"] },
  "vendor/sshkeda-check-1.0.0.tgz": { allow: ["hasAnUnlistedFileType"] },
  "src/lib/main-thread": { cannotImport: ["src/lib/worker-thread"] },
  "src/lib/worker-thread": { cannotImport: ["src/lib/main-thread"] },
});
