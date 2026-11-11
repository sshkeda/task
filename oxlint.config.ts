import { lintConfig } from "@sshkeda/check";

export default lintConfig({
  "src/lib/task/internal/trust-task-outputs.ts": {
    allow: ["trustsUnverifiedTypes"],
  },
  "src/lib/task/internal/trust-output-mode-names.ts": {
    allow: ["trustsUnverifiedTypes"],
  },
  "src/lib/task/internal/run-command.ts": {
    allow: ["usesAbbreviations", "spawnsCommands", "handlesEveryFailure"],
  },
  "tests/shared/run-task-processes.ts": {
    allow: ["readsConfig", "spawnsCommands", "handlesEveryFailure"],
  },
  "tests/internal/supervises-each-run.ts": { allow: ["spawnsCommands", "pollsWithADeadline"] },
  "tests/internal/terminates-leftover-process-groups.ts": {
    allow: ["pollsWithADeadline", "spawnsCommands"],
  },
  "tests/internal/kills-processes-that-leave-their-session.ts": { allow: ["pollsWithADeadline"] },
  "tests/internal/passes-the-terminal-to-commands.ts": { allow: ["spawnsCommands"] },
  "src/lib/task/internal/run-root-task.ts": {
    allow: ["exitsTheProcess", "importsDynamically", "writesToTheTerminal", "handlesEveryFailure"],
  },
  "src/shared/task-services.ts": { services: ["Tasks", "Serving"] },
  "src/shared/read-environment-config.ts": { allow: ["readsConfig"] },
  "src/shared/run-in-terminal.ts": { allow: ["spawnsCommands"], services: ["TerminalCommands"] },
  "src/lib/supervise-the-run/internal/supervise-run.ts": {
    allow: ["spawnsCommands", "handlesEveryFailure"],
  },
  "src/shared/mark-task-worker-thread-contexts.ts": {
    services: ["InsideTaskBody", "CommandLineFlags"],
  },
  "src/lib/main-thread/shared/find-tasks-made-by-root-task-file.ts": {
    allow: ["importsDynamically"],
  },
  "src/lib/main-thread/shared/provide-discovered-tasks.ts": { services: ["DiscoveredTasks"] },
  "src/lib/main-thread/shared/provide-process-groups.ts": { services: ["ProcessGroups"] },
  "src/lib/main-thread/shared/provide-run-root-task-config.ts": { services: ["RunRootTaskConfig"] },
  "src/lib/main-thread/shared/provide-stop-state.ts": { services: ["StopState"] },
  "src/lib/main-thread/shared/write-trace-file.ts": { services: ["TraceFile"] },
  "src/lib/worker-thread/internal/run-task-body.ts": { allow: ["importsDynamically"] },
  "src/lib/worker-thread/internal/start-worker-thread.ts": {
    allow: ["exitsTheProcess", "handlesEveryFailure", "writesToTheTerminal"],
  },
  "src/lib/worker-thread/lib/provide-task-services/internal/spawn-commands-with-bun.ts": {
    allow: ["spawnsCommands", "handlesEveryFailure"],
  },
  "src/lib/worker-thread/lib/provide-task-services/internal/run-commands-in-package-directory.ts": {
    allow: ["spawnsCommands"],
  },
  "src/lib/worker-thread/lib/provide-task-services/internal/provide-task-services.ts": {
    allow: ["spawnsCommands"],
  },
  "src/lib/worker-thread/lib/provide-task-services/internal/spawn-reported-process.ts": {
    allow: ["spawnsCommands", "handlesEveryFailure"],
  },
  "src/lib/supervise-the-run/internal/watch-the-supervisor.ts": {
    allow: ["writesToTheTerminal", "handlesEveryFailure"],
  },
  "src/lib/worker-thread/lib/provide-task-services/internal/read-standard-input-from-main-thread.ts":
    { allow: ["writesToTheTerminal"] },
  "tests/shared/make-projects.ts": { allow: ["spawnsCommands"] },
  "tests/internal/runs-commands.ts": { allow: ["spawnsCommands"] },
  "tests/internal/lists-processes.ts": { allow: ["spawnsCommands"] },
  "tests/internal/reports-each-killed-process-once.ts": {
    allow: ["spawnsCommands", "handlesEveryFailure"],
  },
  "tests/internal/configures-runtime-state-directory.ts": { allow: ["spawnsCommands"] },
  "src/shared/invariant-errors.ts": {
    allow: ["handlesEveryFailure"],
  },
  "src/lib/main-thread/shared/fail-run.ts": {
    allow: ["handlesEveryFailure"],
  },
  "src/lib/main-thread/internal/run-main-thread.ts": {
    allow: ["handlesEveryFailure"],
  },
  "src/lib/main-thread/internal/run-root-task-fiber.ts": {
    allow: ["handlesEveryFailure"],
  },
  "src/lib/main-thread/lib/run-tasks-on-workers/internal/handle-worker-events.ts": {
    allow: ["handlesEveryFailure"],
  },
  "src/lib/main-thread/lib/run-tasks-on-workers/lib/run-requested-task-once/internal/await-within-counted-limit.ts":
    {
      allow: ["handlesEveryFailure"],
    },
  "src/lib/main-thread/lib/run-tasks-on-workers/lib/run-requested-task-once/internal/execute-task.ts":
    {
      allow: ["handlesEveryFailure"],
    },
  "src/lib/main-thread/lib/run-tasks-on-workers/lib/run-requested-task-once/internal/make-task-setup-clock.ts":
    {
      allow: ["handlesEveryFailure"],
    },
  "src/lib/main-thread/lib/run-tasks-on-workers/shared/share-standard-input-with-root-task.ts": {
    allow: ["handlesEveryFailure"],
  },
  "src/lib/task/internal/run-task-lists.ts": {
    allow: ["handlesEveryFailure"],
  },
  "src/lib/worker-thread/internal/answer-main-thread-messages.ts": {
    allow: ["handlesEveryFailure"],
  },
  "src/lib/supervise-the-run/internal/complete-discarded-calls-once-sent.ts": {
    allow: ["handlesEveryFailure"],
  },
  "tests/shared/fail-test-cleanup.ts": {
    allow: ["handlesEveryFailure"],
  },
  "tests/shared/failure-as-value.ts": {
    allow: ["handlesEveryFailure"],
  },
  "src/lib/main-thread/internal/listen-for-stop-requests.ts": {
    allow: ["handlesEveryFailure"],
  },
  "src/lib/main-thread/internal/trace-the-run.ts": {
    allow: ["handlesEveryFailure", "fallsBackToFixedValues"],
  },
  "src/lib/main-thread/lib/run-tasks-on-workers/internal/make-task-runs.ts": {
    services: ["TaskRuns"],
    allow: ["handlesEveryFailure"],
  },
  "src/lib/main-thread/lib/run-tasks-on-workers/lib/run-requested-task-once/internal/run-requested-tasks.ts":
    {
      allow: ["handlesEveryFailure"],
    },
  "src/lib/main-thread/shared/provide-worker-pool.ts": {
    services: ["WorkerPool"],
    allow: ["writesToTheTerminal", "handlesEveryFailure", "fallsBackToFixedValues"],
  },
  "src/lib/supervise-the-run/internal/adopt-orphans-on-linux.ts": {
    allow: ["handlesEveryFailure"],
  },
  "src/lib/supervise-the-run/internal/start-watching-the-supervisor.ts": {
    allow: ["handlesEveryFailure"],
  },
  "src/lib/worker-thread/internal/serve-on-the-parent-port.ts": {
    allow: ["handlesEveryFailure"],
  },
  "src/lib/worker-thread/shared/provide-worker-state.ts": {
    services: ["WorkerState"],
    allow: ["handlesEveryFailure"],
  },
  "src/shared/run-in-child-fiber-and-join.ts": {
    allow: ["handlesEveryFailure"],
  },
  "tests/shared/wait-until-traced.ts": {
    allow: ["handlesEveryFailure"],
  },
  "tests/shared/run-task-process-under-parent-environment.ts": {
    allow: ["handlesEveryFailure"],
  },
  "src/lib/supervise-the-run/internal/talk-to-the-supervisor.ts": {
    allow: ["fallsBackToFixedValues"],
  },
  "src/lib/main-thread": { cannotImport: ["src/lib/worker-thread"] },
  "src/lib/worker-thread": { cannotImport: ["src/lib/main-thread"] },
  ".github": { allow: ["isAnUnlistedDirectory"] },
  ".github/workflows/publish.yml": { allow: ["hasAnUnlistedFileType"] },
});
