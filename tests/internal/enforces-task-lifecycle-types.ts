import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import { encodeJson } from "../shared/encode-json.ts";
import { failureAsValue } from "../shared/failure-as-value.ts";
import { REPOSITORY_DIRECTORY } from "../shared/find-repository-directory.ts";
import { typeCheckSource } from "../shared/type-check-source.ts";

export const invalidLifecycleStates = Task.make(
  "task lifecycle types reject missing workers, workers on states without one and caller state mutation",
  Effect.scoped(
    Effect.gen(function* () {
      const repository = yield* REPOSITORY_DIRECTORY;
      const taskRunStateModule = yield* encodeJson(
        `${repository}/src/lib/main-thread/lib/run-tasks-on-workers/shared/define-task-run-state.ts`,
      );
      const failure = yield* failureAsValue(
        typeCheckSource(`import type { Effect } from "effect";
import type { makeTaskRunsState, TaskRunStatus } from ${taskRunStateModule};
const serving: typeof TaskRunStatus.Type = { _tag: "Serving" };
const preparing: typeof TaskRunStatus.Type = { _tag: "Preparing", workerId: 1 };
const running: typeof TaskRunStatus.Type = { _tag: "Running", workerId: null };
declare const taskRuns: Effect.Success<typeof makeTaskRunsState>;
taskRuns.createRecord("created").status = { _tag: "Preparing" };`),
      );
      expect(failure.message).toContain("Property 'workerId' is missing");
      expect(failure.message).toContain("Object literal may only specify known properties");
      expect(failure.message).toContain("Type 'null' is not assignable to type 'number'");
      expect(failure.message).toContain(
        "Cannot assign to 'status' because it is a read-only property",
      );
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);

export const validLifecycleStates = Task.make(
  "task lifecycle types allow pre-worker failures and worker-owned completion states",
  Effect.scoped(
    Effect.gen(function* () {
      const repository = yield* REPOSITORY_DIRECTORY;
      const taskRunStateModule = yield* encodeJson(
        `${repository}/src/lib/main-thread/lib/run-tasks-on-workers/shared/define-task-run-state.ts`,
      );
      yield* typeCheckSource(`import type { TaskRunStatus } from ${taskRunStateModule};
export const states: ReadonlyArray<typeof TaskRunStatus.Type> = [
  { _tag: "Preparing" },
  { _tag: "Running", workerId: 0 },
  { _tag: "Serving", workerId: 0 },
  { _tag: "Closed", workerId: 0 },
  { _tag: "Passed", workerId: 0 },
  { _tag: "Failed", workerId: null, causedByAwaitedTasks: false },
  { _tag: "Failed", workerId: 0, causedByAwaitedTasks: true },
  { _tag: "Cancelled", workerId: null },
  { _tag: "Cancelled", workerId: 0 },
];`);
    }),
  ),
  { timeout: "1 minute", cleanup: "5 seconds" },
);
