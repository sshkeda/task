import { expect } from "bun:test";

import { Effect } from "effect";

import { encodeJson } from "../shared/encode-json.ts";
import { failureAsValue } from "../shared/failure-as-value.ts";
import { REPOSITORY_DIRECTORY } from "../shared/find-repository-directory.ts";
import { makeTest } from "../shared/make-tests.ts";
import { typeCheckSource } from "../shared/type-check-source.ts";

export const invalidLifecycleStates = makeTest(
  "task lifecycle types reject missing workers, workers on states without one and caller state mutation",
  Effect.scoped(
    Effect.gen(function* () {
      const repository = yield* REPOSITORY_DIRECTORY;
      const taskRunStateModule = yield* encodeJson(
        `${repository}/src/lib/main-thread/lib/run-tasks-on-workers/shared/define-task-run-state.ts`,
      );
      const taskRunStatusModule = yield* encodeJson(
        `${repository}/src/lib/main-thread/lib/run-tasks-on-workers/shared/define-task-run-status.ts`,
      );
      const failure = yield* failureAsValue(
        typeCheckSource(`import type { Effect } from "effect";
import type { makeTaskRunsState } from ${taskRunStateModule};
import type { TaskRunOutcome, TaskRunStatus } from ${taskRunStatusModule};
const serving: typeof TaskRunStatus.Type = { _tag: "Serving" };
const preparing: typeof TaskRunStatus.Type = { _tag: "Preparing", workerId: 1 };
const running: typeof TaskRunStatus.Type = { _tag: "Running", workerId: null };
const failed: typeof TaskRunOutcome.Type = { _tag: "Failed", workerId: 1, causedByAwaitedTasks: false };
declare const taskRuns: Effect.Success<typeof makeTaskRunsState>;
taskRuns.createRecord("created").status = { _tag: "Preparing" };
taskRuns.createRecord("created").outcome = { _tag: "Pending" };`),
      );
      expect(failure.message).toContain("Property 'workerId' is missing");
      expect(failure.message).toContain("Object literal may only specify known properties");
      expect(failure.message).toContain("Type 'null' is not assignable to type 'number'");
      expect(failure.message).toContain(
        "Cannot assign to 'status' because it is a read-only property",
      );
      expect(failure.message).toContain(
        "Cannot assign to 'outcome' because it is a read-only property",
      );
    }),
  ),
);

export const validLifecycleStates = makeTest(
  "task lifecycle types allow pre-worker failures and worker-owned completion states",
  Effect.scoped(
    Effect.gen(function* () {
      const repository = yield* REPOSITORY_DIRECTORY;
      const taskRunStatusModule = yield* encodeJson(
        `${repository}/src/lib/main-thread/lib/run-tasks-on-workers/shared/define-task-run-status.ts`,
      );
      yield* typeCheckSource(`import type { TaskRunOutcome, TaskRunStatus } from ${taskRunStatusModule};
export const states: ReadonlyArray<typeof TaskRunStatus.Type> = [
  { _tag: "Preparing" },
  { _tag: "Assigned", workerId: 0 },
  { _tag: "Loading", workerId: 0 },
  { _tag: "Running", workerId: 0 },
  { _tag: "Serving", workerId: 0 },
  { _tag: "Failing", workerId: 0 },
  { _tag: "Finished", workerId: 0 },
  { _tag: "Closed", workerId: 0 },
];
export const outcomes: ReadonlyArray<typeof TaskRunOutcome.Type> = [
  { _tag: "Pending" },
  { _tag: "Served" },
  { _tag: "Passed" },
  { _tag: "Failed", causedByAwaitedTasks: false },
  { _tag: "Failed", causedByAwaitedTasks: true },
  { _tag: "Cancelled" },
];`);
    }),
  ),
);
