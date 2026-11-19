import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import { decideNoTaskStarted } from "../../src/lib/main-thread/internal/decide-no-task-started.ts";

const ROOT_TASK_GROUP_NAME = "default";

const MADE_TASK_NAME = "lint";

const madeTaskNameLists = [
  ["made none", []],
  ["made lint", [MADE_TASK_NAME]],
] as const;

const startedTaskNameLists = [
  ["started none", []],
  ["started only the group", [ROOT_TASK_GROUP_NAME]],
  ["started lint", [MADE_TASK_NAME]],
] as const;

const rootTaskGroups = [
  ["no group", null],
  ["empty group", { name: ROOT_TASK_GROUP_NAME, taskNames: [] }],
  ["group of lint", { name: ROOT_TASK_GROUP_NAME, taskNames: [MADE_TASK_NAME] }],
] as const;

const EARLIER_FAILURES = [
  ["no earlier failure", false],
  ["earlier failure", true],
] as const;

const EXPECTED_DECISIONS = `
made none | started none | no group | no earlier failure -> passes
made none | started none | no group | earlier failure -> passes
made none | started none | empty group | no earlier failure -> passes
made none | started none | empty group | earlier failure -> passes
made none | started none | group of lint | no earlier failure -> passes
made none | started none | group of lint | earlier failure -> passes
made none | started only the group | no group | no earlier failure -> passes
made none | started only the group | no group | earlier failure -> passes
made none | started only the group | empty group | no earlier failure -> passes
made none | started only the group | empty group | earlier failure -> passes
made none | started only the group | group of lint | no earlier failure -> passes
made none | started only the group | group of lint | earlier failure -> passes
made none | started lint | no group | no earlier failure -> passes
made none | started lint | no group | earlier failure -> passes
made none | started lint | empty group | no earlier failure -> passes
made none | started lint | empty group | earlier failure -> passes
made none | started lint | group of lint | no earlier failure -> passes
made none | started lint | group of lint | earlier failure -> passes
made lint | started none | no group | no earlier failure -> NoTaskStarted lint
made lint | started none | no group | earlier failure -> passes
made lint | started none | empty group | no earlier failure -> passes
made lint | started none | empty group | earlier failure -> passes
made lint | started none | group of lint | no earlier failure -> NoTaskStarted lint
made lint | started none | group of lint | earlier failure -> passes
made lint | started only the group | no group | no earlier failure -> passes
made lint | started only the group | no group | earlier failure -> passes
made lint | started only the group | empty group | no earlier failure -> passes
made lint | started only the group | empty group | earlier failure -> passes
made lint | started only the group | group of lint | no earlier failure -> NoTaskStarted lint
made lint | started only the group | group of lint | earlier failure -> passes
made lint | started lint | no group | no earlier failure -> passes
made lint | started lint | no group | earlier failure -> passes
made lint | started lint | empty group | no earlier failure -> passes
made lint | started lint | empty group | earlier failure -> passes
made lint | started lint | group of lint | no earlier failure -> passes
made lint | started lint | group of lint | earlier failure -> passes
`;

export const everyNoTaskStartedDecision = Task.make({
  name: "whether a run that started no task fails with NoTaskStarted is decided as its table says for every made, started, root group and earlier failure case",
  run: Effect.sync(() => {
    const decisions = madeTaskNameLists.flatMap(([madeName, madeTaskNames]) =>
      startedTaskNameLists.flatMap(([startedName, startedTaskNames]) =>
        rootTaskGroups.flatMap(([groupName, rootTaskGroup]) =>
          EARLIER_FAILURES.map(([failureName, runAlreadyFailed]) => {
            const notStartedTaskNames = decideNoTaskStarted({
              madeTaskNames,
              startedTaskNames,
              rootTaskGroup,
              runAlreadyFailed,
            });
            const decision =
              notStartedTaskNames === null
                ? "passes"
                : `NoTaskStarted ${notStartedTaskNames.join(", ")}`;
            return `${madeName} | ${startedName} | ${groupName} | ${failureName} -> ${decision}`;
          }),
        ),
      ),
    );
    expect(`\n${decisions.join("\n")}\n`).toBe(EXPECTED_DECISIONS);
  }),
});
