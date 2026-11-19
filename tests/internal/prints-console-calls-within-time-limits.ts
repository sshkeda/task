import { expect } from "bun:test";

import { Effect } from "effect";

import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

const LOOPED_CALL_COUNT = 100_000;

const SHOWN_FAILURE_CHARACTERS = 2000;

export const loopedCallsWithinLimits = makeTest(
  "a body that calls console.log 100,000 times in one loop passes under a 2-second timeout and a 2-second cleanup limit, with every line printed under its name",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "chatty", timeout: "2 seconds", cleanup: "2 seconds", run: Effect.sync(() => {
  for (let line = 0; line < ${LOOPED_CALL_COUNT}; line++) console.log("line " + line);
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError.slice(-SHOWN_FAILURE_CHARACTERS)).toBe(0);
      const printed = ran.standardError.split("\n").filter((line) => line.startsWith("[chatty] "));
      expect(printed.length).toBe(LOOPED_CALL_COUNT);
      expect(printed.at(-1)).toBe(`[chatty] line ${String(LOOPED_CALL_COUNT - 1)}`);
    }),
  ),
);

export const alternatingStreamsWithinLimits = makeTest(
  "a body that alternates console.log and console.error 100,000 times in one loop passes under a 2-second timeout and a 2-second cleanup limit, with every line printed under its name in order",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
export default Task.make({ name: "chatty", timeout: "2 seconds", cleanup: "2 seconds", run: Effect.sync(() => {
  for (let line = 0; line < ${LOOPED_CALL_COUNT}; line++) (line % 2 === 0 ? console.log : console.error)("line " + line);
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.exitCode, ran.standardError.slice(-SHOWN_FAILURE_CHARACTERS)).toBe(0);
      const printed = ran.standardError.split("\n").filter((line) => line.startsWith("[chatty] "));
      expect(printed).toEqual(
        [...Array.from({ length: LOOPED_CALL_COUNT }).keys()].map(
          (line) => `[chatty] line ${String(line)}`,
        ),
      );
    }),
  ),
);
