import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import packageJson from "../../package.json" with { type: "json" };
import { TASK_PACKAGE } from "../../src/shared/task-package.ts";

export const packageFields = Task.make(
  "the package name, version and engines in the source match package.json",
  Effect.sync(() => {
    expect(packageJson).toMatchObject(TASK_PACKAGE);
  }),
  { timeout: "1 minute", cleanup: "5 seconds" },
);
