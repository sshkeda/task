import { expect } from "bun:test";

import { Effect } from "effect";

import packageJson from "../../package.json" with { type: "json" };
import { TASK_PACKAGE } from "../../src/shared/task-package.ts";
import { makeTest } from "../shared/make-tests.ts";

export const packageFields = makeTest(
  "the package name, version and engines in the source match package.json",
  Effect.sync(() => {
    expect(packageJson).toMatchObject(TASK_PACKAGE);
  }),
);
