import { Task } from "@sshkeda/task";
import type { Serving } from "@sshkeda/task";
import type { Duration, Effect } from "effect";

import type { TASK_BODY } from "../../src/shared/task-body-key.ts";

export const makeTest = (
  name: string,
  body: Effect.Effect<
    void,
    { readonly _tag: string },
    Exclude<Effect.Services<Task.Task[typeof TASK_BODY]>, Serving>
  >,
  timeout: Duration.Input = "1 minute",
) => Task.make({ name, run: body, timeout, cleanup: "5 seconds" });
