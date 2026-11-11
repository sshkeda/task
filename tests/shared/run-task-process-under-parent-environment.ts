import * as BunRuntime from "@effect/platform-bun/BunRuntime";
import * as BunServices from "@effect/platform-bun/BunServices";
import { Effect, Schema } from "effect";

import { makeProject } from "./make-projects.ts";
import { runTaskProcess } from "./run-task-processes.ts";

class ParentTaskConfigLeaked extends Schema.TaggedError<ParentTaskConfigLeaked>()(
  "ParentTaskConfigLeaked",
  { exitCode: Schema.Int, standardError: Schema.String },
) {
  public override get message() {
    return `the test child saw the parent's Task config or failed with exit code ${this.exitCode}:\n${this.standardError}`;
  }
}

const program = Effect.scoped(
  Effect.gen(function* () {
    const project = yield* makeProject({
      "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
if (import.meta.main) {
  console.error(\`state directory \${process.env.SSHKEDA_TASK_STATE_DIRECTORY ?? "unset"}\`);
  console.error(\`unrelated setting \${process.env.SSHKEDA_TASK_UNRELATED_SETTING ?? "unset"}\`);
}
export default Task.make({ name: "main", run: Effect.void });`,
    });
    const ran = yield* runTaskProcess(project, "main.ts");
    const isIsolatedFromParentConfig =
      ran.exitCode === 0 &&
      ran.standardError.includes("state directory unset\n") &&
      ran.standardError.includes("unrelated setting unset\n");
    if (!isIsolatedFromParentConfig) {
      return yield* new ParentTaskConfigLeaked(ran);
    }
  }),
);

BunRuntime.runMain(program.pipe(Effect.provide(BunServices.layer)));
