# Task

Delete your bash scripts! Your agent will thank you.

Task is a task runner for Bun. It runs your builds, servers and CI, does independent work in parallel, waits only where one piece needs another, and stops everything at the end.

## How it works

A task is one named piece of work, such as a build, a typecheck or a server. When a task needs another task, it waits for that task and gets its result. Each task runs once per run, however many tasks need it. You write tasks in TypeScript, but the commands they start can use any runtime, such as Node or Python.

Here's an example CI script. The end-to-end tests start your server, get its URL and test against it, while typecheck runs alongside:

```ts
// scripts/ci.ts
import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect, Schema } from "effect";

import { app } from "../src/app.ts";

const server = Task.make({
  name: "server",
  output: Schema.String,
  run: Effect.gen(function* () {
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    return yield* Task.serve(server.url.href);
  }),
});
const typecheck = Task.make({
  name: "typecheck",
  run: Effect.asVoid(Task.exec("bunx", ["tsc", "--noEmit"])),
});
const e2e = Task.make({
  name: "e2e",
  run: Effect.gen(function* () {
    const url = yield* server;
    const response = yield* Effect.promise(() => fetch(`${url}health`));
    expect(response.status).toBe(200);
  }),
});

export default Task.make({ name: "ci", run: Task.allSettled([typecheck, e2e]) });
```

Run it with `bun scripts/ci.ts`:

```
run: /Users/you/.sshkeda-task/runs/my-app/2026-11-18T07-17-39.064Z-5161/
serving: [server], 0.0s
closed: [server]
passed: 4 tasks, 0.4s
run: /Users/you/.sshkeda-task/runs/my-app/2026-11-18T07-17-39.064Z-5161/
```

The `run:` folder holds each task's full output and an OpenTelemetry trace of the run.

When a task fails, Task prints its error, the line it came from, and the file with its output:

```ts
// scripts/deploy.ts
import { Task } from "@sshkeda/task";
import { Effect } from "effect";

export default Task.make({
  name: "deploy",
  run: Effect.die(new Error("API_KEY is not set")),
});
```

Run it with `bun scripts/deploy.ts`:

```
run: /Users/you/.sshkeda-task/runs/my-app/2026-11-19T02-33-06.237Z-75432/
error: [deploy] scripts/deploy.ts:7:23: Error: API_KEY is not set
error: [deploy] its output is in /Users/you/.sshkeda-task/runs/my-app/2026-11-19T02-33-06.237Z-75432/1.output
ended: [deploy], 0.1s, failed
recap: [deploy] 1.output | scripts/deploy.ts:7:23: Error: API_KEY is not set
failed: [deploy] (1 of 1 task), 0.2s
run: /Users/you/.sshkeda-task/runs/my-app/2026-11-19T02-33-06.237Z-75432/
```

The `recap:` lines repeat each failure at the end, so you see them even after a long run. The `run:` line prints first and last for the same reason.

## What you get

- **Multithreaded parallelism.** Task bodies run on a pool of worker threads, so independent tasks run side by side instead of blocking a single thread.
- **Any runtime in one run.** Vitest on Node, `bun test` and a Python script can all run side by side.
- **Values passed straight between tasks.** A task hands its result, such as a server's URL, to the tasks that use it, checked against the schema it declares, with no temporary files or wait scripts in between.
- **Observable errors.** Each error names its task and the line in your code it came from, and a failed run ends with each failed task's error and output file, so you don't have to dig through logs.
- **Timeouts per task.** A hung test fails the run after the limit you set instead of blocking CI forever.
- **Everything stops when the run ends.** Whether it passes, fails, times out or you press Ctrl-C, Task stops every task and the programs they started.

## API

- `Task.make({ name, run, output?, flags?, timeout?, cleanup?, isolated? })` makes a task from an Effect. `output` is the schema of the value the task returns or serves. Without it, `run` must return nothing, so wrap a value you don't need in `Effect.asVoid`. `flags` is a `Schema.Struct` of the command-line flags the task reads; with it, `run` is a function of their values. Every task that reads a flag gets the value you pass. `timeout` is how long the task's own work may take, not counting time it waits for a task it uses. `cleanup` is how long the task may take to stop, and it defaults to 5 seconds. `isolated: true` runs the task on its own worker, so globals it changes cannot reach later tasks.

  ```ts
  // bun scripts/deploy.ts --stage prod --dry-run
  export default Task.make({
    name: "deploy",
    flags: Schema.Struct({
      stage: Schema.Literals(["dev", "prod"]),
      dryRun: Schema.optional(Schema.Boolean),
    }),
    run: ({ stage, dryRun }) =>
      Effect.asVoid(Task.exec("./deploy.sh", dryRun ? [stage, "--dry-run"] : [stage])),
  });
  ```

- `Task.exec(command, args, { env?, cwd?, stdin?, print?, secret?, stdio? })` runs a command, returns its output and fails if it exits with an error. What it writes goes to its task's file, and prints only with `print: true` or when its task fails. The command sees only `HOME`, `PATH`, `TMPDIR` and some of Task's own settings from your environment, so pass anything else it needs in `env`. It runs in the folder of the `package.json` nearest the file you ran, wherever the task is defined, and a relative `cwd` starts from there. To run in the task's own folder, pass `cwd: import.meta.dir`. With `secret: true`, the output comes back as a `Redacted` value and stays out of logs. With `stdio: "inherit"`, the command reads from and writes to your terminal directly, so interactive programs work, and nothing is returned. Only the task the run starts with can do this.
- `return yield* Task.serve(value)` ends a task's body but keeps the task running, such as a dev server, and hands `value` to the tasks that use it.
- `Task.all(tasks)` runs tasks in parallel and stops at the first failure. `Task.allSettled(tasks)` lets every task finish, then reports each failure.
- `bun scripts/ci.ts` runs the file's default export and everything it uses.
- `bun scripts/ci.ts "e2e"` runs only that task and the tasks it uses.
- `bun scripts/ci.ts --help` prints the task the file runs and the flags each task reads, without starting a task.
- `SSHKEDA_TASK_LOG=error` leaves out what tasks print and the `ended:` and `running:` lines. `SSHKEDA_TASK_LOG=debug` adds a `started:` line for every task and an `ended:` line for each task that passed.

## Get started

Task needs Bun 1.4.4 or newer, and TypeScript 7 or newer.

1. Install Task, Effect and Bun's types.

   ```bash
   bun add @sshkeda/task effect
   bun add -d @types/bun typescript
   ```

2. Add these options to `compilerOptions` in `tsconfig.json`.

   ```json
   "allowImportingTsExtensions": true,
   "types": ["bun"]
   ```

3. Write a task in a TypeScript file, such as `scripts/typecheck.ts`, and export it as the default.

   ```ts
   import { Task } from "@sshkeda/task";
   import { Effect } from "effect";

   export default Task.make({
     name: "typecheck",
     run: Effect.asVoid(Task.exec("bunx", ["tsc", "--noEmit"])),
   });
   ```

4. Run the file.

   ```bash
   bun scripts/typecheck.ts
   ```

## Limits

- Task runs only on Bun.
- Task runs on macOS and on Linux with glibc. It does not run on Windows or on musl Linux such as Alpine.
- Task bodies are Effect, so every step gets typed errors, retries and timeouts.
- Task has no caching. Every run runs every task it reaches.
- Task loads your task file more than once in a run, so keep work inside a task's `run`.

## Development

To run the checks and tests, run `bun scripts/ci.ts`.

[MIT](LICENSE)
