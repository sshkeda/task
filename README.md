# Task

Delete your bash scripts! Your agent will thank you.

Task is a task runner for Bun. It runs your builds, servers and CI, does independent work in parallel, waits only where one piece needs another, and stops everything at the end.

## How it works

A task is one named piece of work, such as a build, a typecheck or a server. When a task needs another task, it waits for that task and gets its result. Each task runs once per run, however many tasks need it. You write tasks in TypeScript, but the commands they start can use any runtime, such as Node or Python.

Here's an example CI script. The end-to-end tests start your server, get its URL and test against it, while typecheck runs alongside:

```ts
// scripts/ci.ts
import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import { app } from "../src/app.ts";

export const server = Task.make(
  "server",
  Effect.gen(function* () {
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    return yield* Task.serve(server.url.href);
  }),
);
export const typecheck = Task.make("typecheck", Task.exec("bunx", ["tsc", "--noEmit"]));
export const e2e = Task.make(
  "e2e",
  Effect.gen(function* () {
    const url = yield* server;
    yield* Task.exec("bun", ["test", "e2e"], { env: { APP_URL: url } });
  }),
);
export const ci = Task.make("ci", Task.allSettled([typecheck, e2e]));

if (import.meta.main) {
  Task.main(ci);
}
```

Run it with `bun scripts/ci.ts`:

```
[ci] started
[typecheck] started
[e2e] started
[server] started
[server] serving after 0.0s
[e2e] bun test v1.4.4
[e2e]
[e2e] e2e/health.test.ts:
[e2e] (pass) the server is healthy [1.70ms]
[e2e]
[e2e]  1 pass
[e2e]  0 fail
[e2e]  1 expect() calls
[e2e] Ran 1 test across 1 file. [9.00ms]
[e2e] passed in 0.0s
[typecheck] passed in 0.5s
[ci] passed in 0.5s
[server] closed
passed: 4 tasks, 0.6s
wrote the trace to /Users/you/.sshkeda-task/traces/my-app/2026-10-29T06-57-42.654Z-48677.jsonl
```

Every run writes an OpenTelemetry trace file, so you can see exactly what makes it slow.

## What you get

- **Multithreaded parallelism.** Task bodies run on a pool of worker threads, so independent tasks run side by side instead of blocking a single thread.
- **Any runtime in one run.** Vitest on Node, `bun test` and a Python script can all run side by side.
- **Values passed straight between tasks.** A task hands its result, such as a server's URL, to the tasks that use it, with no temporary files or wait scripts in between.
- **Observable errors.** The last line names the command that failed and its error, so you don't have to dig through logs.
- **Timeouts per task.** A hung test fails the run after the limit you set instead of blocking CI forever.
- **Everything stops when the run ends.** Whether it passes, fails, times out or you press Ctrl-C, Task stops every task and the programs they started.

## API

- `Task.make(name, body, { timeout?, cleanup? })` makes a task from an Effect. `cleanup` is how long the task may take to stop, and it defaults to 5 seconds.
- `Task.exec(command, args, { env?, cwd?, stdin?, secret? })` runs a command, returns its output and fails if it exits with an error. The command sees only `HOME`, `PATH` and `TMPDIR` from your environment, so pass anything else it needs in `env`. With `secret: true`, the output comes back as a `Redacted` value and stays out of logs.
- `return yield* Task.serve(value)` ends a task's body but keeps the task running, such as a dev server, and hands `value` to the tasks that use it.
- `Task.main(task)` runs a task and everything it uses, then exits.
- `Task.all(tasks)` runs tasks in parallel and stops at the first failure. `Task.allSettled(tasks)` lets every task finish, then reports each failure.

## Get started

Task needs Bun 1.4.4 or newer.

1. Install Task, Effect and Bun's types.

   ```bash
   bun add @sshkeda/task effect
   bun add -d @types/bun
   ```

2. Add these options to `compilerOptions` in `tsconfig.json`.

   ```json
   "allowImportingTsExtensions": true,
   "types": ["bun"]
   ```

3. Write your tasks in a TypeScript file, such as `scripts/ci.ts`.

   ```ts
   import { Task } from "@sshkeda/task";

   export const test = Task.make("test", Task.exec("bun", ["test"]));

   if (import.meta.main) {
     Task.main(test);
   }
   ```

4. Run the file.

   ```bash
   bun scripts/ci.ts
   ```

## Limits

- Task runs only on Bun. Your commands can use any runtime, but your task files run on Bun.
- Task bodies are Effect, so every step gets typed errors, retries and timeouts. If you haven't used Effect, your agent has.
- Task has no caching. Every run runs every task it reaches.

## Development

To run the checks and tests, run `bun scripts/ci.ts`.

[MIT](LICENSE)
