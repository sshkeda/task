import { expect } from "bun:test";

import { Effect } from "effect";

import { write } from "../shared/access-files.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

export const config = makeTest(
  "a body reads Config from the environment and a redacted value stays hidden in its logs",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { Config, Redacted } from "effect";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const name = yield* Config.String("NAME");
  const token = yield* Config.Redacted("TOKEN");
  yield* Effect.log(\`hello \${name}, token \${token}\`);
  yield* Effect.log(\`length \${Redacted.value(token).length}\`);
}) });`,
      });
      const ran = yield* runTaskProcess(project, "main.ts", { NAME: "jeff", TOKEN: "hunter2" });
      expect(ran.standardError).toContain("[main] hello jeff, token <redacted>\n[main] length 7");
      expect(`${ran.standardOutput}${ran.standardError}`).not.toContain("hunter2");
    }),
  ),
);

export const provided = makeTest(
  "a body uses FileSystem, ChildProcess and HttpClient",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
import { FileSystem } from "effect";
import { HttpClient } from "effect/http";
import { ChildProcess } from "effect/process";
export default Task.make({ name: "main", run: Effect.gen(function* () {
  const text = yield* FileSystem.FileSystem.use((fileSystem) => fileSystem.readFileString("note.txt"));
  const code = yield* ChildProcess.make("sh", ["-c", "exit 3"]).pipe(Effect.flatMap((child) => child.exitCode), Effect.scoped);
  const server = yield* Effect.acquireRelease(
    Effect.sync(() => Bun.serve({ port: 0, fetch: () => new Response("served") })),
    (server) => Effect.promise(() => server.stop(true)),
  );
  const response = yield* HttpClient.get(\`http://localhost:\${server.port}/\`);
  yield* Effect.log(JSON.stringify([text.trim(), code, yield* response.text]));
}).pipe(Effect.scoped) });`,
      });
      yield* write(`${project.directory}/note.txt`, "noted\n");
      const ran = yield* runTaskProcess(project, "main.ts");
      expect(ran.standardError).toContain('[main] ["noted",3,"served"]');
    }),
  ),
);
