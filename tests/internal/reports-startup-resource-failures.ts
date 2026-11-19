import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect } from "effect";
import { SocketServer } from "effect/socket";

import { supervisorSocketNotListening } from "../../src/lib/supervise-the-run/internal/supervise-run.ts";
import { describeCauseMessage } from "../../src/shared/report-errors.ts";
import { exists } from "../shared/access-files.ts";
import { findMisprintedErrorLines } from "../shared/find-misprinted-error-lines.ts";
import { makeProject } from "../shared/make-projects.ts";
import { makeTest } from "../shared/make-tests.ts";
import { runTaskProcess } from "../shared/run-task-processes.ts";

export const removedRootFile = makeTest(
  "a root task file removed before run fails startup with the original filename",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { unlinkSync } from "node:fs";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
unlinkSync(import.meta.filename);
export default Task.make({ name: "main", run: Effect.log("body ran") });`,
      });
      const failedRun = yield* runTaskProcess(project, "main.ts");
      expect(failedRun.exitCode).toBe(1);
      expect(failedRun.standardError).toContain(
        "main.ts: Error: ENOENT: no such file or directory, lstat ",
      );
      expect(failedRun.standardError).toContain(
        " (ENOENT)\nerror: could not resolve the root task file",
      );
      expect(failedRun.standardError).not.toContain("FileSystem.");
      expect(findMisprintedErrorLines(failedRun.standardError)).toEqual([]);
      expect(failedRun.standardError).not.toContain("body ran");
      expect(failedRun.standardError).not.toContain("PlatformError");
    }),
  ),
);

export const inaccessibleWorkingDirectory = makeTest(
  "a chdir failure after task discovery reports the package directory before any body starts, followed by a failed summary of no tasks and the run line naming the run folder",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const changeWorkingDirectory = process.chdir;
process.chdir = (directory) => changeWorkingDirectory(directory + "/missing-directory");
export default Task.make({ name: "main", run: Effect.log("body ran") });`,
      });
      const failedRun = yield* runTaskProcess(project, "main.ts");
      expect(failedRun.exitCode).toBe(1);
      const [firstRunLine, errorLine, contextLine, summaryLine, runLine, ...linesAfterIt] =
        failedRun.standardError.split("\n");
      expect(firstRunLine).toBe(runLine);
      expect(summaryLine).toMatch(/^failed: 0 of 0 tasks, \d+\.\ds$/u);
      expect(runLine).toStartWith(`run: ${project.home}/.sshkeda-task/runs/project/`);
      expect(runLine).toEndWith("/");
      expect(linesAfterIt).toEqual([]);
      expect(yield* exists(`${runLine?.slice("run: ".length)}trace.jsonl`)).toBe(true);
      expect(errorLine).toBe(
        `error: Error: ENOENT: no such file or directory, chdir '${project.directory}' -> '${project.directory}/missing-directory' (ENOENT)`,
      );
      expect(contextLine).toBe(`error: could not enter root task directory "${project.directory}"`);
      expect(findMisprintedErrorLines(failedRun.standardError)).toEqual([]);
      expect(failedRun.standardError).not.toContain("body ran");
    }),
  ),
);

const SOCKET_PATH = "/tmp/sshkeda_task-supervisor-test/supervisor.sock";

const listenError = (message: string) =>
  Object.assign(new Error(message), { code: "EPERM", errno: 1, syscall: "listen" });

const supervisorSocketMessage = (message: string) =>
  supervisorSocketNotListening(
    new SocketServer.SocketServerError({
      reason: new SocketServer.SocketServerOpenError({ cause: listenError(message) }),
    }),
  ).message;

export const supervisorSocket = Task.make({
  name: "a supervisor socket that cannot listen names the listen failure and its error code",
  run: Effect.sync(() => {
    expect(supervisorSocketMessage(`Failed to listen at ${SOCKET_PATH}`)).toBe(
      `Failed to listen at ${SOCKET_PATH} (EPERM)\ncould not open the socket the run reports to its supervisor on`,
    );
    expect(supervisorSocketMessage(`listen EPERM: operation not permitted ${SOCKET_PATH}`)).toBe(
      `listen EPERM: operation not permitted ${SOCKET_PATH} (EPERM)\ncould not open the socket the run reports to its supervisor on`,
    );
  }),
});

export const locatedSystemError = Task.make({
  name: "a system error that names its file keeps both the file and its error code",
  run: Effect.sync(() => {
    expect(
      describeCauseMessage({
        filePath: SOCKET_PATH,
        message: "permission denied",
        code: "EPERM",
        errno: 1,
        syscall: "open",
      }),
    ).toBe(`${SOCKET_PATH} permission denied (EPERM)`);
  }),
});

export const applicationErrorCode = Task.make({
  name: "an error code from the task's own error is not printed as a system error code",
  run: Effect.sync(() => {
    expect(describeCauseMessage({ code: "EXPIRED", message: "session ended" })).toBe(
      "session ended",
    );
  }),
});

export const packageSearch = makeTest(
  "a package manifest symlink loop fails the package search with its path and cause",
  Effect.scoped(
    Effect.gen(function* () {
      const project = yield* makeProject({
        "main.ts": `import { unlinkSync, symlinkSync } from "node:fs";
import { Effect } from "effect";
import { Task } from "@sshkeda/task";
const packageJsonFilePath = import.meta.dir + "/package.json";
unlinkSync(packageJsonFilePath);
symlinkSync(packageJsonFilePath, packageJsonFilePath);
export default Task.make({ name: "main", run: Effect.log("body ran") });`,
      });
      const failedRun = yield* runTaskProcess(project, "main.ts");
      expect(failedRun.exitCode).toBe(1);
      expect(failedRun.standardError).toContain(
        "package.json: Error: ELOOP: too many symbolic links encountered, access ",
      );
      expect(failedRun.standardError).toContain(
        " (ELOOP)\nerror: could not check for package.json",
      );
      expect(failedRun.standardError).not.toContain("FileSystem.");
      expect(findMisprintedErrorLines(failedRun.standardError)).toEqual([]);
      expect(failedRun.standardError).not.toContain("body ran");
      expect(failedRun.standardError).not.toContain("PlatformError");
    }),
  ),
);
