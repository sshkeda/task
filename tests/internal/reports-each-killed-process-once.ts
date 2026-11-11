import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect, Exit, Fiber, FileSystem, PlatformError, Ref, Tracer } from "effect";
import { ChildProcess } from "effect/process";

import {
  killTaskProcessGroups,
  startedProcessGroup,
} from "../../src/lib/kill-leftover-processes/index.ts";
import {
  decodeProcStatText,
  listProcessesFromProcDirectory,
} from "../../src/lib/kill-leftover-processes/internal/list-linux-processes-from-proc.ts";
import { listProcesses } from "../../src/lib/kill-leftover-processes/internal/list-processes.ts";
import { makeProcessReportClaimer } from "../../src/shared/claim-processes-to-report.ts";
import type { ProcessIdentity } from "../../src/shared/claim-processes-to-report.ts";
import { TASK_TRACE_LABEL_PREFIX } from "../../src/shared/names-from-package-json.ts";
import { failTestCleanup } from "../shared/fail-test-cleanup.ts";
import { exitAsValue } from "../shared/failure-as-value.ts";
import { isProcessAlive } from "../shared/is-process-alive.ts";
import { makeTest } from "../shared/make-tests.ts";

const READABLE_PROCESS_ID = 300;

const ACCESS_DENIED_PROCESS_ID = 2;

const startLeftoverGroupLeader = Effect.gen(function* () {
  const sleep = yield* ChildProcess.make("sleep", ["1000"]);
  yield* Effect.addFinalizer(() =>
    Task.exec("kill", ["-KILL", String(sleep.pid)]).pipe(
      Effect.catchTag("CommandFailed", () => Effect.void),
      failTestCleanup(`kill test process ${sleep.pid}`),
    ),
  );
  const listedSleep = yield* Effect.fromNullishOr((yield* listProcesses(null)).get(sleep.pid));
  expect(listedSleep.processGroupId).toBe(sleep.pid);
  return { processId: sleep.pid, startTime: listedSleep.startTime };
});

const killAsTaskCollectingReports = (
  leftover: typeof ProcessIdentity.Type,
  claimProcessesToReport: ReturnType<typeof makeProcessReportClaimer>["claimProcessesToReport"],
) =>
  Effect.gen(function* () {
    const reported = yield* Ref.make<readonly (typeof ProcessIdentity.Type)[]>([]);
    yield* killTaskProcessGroups(
      new Map([[leftover.processId, startedProcessGroup("leaver", leftover.startTime, null)]]),
      "leaver",
      (processes) =>
        claimProcessesToReport(processes).pipe(
          Effect.tap((firstReports) => Ref.set(reported, firstReports)),
        ),
    );
    return yield* Ref.get(reported);
  });

export const claimedOnce = makeTest(
  "a run's record of reported kills grants each process, keyed by pid and start time, once",
  Effect.gen(function* () {
    const { claimProcessesToReport } = makeProcessReportClaimer();
    const firstProcess = { processId: 4242, startTime: "1700000000.1" };
    const reusedProcessId = { processId: 4242, startTime: "1700000999.1" };
    expect(yield* claimProcessesToReport([firstProcess])).toEqual([firstProcess]);
    expect(yield* claimProcessesToReport([firstProcess, reusedProcessId])).toEqual([
      reusedProcessId,
    ]);
    expect(yield* claimProcessesToReport([firstProcess, reusedProcessId])).toEqual([]);
  }),
);

export const signaledAgainReportedOnce = makeTest(
  "a kill pass signals again a process an earlier pass of the run already reported, and does not report it a second time",
  Effect.scoped(
    Effect.gen(function* () {
      const leftover = yield* startLeftoverGroupLeader;
      const { claimProcessesToReport } = makeProcessReportClaimer();
      yield* claimProcessesToReport([leftover]);
      expect(yield* killAsTaskCollectingReports(leftover, claimProcessesToReport)).toEqual([]);
      expect(yield* isProcessAlive(leftover.processId)).toBe(false);
    }),
  ),
);

export const interruptedPass = makeTest(
  "a kill pass interrupted before it signals records nothing, so the next pass kills the leftover and reports it",
  Effect.scoped(
    Effect.gen(function* () {
      const leftover = yield* startLeftoverGroupLeader;
      const { claimProcessesToReport } = makeProcessReportClaimer();
      const interruptedPassFiber = yield* Effect.forkChild(
        killAsTaskCollectingReports(leftover, claimProcessesToReport),
      );
      yield* Fiber.interrupt(interruptedPassFiber);
      expect(Exit.hasInterrupts(yield* exitAsValue(Fiber.join(interruptedPassFiber)))).toBe(true);
      expect(yield* isProcessAlive(leftover.processId)).toBe(true);
      expect(yield* killAsTaskCollectingReports(leftover, claimProcessesToReport)).toEqual([
        leftover,
      ]);
      expect(yield* isProcessAlive(leftover.processId)).toBe(false);
    }),
  ),
);

export const linuxStartTime = makeTest(
  "a Linux /proc/<pid>/stat line yields the state, parent, group, session and field 22 start time, even when the command name holds parentheses and spaces",
  Effect.gen(function* () {
    expect(
      yield* decodeProcStatText(
        "1234 (my (odd) name) S 1 1234 1234 0 -1 4194560 100 0 0 0 1 2 0 0 20 0 1 0 98765 1000 10\n",
      ),
    ).toEqual({
      processName: "my (odd) name",
      state: "S",
      parentProcessId: 1,
      processGroupId: 1234,
      sessionId: 1234,
      startTime: "98765",
    });
    expect(
      Exit.isFailure(yield* exitAsValue(decodeProcStatText("1234 (short) S 1 1234 1234 0\n"))),
    ).toBe(true);
  }),
);

const deniedWith = (code: string, filePath: string) =>
  PlatformError.systemError({
    _tag: code === "EACCES" ? "PermissionDenied" : "Unknown",
    module: "FileSystem",
    method: "readFileString",
    pathOrDescriptor: filePath,
    cause: Object.assign(new Error(`${code}: denied`), { code }),
  });

export const linuxHiddenProcessesSkipped = makeTest(
  "a /proc/<pid> that cannot be read, as under hidepid or another user's process, is skipped and traced instead of failing cleanup",
  Effect.gen(function* () {
    const deniedCodeOfFile = new Map([
      ["/proc/1/stat", "EPERM"],
      ["/proc/2/stat", "EACCES"],
    ]);
    const spans: Tracer.NativeSpan[] = [];
    const recordingTracer = Tracer.make({
      span: (options) => {
        const span = new Tracer.NativeSpan(options);
        spans.push(span);
        return span;
      },
    });
    const listed = yield* listProcessesFromProcDirectory("main").pipe(
      Effect.withTracer(recordingTracer),
      Effect.provideService(
        FileSystem.FileSystem,
        FileSystem.makeNoop({
          readDirectory: () => Effect.succeed(["1", "2", "self", "300"]),
          readFileString: (filePath) => {
            const deniedCode = deniedCodeOfFile.get(filePath);
            return deniedCode === undefined
              ? Effect.succeed(
                  "300 (sleep) S 1 300 300 0 -1 4194560 100 0 0 0 1 2 0 0 20 0 1 0 98765 1000 10\n",
                )
              : Effect.fail(deniedWith(deniedCode, filePath));
          },
        }),
      ),
    );
    expect([...listed.keys()]).toEqual([READABLE_PROCESS_ID]);
    expect(
      spans
        .find((span) => span.name === "listProcessesFromProcDirectory")
        ?.attributes.get(`${TASK_TRACE_LABEL_PREFIX}.unreadableProcessIds`),
    ).toEqual([1, ACCESS_DENIED_PROCESS_ID]);
  }),
);
