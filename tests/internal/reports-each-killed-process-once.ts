import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import {
  Deferred,
  Effect,
  Exit,
  Fiber,
  FileSystem,
  Logger,
  PlatformError,
  Ref,
  References,
} from "effect";
import { ChildProcess } from "effect/process";
import { RpcTest } from "effect/rpc";

import {
  claimUnknownOwnership,
  killTaskProcessGroups,
  ProcessOwnershipUnknown,
  startedProcessGroup,
} from "../../src/lib/kill-leftover-processes/index.ts";
import { findPossibleRunProcesses } from "../../src/lib/kill-leftover-processes/internal/find-unknown-ownership.ts";
import {
  decodeProcStatText,
  listProcessesFromProcDirectory,
} from "../../src/lib/kill-leftover-processes/internal/list-linux-processes-from-proc.ts";
import { listProcesses } from "../../src/lib/kill-leftover-processes/internal/list-processes.ts";
import {
  MarkerReading,
  readLinuxMarkers,
} from "../../src/lib/kill-leftover-processes/internal/mark-run-processes.ts";
import { sendSignal } from "../../src/lib/kill-leftover-processes/internal/send-signal.ts";
import { makeUnknownOwnershipClaimer } from "../../src/lib/supervise-the-run/internal/claim-unknown-ownership-in-the-run.ts";
import { SupervisorRpcs } from "../../src/lib/supervise-the-run/internal/talk-to-the-supervisor.ts";
import type { connectToTheSupervisor } from "../../src/lib/supervise-the-run/internal/talk-to-the-supervisor.ts";
import { makeProcessReportClaimer } from "../../src/shared/claim-processes-to-report.ts";
import type { ProcessIdentity } from "../../src/shared/claim-processes-to-report.ts";
import { TASK_TRACE_LABEL_PREFIX } from "../../src/shared/names-from-package-json.ts";
import { RUN_MARKERS_VARIABLE_NAME } from "../../src/shared/read-environment-config.ts";
import { RunMarker } from "../../src/shared/run-marker.ts";
import { failTestCleanup } from "../shared/fail-test-cleanup.ts";
import { exitAsValue } from "../shared/failure-as-value.ts";
import { isProcessAlive, waitUntilProcessEnds } from "../shared/is-process-alive.ts";
import { makeTest } from "../shared/make-tests.ts";

const REGISTRATION = 1;

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

const sweepAsTask = (
  leftover: typeof ProcessIdentity.Type,
  claimProcessesToReport: ReturnType<typeof makeProcessReportClaimer>["claimProcessesToReport"],
  sendSignalNow: typeof sendSignal = sendSignal,
) =>
  Effect.gen(function* () {
    const reported = yield* Ref.make<readonly (typeof ProcessIdentity.Type)[]>([]);
    const endedProcessGroupIds = yield* killTaskProcessGroups(
      new Map([
        [leftover.processId, startedProcessGroup(REGISTRATION, "leaver", leftover.startTime, null)],
      ]),
      "leaver",
      (processes) =>
        claimProcessesToReport(processes).pipe(
          Effect.tap((firstReports) => Ref.set(reported, firstReports)),
        ),
      null,
      listProcesses,
      sendSignalNow,
    );
    return {
      reported: yield* Ref.get(reported),
      isForgotten: endedProcessGroupIds.has(leftover.processId),
    };
  });

const killAsTaskCollectingReports = (
  leftover: typeof ProcessIdentity.Type,
  claimProcessesToReport: ReturnType<typeof makeProcessReportClaimer>["claimProcessesToReport"],
) => Effect.map(sweepAsTask(leftover, claimProcessesToReport), (swept) => swept.reported);

const PROBE_SIGNAL = 0;

const deniedFor =
  (isDenied: (processId: number, signal: Parameters<typeof sendSignal>[2]) => boolean) =>
  (...[taskName, processId, signal]: Parameters<typeof sendSignal>) =>
    isDenied(processId, signal)
      ? Effect.succeed("EPERM" as const)
      : sendSignal(taskName, processId, signal);

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
      yield* waitUntilProcessEnds(leftover.processId);
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
      yield* waitUntilProcessEnds(leftover.processId);
    }),
  ),
);

export const probeDenied = makeTest(
  "a recorded process group whose existence probe returns EPERM is still swept, so its leftover is killed and reported, and the group is forgotten only once it is killed",
  Effect.scoped(
    Effect.gen(function* () {
      const leftover = yield* startLeftoverGroupLeader;
      const { claimProcessesToReport } = makeProcessReportClaimer();
      const swept = yield* sweepAsTask(
        leftover,
        claimProcessesToReport,
        deniedFor((processId, signal) => processId < 0 && signal === PROBE_SIGNAL),
      );
      yield* waitUntilProcessEnds(leftover.processId);
      expect(swept).toEqual({ reported: [leftover], isForgotten: true });
    }),
  ),
);

export const groupKillDenied = makeTest(
  "a process group whose kill returns EPERM stays recorded while its member lives, and the member is reported as not killable once across sweeps",
  Effect.scoped(
    Effect.gen(function* () {
      const leftover = yield* startLeftoverGroupLeader;
      const { claimProcessesToReport } = makeProcessReportClaimer();
      const denyEveryKill = deniedFor((_processId, signal) => signal === "SIGKILL");
      expect(yield* sweepAsTask(leftover, claimProcessesToReport, denyEveryKill)).toEqual({
        reported: [leftover],
        isForgotten: false,
      });
      expect(yield* sweepAsTask(leftover, claimProcessesToReport, denyEveryKill)).toEqual({
        reported: [],
        isForgotten: false,
      });
      expect(yield* isProcessAlive(leftover.processId)).toBe(true);
      expect(yield* sweepAsTask(leftover, claimProcessesToReport)).toEqual({
        reported: [],
        isForgotten: true,
      });
      yield* waitUntilProcessEnds(leftover.processId);
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
      isExiting: false,
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
    const logged: {
      message: unknown;
      unreadableProcessIds: unknown;
      spanId: string | undefined;
    }[] = [];
    const recordingLogger = Logger.make((options) => {
      logged.push({
        message: options.message,
        unreadableProcessIds: options.fiber.getRef(References.CurrentLogAnnotations)[
          `${TASK_TRACE_LABEL_PREFIX}.unreadableProcessIds`
        ],
        spanId: options.fiber.cache.span?.spanId,
      });
    });
    const { cleanupSpan, listed } = yield* Effect.all({
      cleanupSpan: Effect.currentSpan,
      listed: listProcessesFromProcDirectory("main"),
    }).pipe(
      Effect.withSpan("cleanup main"),
      Effect.provideService(Logger.CurrentLoggers, new Set([recordingLogger])),
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
    expect(logged).toEqual([
      {
        message: ["skipped processes whose /proc files cannot be read"],
        unreadableProcessIds: [1, ACCESS_DENIED_PROCESS_ID],
        spanId: cleanupSpan.spanId,
      },
    ]);
  }),
);

const FIXTURE_MARKER = RunMarker.make({
  fileDescriptor: 3,
  token: "run-token",
  environmentValue: "run-token",
});

const FIXTURE_MARKER_PIPE = "pipe:[42]";

const PROC_STAT_FIELDS_BEFORE_START_TIME = 15;

const PROC_PATH_PROCESS_ID_INDEX = 2;

const ENVIRONMENT_READS_OF_A_HIDDEN_PROCESS = 2;

const procStatLine = (filePath: string, state: string) => {
  const processId = filePath.split("/")[PROC_PATH_PROCESS_ID_INDEX];
  return `${processId} (tsc) ${state} 1 ${processId} ${processId} ${"0 ".repeat(PROC_STAT_FIELDS_BEFORE_START_TIME)}300`;
};

const RUNNING_STATE = "S";

const UNMARKED_PROCESS_ID = 400;

const MARKED_PROCESS_ID = 500;

const OLDER_ACCESS_DENIED_PROCESS_ID = 600;

const BEFORE_THE_RUN_START_TIME = "100";

const RUN_START_TIME = "200";

const AFTER_THE_RUN_START_TIME = "300";

export const linuxHiddenMarkersUnknown = makeTest(
  "a Linux process started since the run whose environment and file descriptors cannot be read is unknown to the run, not unmarked, while one started before the run, one whose environment does not name the run and one holding the run's marker pipe are unmarked, unmarked and marked",
  Effect.gen(function* () {
    const deniedFilePaths = new Set(
      [ACCESS_DENIED_PROCESS_ID, OLDER_ACCESS_DENIED_PROCESS_ID].flatMap((processId) => [
        `/proc/${processId}/environ`,
        `/proc/${processId}/fd`,
      ]),
    );
    const readOrDeny =
      <Success>(read: (filePath: string) => Success) =>
      (filePath: string) =>
        deniedFilePaths.has(filePath)
          ? Effect.fail(deniedWith("EACCES", filePath))
          : Effect.succeed(read(filePath));
    const readings = yield* readLinuxMarkers(
      FIXTURE_MARKER,
      FIXTURE_MARKER_PIPE,
      [
        { processId: ACCESS_DENIED_PROCESS_ID, startTime: RUN_START_TIME },
        { processId: UNMARKED_PROCESS_ID, startTime: AFTER_THE_RUN_START_TIME },
        { processId: MARKED_PROCESS_ID, startTime: AFTER_THE_RUN_START_TIME },
        { processId: OLDER_ACCESS_DENIED_PROCESS_ID, startTime: BEFORE_THE_RUN_START_TIME },
      ],
      RUN_START_TIME,
    ).pipe(
      Effect.provideService(
        FileSystem.FileSystem,
        FileSystem.makeNoop({
          readFile: readOrDeny(() =>
            new TextEncoder().encode(`PATH=/bin\0${RUN_MARKERS_VARIABLE_NAME}=another-run\0`),
          ),
          readFileString: (filePath) => Effect.succeed(procStatLine(filePath, RUNNING_STATE)),
          readDirectory: readOrDeny(() => ["0", "3"]),
          readLink: readOrDeny((filePath) =>
            filePath === `/proc/${MARKED_PROCESS_ID}/fd/3` ? FIXTURE_MARKER_PIPE : "/dev/null",
          ),
        }),
      ),
    );
    expect(
      readings.map(([processId, reading]) => [
        processId,
        MarkerReading.match(reading, {
          Marked: () => "marked",
          Unmarked: () => "unmarked",
          Unreadable: () => "unknown",
        }),
      ]),
    ).toEqual([
      [ACCESS_DENIED_PROCESS_ID, "unknown"],
      [UNMARKED_PROCESS_ID, "unmarked"],
      [MARKED_PROCESS_ID, "marked"],
      [OLDER_ACCESS_DENIED_PROCESS_ID, "unmarked"],
    ]);
  }),
);

const ANCESTOR_PROCESS_ID = 700;

const UNRELATED_PARENT_PROCESS_ID = 710;

const RUN_CHILD_PROCESS_ID = 720;

const ORPHAN_PROCESS_ID = 730;

const ADOPTED_BY_ANCESTOR_PROCESS_ID = 740;

const ORPHAN_CHILD_PROCESS_ID = 750;

const MARKED_CHILD_PROCESS_ID = 760;

const UNRELATED_CHILD_PROCESS_ID = 770;

const UNRELATED_GRANDCHILD_PROCESS_ID = 780;

const FIRST_PROCESS_ID = 1;

export const linuxUnreadableUnrelatedProcessesUnmarked = makeTest(
  "a Linux process whose environment and file descriptors cannot be read is unknown only when its parent is the run, a process of the run, PID 1, an ancestor of the run that may have adopted it, or another such unknown process, and is unmarked when its parent is a readable process that is not the run's",
  Effect.gen(function* () {
    const parents = new Map([
      [process.pid, ANCESTOR_PROCESS_ID],
      [ANCESTOR_PROCESS_ID, FIRST_PROCESS_ID],
      [UNRELATED_PARENT_PROCESS_ID, FIRST_PROCESS_ID],
      [MARKED_PROCESS_ID, process.pid],
      [RUN_CHILD_PROCESS_ID, process.pid],
      [ORPHAN_PROCESS_ID, FIRST_PROCESS_ID],
      [ADOPTED_BY_ANCESTOR_PROCESS_ID, ANCESTOR_PROCESS_ID],
      [ORPHAN_CHILD_PROCESS_ID, ORPHAN_PROCESS_ID],
      [MARKED_CHILD_PROCESS_ID, MARKED_PROCESS_ID],
      [UNRELATED_CHILD_PROCESS_ID, UNRELATED_PARENT_PROCESS_ID],
      [UNRELATED_GRANDCHILD_PROCESS_ID, UNRELATED_CHILD_PROCESS_ID],
    ]);
    const deniedProcessIds = [
      RUN_CHILD_PROCESS_ID,
      ORPHAN_PROCESS_ID,
      ADOPTED_BY_ANCESTOR_PROCESS_ID,
      ORPHAN_CHILD_PROCESS_ID,
      MARKED_CHILD_PROCESS_ID,
      UNRELATED_CHILD_PROCESS_ID,
      UNRELATED_GRANDCHILD_PROCESS_ID,
    ];
    const deniedFilePaths = new Set(
      deniedProcessIds.flatMap((processId) => [
        `/proc/${processId}/environ`,
        `/proc/${processId}/fd`,
      ]),
    );
    const readOrDeny =
      <Success>(read: (filePath: string) => Success) =>
      (filePath: string) =>
        deniedFilePaths.has(filePath)
          ? Effect.fail(deniedWith("EACCES", filePath))
          : Effect.succeed(read(filePath));
    const readings = yield* readLinuxMarkers(
      FIXTURE_MARKER,
      FIXTURE_MARKER_PIPE,
      [UNRELATED_PARENT_PROCESS_ID, MARKED_PROCESS_ID, ...deniedProcessIds].map((processId) => ({
        processId,
        startTime: AFTER_THE_RUN_START_TIME,
      })),
      RUN_START_TIME,
    ).pipe(
      Effect.provideService(
        FileSystem.FileSystem,
        FileSystem.makeNoop({
          readFile: readOrDeny(() => new TextEncoder().encode("PATH=/bin\0")),
          readFileString: (filePath) => Effect.succeed(procStatLine(filePath, RUNNING_STATE)),
          readDirectory: readOrDeny(() => ["0", "3"]),
          readLink: readOrDeny((filePath) =>
            filePath === `/proc/${MARKED_PROCESS_ID}/fd/3` ? FIXTURE_MARKER_PIPE : "/dev/null",
          ),
        }),
      ),
    );
    const possibleRunProcesses = findPossibleRunProcesses(
      new Map([...parents].map(([processId, parentProcessId]) => [processId, { parentProcessId }])),
      new Map(
        readings.flatMap(([processId, reading]) =>
          MarkerReading.guards.Unreadable(reading) ? [[processId, reading.cause] as const] : [],
        ),
      ),
      readings.flatMap(([processId, reading]) =>
        MarkerReading.guards.Marked(reading) ? [processId] : [],
      ),
    );
    expect([...possibleRunProcesses.keys()].toSorted((left, right) => left - right)).toEqual([
      RUN_CHILD_PROCESS_ID,
      ORPHAN_PROCESS_ID,
      ADOPTED_BY_ANCESTOR_PROCESS_ID,
      ORPHAN_CHILD_PROCESS_ID,
      MARKED_CHILD_PROCESS_ID,
    ]);
  }),
);

const unknownOwnershipOf = (processId: number) =>
  new ProcessOwnershipUnknown({
    processId,
    startTime: AFTER_THE_RUN_START_TIME,
    command: "ssh-agent",
    cause: "EACCES",
  });

const reportedProcessIds = (claimed: readonly ProcessOwnershipUnknown[]) =>
  claimed.map((unknownOwnership) => unknownOwnership.processId);

export const unknownOwnershipReportedOnce = makeTest(
  "each process whose ownership is unknown is reported once per run, across the run's cleanup stages, the supervisor's sweep and the run's own sweep after the supervisor is gone",
  Effect.scoped(
    Effect.gen(function* () {
      const { claimProcessesToReport: claimInTheSupervisor } = makeProcessReportClaimer();
      const handlers = yield* SupervisorRpcs.toHandlers({
        ReportReady: () => Effect.void,
        AcknowledgeStop: () => Effect.void,
        ShareProcessGroup: () => Effect.void,
        RunInTerminal: () => Effect.die("the test supervisor runs no terminal commands"),
        AwaitTerminalCommandExit: () => Effect.void,
        ClaimUnknownOwnership: ({ processes }) => claimInTheSupervisor(processes),
        SweepBeforeExit: () => Effect.succeed([]),
      });
      const supervisorConnection =
        yield* Deferred.make<Effect.Success<ReturnType<typeof connectToTheSupervisor>>>();
      yield* Deferred.succeed(
        supervisorConnection,
        yield* RpcTest.makeClient(SupervisorRpcs).pipe(Effect.provideContext(handlers)),
      );
      const disconnected = yield* Deferred.make<void>();
      const claimInTheRun = makeUnknownOwnershipClaimer(supervisorConnection, disconnected);
      const first = unknownOwnershipOf(RUN_CHILD_PROCESS_ID);
      const second = unknownOwnershipOf(ORPHAN_PROCESS_ID);
      const third = unknownOwnershipOf(ORPHAN_CHILD_PROCESS_ID);
      const fourth = unknownOwnershipOf(MARKED_CHILD_PROCESS_ID);
      expect(reportedProcessIds(yield* claimUnknownOwnership([first], claimInTheRun))).toEqual([
        RUN_CHILD_PROCESS_ID,
      ]);
      expect(
        reportedProcessIds(yield* claimUnknownOwnership([first, second], claimInTheRun)),
      ).toEqual([ORPHAN_PROCESS_ID]);
      expect(
        reportedProcessIds(
          yield* claimUnknownOwnership([first, second, third], claimInTheSupervisor),
        ),
      ).toEqual([ORPHAN_CHILD_PROCESS_ID]);
      yield* Deferred.done(disconnected, Exit.void);
      expect(
        reportedProcessIds(yield* claimUnknownOwnership([first, fourth], claimInTheRun)),
      ).toEqual([MARKED_CHILD_PROCESS_ID]);
    }),
  ),
);

const ZOMBIE_PROCESS_ID = 800;

const GONE_PROCESS_ID = 810;

const READABLE_ON_REREAD_PROCESS_ID = 820;

const STILL_HIDDEN_PROCESS_ID = 830;

const ZOMBIE_STATE = "Z";

const goneWith = (filePath: string) =>
  PlatformError.systemError({
    _tag: "NotFound",
    module: "FileSystem",
    method: "readFileString",
    pathOrDescriptor: filePath,
    cause: Object.assign(new Error("ENOENT: gone"), { code: "ENOENT" }),
  });

export const linuxEndingProcessesUnmarked = makeTest(
  "a Linux process whose environment cannot be read is unmarked when its stat shows it a zombie or it is gone, is read once more while it runs, and is unknown only when that second read is still denied",
  Effect.gen(function* () {
    const environmentReads = new Map<string, number>();
    const readEnvironment = (filePath: string) => {
      const readsSoFar = (environmentReads.get(filePath) ?? 0) + 1;
      environmentReads.set(filePath, readsSoFar);
      const isReadableNow =
        filePath === `/proc/${READABLE_ON_REREAD_PROCESS_ID}/environ` && readsSoFar > 1;
      return isReadableNow
        ? Effect.succeed(new TextEncoder().encode("PATH=/bin\0"))
        : Effect.fail(deniedWith("EACCES", filePath));
    };
    const readings = yield* readLinuxMarkers(
      FIXTURE_MARKER,
      FIXTURE_MARKER_PIPE,
      [
        ZOMBIE_PROCESS_ID,
        GONE_PROCESS_ID,
        READABLE_ON_REREAD_PROCESS_ID,
        STILL_HIDDEN_PROCESS_ID,
      ].map((processId) => ({ processId, startTime: AFTER_THE_RUN_START_TIME })),
      RUN_START_TIME,
    ).pipe(
      Effect.provideService(
        FileSystem.FileSystem,
        FileSystem.makeNoop({
          readFile: readEnvironment,
          readDirectory: (filePath) =>
            filePath === `/proc/${READABLE_ON_REREAD_PROCESS_ID}/fd`
              ? Effect.succeed(["0"])
              : Effect.fail(deniedWith("EACCES", filePath)),
          readLink: () => Effect.succeed("/dev/null"),
          readFileString: (filePath) => {
            if (filePath === `/proc/${GONE_PROCESS_ID}/stat`) {
              return Effect.fail(goneWith(filePath));
            }
            return Effect.succeed(
              procStatLine(
                filePath,
                filePath === `/proc/${ZOMBIE_PROCESS_ID}/stat` ? ZOMBIE_STATE : RUNNING_STATE,
              ),
            );
          },
        }),
      ),
    );
    expect(
      readings.map(([processId, reading]) => [
        processId,
        MarkerReading.match(reading, {
          Marked: () => "marked",
          Unmarked: () => "unmarked",
          Unreadable: () => "unknown",
        }),
      ]),
    ).toEqual([
      [ZOMBIE_PROCESS_ID, "unmarked"],
      [GONE_PROCESS_ID, "unmarked"],
      [READABLE_ON_REREAD_PROCESS_ID, "unmarked"],
      [STILL_HIDDEN_PROCESS_ID, "unknown"],
    ]);
    expect(environmentReads.get(`/proc/${STILL_HIDDEN_PROCESS_ID}/environ`)).toBe(
      ENVIRONMENT_READS_OF_A_HIDDEN_PROCESS,
    );
    expect(environmentReads.get(`/proc/${ZOMBIE_PROCESS_ID}/environ`)).toBe(1);
  }),
);
