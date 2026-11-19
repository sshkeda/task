import { Effect, FileSystem, Predicate, Schema } from "effect";
import type { PlatformError } from "effect";

import { TASK_TRACE_LABEL_PREFIX } from "../../../shared/names-from-package-json.ts";
import {
  ProcCommandLineUnreadable,
  ProcDirectoryUnlistable,
  ProcStatMalformed,
  ProcStatUnreadable,
} from "../../../shared/process-errors.ts";
import { availableParallelism } from "../../../shared/read-environment-config.ts";
import { baseNameOf, nameFromArgumentsOf, nameWithoutExecutable } from "./command-words.ts";

const PROC_DIRECTORY_PATH = "/proc";
const PROC_STAT_STATE_FIELD_NUMBER = 3;
const PROC_STAT_FLAGS_FIELD_NUMBER = 9;
const PROC_STAT_START_TIME_FIELD_NUMBER = 22;
const PF_EXITING = 0x00_00_00_04n;
const NO_FLAGS = 0n;

const ProcessIdFromText = Schema.FiniteFromString.check(Schema.isInt());

const decodeProcStatFields = Schema.decodeUnknownEffect(
  Schema.TupleWithRest(
    Schema.Tuple([Schema.NonEmptyString, ProcessIdFromText, ProcessIdFromText, ProcessIdFromText]),
    [Schema.String],
  ),
);

const decodeProcStatNumber = Schema.decodeUnknownEffect(
  Schema.String.check(Schema.isPattern(/^\d+$/u)),
);

export const decodeProcStatText = (statText: string) =>
  Effect.gen(function* () {
    const fieldsAfterCommand = statText
      .slice(statText.lastIndexOf(")") + ") ".length)
      .trim()
      .split(" ");
    const [state, parentProcessId, processGroupId, sessionId] =
      yield* decodeProcStatFields(fieldsAfterCommand);
    const flags = yield* decodeProcStatNumber(
      fieldsAfterCommand[PROC_STAT_FLAGS_FIELD_NUMBER - PROC_STAT_STATE_FIELD_NUMBER],
    );
    return {
      processName: statText.slice(statText.indexOf("(") + 1, statText.lastIndexOf(")")),
      state,
      isExiting: (BigInt(flags) & PF_EXITING) !== NO_FLAGS,
      parentProcessId,
      processGroupId,
      sessionId,
      startTime: yield* decodeProcStatNumber(
        fieldsAfterCommand[PROC_STAT_START_TIME_FIELD_NUMBER - PROC_STAT_STATE_FIELD_NUMBER],
      ),
    };
  });

const isEndedState = (state: string) => state.startsWith("Z") || state.startsWith("X");

const hasEndedOrIsExiting = (stat: { readonly state: string; readonly isExiting: boolean }) =>
  isEndedState(stat.state) || stat.isExiting;

const HIDDEN_PROCESS_ERROR_CODES = new Set<unknown>(["EACCES", "EPERM"]);

const errorCodeOf = (error: PlatformError.PlatformError) =>
  Predicate.hasProperty(error.reason.cause, "code") ? error.reason.cause.code : undefined;

export const nullWhenProcessGoneOrHidden = <Success, Services>(
  readProcessFile: Effect.Effect<Success, PlatformError.PlatformError, Services>,
  onHidden: Effect.Effect<void>,
) =>
  readProcessFile.pipe(
    Effect.catchReason("PlatformError", "NotFound", () => Effect.succeed(null)),
    Effect.catchTag("PlatformError", (error) => {
      const code = errorCodeOf(error);
      if (code === "ESRCH") {
        return Effect.succeed(null);
      }
      return HIDDEN_PROCESS_ERROR_CODES.has(code) ? Effect.as(onHidden, null) : Effect.fail(error);
    }),
  );

const procFileRead = <Success>(
  content: Success | null,
  hiddenBy: PlatformError.PlatformError | null,
) => ({ content, hiddenBy });

export const readProcFileUnlessGone = <Success, Services>(
  readProcessFile: Effect.Effect<Success, PlatformError.PlatformError, Services>,
) =>
  readProcessFile.pipe(
    Effect.map((content) => procFileRead<Success>(content, null)),
    Effect.catchReason("PlatformError", "NotFound", () =>
      Effect.succeed(procFileRead<Success>(null, null)),
    ),
    Effect.catchTag("PlatformError", (error) => {
      const code = errorCodeOf(error);
      if (code === "ESRCH") {
        return Effect.succeed(procFileRead<Success>(null, null));
      }
      return HIDDEN_PROCESS_ERROR_CODES.has(code)
        ? Effect.succeed(procFileRead<Success>(null, error))
        : Effect.fail(error);
    }),
  );

const readStatFile = (
  fileSystem: FileSystem.FileSystem,
  taskName: string | null,
  statFilePath: string,
  onHidden: Effect.Effect<void>,
) =>
  nullWhenProcessGoneOrHidden(fileSystem.readFileString(statFilePath), onHidden).pipe(
    Effect.mapError((cause) => new ProcStatUnreadable({ taskName, filePath: statFilePath, cause })),
    Effect.flatMap((statText) =>
      statText === null
        ? Effect.succeed(null)
        : decodeProcStatText(statText).pipe(
            Effect.mapError(
              (cause) => new ProcStatMalformed({ taskName, filePath: statFilePath, cause }),
            ),
          ),
    ),
  );

export const readProcessFromProcDirectory = Effect.fn(function* (
  taskName: string | null,
  processId: number,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const stat = yield* readStatFile(
    fileSystem,
    taskName,
    `${PROC_DIRECTORY_PATH}/${processId}/stat`,
    Effect.void,
  );
  return stat === null
    ? null
    : {
        processGroupId: stat.processGroupId,
        startTime: stat.startTime,
        hasEnded: hasEndedOrIsExiting(stat),
      };
});

export const listProcessesFromProcDirectory = Effect.fn(function* (taskName: string | null) {
  const fileSystem = yield* FileSystem.FileSystem;
  const unreadableProcessIds: number[] = [];
  const processIdTexts = (yield* fileSystem
    .readDirectory(PROC_DIRECTORY_PATH)
    .pipe(
      Effect.mapError(
        (cause) =>
          new ProcDirectoryUnlistable({ taskName, directoryPath: PROC_DIRECTORY_PATH, cause }),
      ),
    )).filter((name) => /^\d+$/u.test(name));
  const listedProcesses = yield* Effect.forEach(
    processIdTexts,
    (processIdText) =>
      Effect.gen(function* () {
        const statFilePath = `${PROC_DIRECTORY_PATH}/${processIdText}/stat`;
        const commandLineFilePath = `${PROC_DIRECTORY_PATH}/${processIdText}/cmdline`;
        const executableLinkPath = `${PROC_DIRECTORY_PATH}/${processIdText}/exe`;
        const readStat = (onHidden: Effect.Effect<void>) =>
          readStatFile(fileSystem, taskName, statFilePath, onHidden);
        const stat = yield* readStat(
          Effect.sync(() => unreadableProcessIds.push(Number(processIdText))),
        );
        if (stat === null) {
          return [];
        }
        const { processName, parentProcessId, processGroupId, sessionId, startTime } = stat;
        const readCommandLineUnlessGone = Effect.gen(function* () {
          const executablePath = yield* nullWhenProcessGoneOrHidden(
            fileSystem.readLink(executableLinkPath),
            Effect.void,
          ).pipe(
            Effect.mapError(
              (cause) =>
                new ProcCommandLineUnreadable({ taskName, filePath: executableLinkPath, cause }),
            ),
          );
          if (executablePath !== null) {
            return baseNameOf(executablePath);
          }
          const commandLine = yield* nullWhenProcessGoneOrHidden(
            fileSystem
              .readFile(commandLineFilePath)
              .pipe(
                Effect.map((commandLineBytes) =>
                  new TextDecoder("utf-8", { ignoreBOM: true }).decode(commandLineBytes),
                ),
              ),
            Effect.void,
          ).pipe(
            Effect.mapError(
              (cause) =>
                new ProcCommandLineUnreadable({ taskName, filePath: commandLineFilePath, cause }),
            ),
          );
          if (commandLine === null) {
            return null;
          }
          const argumentWords = commandLine.split("\0");
          if (argumentWords.some((argumentWord) => argumentWord !== "")) {
            return nameFromArgumentsOf(argumentWords, processName);
          }
          const statNow = yield* readStat(Effect.void);
          const isGone = statNow === null || hasEndedOrIsExiting(statNow);
          return isGone ? null : nameWithoutExecutable(processName);
        });
        return [
          [
            Number(processIdText),
            {
              parentProcessId,
              processGroupId,
              sessionId,
              startTime,
              hasEnded: hasEndedOrIsExiting(stat),
              readCommandLineUnlessGone,
            },
          ] as const,
        ];
      }),
    { concurrency: yield* availableParallelism },
  );
  if (unreadableProcessIds.length > 0) {
    yield* Effect.logInfo("skipped processes whose /proc files cannot be read").pipe(
      Effect.annotateLogs(`${TASK_TRACE_LABEL_PREFIX}.unreadableProcessIds`, unreadableProcessIds),
    );
  }
  return new Map(listedProcesses.flat());
});
