import { expect } from "bun:test";

import { Task } from "@sshkeda/task";
import { Effect, Exit, PlatformError } from "effect";

import { CommandStandardInputFailed } from "../../src/lib/errors/index.ts";
import { CommandEnd, decideCommandEnd } from "../../src/lib/task/internal/decide-command-end.ts";
import { markKilledBySignal } from "../../src/shared/killed-by-signal.ts";

const FAILED_EXIT_CODE = 3;

const exitCodeNotRead = () =>
  PlatformError.systemError({
    _tag: "Unknown",
    module: "ChildProcess",
    method: "exitCode",
    description: "exit code not read",
  });

const exitCodeExits = [
  ["exit 0", Exit.succeed(0)],
  [`exit ${FAILED_EXIT_CODE}`, Exit.succeed(FAILED_EXIT_CODE)],
  ["killed by SIGTERM", Exit.fail(markKilledBySignal(exitCodeNotRead(), "SIGTERM"))],
  ["exit failed", Exit.fail(exitCodeNotRead())],
  ["exit died", Exit.die(new Error("exit died"))],
] as const;

const standardInputFailureLists = [
  ["no input failure", []],
  [
    "input failed",
    [new CommandStandardInputFailed({ command: "cat", cause: new Error("input failed") })],
  ],
] as const;

const describeCommandEnd = CommandEnd.match({
  Passed: () => "Passed",
  ExitedWithCode: ({ code, standardInputFailures }) =>
    `ExitedWithCode ${code} with ${standardInputFailures.length} input failures`,
  KilledBySignal: ({ signal }) => `KilledBySignal ${signal}`,
  ExitFailedWithoutSignal: () => "ExitFailedWithoutSignal",
  StandardInputFailed: ({ standardInputFailures }) =>
    `StandardInputFailed with ${standardInputFailures.length} input failures`,
});

const EXPECTED_ENDS = `
exit 0 | no input failure -> Passed
exit 0 | input failed -> StandardInputFailed with 1 input failures
exit 3 | no input failure -> ExitedWithCode 3 with 0 input failures
exit 3 | input failed -> ExitedWithCode 3 with 1 input failures
killed by SIGTERM | no input failure -> KilledBySignal SIGTERM
killed by SIGTERM | input failed -> StandardInputFailed with 1 input failures
exit failed | no input failure -> ExitFailedWithoutSignal
exit failed | input failed -> StandardInputFailed with 1 input failures
exit died | no input failure -> ExitFailedWithoutSignal
exit died | input failed -> StandardInputFailed with 1 input failures
`;

export const everyCommandEnd = Task.make({
  name: "how a command ended is decided as its table says for every exit and standard input result",
  run: Effect.sync(() => {
    const ends = exitCodeExits.flatMap(([exitName, exitCodeExit]) =>
      standardInputFailureLists.map(([failuresName, standardInputFailures]) => {
        const end = decideCommandEnd(exitCodeExit, standardInputFailures);
        return `${exitName} | ${failuresName} -> ${describeCommandEnd(end)}`;
      }),
    );
    expect(`\n${ends.join("\n")}\n`).toBe(EXPECTED_ENDS);
  }),
});
