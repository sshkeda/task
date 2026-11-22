import { workerData } from "node:worker_threads";

import { Context, Effect, Fiber, Schema } from "effect";
import { constVoid } from "effect/Function";

import type { OutputPiece, PRINTED_TO } from "./label-output.ts";
import { WorkerData } from "./rpc.ts";
import { TASK_PACKAGE } from "./task-package.ts";

const isTaskWorkerData = Schema.is(WorkerData);

export const isInsideTaskWorkerThread = () => isTaskWorkerData(workerData);

export const InsideTaskBody = Context.Reference<boolean>(`${TASK_PACKAGE.name}/InsideTaskBody`, {
  defaultValue: () => false,
});

export const isInsideTaskBody = () => Fiber.getCurrent()?.getRef(InsideTaskBody) === true;

export const CurrentTaskName = Context.Reference<string | null>(
  `${TASK_PACKAGE.name}/CurrentTaskName`,
  { defaultValue: () => null },
);

export const CommandCalledAt = Context.Reference<string | null>(
  `${TASK_PACKAGE.name}/CommandCalledAt`,
  { defaultValue: () => null },
);

export const CommandLineFlags = Context.Reference<Readonly<Record<string, string>>>(
  `${TASK_PACKAGE.name}/CommandLineFlags`,
  { defaultValue: () => ({}) },
);

const logStreamOutput = (
  _streamId: number,
  pieces: readonly (typeof OutputPiece.Type)[],
  printedTo: (typeof PRINTED_TO)[number],
): Effect.Effect<void> =>
  printedTo === "held" ? Effect.void : Effect.log(pieces.map((piece) => piece.text).join("\n"));

export const TaskLines = Context.Reference<{
  readonly printStreamOutput: typeof logStreamOutput;
  readonly postStreamOutputNow: (
    streamId: number,
    pieces: readonly (typeof OutputPiece.Type)[],
    spanIds: { readonly traceId: string; readonly spanId: string } | undefined,
  ) => void;
  readonly settleHeldOutput: (
    streamIds: readonly number[],
    outcome: "failed" | "passed",
  ) => Effect.Effect<void>;
  readonly printWarning: (text: string) => Effect.Effect<void>;
  readonly printError: (text: string) => Effect.Effect<void>;
}>(`${TASK_PACKAGE.name}/TaskLines`, {
  defaultValue: () => ({
    printStreamOutput: logStreamOutput,
    postStreamOutputNow: constVoid,
    settleHeldOutput: () => Effect.void,
    printWarning: (text) => Effect.logWarning(text),
    printError: (text) => Effect.logError(text),
  }),
});
