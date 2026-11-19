import { Effect } from "effect";

import { nameTask } from "./label-lines.ts";
import type { OutputPiece } from "./label-output.ts";

const BUDGET_INDEX = 0;

const LIFTED = -1;

const MEBIBYTES_WAITING_TO_BE_WRITTEN = 16;

const BYTES_PER_MEBIBYTE = 1_048_576;

export const BYTES_WAITING_TO_BE_WRITTEN = MEBIBYTES_WAITING_TO_BE_WRITTEN * BYTES_PER_MEBIBYTE;

export const makeTaskOutputBudget = () =>
  new Int32Array(new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT));

export const liftTaskOutputBudget = (budget: Int32Array) => {
  Atomics.store(budget, BUDGET_INDEX, LIFTED);
  Atomics.notify(budget, BUDGET_INDEX);
};

const subtractUnlessLifted = (budget: Int32Array, bytes: number, waiting: number): void => {
  if (waiting === LIFTED) {
    return;
  }
  const seen = Atomics.compareExchange(budget, BUDGET_INDEX, waiting, waiting - bytes);
  if (seen !== waiting) {
    subtractUnlessLifted(budget, bytes, seen);
    return;
  }
  Atomics.notify(budget, BUDGET_INDEX);
};

export const releaseTaskOutputBytes = (budget: Int32Array, bytes: number) => {
  if (bytes === 0) {
    return;
  }
  subtractUnlessLifted(budget, bytes, Atomics.load(budget, BUDGET_INDEX));
};

export const tryToTakeTaskOutputBytes = (
  budget: Int32Array,
  bytes: number,
): { readonly taken: number } | { readonly waitWhile: number | null } => {
  const waiting = Atomics.load(budget, BUDGET_INDEX);
  if (waiting === LIFTED) {
    return { taken: 0 };
  }
  const fits = waiting === 0 || waiting + bytes <= BYTES_WAITING_TO_BE_WRITTEN;
  if (!fits) {
    return { waitWhile: waiting };
  }
  const isTaken =
    Atomics.compareExchange(budget, BUDGET_INDEX, waiting, waiting + bytes) === waiting;
  return isTaken ? { taken: bytes } : { waitWhile: null };
};

export const waitForTaskOutputBudgetChange = (budget: Int32Array, waitWhile: number) =>
  Effect.suspend(() => {
    const waited = Atomics.waitAsync(budget, BUDGET_INDEX, waitWhile);
    return waited.async ? Effect.promise(() => waited.value) : Effect.succeed(waited.value);
  });

export const takeTaskOutputBytesNow = (budget: Int32Array, bytes: number): number => {
  const attempt = tryToTakeTaskOutputBytes(budget, bytes);
  if ("taken" in attempt) {
    return attempt.taken;
  }
  if (attempt.waitWhile !== null) {
    Atomics.wait(budget, BUDGET_INDEX, attempt.waitWhile);
  }
  return takeTaskOutputBytesNow(budget, bytes);
};

export const takeTaskOutputBytes = (budget: Int32Array, bytes: number): Effect.Effect<number> =>
  Effect.suspend(() => {
    const attempt = tryToTakeTaskOutputBytes(budget, bytes);
    if ("taken" in attempt) {
      return Effect.succeed(attempt.taken);
    }
    if (attempt.waitWhile === null) {
      return takeTaskOutputBytes(budget, bytes);
    }
    return waitForTaskOutputBudgetChange(budget, attempt.waitWhile).pipe(
      Effect.andThen(takeTaskOutputBytes(budget, bytes)),
    );
  });

const LINE_BREAKS_A_PIECE_MAY_PRINT = 2;

export const reserveTaskOutputBytes = (
  taskName: string,
  pieces: readonly (typeof OutputPiece.Type)[],
) => {
  const bytesAroundEachPiece =
    Buffer.byteLength(`${nameTask(taskName)} `) + LINE_BREAKS_A_PIECE_MAY_PRINT;
  return pieces.reduce(
    (bytes, piece) => bytes + bytesAroundEachPiece + Buffer.byteLength(piece.text),
    0,
  );
};
