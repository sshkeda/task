import type { Clock } from "effect";

import { waitWhileTaskLogsAreHeld } from "../../../shared/hold-task-logs.ts";
import type { OutputPiece } from "../../../shared/label-output.ts";
import { ConsoleLineWritten } from "../../../shared/rpc.ts";
import {
  reserveTaskOutputBytes,
  takeTaskOutputBytesNow,
} from "../../../shared/task-output-budget.ts";

export const makeStreamOutputPoster =
  (
    clock: Clock.Clock,
    workerId: number,
    budget: Int32Array,
    taskLogHold: Int32Array,
    post: (message: typeof ConsoleLineWritten.Type) => void,
  ) =>
  (
    taskName: string,
    streamId: number,
    pieces: readonly (typeof OutputPiece.Type)[],
    spanIds: { readonly traceId: string; readonly spanId: string } | undefined,
  ) => {
    const heldBytes = takeTaskOutputBytesNow(budget, reserveTaskOutputBytes(taskName, pieces));
    const writtenAtMilliseconds = clock.currentTimeMillisUnsafe();
    waitWhileTaskLogsAreHeld(taskLogHold);
    post(
      ConsoleLineWritten.make({
        workerId,
        taskName,
        streamId,
        pieces,
        heldBytes,
        traceId: spanIds?.traceId ?? null,
        spanId: spanIds?.spanId ?? null,
        writtenAtMilliseconds,
      }),
    );
  };
