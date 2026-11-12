import { Effect, Stream } from "effect";
import type { Queue } from "effect";

import type { STOP_SIGNALS } from "../../../shared/stop-signals.ts";
import { completeOnceSent } from "./complete-discarded-calls-once-sent.ts";
import { callTheSupervisor } from "./talk-to-the-supervisor.ts";

export const acknowledgeStops = Effect.fn("acknowledgeStops")(function* (
  received: Queue.Dequeue<(typeof STOP_SIGNALS)[number]>,
) {
  const scope = yield* Effect.scope;
  yield* Stream.fromQueue(received).pipe(
    Stream.runForEach((signal) =>
      callTheSupervisor((supervisor) =>
        supervisor.AcknowledgeStop({ signal }, { discard: true }).pipe(completeOnceSent(scope)),
      ),
    ),
    Effect.interruptible,
    Effect.forkScoped,
  );
});
