import { Effect } from "effect";

import { completeOnceSent } from "./complete-discarded-calls-once-sent.ts";
import { callTheSupervisor } from "./talk-to-the-supervisor.ts";

export const reportReady = Effect.scope.pipe(
  Effect.flatMap((scope) =>
    callTheSupervisor((supervisor) =>
      supervisor.ReportReady(undefined, { discard: true }).pipe(completeOnceSent(scope)),
    ),
  ),
);
