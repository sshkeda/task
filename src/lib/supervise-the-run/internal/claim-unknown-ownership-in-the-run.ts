import { Deferred, Effect } from "effect";

import { makeProcessReportClaimer } from "../../../shared/claim-processes-to-report.ts";
import type { ProcessIdentity } from "../../../shared/claim-processes-to-report.ts";
import type { connectToTheSupervisor } from "./talk-to-the-supervisor.ts";
import { supervisorConnectionDeferred, supervisorDisconnected } from "./talk-to-the-supervisor.ts";

export const makeUnknownOwnershipClaimer = (
  supervisorConnection: Deferred.Deferred<
    Effect.Success<ReturnType<typeof connectToTheSupervisor>>
  >,
  disconnected: Deferred.Deferred<void>,
) => {
  const { claimProcessesToReport: claimInTheRun } = makeProcessReportClaimer();
  const claimWithTheSupervisor = (processes: readonly (typeof ProcessIdentity.Type)[]) =>
    Deferred.await(supervisorConnection).pipe(
      Effect.flatMap((supervisor) => supervisor.ClaimUnknownOwnership({ processes })),
      Effect.catchTag("RpcClientError", () => Effect.succeed(processes)),
      Effect.raceFirst(Deferred.await(disconnected).pipe(Effect.as(processes))),
    );
  return (processes: readonly (typeof ProcessIdentity.Type)[]) =>
    Effect.flatMap(claimInTheRun(processes), (claimedInTheRun) => {
      const asksTheSupervisor =
        claimedInTheRun.length > 0 &&
        Deferred.isDoneUnsafe(supervisorConnection) &&
        !Deferred.isDoneUnsafe(disconnected);
      return asksTheSupervisor
        ? claimWithTheSupervisor(claimedInTheRun)
        : Effect.succeed(claimedInTheRun);
    });
};

export const claimUnknownOwnershipInTheRun = makeUnknownOwnershipClaimer(
  supervisorConnectionDeferred,
  supervisorDisconnected,
);
