import { Effect, Schema } from "effect";

export const ProcessIdentity = Schema.Struct({ processId: Schema.Int, startTime: Schema.String });

export const processIdentityKey = (identity: typeof ProcessIdentity.Type) =>
  `${identity.processId}@${identity.startTime}`;

export const makeProcessReportClaimer = () => {
  const reportedProcessKeys = new Set<string>();
  const claimProcessesToReport = (processes: readonly (typeof ProcessIdentity.Type)[]) =>
    Effect.sync(() =>
      processes.filter((identity) => {
        const key = processIdentityKey(identity);
        if (reportedProcessKeys.has(key)) {
          return false;
        }
        reportedProcessKeys.add(key);
        return true;
      }),
    );
  return { claimProcessesToReport };
};
