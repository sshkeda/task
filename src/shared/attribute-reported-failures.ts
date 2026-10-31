import { Array as Arrays } from "effect";

import type { PrintableError } from "../lib/errors/index.ts";
import { UNTAGGED_FAILURE_TAG, withLocationOrElse } from "./report-errors.ts";

export const attributeFailures = (
  carrier: {
    readonly _tag: string;
    readonly taskName: string;
    readonly failures: Arrays.NonEmptyReadonlyArray<PrintableError>;
  },
  describeUntaggedFailure: (failureMessage: string) => string,
): Arrays.NonEmptyReadonlyArray<PrintableError> =>
  Arrays.map(carrier.failures, (failure) =>
    failure.tag === UNTAGGED_FAILURE_TAG
      ? {
          tag: carrier._tag,
          location: failure.location ?? carrier.taskName,
          message: describeUntaggedFailure(failure.message),
        }
      : withLocationOrElse(failure, carrier.taskName),
  );

export const joinMessages = (reportedErrors: readonly PrintableError[]) =>
  reportedErrors.map((reportedError) => reportedError.message).join("\n");
