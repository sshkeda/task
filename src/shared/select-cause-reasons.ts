import { Cause } from "effect";

export const failuresOfCause = <Failure>(cause: Cause.Cause<Failure>) =>
  Cause.fromReasons(cause.reasons.filter((reason) => !Cause.isInterruptReason(reason)));

export const failureValuesOfReason = <Failure>(reason: Cause.Reason<Failure>) => {
  if (Cause.isFailReason(reason)) {
    return [reason.error];
  }
  return Cause.isDieReason(reason) ? [reason.defect] : [];
};

export const failureValuesOfCause = <Failure>(cause: Cause.Cause<Failure>) =>
  cause.reasons.flatMap(failureValuesOfReason);
