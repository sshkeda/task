import { Schema } from "effect";

import { dieOnInvariantFailure } from "../../../shared/invariant-errors.ts";

export const trustOutputModeName = <Secret extends boolean>(secret: Secret | undefined) => {
  const outputModeName = secret === true ? "secret" : "logged";
  return Schema.decodeUnknownEffect(
    Schema.declare(
      (candidate): candidate is Secret extends true ? "secret" : "logged" =>
        candidate === outputModeName,
    ),
  )(outputModeName).pipe(
    dieOnInvariantFailure(
      "Task.exec",
      "the output mode named by the secret option matches the secret option's type",
    ),
  );
};
