import { Schema } from "effect";

import { dieOnInvariantFailure } from "../../../shared/invariant-errors.ts";

const nameOutputMode = (options: {
  readonly secret?: boolean | undefined;
  readonly stdio?: "inherit" | false | undefined;
}) => {
  if (options.stdio === "inherit") {
    return "terminal";
  }
  return options.secret === true ? "secret" : "logged";
};

export const trustOutputModeName = <
  Options extends {
    readonly secret?: boolean | undefined;
    readonly stdio?: "inherit" | false | undefined;
  },
>(
  options: Options,
) => {
  const outputModeName = nameOutputMode(options);
  return Schema.decodeUnknownEffect(
    Schema.declare(
      (
        candidate,
      ): candidate is [
        Exclude<Options["stdio"], undefined>,
        Exclude<Options["secret"], undefined>,
      ] extends [infer Stdio, infer Secret]
        ? Stdio extends "inherit"
          ? "terminal"
          : Secret extends true
            ? "secret"
            : "logged"
        : never => candidate === outputModeName,
    ),
  )(outputModeName).pipe(
    dieOnInvariantFailure(
      "Task.exec",
      "the output mode named by the secret and stdio options matches their types",
    ),
  );
};
