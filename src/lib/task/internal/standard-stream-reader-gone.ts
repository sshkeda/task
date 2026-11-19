import { flow, Option, Schema } from "effect";

export const findErrnoCode = flow(
  Schema.decodeUnknownOption(Schema.Struct({ cause: Schema.Struct({ code: Schema.String }) })),
  Option.map((reason) => reason.cause.code),
  Option.getOrNull,
);

export const readerHasGone = (errnoCode: string | null, isTerminal: boolean) =>
  errnoCode === "EPIPE" || (errnoCode === "EIO" && isTerminal);
