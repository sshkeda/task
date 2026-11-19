import { Schema } from "effect";

const writeNonFiniteNumbersAsStrings = (_key: string, value: Schema.Json) => {
  const isNonFiniteNumber = typeof value === "number" && !Number.isFinite(value);
  return isNonFiniteNumber ? String(value) : value;
};

export const encodeTraceJson = Schema.encodeOption(
  Schema.fromJsonString(Schema.Unknown, { replacer: writeNonFiniteNumbersAsStrings }),
);
