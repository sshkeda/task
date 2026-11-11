import { Effect, flow, Schema } from "effect";

export const NO_TASK_OUTPUT = { isDeclared: false } as const;

const decodeJson = Schema.decodeUnknownEffect(Schema.Json);

export const declareTaskOutput = (output: Schema.Codec<unknown, unknown>) => {
  const codec = Schema.toCodecJson(output);
  return {
    isDeclared: true,
    encode: flow(Schema.encodeUnknownEffect(codec), Effect.flatMap(decodeJson)),
    decode: Schema.decodeUnknownEffect(codec),
  } as const;
};
