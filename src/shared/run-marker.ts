import { Schema } from "effect";

export const RunMarker = Schema.Struct({
  fileDescriptor: Schema.Int,
  token: Schema.String,
  environmentValue: Schema.String,
});
