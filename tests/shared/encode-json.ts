import { Schema } from "effect";

export const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Json));
