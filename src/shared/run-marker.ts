import { Schema } from "effect";

export const RunMarker = Schema.Struct({
  fileDescriptor: Schema.Int,
  token: Schema.String,
  environmentValue: Schema.String,
});

const taskTokenPrefix = (marker: typeof RunMarker.Type) => `${marker.token}-`;

export const markCommandOfTask = (marker: typeof RunMarker.Type, taskName: string) =>
  `${marker.environmentValue} ${taskTokenPrefix(marker)}${Buffer.from(taskName).toString("hex")}`;

export const findTaskOfMarkedCommand = (
  marker: typeof RunMarker.Type,
  markerTokens: readonly string[],
) => {
  const taskToken = markerTokens.find((markerToken) =>
    markerToken.startsWith(taskTokenPrefix(marker)),
  );
  return taskToken === undefined
    ? null
    : Buffer.from(taskToken.slice(taskTokenPrefix(marker).length), "hex").toString();
};
