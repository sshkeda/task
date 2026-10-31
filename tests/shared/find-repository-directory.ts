import { Effect, Path } from "effect";

export const REPOSITORY_DIRECTORY = Path.Path.use((path) =>
  path
    .fromFileUrl(new URL("../..", import.meta.url))
    .pipe(Effect.map((directory) => path.resolve(directory))),
);
