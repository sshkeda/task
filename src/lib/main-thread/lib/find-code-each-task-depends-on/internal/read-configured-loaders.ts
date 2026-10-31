import { Array as Arrays, Effect, FileSystem, Option, Path, Schema } from "effect";

import { BunfigUnparsable, CodeFileUnreadable } from "../../../../../shared/module-graph-errors.ts";
import { bunArguments } from "../../../../../shared/read-environment-config.ts";

const listFlagValues = (
  argumentList: readonly string[],
  flag: { readonly name: string; readonly takesNextArgument: boolean },
) =>
  argumentList.flatMap((argument, index) => {
    if (argument.startsWith(`${flag.name}=`)) {
      return [argument.slice(flag.name.length + 1)];
    }
    const nextArgument = argumentList[index + 1];
    const isFlagFollowedByValue =
      flag.takesNextArgument && argument === flag.name && nextArgument !== undefined;
    return isFlagFollowedByValue ? [nextArgument] : [];
  });

const decodeBunfigLoaders = Schema.decodeUnknownEffect(
  Schema.Struct({ loader: Schema.optional(Schema.Record(Schema.String, Schema.String)) }),
);

export const readConfiguredLoaders = Effect.fn("readConfiguredLoaders")(function* (input: {
  readonly labelCodeFile: (filePath: string) => string;
  readonly workingDirectoryPathAtStart: string;
}) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const startArguments = yield* bunArguments;
  const configFilePath = Arrays.last(
    listFlagValues(startArguments, { name: "--config", takesNextArgument: false }),
  );
  const bunfigFilePath = Option.isSome(configFilePath)
    ? path.resolve(input.workingDirectoryPathAtStart, configFilePath.value)
    : path.join(input.workingDirectoryPathAtStart, "bunfig.toml");
  const bunfigText = yield* fileSystem.readFileString(bunfigFilePath).pipe(
    Effect.asSome,
    Effect.catchReason("PlatformError", "NotFound", () => Effect.succeedNone),
    Effect.mapError((cause) => new CodeFileUnreadable({ filePath: bunfigFilePath, cause })),
  );
  const bunfigLoaders = Option.isNone(bunfigText)
    ? {}
    : ((yield* Effect.try(() => Bun.TOML.parse(bunfigText.value)).pipe(
        Effect.flatMap(decodeBunfigLoaders),
        Effect.mapError(
          (cause) => new BunfigUnparsable({ filePath: input.labelCodeFile(bunfigFilePath), cause }),
        ),
      )).loader ?? {});
  const argumentLoaders = ["--loader", "-l"]
    .flatMap((name) => listFlagValues(startArguments, { name, takesNextArgument: true }))
    .flatMap((extensionAndLoader) => {
      const separatorIndex = extensionAndLoader.indexOf(":");
      return separatorIndex === -1
        ? []
        : [
            [
              extensionAndLoader.slice(0, separatorIndex),
              extensionAndLoader.slice(separatorIndex + 1),
            ] as const,
          ];
    });
  return new Map([...Object.entries(bunfigLoaders), ...argumentLoaders]);
});
