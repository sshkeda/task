import { isUtf8 } from "node:buffer";

import { ByteSize, Context, Effect, Option, Schema, Stream } from "effect";
import type { FileSystem, Path, Scope } from "effect";
import { SecureRandom } from "effect/crypto";

import { inheritedRecapFile } from "./read-environment-config.ts";
import { describeNestedLinesNotShown, recapLine } from "./recap-entries.ts";
import { TASK_PACKAGE } from "./task-package.ts";

export class CommandRecapFiles extends Context.Service<
  CommandRecapFiles,
  {
    readonly makeCommandRecapPath: Effect.Effect<string | null, never, Scope.Scope>;
    readonly readCommandRecap: (path: string) => Effect.Effect<readonly string[] | undefined>;
  }
>()(`${TASK_PACKAGE.name}/CommandRecapFiles`) {}

const MAX_RECAP_FILE_KIBIBYTES = 64;

const MAX_RECAP_FILE_SIZE = ByteSize.kibibytes(MAX_RECAP_FILE_KIBIBYTES);

const MAX_RECAP_FILE_BYTES = ByteSize.toNumberUnsafe(MAX_RECAP_FILE_SIZE);

const MAX_NESTED_RECAP_LINES = 20;

const RecapFile = Schema.fromJsonString(Schema.Array(Schema.String));

const decodeRecapLines = Schema.decodeUnknownOption(RecapFile);

const encodeRecapLines = Schema.encodeOption(RecapFile);

const utf8 = new TextDecoder();

export const makeCommandRecapPath = (
  fileSystem: FileSystem.FileSystem,
  path: Path.Path,
  runDirectory: string | null,
) =>
  Effect.gen(function* () {
    const directory =
      runDirectory ??
      (yield* fileSystem.makeTempDirectoryScoped().pipe(
        Effect.asSome,
        Effect.catchTag("PlatformError", () => Effect.succeedNone),
        Effect.map(Option.getOrNull),
      ));
    return directory === null
      ? null
      : path.resolve(directory, `recap-${SecureRandom.uuidV4Unsafe()}.json`);
  });

export const readCommandRecap = (fileSystem: FileSystem.FileSystem, recapPath: string) =>
  Effect.gen(function* () {
    const information = yield* fileSystem.stat(recapPath);
    const canRead = information.type === "File" && information.size <= MAX_RECAP_FILE_SIZE;
    if (!canRead) {
      return Option.none();
    }
    const chunks = yield* fileSystem
      .stream(recapPath, { bytesToRead: MAX_RECAP_FILE_BYTES + 1 })
      .pipe(Stream.runCollect);
    const bytes = Bun.concatArrayBuffers(chunks);
    const canDecode = bytes.byteLength <= MAX_RECAP_FILE_BYTES && isUtf8(bytes);
    return canDecode ? decodeRecapLines(utf8.decode(bytes)) : Option.none();
  }).pipe(
    Effect.catchTag("PlatformError", () => Effect.succeedNone),
    Effect.map((lines) =>
      Option.getOrUndefined(
        Option.map(lines, (recap) => [
          ...recap.slice(0, MAX_NESTED_RECAP_LINES).map((line) => recapLine(line)),
          ...(recap.length > MAX_NESTED_RECAP_LINES
            ? [describeNestedLinesNotShown(recap.length - MAX_NESTED_RECAP_LINES)]
            : []),
        ]),
      ),
    ),
  );

const fitsTheRecapFile = (json: string) => Buffer.byteLength(json) <= MAX_RECAP_FILE_BYTES;

const encodeWithinRecapFile = (
  lines: readonly string[],
  shownCount: number,
): Option.Option<string> => {
  const json = encodeRecapLines(
    shownCount === lines.length
      ? lines
      : [...lines.slice(0, shownCount), describeNestedLinesNotShown(lines.length - shownCount)],
  );
  const isWritten = shownCount === 0 || Option.exists(json, fitsTheRecapFile);
  return isWritten ? json : encodeWithinRecapFile(lines, shownCount - 1);
};

export const writeCommandRecap = (fileSystem: FileSystem.FileSystem, lines: readonly string[]) =>
  Effect.gen(function* () {
    const recapFile = yield* inheritedRecapFile;
    if (Option.isNone(recapFile)) {
      return;
    }
    const json = encodeWithinRecapFile(
      lines,
      lines.length > MAX_NESTED_RECAP_LINES ? MAX_NESTED_RECAP_LINES - 1 : lines.length,
    );
    if (Option.isSome(json)) {
      yield* fileSystem.writeFileString(recapFile.value, json.value, { flag: "wx", mode: 0o600 });
    }
  }).pipe(Effect.catchTag("PlatformError", () => Effect.void));
