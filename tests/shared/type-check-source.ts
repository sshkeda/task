import { Task } from "@sshkeda/task";
import { Effect } from "effect";

import { write } from "./access-files.ts";
import { encodeJson } from "./encode-json.ts";
import { REPOSITORY_DIRECTORY } from "./find-repository-directory.ts";
import { makeScratchDirectory } from "./make-temporary-directories.ts";

export const typeCheckSource = (text: string) =>
  Effect.gen(function* () {
    const repository = yield* REPOSITORY_DIRECTORY;
    const directory = yield* makeScratchDirectory;

    yield* write(
      `${directory}/tsconfig.json`,
      yield* encodeJson({ extends: `${repository}/tsconfig.json`, include: ["main.ts"] }),
    );
    yield* write(`${directory}/main.ts`, text);
    return yield* Task.exec(`${repository}/node_modules/.bin/tsc`, ["-p", directory]);
  });
