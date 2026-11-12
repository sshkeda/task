import { inspect } from "node:util";

import { Option, Schema } from "effect";

const decodeMessage = Schema.decodeUnknownOption(Schema.Struct({ message: Schema.String }));

export class TestCleanupFailed extends Schema.TaggedError<TestCleanupFailed>()(
  "TestCleanupFailed",
  {
    operation: Schema.String,
    cause: Schema.Defect(),
  },
) {
  public override get message() {
    return `could not ${this.operation}: ${Option.match(decodeMessage(this.cause), {
      onSome: (failure) => failure.message,
      onNone: () => inspect(this.cause),
    })}`;
  }
}

export class MissingText extends Schema.TaggedError<MissingText>()("MissingText", {
  file: Schema.String,
  text: Schema.String,
}) {
  public override get message() {
    return `${this.file} does not contain ${this.text}`;
  }
}

export class NeverPrinted extends Schema.TaggedError<NeverPrinted>()("NeverPrinted", {
  wanted: Schema.String,
  seen: Schema.String,
}) {
  public override get message() {
    return `never printed ${this.wanted}:\n${this.seen}`;
  }
}

export class SpawnLockNeverFreed extends Schema.TaggedError<SpawnLockNeverFreed>()(
  "SpawnLockNeverFreed",
  { lock: Schema.String, waited: Schema.String },
) {
  public override get message() {
    return `${this.lock} was still held after ${this.waited}, so the terminal command was not started`;
  }
}

export class PackageJsonAbove extends Schema.TaggedError<PackageJsonAbove>()("PackageJsonAbove", {
  file: Schema.String,
}) {
  public override get message() {
    return `the standalone directory has ${this.file} above it`;
  }
}

export class GitRepositoryNotCreated extends Schema.TaggedError<GitRepositoryNotCreated>()(
  "GitRepositoryNotCreated",
  {
    directory: Schema.String,
    exitCode: Schema.Int,
  },
) {
  public override get message() {
    return `git init in ${this.directory} exited with code ${this.exitCode}`;
  }
}

export class ChildStillRunning extends Schema.TaggedError<ChildStillRunning>()(
  "ChildStillRunning",
  { processId: Schema.Int },
) {
  public override get message() {
    return `the test failed while child process ${this.processId} was still running`;
  }
}

export class SucceededInsteadOfFailing extends Schema.TaggedError<SucceededInsteadOfFailing>()(
  "SucceededInsteadOfFailing",
  { value: Schema.Unknown },
) {
  public override get message() {
    return `the effect succeeded with ${inspect(this.value)}, but the test expects it to fail`;
  }
}

export class ErrorsMisprinted extends Schema.TaggedError<ErrorsMisprinted>()("ErrorsMisprinted", {
  misprinted: Schema.Array(Schema.Struct({ line: Schema.String, fault: Schema.String })),
}) {
  public override get message() {
    return `the child printed errors in the wrong format:\n${this.misprinted.map(({ line, fault }) => `${line}\n  ${fault}`).join("\n")}`;
  }
}
