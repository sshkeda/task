import { Schema } from "effect";

import { StandardInputRead } from "../../../../../shared/rpc.ts";

export const StandardInputStage = Schema.TaggedUnion({
  NotOpened: {},
  Reading: {},
  Idle: {},
  Ended: {},
  Failed: { reason: Schema.String },
});

export const StandardInputEvent = Schema.TaggedUnion({
  Read: { rootTaskName: Schema.String, taskName: Schema.String },
  ReceiveChunks: { chunks: Schema.NonEmptyArray(Schema.Uint8Array) },
  ReceiveEnd: {},
  ReceiveFailure: { reason: Schema.String },
});

export const StandardInputAction = Schema.TaggedUnion({
  Answer: { read: StandardInputRead },
  DieOutOfOrder: {},
  Pull: {},
});

const answer = (stage: typeof StandardInputStage.Type, read: typeof StandardInputRead.Type) =>
  [stage, StandardInputAction.cases.Answer.make({ read })] as const;

const startReading = () =>
  [StandardInputStage.cases.Reading.make({}), StandardInputAction.cases.Pull.make({})] as const;

const moveOnRead = (stage: typeof StandardInputStage.Type) =>
  StandardInputStage.match(stage, {
    NotOpened: startReading,
    Reading: () => [stage, StandardInputAction.cases.DieOutOfOrder.make({})] as const,
    Idle: startReading,
    Ended: () => answer(stage, StandardInputRead.cases.Ended.make({})),
    Failed: ({ reason }) => answer(stage, StandardInputRead.cases.Failed.make({ reason })),
  });

const answerIfReading = (
  stage: typeof StandardInputStage.Type,
  next: typeof StandardInputStage.Type,
  read: typeof StandardInputRead.Type,
) =>
  StandardInputStage.guards.Reading(stage)
    ? answer(next, read)
    : ([stage, StandardInputAction.cases.DieOutOfOrder.make({})] as const);

export const moveStandardInputStage = (
  stage: typeof StandardInputStage.Type,
  event: typeof StandardInputEvent.Type,
) =>
  StandardInputEvent.match(event, {
    Read: ({ rootTaskName, taskName }) =>
      taskName === rootTaskName
        ? moveOnRead(stage)
        : answer(stage, StandardInputRead.cases.Refused.make({ rootTaskName, taskName })),
    ReceiveChunks: ({ chunks }) =>
      answerIfReading(
        stage,
        StandardInputStage.cases.Idle.make({}),
        StandardInputRead.cases.Chunks.make({ chunks }),
      ),
    ReceiveEnd: () =>
      answerIfReading(
        stage,
        StandardInputStage.cases.Ended.make({}),
        StandardInputRead.cases.Ended.make({}),
      ),
    ReceiveFailure: ({ reason }) =>
      answerIfReading(
        stage,
        StandardInputStage.cases.Failed.make({ reason }),
        StandardInputRead.cases.Failed.make({ reason }),
      ),
  });
