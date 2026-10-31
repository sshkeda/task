import { Effect } from "effect";

const TASK_LOGS_RELEASED = 0;
const TASK_LOGS_HELD = 1;

export const makeTaskLogHold = () =>
  new Int32Array(new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT));

export const holdTaskLogsDuring =
  (taskLogHold: Int32Array) =>
  <Success, Failure, Services>(effect: Effect.Effect<Success, Failure, Services>) =>
    Effect.sync(() => Atomics.store(taskLogHold, 0, TASK_LOGS_HELD)).pipe(
      Effect.andThen(effect),
      Effect.ensuring(
        Effect.sync(() => {
          Atomics.store(taskLogHold, 0, TASK_LOGS_RELEASED);
          Atomics.notify(taskLogHold, 0);
        }),
      ),
    );

export const waitWhileTaskLogsAreHeld = (taskLogHold: Int32Array) => {
  Atomics.wait(taskLogHold, 0, TASK_LOGS_HELD);
};
