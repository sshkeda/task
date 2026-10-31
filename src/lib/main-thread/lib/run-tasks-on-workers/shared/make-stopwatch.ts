import { Option } from "effect";

const STOPPED_STOPWATCH = {
  millisecondsBeforeRunningSince: 0,
  runningSince: Option.none<number>(),
};

const readStopwatchMilliseconds = (stopwatch: typeof STOPPED_STOPWATCH, nowMilliseconds: number) =>
  stopwatch.millisecondsBeforeRunningSince +
  Option.match(stopwatch.runningSince, {
    onNone: () => 0,
    onSome: (runningSince) => nowMilliseconds - runningSince,
  });

const switchStopwatch = (
  stopwatch: typeof STOPPED_STOPWATCH,
  isRunning: boolean,
  nowMilliseconds: number,
) => {
  const isAlreadyInState = isRunning === Option.isSome(stopwatch.runningSince);
  if (isAlreadyInState) {
    return stopwatch;
  }
  return {
    millisecondsBeforeRunningSince: readStopwatchMilliseconds(stopwatch, nowMilliseconds),
    runningSince: isRunning ? Option.some(nowMilliseconds) : Option.none(),
  };
};

export const makeStopwatch = () => {
  const stopwatch = { ...STOPPED_STOPWATCH };
  return {
    readMilliseconds: (nowMilliseconds: number) =>
      readStopwatchMilliseconds(stopwatch, nowMilliseconds),
    update: (isRunning: boolean, nowMilliseconds: number) => {
      Object.assign(stopwatch, switchStopwatch(stopwatch, isRunning, nowMilliseconds));
    },
  };
};
