import { Deferred } from "effect";

const makeRunCallDeferred = () =>
  Deferred.makeUnsafe<{
    readonly rootTaskName: string;
    readonly stack: string | undefined;
  }>();

export const runningRootTaskDeferred = makeRunCallDeferred();

export const laterRunCallDeferred = makeRunCallDeferred();
