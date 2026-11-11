import type { PlatformError } from "effect";

const signalsOfKilledCommands = new WeakMap<PlatformError.PlatformError, string>();

export const markKilledBySignal = (failure: PlatformError.PlatformError, signal: string) => {
  signalsOfKilledCommands.set(failure, signal);
  return failure;
};

export const signalThatKilled = (failure: PlatformError.PlatformError) =>
  signalsOfKilledCommands.get(failure) ?? null;
