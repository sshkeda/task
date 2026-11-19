import { Iterable as Iterables, Predicate, Result } from "effect";

const MOST_STACK_FRAMES_PER_ERROR = 100;

const MOST_READ_STACK_CHARACTERS = 1_000_000;

const MOST_ERROR_LINE_CHARACTERS = 200;

const MOST_ERRORS_WITH_FRAMES = 20;

const MOST_FOLLOWED_INNER_ERRORS = 5;

const STACK_FRAME_LINE = /^[ \t]*at .*$/gmu;

const LINE_BREAK = /[\r\n]/u;

const readSafely = <Value>(read: () => Value, fallback: Value) =>
  Result.getOrElse(Result.try(read), () => fallback);

const readField = (held: { readonly value: unknown }, name: string): unknown =>
  readSafely(() => (Predicate.hasProperty(held.value, name) ? held.value[name] : null), null);

const keptFramesOf = (stack: string) => {
  const isCut = stack.length > MOST_READ_STACK_CHARACTERS;
  const frameLines = stack.slice(0, MOST_READ_STACK_CHARACTERS).matchAll(STACK_FRAME_LINE);
  const keptFrames = Array.from({ length: MOST_STACK_FRAMES_PER_ERROR }, () =>
    frameLines.next(),
  ).flatMap((frameLine) => (frameLine.done === true ? [] : [frameLine.value[0].trim()]));
  const leftOutCount = Iterables.size(frameLines);
  const leftOutLine = `${leftOutCount} more stack frames left out, past the first ${MOST_STACK_FRAMES_PER_ERROR}${isCut ? `, and the stack past its first ${MOST_READ_STACK_CHARACTERS} characters` : ""}`;
  const leftOutNothing = leftOutCount === 0 && !isCut;
  return leftOutNothing ? keptFrames : [...keptFrames, leftOutLine];
};

const frameBlocksOf = (held: { readonly value: unknown }) => {
  const stack = readField(held, "stack");
  const name = readField(held, "name");
  const message = readField(held, "message");
  const isReadable =
    Predicate.isString(stack) && Predicate.isString(name) && Predicate.isString(message);
  if (!isReadable) {
    return [];
  }
  const frames = keptFramesOf(stack);
  const errorText = message === "" ? name : `${name}: ${message}`;
  const [errorLine = ""] = errorText.slice(0, MOST_ERROR_LINE_CHARACTERS).split(LINE_BREAK, 1);
  return frames.length === 0 ? [] : [{ error: `stack frames of ${errorLine}`, frames }];
};

const innerErrorsOf = (held: { readonly value: unknown }): readonly unknown[] => {
  const errors = readField(held, "errors");
  const length = readField({ value: errors }, "length");
  const isList = Array.isArray(errors) && Predicate.isNumber(length);
  const count = isList ? Math.min(length, MOST_FOLLOWED_INNER_ERRORS) : 0;
  return Array.from({ length: count }, (_entry, index) =>
    readField({ value: errors }, String(index)),
  );
};

const relatedErrorsOf = (held: { readonly value: unknown }): readonly unknown[] => [
  ...innerErrorsOf(held),
  readField(held, "cause"),
];

const collectAll = (failureValues: readonly unknown[]) => {
  const followed: unknown[] = [];
  const toFollow = [...failureValues];
  for (const value of toFollow) {
    const isNew =
      value !== null && !followed.includes(value) && followed.length < MOST_ERRORS_WITH_FRAMES;
    if (isNew) {
      followed.push(value);
      toFollow.push(...relatedErrorsOf({ value }));
    }
  }
  return followed.flatMap((value) => frameBlocksOf({ value }));
};

export const collectStackFrames = (failureValues: readonly unknown[]) =>
  readSafely(() => collectAll(failureValues), []);
