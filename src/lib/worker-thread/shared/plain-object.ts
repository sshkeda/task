import { Schema } from "effect";

export const PlainObject = Schema.ObjectKeyword.check(
  Schema.makeFilter((candidate) => {
    const prototype: unknown = Object.getPrototypeOf(candidate);
    return prototype === Object.prototype || prototype === null;
  }),
);
