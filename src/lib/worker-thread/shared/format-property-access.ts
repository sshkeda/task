import { Formatter, Predicate } from "effect";

const IDENTIFIER = /^[A-Za-z_$][\w$]*$/u;

export const formatPropertyAccess = (key: PropertyKey) => {
  if (Predicate.isNumber(key)) {
    return `[${key}]`;
  }
  const isIdentifierKey = Predicate.isString(key) && IDENTIFIER.test(key);
  if (isIdentifierKey) {
    return `.${key}`;
  }
  return `[${Formatter.format(key)}]`;
};
