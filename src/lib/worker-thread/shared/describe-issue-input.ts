import { Formatter, Predicate, SchemaIssue } from "effect";

export const REPORT_INPUT = { reportInput: true } as const;

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

const withArticle = (typeName: string) => `${/^[aeiou]/iu.test(typeName) ? "an" : "a"} ${typeName}`;

export const describeIssueInput = (issue: SchemaIssue.Issue) => {
  if (!SchemaIssue.hasInput(issue)) {
    return "missing";
  }
  const input = issue.input;
  const isPrintedAsIs = input === undefined || input === null || Predicate.isNumber(input);
  if (isPrintedAsIs) {
    return String(input);
  }
  if (Predicate.isString(input)) {
    return `the string ${Formatter.format(input)}`;
  }
  if (typeof input !== "object") {
    return withArticle(typeof input);
  }
  if (Array.isArray(input)) {
    return "an array";
  }
  const prototype: unknown = Object.getPrototypeOf(input);
  const isPlainObject = prototype === Object.prototype || prototype === null;
  if (isPlainObject) {
    return "an object";
  }
  const hasNamedConstructor =
    Predicate.hasProperty(prototype, "constructor") &&
    Predicate.hasProperty(prototype.constructor, "name");
  const constructorName = hasNamedConstructor ? prototype.constructor.name : undefined;
  const isInstanceOfANamedClass =
    Predicate.isString(constructorName) && constructorName !== "" && constructorName !== "Object";
  return isInstanceOfANamedClass
    ? `an instance of ${constructorName}`
    : "an object whose prototype isn't Object.prototype";
};
