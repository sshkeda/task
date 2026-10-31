import type { Argument } from "oxc-parser";

export const readLiteralString = (argument: Argument | undefined) => {
  const isStringLiteral = argument?.type === "Literal" && typeof argument.value === "string";
  if (isStringLiteral) {
    return argument.value;
  }
  const isPlainTemplateLiteral =
    argument?.type === "TemplateLiteral" && argument.expressions.length === 0;
  if (isPlainTemplateLiteral) {
    const [onlyQuasi] = argument.quasis;
    return onlyQuasi === undefined ? null : onlyQuasi.value.cooked;
  }
  return null;
};
