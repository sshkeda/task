import { expect } from "bun:test";

export const expectOneOfTheOrders = (
  lines: readonly string[],
  orders: readonly (readonly string[])[],
) => {
  expect(
    orders.map((order) => [...order]),
    lines.join("\n"),
  ).toContainEqual([...lines]);
};

export const withEitherEndFirst = (
  before: readonly string[],
  oneEnd: readonly string[],
  otherEnd: readonly string[],
  after: readonly string[],
) => [
  [...before, ...oneEnd, ...otherEnd, ...after],
  [...before, ...otherEnd, ...oneEnd, ...after],
];
