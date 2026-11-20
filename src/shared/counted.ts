export const counted = (count: number, noun: string) =>
  `${String(count)} ${noun}${count === 1 ? "" : "s"}`;
