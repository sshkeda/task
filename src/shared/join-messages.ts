import type { PrintableError } from "../lib/errors/index.ts";

export const joinMessages = (printables: readonly PrintableError[]) =>
  printables.flatMap((printable) => [printable.message, ...(printable.context ?? [])]).join("\n");
