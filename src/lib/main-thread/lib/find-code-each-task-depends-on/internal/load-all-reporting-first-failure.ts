import { Effect, Result } from "effect";

export const loadAllReportingFirstFailure = <Item, Loaded, Failure, Requirements>(
  items: readonly Item[],
  load: (item: Item) => Effect.Effect<Loaded, Failure, Requirements>,
  concurrency: number,
) =>
  Effect.all(
    items.map((item) => load(item)),
    { concurrency, mode: "result" },
  ).pipe(Effect.flatMap((results) => Effect.fromResult(Result.all(results))));
