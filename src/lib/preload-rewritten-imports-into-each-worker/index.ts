import { Effect } from "effect";

import { loadRewrittenImports } from "./internal/load-rewritten-imports.ts";

Effect.runSync(loadRewrittenImports);
