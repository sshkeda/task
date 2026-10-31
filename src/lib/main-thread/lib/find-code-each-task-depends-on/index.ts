import { Layer } from "effect";

import { makeModuleGraph, ModuleGraph } from "./internal/make-module-graph.ts";

export { ModuleGraph } from "./internal/make-module-graph.ts";

export const ModuleGraphLive = Layer.effect(ModuleGraph, makeModuleGraph);
