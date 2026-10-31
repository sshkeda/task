import type { VisitorObject } from "oxc-parser";

export const visitNonReferenceIdentifiers = (nonReferenceStarts: Set<number>) =>
  ({
    Property: (property) => {
      const hasNonReferenceKey = !property.computed && !property.shorthand;
      if (hasNonReferenceKey) {
        nonReferenceStarts.add(property.key.start);
      }
    },
    PropertyDefinition: (definition) => {
      if (!definition.computed) {
        nonReferenceStarts.add(definition.key.start);
      }
    },
    MethodDefinition: (definition) => {
      if (!definition.computed) {
        nonReferenceStarts.add(definition.key.start);
      }
    },
  }) satisfies VisitorObject;
