import { Ref } from "effect";

const nextOutputStreamId = Ref.makeUnsafe(0);

export const takeOutputStreamId = Ref.getAndUpdate(nextOutputStreamId, (id) => id + 1);
