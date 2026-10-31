import { Serving } from "../../../shared/task-services.ts";

export const serve = <Success>(servedValue: Success) =>
  Serving.use((serving) => serving.serve(servedValue));
