import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/unstable/ai";

export const EchoTool = Tool.make("echo", {
  description: "Return the supplied message.",
  parameters: Schema.Struct({
    message: Schema.String,
  }),
  success: Schema.String,
});

export const ServerToolkit = Toolkit.make(EchoTool);

const serverToolHandlers = ServerToolkit.of({
  echo: ({ message }) => Effect.succeed(message),
});

export const serverToolkitLayer = ServerToolkit.toLayer(serverToolHandlers);
