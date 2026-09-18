import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { describe, expect, it } from "vitest";
import { ServerToolkit, serverToolkitLayer } from "./tools";

describe("MCP tools", () => {
  it("echoes a message through the Effect toolkit", async () => {
    const results = await Effect.runPromise(
      Effect.gen(function* () {
        const toolkit = yield* ServerToolkit;
        const stream = yield* toolkit.handle("echo", { message: "hello" });
        return yield* Stream.runCollect(stream);
      }).pipe(Effect.provide(serverToolkitLayer)),
    );

    expect(results).toHaveLength(1);
    expect(results[0]?.result).toBe("hello");
  });
});
