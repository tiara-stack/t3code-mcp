import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import type { JsonObject } from "effect/Schema";
import { Tool, Toolkit } from "effect/unstable/ai";
import {
  InstanceListInputSchema,
  makeToolSuccess,
  MAX_SERIALIZED_RESULT_BYTES,
  ToolResultSchema,
} from "./domain";
import type { ToolFailure } from "./domain";
import { LocalStore, LocalStoreError } from "./local-store";

// fallow-ignore-next-line unused-export
export const InstanceListTool = Tool.make("instance_list", {
  description: "List saved T3Code instance registrations without probing them.",
  parameters: InstanceListInputSchema,
  success: ToolResultSchema,
})
  .addDependency(LocalStore)
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

export const ServerToolkit = Toolkit.make(InstanceListTool);

const toToolFailure = (error: LocalStoreError) => {
  const failure = (
    code: ToolFailure["code"],
    retry: ToolFailure["retry"],
    details: JsonObject = {},
  ) => ({ code, message: error.message, retry, details });

  switch (error.kind) {
    case "cursor_expired":
      return failure("cursor_expired", "safe_read", { action: "resync" });
    case "cursor_mismatch":
      return failure("cursor_mismatch", "safe_read", { action: "resync" });
    case "result_too_large":
      return failure("result_too_large", "change_request", {
        action: "reduce_page_size",
        maxBytes: MAX_SERIALIZED_RESULT_BYTES,
      });
    case "capture_budget":
      return failure("unavailable", "safe_read", { action: "retry_read" });
    case "contention":
      return failure("unavailable", "safe_read");
    case "disk":
    case "storage":
      return failure("unavailable", "safe_read");
    case "malformed_row":
      return failure("stale_state", "reconcile_first");
    default: {
      const unhandled: never = error.kind;
      return unhandled;
    }
  }
};

const serverToolHandlers = ServerToolkit.of({
  instance_list: ({ cursor, limit }) =>
    Effect.gen(function* () {
      const store = yield* LocalStore;
      const page = yield* store.listRegistrations({
        ...(cursor === undefined ? {} : { cursor }),
        ...(limit === undefined ? {} : { limit }),
      });
      const observedAt = new Date(yield* Clock.currentTimeMillis).toISOString();
      return makeToolSuccess(page, observedAt);
    }).pipe(
      Effect.catchTag("LocalStoreError", (error) =>
        Effect.succeed({
          result: { kind: "error" as const, error: toToolFailure(error) },
          observations: [],
          warnings: [],
        }),
      ),
    ),
});

export const serverToolkitLayer = ServerToolkit.toLayer(serverToolHandlers);
