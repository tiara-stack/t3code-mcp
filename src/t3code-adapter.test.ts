import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { mapThreadInterruptDispatchError } from "./t3code-adapter";

describe("T3Code thread interrupt dispatch errors", () => {
  it.effect("keeps generic dispatch failures uncertain", () =>
    Effect.sync(() => {
      expect(mapThreadInterruptDispatchError({ message: "dispatch failed" })).toMatchObject({
        kind: "transport",
        message: "dispatch failed",
        uncertain: true,
        status: null,
      });
      expect(mapThreadInterruptDispatchError({ message: "  " })).toMatchObject({
        kind: "transport",
        message: "The T3Code thread interruption dispatch outcome is unknown.",
        uncertain: true,
        status: null,
      });
    }),
  );
});
