import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import { LocalStoreError } from "./local-store";
import { T3CodeAdapterError, type T3CodeAdapterErrorKind } from "./t3code-adapter";

const WAIT_POLL_INTERVAL_MILLIS = 100;
const WAIT_POLL_MAX_INTERVAL_MILLIS = 1_000;
const RETRIABLE_T3_CODE_ERROR_KINDS: ReadonlySet<T3CodeAdapterErrorKind> = new Set([
  "transport",
  "timeout",
  "capacity",
  "resource_not_found",
]);

export const isRetriableWaitFailure = (
  error: unknown,
  isObservationError: (error: unknown) => boolean,
): boolean => {
  if (error instanceof LocalStoreError) return error.kind === "contention";
  if (isObservationError(error)) return true;
  if (error instanceof T3CodeAdapterError) return RETRIABLE_T3_CODE_ERROR_KINDS.has(error.kind);
  return false;
};

type WaitFailureRecovery<Failure, Outcome> =
  | { readonly kind: "fail"; readonly failure: Failure }
  | { readonly kind: "unavailable"; readonly result: Outcome }
  | { readonly kind: "retry"; readonly interval: number };

const recoverWaitFailure = <Failure, Outcome>(options: {
  readonly deadline: number;
  readonly failure: Failure;
  readonly first: boolean;
  readonly interval: number;
  readonly isRetriable: (failure: Failure) => boolean;
  readonly unavailable: (failure: Failure) => Outcome;
}): Effect.Effect<WaitFailureRecovery<Failure, Outcome>> =>
  Effect.gen(function* () {
    if (options.first) return { kind: "fail", failure: options.failure } as const;
    const now = yield* Clock.currentTimeMillis;
    if (!options.isRetriable(options.failure) || now >= options.deadline) {
      return { kind: "unavailable", result: options.unavailable(options.failure) } as const;
    }
    yield* Effect.sleep(Duration.millis(Math.min(options.interval, options.deadline - now)));
    return {
      kind: "retry",
      interval: Math.min(WAIT_POLL_MAX_INTERVAL_MILLIS, options.interval * 2),
    } as const;
  });

export type WaitPollDelay =
  | { readonly kind: "timed_out" }
  | { readonly kind: "continue"; readonly interval: number };

const sleepUntilNextBoundedPoll = (options: {
  readonly deadline: number;
  readonly interval: number;
  readonly maxInterval: number;
}): Effect.Effect<WaitPollDelay> =>
  Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    const remaining = options.deadline - now;
    if (remaining <= 0) return { kind: "timed_out" } as const;
    yield* Effect.sleep(Duration.millis(Math.min(options.interval, remaining)));
    return {
      kind: "continue",
      interval: Math.min(options.maxInterval, options.interval * 2),
    } as const;
  });

export type BoundedWaitPoll<Outcome, Pending> =
  | { readonly kind: "result"; readonly result: Outcome }
  | { readonly kind: "pending"; readonly pending: Pending };

export const runBoundedWaitLoop = <Failure, Outcome, Pending>(options: {
  readonly waitMs: number;
  readonly poll: () => Effect.Effect<BoundedWaitPoll<Outcome, Pending>, Failure>;
  readonly isRetriable: (failure: Failure) => boolean;
  readonly unavailable: (failure: Failure) => Outcome;
  readonly timedOut: (pending: Pending) => Outcome;
}): Effect.Effect<Outcome, Failure> =>
  Effect.gen(function* () {
    const deadline = (yield* Clock.currentTimeMillis) + options.waitMs;
    let first = true;
    let interval = WAIT_POLL_INTERVAL_MILLIS;
    while (true) {
      const polled = yield* Effect.result(options.poll());
      if (Result.isFailure(polled)) {
        const recovery = yield* recoverWaitFailure({
          deadline,
          failure: polled.failure,
          first,
          interval,
          isRetriable: options.isRetriable,
          unavailable: options.unavailable,
        });
        if (recovery.kind === "fail") return yield* Effect.fail(recovery.failure);
        if (recovery.kind === "unavailable") return recovery.result;
        interval = recovery.interval;
        continue;
      }
      first = false;
      if (polled.success.kind === "result") return polled.success.result;
      const nextPoll = yield* sleepUntilNextBoundedPoll({
        deadline,
        interval,
        maxInterval: WAIT_POLL_MAX_INTERVAL_MILLIS,
      });
      if (nextPoll.kind === "timed_out") return options.timedOut(polled.success.pending);
      interval = nextPoll.interval;
    }
  });
