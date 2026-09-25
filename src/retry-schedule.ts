import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";

/** A jittered exponential retry schedule with a bounded delay and total lifetime. */
export const makeBoundedJitteredRetrySchedule = (maxDurationMillis: number) =>
  Schedule.exponential("25 millis").pipe(
    Schedule.jittered,
    Schedule.modifyDelay(({ duration }) =>
      Effect.succeed(Duration.millis(Math.min(250, Math.max(25, Duration.toMillis(duration))))),
    ),
    Schedule.upTo({ duration: Duration.millis(maxDurationMillis) }),
  );
