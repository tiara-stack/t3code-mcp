import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";

/** Build the shared bounded exponential schedule used for safe local and remote retries. */
export const jitteredExponential = (options: {
  readonly initialDelay: Duration.Input;
  readonly minimumDelayMillis: number;
  readonly maximumDelayMillis: number;
  readonly maxElapsed: Duration.Input;
}) =>
  Schedule.exponential(options.initialDelay).pipe(
    Schedule.jittered,
    Schedule.modifyDelay(({ duration }) =>
      Effect.succeed(
        Duration.millis(
          Math.min(
            options.maximumDelayMillis,
            Math.max(options.minimumDelayMillis, Duration.toMillis(duration)),
          ),
        ),
      ),
    ),
    Schedule.upTo({ duration: options.maxElapsed }),
  );
