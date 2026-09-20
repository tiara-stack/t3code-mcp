import * as Context from "effect/Context";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { homedir } from "node:os";

export interface LocalStoreConfigValue {
  readonly databasePath: string;
  readonly captureRetentionMillis?: number;
  readonly captureBudgetBytes?: number;
}

const DEFAULT_CAPTURE_RETENTION_MILLIS = 10 * 60 * 1000;
const DEFAULT_CAPTURE_BUDGET_BYTES = 256 * 1024 * 1024;

export class LocalStoreConfig extends Context.Service<LocalStoreConfig, LocalStoreConfigValue>()(
  "t3code-mcp/LocalStoreConfig",
) {
  static readonly fromEnvironment = Effect.gen(function* () {
    const defaultDatabasePath = Config.schema(Schema.NonEmptyString, "XDG_DATA_HOME").pipe(
      Config.orElse(() => Config.succeed(join(homedir(), ".local", "share"))),
      Config.map((dataHome) => join(dataHome, "t3code-mcp", "state.sqlite")),
    );
    const databasePath = yield* Config.schema(
      Schema.NonEmptyString,
      "T3CODE_MCP_DATABASE_PATH",
    ).pipe(
      Config.orElse(() => defaultDatabasePath),
      Config.map((value) =>
        value === ":memory:" ? value : isAbsolute(value) ? value : resolve(value),
      ),
    );

    return {
      databasePath,
      captureRetentionMillis: DEFAULT_CAPTURE_RETENTION_MILLIS,
      captureBudgetBytes: DEFAULT_CAPTURE_BUDGET_BYTES,
    } satisfies LocalStoreConfigValue;
  });
}

export const databaseDirectory = (config: LocalStoreConfigValue): string | undefined =>
  config.databasePath === ":memory:" ? undefined : dirname(config.databasePath);

export const normalizeLocalStoreConfig = (
  config: LocalStoreConfigValue,
): Required<LocalStoreConfigValue> => ({
  databasePath: config.databasePath,
  captureRetentionMillis: config.captureRetentionMillis ?? DEFAULT_CAPTURE_RETENTION_MILLIS,
  captureBudgetBytes: config.captureBudgetBytes ?? DEFAULT_CAPTURE_BUDGET_BYTES,
});
