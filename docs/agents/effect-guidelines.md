# Effect and library usage

Use the dependency versions pinned in `package.json` and the overrides in
`pnpm-workspace.yaml`. These rules capture project conventions that package
configuration does not express.

## Effect

- Use Effect/Schema for runtime validation unless the surrounding code already uses another validation library.
- Do not erase Effect or Layer environment requirements with casts such as `as never` or `as Effect.Effect<..., ..., never>`. Let missing services surface at compile time; keep any necessary adapter cast local.
- Use `Predicate` for reusable predicates and type guards. Prefer `Predicate.hasProperty`, primitive predicates, and combinators over handwritten structural checks.
- Use library guards for Effect data types: `Result.isSuccess` / `Result.isFailure`, `Exit.isSuccess` / `Exit.isFailure`, and `Option.isSome` / `Option.isNone` instead of `_tag` comparisons.
- Handle nested reason errors with `Effect.catchReason` / `Effect.catchReasons` and tagged errors with `Effect.catchTag` / `Effect.catchTags` instead of manually unwrapping `error.reason` or checking `_tag` inside `Effect.catch`.
- Use `Match` for tagged-union or structured value dispatch. Use typed lookup tables for simple enum or string mappings. Keep imperative branching for genuinely stateful algorithms and early exits.
- Use Effect HTTP client APIs for outbound requests. Prefer `HttpClientResponse.filterStatusOk` and response decoding helpers over manual status checks, unless the endpoint maps specific status codes to distinct typed errors.
