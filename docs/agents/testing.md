# Testing conventions

Effect-based tests use the Effect v4 APIs from `@effect/vitest`, pinned to the
`4.0.0-rc.*` line matching the workspace `effect` version. `vite-plus` runs its
own vitest runner, so `@effect/vitest` must resolve the same `vitest` instance
as `vite-plus`; `pnpm-workspace.yaml` pins `vitest` and allows any peer range
to keep them identical.

- Prefer `it.effect("case", () => Effect.gen(function* () { ... }))` for deterministic Effect tests.
- Use `const value = yield* program` instead of `await Effect.runPromise(program)`.
- Use `it.live` for real timers, subprocesses, filesystem behavior, process-environment mutation, or runtime services whose behavior changes under the test runtime.
- For failure assertions, use `const exit = yield* Effect.exit(program)` and inspect `Exit`, `Cause`, or tagged errors. Avoid Promise `.rejects` for Effect programs.
- Helpers named `run`, `provide`, or `runWith...` in Effect tests return an `Effect`, not a `Promise`.
- Manage temporary directories, subprocesses, and other resources with `Effect.acquireUseRelease` or scoped layers instead of `try`/`finally`.
- Prefer the owning `Context.Service`'s typed test-layer adapter (for example `InstanceConnections.layerTest` / `layerWithAdapter`). Otherwise use `Layer.succeed` and shared layers where appropriate.
- Put stateful mocks inside `Layer.sync` so each test gets isolated state unless shared state is intentional.
- Keep ordinary synchronous schema, parser, and transformation tests as plain `it` tests.
- Avoid explicit `TestClock.layer()` when `it.effect` already provides test services, unless custom clock setup is required.
- Do not introduce `Effect.runPromise` or `Effect.runPromiseExit` in `*.test.ts` unless the test explicitly validates those runtime APIs.
