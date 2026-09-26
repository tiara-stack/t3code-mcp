/**
 * Disposable wire check for the thread_stop_session slice (TIA-289).
 *
 * Pair with a pinned T3Code 0.0.38 server, create one fixture thread, verify
 * that the public tool handles a missing session, dispatch the pinned native
 * thread.session.stop command through the production adapter, then read the
 * observed stopped state and verify the public tool treats it as already
 * stopped. No provider is configured or prompted by this check.
 *
 * Usage:
 *   T3CODE_MCP_LIVE_PAIRING_TOKEN=<token> pnpm exec tsx scripts/live-thread-stop-session.ts <endpoint>
 */
import { NodeFileSystem, NodeRuntime } from "@effect/platform-node";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import * as Exit from "effect/Exit";
import * as Cause from "effect/Cause";
import { InstanceConnections } from "../src/instance-connections";
import {
  dispatchFixture,
  firstResult,
  pairLiveCheckInstance,
  requireOk,
} from "./live-check-support";
import type { ThreadStreamItem } from "../src/t3code-adapter";

type StopRequestEvent = Extract<ThreadStreamItem, { readonly kind: "session-stop-requested" }>;
type StoppedSessionEvent = Extract<ThreadStreamItem, { readonly kind: "session-set" }>;

const main = Effect.gen(function* () {
  const { endpoint, staged, run } = yield* pairLiveCheckInstance({
    endpointArgument: process.argv[2],
    pairingToken: process.env.T3CODE_MCP_LIVE_PAIRING_TOKEN,
    usage:
      "usage: T3CODE_MCP_LIVE_PAIRING_TOKEN=<token> pnpm exec tsx scripts/live-thread-stop-session.ts <endpoint>",
    directoryPrefix: "t3code-mcp-live-session-stop-",
  });

  const projects = requireOk(
    "project_list",
    yield* run(
      firstResult("project_list", { scope: { kind: "instance", instanceId: "live-check" } }),
    ),
  ) as { readonly items: ReadonlyArray<{ readonly project: { readonly projectId: string } }> };
  const projectId = projects.items[0]?.project.projectId;
  if (projectId === undefined) {
    throw new Error("UNAVAILABLE: add one disposable project to the pinned server before running");
  }

  const runId = globalThis.crypto.randomUUID().slice(0, 8);
  const threadId = `live-stop-session-${runId}`;
  const command = (type: string, extra: Record<string, unknown>) => ({
    type,
    commandId: globalThis.crypto.randomUUID(),
    ...extra,
    createdAt: new Date().toISOString(),
  });
  const created = yield* Effect.exit(
    dispatchFixture({
      endpoint,
      credential: staged.credential,
      command: command("thread.create", {
        threadId,
        projectId,
        title: `Live stop-session fixture ${runId}`,
        modelSelection: { instanceId: "live-unconfigured", model: "fixture" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      }),
    }),
  );
  if (Exit.isFailure(created)) {
    throw new Error(`UNAVAILABLE: fixture thread creation failed: ${Cause.pretty(created.cause)}`);
  }

  const missing = requireOk(
    "thread_stop_session (missing session)",
    yield* run(
      firstResult("thread_stop_session", {
        requestId: `live-stop-missing-${runId}`,
        thread: { instanceId: "live-check", threadId },
      }),
    ),
  ) as { readonly state: string; readonly dispatch: string };
  if (missing.state !== "completed" || missing.dispatch !== "not_dispatched") {
    throw new Error(`unexpected missing-session operation: ${JSON.stringify(missing)}`);
  }
  console.log("PASS thread_stop_session: a missing session completes without dispatch");

  const commandId = globalThis.crypto.randomUUID();
  const createdAt = new Date().toISOString();
  const stopEvidence = yield* run(
    Effect.gen(function* () {
      const value = yield* InstanceConnections;
      const synchronized = yield* Deferred.make<void, Error>();
      const shutdown = yield* Deferred.make<
        {
          readonly request: StopRequestEvent;
          readonly stopped: StoppedSessionEvent;
        },
        Error
      >();
      let synchronizedOnce = false;
      let matchingRequest: StopRequestEvent | null = null;
      const signalStreamFailure = (message: string) =>
        Effect.gen(function* () {
          yield* Deferred.fail(synchronized, new Error(message));
          yield* Deferred.fail(shutdown, new Error(message));
        });
      const stream = value.openThreadStream("live-check", threadId);
      const subscriber = yield* Stream.runForEach(stream, (item) =>
        Effect.gen(function* () {
          if (item.kind === "synchronized" && !synchronizedOnce) {
            synchronizedOnce = true;
            yield* Deferred.succeed(synchronized, undefined);
            return;
          }
          if (item.kind === "session-stop-requested" && item.threadId === threadId) {
            if (item.commandId !== commandId || item.createdAt !== createdAt) {
              yield* Deferred.fail(
                shutdown,
                new Error("T3Code published a stop request with a different command identity"),
              );
              return;
            }
            matchingRequest = item;
            return;
          }
          if (
            item.kind === "session-set" &&
            item.session.status === "stopped" &&
            matchingRequest !== null
          ) {
            yield* Deferred.succeed(shutdown, { request: matchingRequest, stopped: item });
          }
        }),
      ).pipe(
        Effect.catch((error) =>
          signalStreamFailure(`T3Code thread event stream failed: ${String(error)}`).pipe(
            Effect.andThen(Effect.fail(error)),
          ),
        ),
        Effect.andThen(
          signalStreamFailure("T3Code thread event stream ended before shutdown evidence arrived"),
        ),
        Effect.forkScoped,
      );
      yield* Deferred.await(synchronized).pipe(
        Effect.timeoutOrElse({
          duration: Duration.seconds(10),
          orElse: () => Effect.fail(new Error("thread stream did not synchronize before dispatch")),
        }),
      );
      const prepared = yield* value.prepareThreadSessionStop("live-check");
      const receipt = yield* prepared.dispatch({
        threadId,
        commandId,
        createdAt,
      });
      const events = yield* Deferred.await(shutdown).pipe(
        Effect.timeoutOrElse({
          duration: Duration.seconds(10),
          orElse: () => Effect.fail(new Error("matching stopped-session events were not observed")),
        }),
      );
      yield* Fiber.interrupt(subscriber);
      return { receipt, events };
    }),
  );
  if (stopEvidence.receipt.sequence < 0)
    throw new Error("T3Code returned an invalid dispatch sequence");
  if (stopEvidence.events.request.commandId !== commandId) {
    throw new Error("T3Code did not publish the matching thread.session-stop-requested commandId");
  }
  if (stopEvidence.events.request.createdAt !== createdAt) {
    throw new Error("T3Code did not publish the matching thread.session-stop-requested createdAt");
  }
  if (stopEvidence.events.stopped.sequence <= stopEvidence.events.request.sequence) {
    throw new Error("T3Code published the stopped-session event before its matching stop request");
  }
  if (stopEvidence.events.stopped.session.updatedAt !== createdAt) {
    throw new Error("T3Code stopped-session updatedAt did not match the command createdAt");
  }

  const waited = requireOk(
    "thread_wait (session stopped)",
    yield* run(
      firstResult("thread_wait", {
        thread: { instanceId: "live-check", threadId },
        condition: "session_stopped",
        waitMs: 10_000,
      }),
    ),
  ) as {
    readonly observation: string;
    readonly state: { readonly session: { readonly state: string } };
  };
  if (waited.observation !== "condition_met" || waited.state.session.state !== "stopped") {
    throw new Error(`T3Code did not publish stopped session evidence: ${JSON.stringify(waited)}`);
  }
  console.log("PASS adapter RPC: T3Code accepted thread.session.stop and published stopped state");

  const alreadyStopped = requireOk(
    "thread_stop_session (already stopped)",
    yield* run(
      firstResult("thread_stop_session", {
        requestId: `live-stop-already-${runId}`,
        thread: { instanceId: "live-check", threadId },
      }),
    ),
  ) as { readonly state: string; readonly dispatch: string };
  if (alreadyStopped.state !== "completed" || alreadyStopped.dispatch !== "not_dispatched") {
    throw new Error(`unexpected already-stopped operation: ${JSON.stringify(alreadyStopped)}`);
  }
  console.log("PASS thread_stop_session: an observed stopped session does not dispatch again");
  console.log(
    "LIMIT: the disposable server had no active provider session, so no provider runtime was closed",
  );
});

NodeRuntime.runMain(Effect.scoped(main).pipe(Effect.provide(NodeFileSystem.layer)));
