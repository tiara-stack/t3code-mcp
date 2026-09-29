import { describe, expect, it } from "vitest";
import { projectThreadState, type ThreadStateProjectionInput } from "./thread-state-projection";

const project = (
  latestTurn: ThreadStateProjectionInput["detail"]["thread"]["latestTurn"],
  sessionStatus: string | null,
  projectedTurnState: boolean,
) =>
  projectThreadState({
    instanceId: "instance-a",
    detail: {
      thread: {
        threadId: "thread-a",
        latestTurn,
        session: sessionStatus === null ? null : { status: sessionStatus },
      },
      projectedTurnState,
      observedAt: "2026-09-29T00:00:00.000Z",
      snapshotSequence: 42,
    },
  });

describe("projectThreadState", () => {
  it("projects an authoritative completed turn and provider session with snapshot evidence", () => {
    const projection = project({ turnId: "turn-a", state: "completed" }, "idle", false);

    expect(projection).toEqual({
      execution: {
        state: "inactive",
        turn: { instanceId: "instance-a", threadId: "thread-a", turnId: "turn-a" },
        nativeState: "completed",
        evidence: [
          {
            kind: "snapshot",
            observedAt: "2026-09-29T00:00:00.000Z",
            sourceSequence: 42,
            nativeEventId: null,
            detail: "The thread detail snapshot published the latest turn as completed.",
          },
        ],
      },
      session: {
        state: "ready",
        nativeState: "idle",
        evidence: [
          {
            kind: "snapshot",
            observedAt: "2026-09-29T00:00:00.000Z",
            sourceSequence: 42,
            nativeEventId: null,
            detail: "The thread detail snapshot published the provider session as idle.",
          },
        ],
      },
      interruptionPending: false,
    });
  });

  it("keeps projected terminal turn state unknown and records interruption separately", () => {
    const projection = project({ turnId: "turn-a", state: "interrupted" }, "interrupted", true);

    expect(projection.execution).toMatchObject({
      state: "unknown",
      nativeState: "interrupted",
      evidence: [
        {
          kind: "snapshot",
          sourceSequence: 42,
          detail: expect.stringContaining("not observed as authoritative turn evidence"),
        },
      ],
    });
    expect(projection.session).toMatchObject({ state: "stopped", nativeState: "interrupted" });
    expect(projection.interruptionPending).toBe(true);
  });

  it("projects an absent latest turn as inactive without inventing a turn or evidence", () => {
    expect(project(null, null, false)).toMatchObject({
      execution: { state: "inactive", turn: null, nativeState: null },
      session: { state: "unknown", nativeState: null, evidence: [] },
      interruptionPending: false,
    });
  });

  it.each(["constructor", "toString", "__proto__"])(
    "keeps inherited object key %s as an unknown provider-session state",
    (sessionStatus) => {
      expect(project(null, sessionStatus, false).session).toMatchObject({
        state: "unknown",
        nativeState: sessionStatus,
      });
    },
  );
});
