import type { Evidence, ThreadState, TurnReference } from "./domain";

export interface ThreadStateProjectionInput {
  readonly instanceId: string;
  readonly detail: {
    readonly thread: {
      readonly threadId: string;
      readonly latestTurn: {
        readonly turnId: string;
        readonly state: "running" | "interrupted" | "completed" | "error";
      } | null;
      readonly session: { readonly status: string } | null;
    };
    readonly projectedTurnState: boolean;
    readonly observedAt: string;
    readonly snapshotSequence: number;
  };
}

export interface ThreadStateProjection {
  readonly execution: ThreadState["execution"];
  readonly session: ThreadState["session"];
  readonly interruptionPending: boolean;
}

const threadSnapshotEvidence = (
  observedAt: string,
  snapshotSequence: number,
  detail: string,
): Evidence[] => [
  {
    kind: "snapshot",
    observedAt,
    sourceSequence: snapshotSequence,
    nativeEventId: null,
    detail,
  },
];

const sessionStateByNativeStatus: Record<string, ThreadState["session"]["state"]> = {
  starting: "starting",
  running: "running",
  // Idle sessions are live sessions awaiting work; preserve the native string too.
  ready: "ready",
  idle: "ready",
  stopped: "stopped",
  interrupted: "stopped",
  error: "error",
};

const sessionState = (status: string | null): ThreadState["session"]["state"] => {
  if (status === null || !Object.hasOwn(sessionStateByNativeStatus, status)) return "unknown";
  return sessionStateByNativeStatus[status] ?? "unknown";
};

/** Project one published thread snapshot without toolkit or transport dependencies. */
export const projectThreadState = (input: ThreadStateProjectionInput): ThreadStateProjection => {
  const { instanceId, detail } = input;
  const { thread, projectedTurnState, observedAt, snapshotSequence } = detail;
  const { latestTurn } = thread;
  let execution: ThreadState["execution"];
  if (latestTurn === null) {
    execution = {
      state: "inactive",
      turn: null,
      nativeState: null,
      evidence: threadSnapshotEvidence(
        observedAt,
        snapshotSequence,
        "The thread detail snapshot published no latest turn.",
      ),
    };
  } else {
    const turn: TurnReference = {
      instanceId,
      threadId: thread.threadId,
      turnId: latestTurn.turnId,
    };
    execution = projectedTurnState
      ? {
          state: "unknown",
          turn,
          nativeState: latestTurn.state,
          evidence: threadSnapshotEvidence(
            observedAt,
            snapshotSequence,
            `The latest turn state ${latestTurn.state} was projected from a session transition racing the snapshot, not observed as authoritative turn evidence.`,
          ),
        }
      : {
          state: latestTurn.state === "running" ? "active" : "inactive",
          turn,
          nativeState: latestTurn.state,
          evidence: threadSnapshotEvidence(
            observedAt,
            snapshotSequence,
            `The thread detail snapshot published the latest turn as ${latestTurn.state}.`,
          ),
        };
  }

  const sessionStatus = thread.session?.status ?? null;
  const session: ThreadState["session"] =
    sessionStatus === null
      ? { state: "unknown", nativeState: null, evidence: [] }
      : {
          state: sessionState(sessionStatus),
          nativeState: sessionStatus,
          evidence: threadSnapshotEvidence(
            observedAt,
            snapshotSequence,
            `The thread detail snapshot published the provider session as ${sessionStatus}.`,
          ),
        };

  return {
    execution,
    session,
    interruptionPending: latestTurn?.state === "interrupted" || sessionStatus === "interrupted",
  };
};
