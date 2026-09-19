# MCP controlling-agent contract

Status: confirmed by the human on 2026-09-19 for [Choose the MCP tools and end-to-end controlling-agent contract](https://linear.app/tiara-stack/issue/TIA-265/choose-the-mcp-tools-and-end-to-end-controlling-agent-contract).

Canonical published asset: [MCP controlling-agent contract](https://linear.app/tiara-stack/document/mcp-controlling-agent-contract-d4e717a3006c). This file is the repository copy of that decision asset.

This document records the agreed contract. It does not describe implemented tools. The repository currently exposes only its starter echo tool. Production implementation belongs to a later effort.

## Decision status

The human agreed to dedicated tools, separate resource creation and submission calls, tool-based reads and bounded waits, caller-supplied mutation request IDs with status lookup, and compact state summaries with separate bounded output.

The human also agreed to:

- Use existing T3Code projects only. Project creation, repository cloning, and directory creation are outside this contract.
- Initial prompt submission accepts text, with explicit provider/model selection or an explicit request to resolve project defaults. Attachments and reassignment of existing threads to different worktrees are deferred.
- Admitted mutations continue independently of MCP request cancellation.
- List pages default to 25 items and allow at most 100. Output chunks default to 16 KiB and allow at most 64 KiB. Waits default to 10 seconds and allow at most 30 seconds.

The human confirmed the concrete tool inventory, schemas, and normal-work, lost-reply, and competing-UI walkthroughs below.

## Authorities

- [Choose multi-instance identity and the T3Code integration boundary](https://linear.app/tiara-stack/issue/TIA-262/choose-multi-instance-identity-and-the-t3code-integration-boundary).
- [Define thread execution, steering, and settled-state semantics](https://linear.app/tiara-stack/issue/TIA-263/define-thread-execution-steering-and-settled-state-semantics).
- [Define guarded cleanup and explicit discard for shared resources](https://linear.app/tiara-stack/issue/TIA-264/define-guarded-cleanup-and-explicit-discard-for-shared-resources).
- [Establish Effect MCP support for state observation and cancellable waits](https://linear.app/tiara-stack/issue/TIA-261/establish-effect-mcp-support-for-state-observation-and-cancellable).

These resolutions govern semantics. This contract defines how tools express them. Retention, persistence, adapter isolation, and service lifecycles remain in [Choose Effect service boundaries, recovery, and design validation](https://linear.app/tiara-stack/issue/TIA-266/choose-effect-service-boundaries-recovery-and-design-validation).

## References

The notation below describes wire data, not implementation types. Implementations use Effect Schema at the boundary.

```ts
type ProjectRef = { instanceId: string; projectId: string };
type ThreadRef = { instanceId: string; threadId: string };
type TurnRef = ThreadRef & { turnId: string };
type WorktreeRef = {
  instanceId: string;
  repositoryPath: string;
  worktreePath: string;
};
type PendingRequestRef = ThreadRef & { pendingRequestId: string };
type Evidence = {
  kind:
    | "command_receipt"
    | "snapshot"
    | "event"
    | "rpc_result"
    | "local_registration"
    | "adapter_inference";
  observedAt: string;
  sourceSequence: number | null;
  nativeEventId: string | null;
  detail: string;
};

type ModelSelection = {
  providerInstanceId: string;
  model: string;
  options?: { id: string; value: string | boolean }[];
};
type RuntimeMode = "approval-required" | "auto-accept-edits" | "auto" | "full-access";
type InteractionMode = "default" | "plan";
type ThreadConfiguration = {
  model: ModelSelection;
  runtimeMode: RuntimeMode;
  interactionMode: InteractionMode;
};
```

References work without prior discovery or resource enrollment. Paths belong to the target instance. Native provider identifiers are distinct from MCP instance registrations. Internal provider session IDs and provider turn IDs are not public resource references.

## Tools

All mutation inputs include a caller-supplied `requestId`. The table omits that repeated field. Targeted tools require explicit instance qualification. Names are the exact tool names, without a transport-dependent prefix.

| Tool                  | Inputs and purpose                                                                                                                                   |
| --------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `instance_list`       | Page through saved registration summaries, including connection and pairing state.                                                                   |
| `instance_get`        | `instanceId`; current identity, version, authorization, capabilities, and connection diagnostics.                                                    |
| `instance_pair`       | `alias`, `endpoint`, `pairingCode`; exchange the code and create a persistent registration. Return no token.                                         |
| `instance_update`     | `instanceId`, optional `alias` and `endpoint`; an endpoint change must verify the bound environment identity.                                        |
| `instance_pair_again` | `instanceId`, `pairingCode`; replace credentials only for the bound environment.                                                                     |
| `instance_remove`     | `instanceId`; disconnect and forget local credentials without changing upstream work.                                                                |
| `project_list`        | A selected instance or an explicit all-instances scope, with pagination. Return per-instance failures and inventory completeness.                    |
| `model_list`          | `instanceId`, optional provider selection, and pagination; discovered provider/model choices and option descriptors.                                 |
| `worktree_list`       | An instance and repository path, with pagination. Return discovered worktrees and explicitly describe inventory limits.                              |
| `worktree_inspect`    | A worktree reference; status, relevant thread references, freshness, available checks, and discard consequences.                                     |
| `worktree_create`     | `instanceId`, `repositoryPath`, `startRef`, optional `newBranch`, optional `path`; create a checkout. Omitted path asks T3Code to choose the path.   |
| `thread_list`         | Project or instance scope, filters including archived threads, and pagination.                                                                       |
| `thread_get`          | A thread reference; compact execution, session, settlement, pending-request, configuration, and worktree state.                                      |
| `thread_create`       | Project reference, `title`, checkout selection, model selection, runtime mode, and interaction mode. Create without submitting a prompt.             |
| `thread_submit`       | Thread reference, `text`, submission intent, and context requirement. Starts or supplies input to execution according to verified provider behavior. |
| `thread_interrupt`    | Thread reference; interrupt the execution T3Code processes at that time. Exact-turn fencing is unsupported in the baseline.                          |
| `thread_stop_session` | Thread reference; request provider-session shutdown. This never stops the T3Code server.                                                             |
| `thread_set_settled`  | Thread reference and `settled: boolean`; preserve native eligibility checks and asynchronous shutdown consequences.                                  |
| `approval_respond`    | Pending-request reference and an offered approval decision.                                                                                          |
| `input_respond`       | Pending-request reference and answers conforming to the observed request.                                                                            |
| `thread_output`       | Thread reference, optional output cursor, and byte budget; bounded conversation and activity output, with provenance and truncation information.     |
| `diff_read`           | An explicit worktree or thread-history diff source, optional cursor, and byte budget.                                                                |
| `turn_wait`           | Exact observed turn reference and wait budget; outcome and evidence for that turn.                                                                   |
| `thread_wait`         | Thread reference, an explicit state condition, optional observation cursor, and wait budget; reports activity from all clients.                      |
| `operation_get`       | Mutation `requestId` and optional bounded wait; admission, dispatch, observed outcome, and partial-step information.                                 |
| `thread_remove`       | Thread reference; guarded removal of its conversation while retaining its worktree.                                                                  |
| `worktree_discard`    | Worktree reference and optional explicitly named sole thread to remove first; retain the branch.                                                     |

No required MCP resources, subscription notifications, progress notifications, or MCP Tasks form part of this contract. Optional protocol additions must not change tool semantics or become necessary for recovery.

## Common input rules

All IDs are nonempty opaque strings. `requestId` is 1 to 128 characters; callers should generate globally unique values. Timestamps are UTC RFC 3339 strings. Sequence values and counts are nonnegative safe integers. Unknown argument fields are rejected. Optional fields use omission; `null` is only accepted where the schema names it. Pairing codes are transient secrets. Endpoints must be supported HTTP(S) T3Code endpoints without embedded credentials.

```ts
type PageInput = { cursor?: string; limit?: number };
type ReadPolicy = { allowStale?: boolean };
type OutputInput = { cursor?: string; maxBytes?: number };
type MutationInput = { requestId: string };
type InstanceScope = { kind: "instance"; instanceId: string } | { kind: "all_instances" };
type ThreadScope =
  { kind: "instance"; instanceId: string } | { kind: "project"; project: ProjectRef };
type ThreadCondition =
  "changed" | "inactive" | "settled" | "unsettled" | "session_stopped" | "needs_response";
type ApprovalDecision = "accept" | "acceptForSession" | "acceptAlways" | "decline" | "cancel";
type DiffSource =
  | { kind: "worktree_changes"; worktree: WorktreeRef }
  | { kind: "worktree_against_base"; worktree: WorktreeRef; baseRef: string }
  | { kind: "thread_turn_range"; thread: ThreadRef; fromTurnCount: number; toTurnCount: number }
  | { kind: "thread_through_turn"; thread: ThreadRef; toTurnCount: number };

type ToolInputs = {
  instance_list: PageInput;
  instance_get: { instanceId: string } & ReadPolicy;
  instance_pair: MutationInput & {
    alias: string;
    endpoint: string;
    pairingCode: string;
  };
  instance_update: MutationInput & {
    instanceId: string;
    alias?: string;
    endpoint?: string;
  };
  instance_pair_again: MutationInput & {
    instanceId: string;
    pairingCode: string;
  };
  instance_remove: MutationInput & { instanceId: string };
  project_list: { scope: InstanceScope } & PageInput & ReadPolicy;
  model_list: { instanceId: string; providerInstanceId?: string } & PageInput & ReadPolicy;
  worktree_list: { instanceId: string; repositoryPath: string } & PageInput & ReadPolicy;
  worktree_inspect: { worktree: WorktreeRef } & PageInput & ReadPolicy;
  worktree_create: MutationInput & {
    instanceId: string;
    repositoryPath: string;
    startRef: string;
    newBranch?: string;
    path?: string;
  };
  thread_list: {
    scope: ThreadScope;
    archived?: "exclude" | "include" | "only";
  } & PageInput &
    ReadPolicy;
  thread_get: { thread: ThreadRef } & PageInput & ReadPolicy;
  thread_create: ThreadCreate;
  thread_submit: ThreadSubmit;
  thread_interrupt: MutationInput & { thread: ThreadRef };
  thread_stop_session: MutationInput & { thread: ThreadRef };
  thread_set_settled: MutationInput & { thread: ThreadRef; settled: boolean };
  approval_respond: MutationInput & {
    pendingRequest: PendingRequestRef;
    decision: ApprovalDecision;
  };
  input_respond: MutationInput & {
    pendingRequest: PendingRequestRef;
    answers: Record<string, unknown>;
  };
  thread_output: { thread: ThreadRef } & OutputInput & ReadPolicy;
  diff_read: { source: DiffSource; ignoreWhitespace?: boolean } & OutputInput & ReadPolicy;
  turn_wait: { turn: TurnRef; waitMs?: number };
  thread_wait: {
    thread: ThreadRef;
    condition: ThreadCondition;
    afterCursor?: string;
    waitMs?: number;
  };
  operation_get: { requestId: string; waitMs?: number };
  thread_remove: MutationInput & { thread: ThreadRef };
  worktree_discard: MutationInput & {
    worktree: WorktreeRef;
    removeSoleThread?: ThreadRef;
  };
};
```

`allowStale` defaults to false. A live read that cannot obtain fresh evidence returns a typed failure; it may include a clearly marked last-known summary. With `allowStale: true`, a read may return cached data with explicit freshness and connection failures. All-instances discovery remains partial when some instances fail. Saved registration listing is a local read; cached connection status there is never presented as a fresh probe.

The page inputs on `thread_get` page pending requests; those on `worktree_inspect` page referencing threads. The captured state accompanies the page. A page boundary must not mix snapshots. Cleanup checks enumerate all relevant references independently of display pagination.

`instance_update` requires at least one changed field. An endpoint update stages and verifies the replacement before committing it; failure leaves the old registration intact. Re-pairing verifies the bound environment before replacing credentials. Duplicate environment bindings fail with `identity_conflict`. Removal invalidates references to that registration but leaves its operation history subject to the recovery retention policy.

`thread_wait` with `condition: "changed"` requires `afterCursor`. Other conditions evaluate the current state before waiting. A cursor gap returns a resync-required result rather than asserting that the condition occurred. A wait budget of zero means check now. Dedicated waits default to 10,000 ms and accept 0 to 30,000 ms. `operation_get` is immediate by default; a positive `waitMs` waits for a record revision change or terminal/unknown outcome, up to 30,000 ms. None of these budgets set an execution deadline.

The default list limit is 25 and the allowed range is 1 to 100. The default output budget is 16,384 bytes and the allowed range is 1,024 to 65,536 bytes. Thread-list archived mode defaults to `exclude`. Cleanup enumeration always includes relevant archived threads. Whitespace differences are included unless `ignoreWhitespace: true` is explicit.

## Creation and submission inputs

```ts
type ThreadCreate = {
  requestId: string;
  project: ProjectRef;
  title: string;
  checkout: { kind: "project_root" } | { kind: "worktree"; worktree: WorktreeRef };
  model: { kind: "explicit"; selection: ModelSelection } | { kind: "project_default" };
  runtimeMode: RuntimeMode;
  interactionMode: InteractionMode;
};

type ThreadSubmit = {
  requestId: string;
  thread: ThreadRef;
  text: string;
  intent: "provider_default" | "steer_current";
  context: "thread_default" | "require_retained";
};
```

Creation resolves and returns the effective settings. Missing project model defaults produce `configuration_required`; the server does not choose another model. Runtime and interaction modes are required explicit inputs, including when resolving a project model default. Unsupported settings fail before dispatch. Cross-instance project/worktree combinations fail validation. The adapter verifies the selected checkout through the target instance before associating it. No implicit worktree creation, provider substitution, privilege increase, or prompt submission occurs during thread creation. The initial contract uses the created or existing thread's configuration for submissions; changing that configuration is outside this contract.

A completed `thread_create` operation must include both `created.thread` and `created.threadConfiguration` in its `OperationRecord`. The latter contains the resolved model, runtime mode, and interaction mode captured for creation, including an expanded project model default. These fields may be absent while creation is pending. Later `thread_get` reads expose the thread's current configuration; UI changes do not rewrite the creation receipt.

`provider_default` exposes the adapter's known submission behavior and its limits. `steer_current` requires a capability that guarantees current-turn steering; otherwise it fails before dispatch. `require_retained` requires retained provider context and cannot silently fall back to a new context. Ordinary submission may start execution when the thread is idle. There is no separate start-execution tool.

Resume requires the adapter to guarantee the requested context behavior. A cached hint that context exists is insufficient if the provider can silently start fresh after resume fails. In that case `require_retained` is unsupported. Preserve the instance's observed provider behavior in capabilities, including queued input, active-turn input, Claude's session-closing interruption, and uncertainty about when a submitted message is consumed.

A submission receipt includes its native message identity if known, its native command identity, and one of:

```ts
type TurnCorrelation =
  | { kind: "established"; turn: TurnRef; evidence: Evidence[] }
  | { kind: "unestablished"; reason: string };
```

The baseline will often return `unestablished`. The next turn observed is not automatically the submitted prompt's turn. A status lookup cannot manufacture this relationship either.

## Domain result shapes

Every listed read returns its named value under the common `ToolResult` envelope. Every admitted mutation returns an `OperationRecord` under that envelope. These result cases are independent of MCP transport request IDs.

```ts
type Page<T> = {
  items: T[];
  nextCursor: string | null;
  coverage: "complete_for_query" | "partial" | "unknown";
  limitations: string[];
  failures: { instanceId: string; error: ToolFailure }[];
};
type Capability = {
  name: string;
  support: "supported" | "unsupported" | "unknown";
  reason: string | null;
  limitations: string[];
};
type InstanceSummary = {
  instanceId: string;
  alias: string;
  endpoint: string;
  environmentId: string | null;
  connection:
    | "connecting"
    | "connected"
    | "disconnected"
    | "pairing_required"
    | "incompatible"
    | "identity_conflict";
  lastObservedAt: string | null;
};
type InstanceDetails = {
  registration: InstanceSummary;
  serverVersion: string | null;
  authorization: {
    read: "allowed" | "denied" | "unknown";
    operate: "allowed" | "denied" | "unknown";
  };
  capabilities: Capability[];
};
type ProjectSummary = {
  project: ProjectRef;
  title: string;
  repositoryPath: string;
  defaultModel: ModelSelection | null;
};
type ModelSummary = {
  instanceId: string;
  providerInstanceId: string;
  providerName: string;
  model: string;
  displayName: string;
  availability: "available" | "unavailable" | "unknown";
  unavailableReason: string | null;
  capabilities: Capability[];
  options: (
    | { kind: "select"; id: string; values: string[]; defaultValue: string | null }
    | {
        kind: "boolean";
        id: string;
        defaultValue: boolean | null;
      }
  )[];
};
type WorktreeSummary = {
  worktree: WorktreeRef;
  branch: string | null;
  evidence: ("thread_association" | "vcs_ref" | "verified_checkout")[];
};
type ThreadSummary = {
  thread: ThreadRef;
  project: ProjectRef;
  title: string;
  archived: boolean;
  worktree: WorktreeRef | null;
  latestTurn: TurnRef | null;
  settlement: "settled" | "unsettled" | "unknown";
};
type PendingRequestForm =
  | { kind: "approval"; detail: string; choices: { decision: ApprovalDecision; label: string }[] }
  | {
      kind: "input";
      questions: {
        id: string;
        header: string;
        question: string;
        options: { label: string; description: string }[];
        multiSelect: boolean;
      }[];
      responseSchema: Record<string, unknown>;
    };
type PendingRequest = {
  activityId: string;
  thread: ThreadRef;
  turn: TurnRef | null;
  state: "pending" | "resolved" | "unknown";
} & (
  | {
      actionable: true;
      pendingRequestId: string;
      unavailableReason: null;
      form: PendingRequestForm;
    }
  | {
      actionable: false;
      pendingRequestId: string | null;
      unavailableReason: string;
      form:
        PendingRequestForm | { kind: "unavailable"; requestKind: "approval" | "input" | "unknown" };
    }
);
type ThreadState = {
  summary: ThreadSummary;
  observationCursor: string;
  configuration: ThreadConfiguration;
  execution: {
    state: "active" | "inactive" | "unknown";
    turn: TurnRef | null;
    nativeState: string | null;
    evidence: Evidence[];
  };
  session: {
    state: "starting" | "running" | "ready" | "stopped" | "error" | "unknown";
    nativeState: string | null;
    evidence: Evidence[];
  };
  pendingRequests: Page<PendingRequest>;
  interruptionPending: boolean;
  limitations: string[];
};
type GuardCheck = {
  name:
    | "target_identity"
    | "association"
    | "reference_coverage"
    | "inactive_execution"
    | "no_pending_requests"
    | "session_stopped";
  state: "passed" | "failed" | "unavailable" | "not_applicable";
  detail: string;
};
type WorktreeInspection = {
  summary: WorktreeSummary;
  status: {
    changedFiles: number | null;
    stagedFiles: number | null;
    untrackedFiles: number | null;
    ahead: number | null;
    behind: number | null;
  };
  referencingThreads: Page<ThreadSummary>;
  checks: GuardCheck[];
  discardConsequences: {
    deletesWorktreeContents: true;
    retainsBranch: true;
    requiresExplicitSoleThreadForThreadRemoval: true;
    atomicReferenceGuard: false;
  };
};
type OutputChunk = {
  captureId: string;
  nextCursor: string | null;
  sourceCompleteness: "retained_projection" | "complete" | "partial" | "unknown";
  upstreamTruncated: boolean | null;
  items: {
    id: string;
    kind: "message" | "activity" | "diff";
    turn: TurnRef | null;
    part: number;
    lastPart: boolean;
    text: string;
  }[];
  limitations: string[];
};
type ThreadWaitResult = {
  condition: ThreadCondition;
  observation: "condition_met" | "timed_out" | "unavailable" | "history_gap";
  state: ThreadState | null;
};
type OperationRecord = {
  requestId: string;
  tool: string;
  revision: number;
  state: OperationState;
  admittedAt: string;
  updatedAt: string;
  recoverableUntil: string | null;
  target: InstanceSummary | ProjectRef | ThreadRef | WorktreeRef | null;
  completionMeans:
    | "registration_saved"
    | "registration_updated"
    | "registration_removed"
    | "worktree_created"
    | "thread_created"
    | "submission_accepted"
    | "response_accepted"
    | "interruption_observed"
    | "session_shutdown_observed"
    | "settlement_observed"
    | "thread_absent"
    | "worktree_absent";
  dispatch: "not_dispatched" | "accepted" | "rejected" | "unknown";
  commandId: string | null;
  messageId: string | null;
  correlation: TurnCorrelation | null;
  created: {
    instanceId?: string;
    thread?: ThreadRef;
    threadConfiguration?: ThreadConfiguration;
    worktree?: WorktreeRef;
  };
  steps: { name: string; state: StepState; evidence: Evidence[]; error: ToolFailure | null }[];
  evidence: Evidence[];
  error: ToolFailure | null;
  recovery:
    "observe_operation" | "observe_thread" | "inspect_target" | "new_explicit_request" | "none";
};
type ReadResults = {
  instance_list: Page<InstanceSummary>;
  instance_get: InstanceDetails;
  project_list: Page<ProjectSummary>;
  model_list: Page<ModelSummary>;
  worktree_list: Page<WorktreeSummary>;
  worktree_inspect: WorktreeInspection;
  thread_list: Page<ThreadSummary>;
  thread_get: ThreadState;
  thread_output: OutputChunk;
  diff_read: OutputChunk;
  turn_wait: TurnWaitResult;
  thread_wait: ThreadWaitResult;
  operation_get: {
    operation: OperationRecord;
    wait: "not_requested" | "record_changed" | "terminal" | "timed_out";
  };
};
```

Nullable fields mean the value is unavailable, not false or zero. A `Page` with no next cursor has exhausted its captured view; only coverage and limitations describe the upstream inventory. Pending requests with unknown lifecycle block cleanup when their resolution cannot be established. Thread summaries and detailed state carry the thread-removal consequence that its conversation is deleted and its worktree retained, through the fixed tool contract; inspection does not authorize removal.

`Capability.name` uses a stable catalog for each callable tool and for conditional guarantees: `steer_current`, `resume_retained`, `exact_turn_interrupt`, `authoritative_turn_outcomes`, `complete_worktree_inventory`, `complete_reference_checks`, and `full_raw_output`. Model entries specialize capabilities for a provider/model. Tool schemas stay stable across connections; unsupported operations remain callable and return `unsupported_capability` with the target and reason. Missing data is `unknown`, never assumed supported. Capabilities are revalidated at invocation when needed.

An implementation expands the enumerated tool and failure names into closed Effect Schema literals. Provider-native state strings remain open diagnostic values alongside normalized state. `responseSchema` is a bounded JSON Schema object describing the actionable input form. If a provider form cannot be represented or validated, expose it as unactionable with an explicit reason. `answers` is open only because native requests define their form at runtime; it is not an arbitrary command passthrough.

## Results and errors

Every tool returns a JSON object with a stable outer object schema, structured MCP content, and matching JSON text for clients that read only text. The specific result is nested, so the pinned Effect adapter can advertise an object output schema even when result cases form a union.

```ts
type ToolResult<T> = {
  result: { kind: "ok"; value: T } | { kind: "error"; error: ToolFailure };
  observations: Observation[];
  warnings: { code: string; message: string }[];
};
type Observation = {
  instanceId: string;
  observedAt: string;
  freshness: "fresh" | "stale" | "unknown";
  sourceSequence: number | null;
  coverage: "complete_for_query" | "partial" | "unknown";
  limitations: string[];
};
type ToolFailure = {
  code: string;
  message: string;
  retry: "safe_read" | "reconcile_first" | "change_request" | "none";
  details: Record<string, unknown>;
};
```

Freshness describes the evidence available at the recorded upstream observation, not a promise that no UI client has changed state since. Across-instance discovery returns healthy data alongside typed per-instance failures. A targeted operation never fails over to another registration.

Failure codes include `invalid_argument`, `registration_not_found`, `pairing_required`, `pairing_failed`, `identity_mismatch`, `identity_conflict`, `incompatible_instance`, `unsupported_capability`, `configuration_required`, `read_denied`, `operate_denied`, `unavailable`, `resource_not_found`, `request_id_conflict`, `request_record_unavailable`, `cursor_expired`, `cursor_mismatch`, `stale_state`, `uncheckable_target`, `uncheckable_references`, `active_execution`, `pending_request`, `shared_worktree`, `pending_request_not_current`, `resume_unavailable`, `result_too_large`, and `upstream_failure`.

An error must not erase completed steps or uncertainty about effects. A timeout waiting for observation is a normal wait result. An unsupported requested operation is an error, not a successful no-op. Partial discovery and partial cleanup are explicit result cases. MCP `isError` is true for the outer error case and for a mutator reporting failed or partial execution. Reading an operation whose state is failed, partial, or unknown is a successful status read with `isError: false`. Mutations with an unknown outcome use their explicit operation state rather than implying a no-effect failure. Invalid protocol arguments remain MCP `InvalidParams`; semantic validation uses the typed envelope. The adapter must map these cases deliberately; the pinned Effect `failureMode: "return"` path does not do so automatically.

## Mutation recovery

`requestId` is caller-generated, unique across the MCP server's mutation namespace, and identifies one exact mutation input, including its target and tool name. It is not a turn reference or the protocol request ID. Reusing a known ID with different inputs fails. Repeating identical known input observes its existing operation rather than creating another mutation. It does not restart a failed operation or reauthorize cleanup at a reused path. A new attempt after reconciliation uses a new explicit request.

A receipt separates admission, upstream dispatch, and observed effects. The cases are:

```ts
type OperationState =
  "admitted" | "pending" | "completed" | "failed" | "partial" | "outcome_unknown";
type StepState =
  | "not_started"
  | "pending"
  | "succeeded"
  | "already_absent"
  | "failed"
  | "skipped"
  | "outcome_unknown";
```

`completed` is completion of the named mutation's stated effect. For prompt submission, that effect is accepted orchestration intent, not provider execution. A receipt describes that effect explicitly. For deletion, completed requires evidence of deletion. For native settlement, it means observed attention state, not provider shutdown or task completion.

| Mutations                             | Completion evidence                                                                                                                                                                       |
| ------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Pair, pair again, update registration | Identity-verified connection metadata and credentials saved as required; replacement does not publish before verification.                                                                |
| Remove registration                   | Local registration and its private credentials removed; no claim about upstream execution or token revocation.                                                                            |
| Create worktree                       | Successful upstream creation result identifying the checkout, or sufficient reconciliation evidence for the intended creation.                                                            |
| Create thread                         | The intended native thread exists with the requested association and resolved configuration.                                                                                              |
| Submit prompt                         | The native orchestration command was accepted. Its turn may remain uncorrelated.                                                                                                          |
| Respond to approval/input             | The native response command was accepted. Provider consumption/resolution remains separately observed in thread state.                                                                    |
| Interrupt                             | Supported evidence of the requested interruption's effect; otherwise remain pending or unknown. A new turn becoming idle alone does not establish causal correlation.                     |
| Stop provider session                 | Shutdown observed for the relevant session. A later replacement session must not be mistaken for that session. Public references identify the thread.                                     |
| Set settlement                        | Requested native attention state observed; no claim about asynchronous provider shutdown. Native settle also clears pin/snooze state and may trigger shutdown; expose those consequences. |
| Remove thread                         | Intended thread confirmed absent after required checks and any required shutdown.                                                                                                         |
| Discard worktree                      | Intended worktree confirmed removed or already absent after required checks; report any explicitly requested thread removal independently.                                                |

Every admitted mutation is tracked even when its first response is already complete. If fresh evidence cannot distinguish a requested effect from a competing UI action, use `outcome_unknown` rather than asserting causal completion. A registration removal can make other operations on it unobservable; preserve those records and uncertainty without migrating work to another registration.

The record includes the original target, operation kind, known created references, dispatch evidence, outcome evidence, steps where relevant, and a recommended recovery action. Status lookup uses the caller's ID even if the original reply never arrived.

Native orchestration retries, where justified, preserve the original native command ID and exact payload. Worktree RPCs do not acquire native deduplication just because the MCP request has an ID. Uncertain worktree creation or discard requires reconciliation. Never dispatch a replacement mutation merely because a status record is missing.

The following service decision must specify durable recovery, retention, ID reuse protection, and what survives restart. The wire contract supports unavailable history and expired recovery authority. It promises neither indefinite retention nor exactly-once provider execution.

Admission is the point after which the MCP server owns the mutation's lifecycle. Before admission, cancellation can prevent the mutation; if the reply is lost, the caller must still check its request ID. After admission, client cancellation or disconnection stops its wait but does not cancel the operation. This includes undispatched remaining cleanup steps, which must still pass their fresh guards. Server crashes are a separate recovery case, not a promise that in-memory work survives. The service decision must define admission evidence and crash behavior.

Pairing codes and tokens are never echoed into receipts, logs, or diagnostic details. Storing mutation bookkeeping must not persist one-use pairing codes. The service decision must account for recovery of enrollment and re-pairing without violating that rule.

## Waiting and pending requests

An exact-turn wait separates execution evidence from observation status:

```ts
type TurnWaitResult = {
  target: TurnRef;
  observation: "condition_met" | "timed_out" | "unavailable" | "history_gap";
  execution:
    | "running"
    | "completed"
    | "interrupted"
    | "failed"
    | "awaiting_approval"
    | "awaiting_input"
    | "outcome_unknown";
  evidence: Evidence[];
  pendingRequests: PendingRequest[];
};
```

Only supported evidence for `completed`, `interrupted`, or `failed` establishes that the referenced turn ended. Projected completion caused by supersession, session readiness, settlement, or the end of an assistant message does not suffice. A newer turn never replaces the target of an exact-turn wait.

Thread waits support conditions `changed`, `inactive`, `settled`, `unsettled`, `session_stopped`, and `needs_response`. `inactive` requires no active execution or pending approval/input requests. Each response returns the observed state and freshness. A satisfied condition is not an exclusive lease on that state.

Pending requests include their native request identity when available, kind, offered decisions or input fields, lifecycle, and nullable observed turn reference. Some input activities have no request ID; expose these with `actionable: false` and a reason. Never invent an actionable identity. Uncorrelated requests remain visible at thread scope. They do not become an `awaiting_approval` or `awaiting_input` outcome for an arbitrary exact-turn wait.

The pinned native approval decisions are `accept`, `acceptForSession`, `acceptAlways`, `decline`, and `cancel`. Expose only choices supported for the observed request and provider. `acceptAlways` and `acceptForSession` retain their broader scope; never relabel them as a one-time approval. Input responses validate against the observed form before dispatch. Stale or resolved request IDs fail explicitly. Response commands have no upstream atomic turn guard, so the contract cannot promise one.

Cancelling a wait cancels observation only. A later wait may target the same turn. Thread interruption and provider-session shutdown use their separate mutation tools. Pending interruption does not block new submissions, within one MCP process or across processes. Distinct requests follow upstream ordering and may race. Operation records preserve evidence and uncertainty without locking the thread.

## Output and cursors

List cursors bind to the target instance/scope, filters, order, and a query snapshot or watermark. Output cursors bind to the target and output view. Pagination must neither silently change the query nor imply that a limited upstream inventory is exhaustive.

Within a captured list view, use stable ordering by instance ID and native resource ID, or by repository/worktree path for worktrees and provider/model key for models. Each instance in an aggregate result has its own observation metadata. For `thread_output`, serve the latest retained messages/activities first, complete an item's text parts in ascending order, then continue to earlier items. Each item retains its native identity so clients can reconstruct conversation order. `diff_read` serves the captured diff from its beginning. A fresh call without a cursor captures a new view; a continuation never changes views.

An expired cursor returns `cursor_expired` with a resync action. The server does not silently restart from the beginning. A mismatch returns `cursor_mismatch`. The later service decision chooses cache retention and restart behavior.

Output chunks include stream or message identity, native turn reference when known, text, continuation position, and source completeness. A single oversized message is chunked rather than silently omitted. Paging may be complete over an incomplete upstream result; report both separately. Limit fields measure UTF-8 content bytes, not model tokens. The total serialized result limit is 128 KiB, excluding duplicate MCP text encoding. A page may contain fewer than the requested item limit to fit. If one indivisible structured item cannot fit, return `result_too_large` with a target and recovery advice instead of silently dropping fields.

Conversation output is T3Code's retained, projected conversation and activities. Full tool outputs may have been summarized or dropped, and superseded activity rows removed. Do not describe this as a complete raw execution log. Recent snapshots retain unresolved approval/input activities even outside the history window, but their identities and turn correlation may still be missing.

The verified older-history paging interface is authenticated HTTP, not the agreed resource RPC interface. The initial adapter must use available RPC snapshots/replay and bounded local views. It must not call that additional HTTP history interface without changing the integration-boundary decision. If retained history cannot be served through supported RPC or has exceeded cache limits, report partial coverage or unavailable history explicitly. The service decision must validate snapshot memory limits and capture/eviction behavior. An output cursor belongs to the MCP server; do not expose an upstream cursor whose invalid-cursor behavior silently restarts a page.

Diff sources are explicit: current worktree changes, a worktree against a named base, a thread turn-count interval, or a full-thread diff through a named turn count. Native turn counts are not turn IDs. Diff paging pins one captured result; it does not concatenate pieces from changing worktree states. If a result cannot be continued after eviction, require resync. Preserve upstream truncation and unknown completeness. In the baseline, an empty review preview alone cannot prove the absence of changes because some Git failures become empty diff strings.

## Cleanup

`thread_get` and `worktree_inspect` expose associations, relevant referencing threads, execution and provider-session states, observation freshness, unavailable checks, and removal consequences. A limited discovery inventory is never evidence of zero references.

`thread_remove` always retains the worktree. `worktree_discard` explicitly authorizes losing the named worktree's contents while retaining its branch. Its optional thread target means remove that sole thread first. It cannot expand to other sharing threads. An orphan discard has no thread target.

Every removal obtains fresh required checks at execution time. It rejects active work, pending requests, stale or unavailable required checks, uncertain identity, and uncheckable references. It does not implicitly interrupt execution. An idle provider session may be stopped, but shutdown must be observed before thread deletion.

Combined removal records each step separately: checks, session shutdown, thread removal, reference recheck, and worktree discard. Do not continue to discard after uncertain thread removal. If a new reference appears after thread removal, report that the thread was removed and the worktree retained. No automatic rollback recreates a thread.

The accepted check-then-remove race remains visible. No preview token, exact-file-state authorization, or cross-client lock is implied. A path may later name a replacement worktree; an old discard request is not authority to delete that replacement.

## Confirmed walkthroughs

### Create, observe, steer, and finish

1. List instance registrations and inspect the selected instance's capabilities and providers.
2. List existing projects and choose one. A missing project must be created outside this toolkit.
3. Discover or create a worktree. Inspect its association before using it. A worktree created through the UI uses the same reference shape.
4. Create a thread on the selected checkout, or use an existing UI-created thread reference directly.
5. Submit text with an explicit request ID. Inspect acceptance evidence and correlation; an unestablished turn does not block observing the thread.
6. Read the thread, pending requests, and bounded output. Wait for an exact turn only after observing that turn's native reference. Otherwise use a thread-state wait without claiming prompt-specific correlation.
7. Respond to an offered approval/input request or submit additional text with explicit provider-default or guaranteed-steering intent. Reject unsupported steering rather than queueing it under that name.
8. If interruption is needed, request it separately and observe the result. To continue, submit a new prompt requiring retained context where that is the intended workflow.
9. Inspect conversation output and diffs. The controlling agent decides work completion. Explicit settlement is available separately.
10. Remove the thread while leaving the worktree, or explicitly request sole-thread/worktree discard. Inspect every cleanup step's observed result.

### Lost mutation reply

1. Submit with caller-selected `requestId = "submit-42"`; the connection drops before the reply.
2. Query `operation_get` with `submit-42`. If accepted evidence exists, inspect the original thread and output. Do not send a fresh prompt.
3. If dispatch or execution remains uncertain, return that uncertainty. Retry orchestration only under the existing command identity and payload when the recovery layer has enough evidence to justify it.
4. If the record is unavailable, do not interpret that as proof the request never ran. Reconcile the target and request a fresh explicit decision where necessary.

### Competing UI activity

1. Observe turn A, then request a thread-scoped interrupt. The UI starts turn B before T3Code processes the request.
2. The interrupt may affect B. Its acknowledgement does not establish that A or B stopped. Exact-turn waits retain their original targets and report only supported evidence.
3. During cleanup, an external client attaches another thread after the initial check. If the required recheck sees it, discard refuses and reports earlier completed steps.
4. A reference or execution change after the last check remains the accepted upstream race. The tools cannot claim atomic protection that the upstream contract does not supply.

## Concrete examples

These values are illustrative. Resource IDs and paths come from the selected instance or creation results. The example instance and project already exist.

Create a worktree and a thread in separate calls, waiting for each mutation's observed result before using its created reference:

```json
{
  "tool": "worktree_create",
  "arguments": {
    "requestId": "create-checkout-01",
    "instanceId": "devbox",
    "repositoryPath": "/srv/app",
    "startRef": "main",
    "newBranch": "fix-timeout"
  }
}
```

```json
{
  "tool": "thread_create",
  "arguments": {
    "requestId": "create-thread-01",
    "project": { "instanceId": "devbox", "projectId": "project-app" },
    "title": "Fix timeout handling",
    "checkout": {
      "kind": "worktree",
      "worktree": {
        "instanceId": "devbox",
        "repositoryPath": "/srv/app",
        "worktreePath": "/srv/worktrees/fix-timeout"
      }
    },
    "model": { "kind": "project_default" },
    "runtimeMode": "approval-required",
    "interactionMode": "default"
  }
}
```

Use a UI-created thread directly, without enrollment:

```json
{
  "tool": "thread_submit",
  "arguments": {
    "requestId": "submit-42",
    "thread": { "instanceId": "devbox", "threadId": "ui-created-thread" },
    "text": "Inspect the timeout failure and propose a fix.",
    "intent": "provider_default",
    "context": "thread_default"
  }
}
```

An accepted operation can have `state: "completed"`, `completionMeans: "submission_accepted"`, and `correlation: { "kind": "unestablished", "reason": "The adapter has no reliable submission-to-turn receipt." }`. The controlling agent reads `thread_get` and `thread_output`; it does not derive a turn ID from `submit-42`.

After observing a native turn reference, wait for that exact turn:

```json
{
  "tool": "turn_wait",
  "arguments": {
    "turn": {
      "instanceId": "devbox",
      "threadId": "ui-created-thread",
      "turnId": "observed-turn-17"
    },
    "waitMs": 10000
  }
}
```

A correlated approval may produce `execution: "awaiting_approval"` with `observation: "condition_met"`. The response uses the pending request identity, not the turn identity:

```json
{
  "tool": "approval_respond",
  "arguments": {
    "requestId": "respond-43",
    "pendingRequest": {
      "instanceId": "devbox",
      "threadId": "ui-created-thread",
      "pendingRequestId": "native-approval-9"
    },
    "decision": "accept"
  }
}
```

This is valid only when `accept` is an offered decision. For an input request, `input_respond` uses the same reference shape and an `answers` object conforming to the returned `responseSchema`. Neither tool supplies an answer or approval automatically.

After a lost reply, call `operation_get` with `requestId: "submit-42"`. If lookup returns `request_record_unavailable`, the prompt may still have run. Do not replace it with a new submission merely to get a receipt.

Explicit sole-thread discard names both resources:

```json
{
  "tool": "worktree_discard",
  "arguments": {
    "requestId": "discard-44",
    "worktree": {
      "instanceId": "devbox",
      "repositoryPath": "/srv/app",
      "worktreePath": "/srv/worktrees/fix-timeout"
    },
    "removeSoleThread": {
      "instanceId": "devbox",
      "threadId": "ui-created-thread"
    }
  }
}
```

If the recheck finds another referencing thread after deletion, the operation is `partial`: session shutdown and thread removal succeeded, the reference recheck failed with `shared_worktree`, and worktree discard was skipped. It leaves the worktree and branch in place. `operation_get` returns that record successfully even though the recorded cleanup did not fully succeed.

## Handoff to the service and recovery decision

The next ticket must choose mechanisms that satisfy this contract and validate their limits:

- Durable admission and recovery records, request-ID lifetime and reuse protection, private credential storage, and enrollment recovery without storing pairing codes.
- Record lookup and safe behavior across restart, retention expiry, interrupted admission, and loss of upstream acknowledgement. Unknown history cannot become authority for blind resubmission.
- Observation freshness, per-instance isolation, stream recovery, retention of exact-turn evidence, RPC snapshot size limits, bounded output captures, and cursor expiry.
- Concurrent interruption/submission behavior without local gates, and observation of shutdown before deletion, including stale or missing provider evidence.
- Correct MCP output schema, structured/text result parity, typed error projection, request cancellation, and detached admitted operations using the pinned Effect version.
- Contract and live tests for UI-created resources, colliding IDs across instances, missing model defaults, lost mutation replies, unsupported steering or resume guarantees, pending requests without IDs/turn correlation, partial inventories, shared-worktree refusal, partial cleanup, and reused worktree paths.

These mechanisms remain in the existing service/recovery ticket. They do not create permission to expand the current RPC boundary, invent authoritative execution receipts, or implement production code during this map.

## Validation and source basis

The contract uses the prerequisite resolutions and read-only inspection of the pinned T3Code 0.0.38 source at release commit `c0995d2eaf8ec787b3318ed1169ae266ed1529f8`. Additional checks covered [orchestration inputs](https://github.com/pingdotgg/t3code/blob/c0995d2eaf8ec787b3318ed1169ae266ed1529f8/packages/contracts/src/orchestration.ts), [provider/model configuration](https://github.com/pingdotgg/t3code/blob/c0995d2eaf8ec787b3318ed1169ae266ed1529f8/packages/contracts/src/server.ts), [snapshot queries and pending-request retention](https://github.com/pingdotgg/t3code/blob/c0995d2eaf8ec787b3318ed1169ae266ed1529f8/apps/server/src/orchestration/Layers/ProjectionSnapshotQuery.ts), and [projected activity output](https://github.com/pingdotgg/t3code/blob/c0995d2eaf8ec787b3318ed1169ae266ed1529f8/apps/server/src/orchestration/ActivityPayloadProjection.ts).

The combined TypeScript schema examples type-check, JSON examples parse, and `pnpm check`, `pnpm test`, `pnpm build`, and `pnpm fallow` pass. These checks validate document/schema consistency and the existing repository. They do not exercise the proposed tools or establish live T3Code/provider compatibility. The service and recovery decision owns that validation plan.
