import * as Schema from "effect/Schema";
import {
  MAX_PENDING_INPUT_ANSWERS_BYTES,
  MAX_PENDING_REQUEST_FORM_BYTES,
  MAX_PENDING_REQUEST_OPTIONS,
  MAX_PENDING_REQUEST_QUESTIONS,
  type InputRespondAnswers,
  type ApprovalDecision,
  type PendingRequest,
  type PendingRequestForm,
  type ThreadReference,
  serializedByteLength,
} from "./domain";
import type { ObservedThreadActivity } from "./t3code-adapter";

const approvalDecisions: ReadonlyArray<ApprovalDecision> = [
  "accept",
  "acceptForSession",
  "acceptAlways",
  "decline",
  "cancel",
];

interface DecodedApprovalOption {
  readonly decision: ApprovalDecision;
  readonly label: string;
}

const record = (value: unknown): Record<string, unknown> | null =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;

const hasOnlyKeys = (value: Record<string, unknown>, keys: ReadonlyArray<string>): boolean => {
  const allowed = new Set(keys);
  return Object.keys(value).every((key) => allowed.has(key));
};

const decodeApprovalOption = (element: unknown): DecodedApprovalOption | null => {
  const candidate = record(element);
  if (
    candidate === null ||
    typeof candidate.decision !== "string" ||
    !approvalDecisions.includes(candidate.decision as ApprovalDecision) ||
    typeof candidate.label !== "string" ||
    candidate.label.length === 0
  ) {
    return null;
  }
  return { decision: candidate.decision as ApprovalDecision, label: candidate.label };
};

const defaultApprovalOptions: ReadonlyArray<DecodedApprovalOption> = [
  { decision: "cancel", label: "Cancel" },
  { decision: "decline", label: "Decline" },
  { decision: "acceptForSession", label: "Always allow this session" },
  { decision: "accept", label: "Approve" },
];

const defaultApprovalRequestKinds: Readonly<Record<string, string>> = {
  command_execution_approval: "command",
  exec_command_approval: "command",
  file_read_approval: "file-read",
  file_change_approval: "file-change",
  apply_patch_approval: "file-change",
  mcp_elicitation_approval: "mcp-elicitation",
};

const defaultApprovalOptionsForPayload = (
  payload: Record<string, unknown>,
): ReadonlyArray<DecodedApprovalOption> | null => {
  const requestType = payload.requestType;
  if (typeof requestType !== "string" || !Object.hasOwn(defaultApprovalRequestKinds, requestType)) {
    return null;
  }
  const expectedRequestKind = defaultApprovalRequestKinds[requestType];
  return typeof expectedRequestKind === "string" && expectedRequestKind === payload.requestKind
    ? defaultApprovalOptions
    : null;
};

const decodeApprovalOptions = (
  payload: Record<string, unknown>,
): ReadonlyArray<DecodedApprovalOption> | null => {
  if (payload.options === undefined) return defaultApprovalOptionsForPayload(payload);
  if (!Array.isArray(payload.options)) return null;
  if (payload.options.length === 0 || payload.options.length > MAX_PENDING_REQUEST_OPTIONS) {
    return null;
  }
  const options: Array<DecodedApprovalOption> = [];
  for (const element of payload.options) {
    const option = decodeApprovalOption(element);
    if (option === null) return null;
    options.push(option);
  }
  return options;
};

interface DecodedInputQuestion {
  readonly id: string;
  readonly header: string;
  readonly question: string;
  readonly options: ReadonlyArray<{ readonly label: string; readonly description: string }>;
  readonly multiSelect: boolean;
}

const INPUT_QUESTION_KEYS = ["id", "header", "question", "options", "multiSelect"] as const;
const INPUT_QUESTION_OPTION_KEYS = ["label", "description"] as const;

const decodeInputQuestionOption = (
  rawOption: unknown,
): { readonly label: string; readonly description: string } | null => {
  const option = record(rawOption);
  if (
    option === null ||
    !hasOnlyKeys(option, INPUT_QUESTION_OPTION_KEYS) ||
    typeof option.label !== "string" ||
    option.label.length === 0 ||
    typeof option.description !== "string"
  ) {
    return null;
  }
  return { label: option.label, description: option.description };
};

const decodeInputQuestionIdentity = (
  candidate: Record<string, unknown>,
): Pick<DecodedInputQuestion, "id" | "header" | "question" | "multiSelect"> | null => {
  if (
    typeof candidate.id !== "string" ||
    candidate.id.length === 0 ||
    typeof candidate.header !== "string" ||
    typeof candidate.question !== "string" ||
    typeof candidate.multiSelect !== "boolean"
  ) {
    return null;
  }
  return {
    id: candidate.id,
    header: candidate.header,
    question: candidate.question,
    multiSelect: candidate.multiSelect,
  };
};

const decodeInputQuestionOptions = (
  candidate: Record<string, unknown>,
): ReadonlyArray<{ readonly label: string; readonly description: string }> | null => {
  if (!Array.isArray(candidate.options) || candidate.options.length > MAX_PENDING_REQUEST_OPTIONS) {
    return null;
  }
  const options: Array<{ label: string; description: string }> = [];
  const labels = new Set<string>();
  for (const rawOption of candidate.options) {
    const option = decodeInputQuestionOption(rawOption);
    if (option === null || labels.has(option.label)) return null;
    labels.add(option.label);
    options.push(option);
  }
  return options;
};

const decodeInputQuestion = (element: unknown): DecodedInputQuestion | null => {
  const candidate = record(element);
  if (candidate === null || !hasOnlyKeys(candidate, INPUT_QUESTION_KEYS)) return null;
  const identity = decodeInputQuestionIdentity(candidate);
  const options = decodeInputQuestionOptions(candidate);
  return identity === null || options === null ? null : { ...identity, options };
};

const decodeInputQuestions = (
  payload: Record<string, unknown>,
): ReadonlyArray<DecodedInputQuestion> | null => {
  if (
    !Array.isArray(payload.questions) ||
    payload.questions.length === 0 ||
    payload.questions.length > MAX_PENDING_REQUEST_QUESTIONS
  ) {
    return null;
  }
  const questions: Array<DecodedInputQuestion> = [];
  const ids = new Set<string>();
  for (const element of payload.questions) {
    const question = decodeInputQuestion(element);
    if (question === null || ids.has(question.id)) return null;
    ids.add(question.id);
    questions.push(question);
  }
  return questions;
};

const inputResponseSchema = (
  questions: ReadonlyArray<DecodedInputQuestion>,
): Record<string, unknown> => {
  const properties: Record<string, unknown> = Object.create(null);
  for (const question of questions) {
    const labels = question.options.map((option) => option.label);
    properties[question.id] =
      labels.length === 0
        ? { type: "string" }
        : question.multiSelect
          ? { type: "array", items: { enum: labels }, minItems: 1, uniqueItems: true }
          : { type: "string", enum: labels };
  }
  return {
    type: "object",
    properties,
    required: questions.map((question) => question.id),
    additionalProperties: false,
  };
};

const activityPayloadRecord = (activity: ObservedThreadActivity): Record<string, unknown> =>
  record(activity.payload) ?? {};

const isRequestId = (value: unknown): value is string =>
  typeof value === "string" && value.length > 0 && value.trim() === value;

const MISSING_REQUEST_ID_REASON =
  "The native request ID is missing; the request cannot be answered.";
const RESOLVED_REQUEST_REASON = "The request is already resolved.";
const UNKNOWN_REQUEST_LIFECYCLE_REASON =
  "The thread history is incomplete, so the request lifecycle cannot be established.";
const UNREPRESENTABLE_APPROVAL_REASON = "The offered approval decisions could not be represented.";
const UNREPRESENTABLE_INPUT_REASON = "The input form could not be represented.";
const INPUT_REQUEST_NOT_CURRENT_REASON =
  "The input request is stale or absent from the current synchronized thread state.";

interface PendingRequestContext {
  readonly thread: ThreadReference;
  readonly resolvedRequestIds: ReadonlySet<string>;
  readonly historyLimited: boolean;
}

const collectResolvedRequestIds = (
  activities: ReadonlyArray<ObservedThreadActivity>,
): ReadonlySet<string> => {
  const resolvedRequestIds = new Set<string>();
  for (const activity of activities) {
    if (activity.kind !== "approval.resolved" && activity.kind !== "user-input.resolved") {
      continue;
    }
    const requestId = activityPayloadRecord(activity).requestId;
    if (isRequestId(requestId)) resolvedRequestIds.add(requestId);
  }
  return resolvedRequestIds;
};

const pendingRequestBase = (context: PendingRequestContext, activity: ObservedThreadActivity) => ({
  activityId: activity.activityId,
  thread: context.thread,
  turn:
    activity.turnId === null
      ? null
      : {
          instanceId: context.thread.instanceId,
          threadId: context.thread.threadId,
          turnId: activity.turnId,
        },
});

type ActionableForm = Extract<PendingRequestForm, { readonly kind: "approval" | "input" }>;

interface RepresentableForm {
  readonly actionable: true;
  readonly form: ActionableForm;
}

interface UnrepresentableForm {
  readonly actionable: false;
  readonly form: PendingRequestForm;
  readonly unavailableReason: string;
}

const pendingRequestWithLifecycle = (options: {
  readonly context: PendingRequestContext;
  readonly base: ReturnType<typeof pendingRequestBase>;
  readonly requestId: string;
  readonly representable: RepresentableForm | UnrepresentableForm;
}): PendingRequest => {
  const { context, base, requestId, representable } = options;
  const resolved = context.resolvedRequestIds.has(requestId);
  const lifecycleKnown = !context.historyLimited || base.turn !== null;
  const state = resolved ? "resolved" : lifecycleKnown ? "pending" : "unknown";
  if (!representable.actionable) {
    return {
      ...base,
      state,
      actionable: false,
      pendingRequestId: requestId,
      unavailableReason:
        state === "unknown" ? UNKNOWN_REQUEST_LIFECYCLE_REASON : representable.unavailableReason,
      form: representable.form,
    };
  }
  if (state === "resolved") {
    return {
      ...base,
      state,
      actionable: false,
      pendingRequestId: requestId,
      unavailableReason: RESOLVED_REQUEST_REASON,
      form: representable.form,
    };
  }
  if (state === "unknown") {
    return {
      ...base,
      state,
      actionable: false,
      pendingRequestId: requestId,
      unavailableReason: UNKNOWN_REQUEST_LIFECYCLE_REASON,
      form: representable.form,
    };
  }
  return {
    ...base,
    state,
    actionable: true,
    pendingRequestId: requestId,
    unavailableReason: null,
    form: representable.form,
  };
};

const approvalPendingRequest = (
  context: PendingRequestContext,
  activity: ObservedThreadActivity,
  requestId: string,
): PendingRequest => {
  const payload = activityPayloadRecord(activity);
  const options = decodeApprovalOptions(payload);
  const representable: RepresentableForm | UnrepresentableForm =
    options === null
      ? {
          actionable: false,
          form: { kind: "unavailable", requestKind: "approval" },
          unavailableReason: UNREPRESENTABLE_APPROVAL_REASON,
        }
      : {
          actionable: true,
          form: {
            kind: "approval" as const,
            detail: typeof payload.detail === "string" ? payload.detail : activity.kind,
            choices: options.map((option) => ({
              decision: option.decision,
              label: option.label,
            })),
          },
        };
  return pendingRequestWithLifecycle({
    context,
    base: pendingRequestBase(context, activity),
    requestId,
    representable,
  });
};

const inputPendingRequest = (
  context: PendingRequestContext,
  activity: ObservedThreadActivity,
  requestId: string,
): PendingRequest => {
  const questions = decodeInputQuestions(activityPayloadRecord(activity));
  const form =
    questions === null
      ? null
      : {
          kind: "input" as const,
          questions: questions.map((question) => ({
            id: question.id,
            header: question.header,
            question: question.question,
            options: question.options.map((option) => ({
              label: option.label,
              description: option.description,
            })),
            multiSelect: question.multiSelect,
          })),
          responseSchema: inputResponseSchema(questions),
        };
  const representable: RepresentableForm | UnrepresentableForm =
    form === null || serializedByteLength(form) > MAX_PENDING_REQUEST_FORM_BYTES
      ? {
          actionable: false,
          form: { kind: "unavailable", requestKind: "input" },
          unavailableReason: UNREPRESENTABLE_INPUT_REASON,
        }
      : { actionable: true, form };
  return pendingRequestWithLifecycle({
    context,
    base: pendingRequestBase(context, activity),
    requestId,
    representable,
  });
};

const compareActivities = (left: ObservedThreadActivity, right: ObservedThreadActivity): number =>
  left.createdAt.localeCompare(right.createdAt) || left.activityId.localeCompare(right.activityId);

const isRequestActivity = (activity: ObservedThreadActivity): boolean =>
  activity.kind === "approval.requested" || activity.kind === "user-input.requested";

const uncorrelatedPendingRequest = (
  context: PendingRequestContext,
  activity: ObservedThreadActivity,
): PendingRequest => ({
  ...pendingRequestBase(context, activity),
  state: "unknown",
  actionable: false,
  pendingRequestId: null,
  unavailableReason: MISSING_REQUEST_ID_REASON,
  form: {
    kind: "unavailable",
    requestKind: activity.kind === "approval.requested" ? "approval" : "input",
  },
});

const requestedPendingRequest = (
  context: PendingRequestContext,
  activity: ObservedThreadActivity,
  requestId: string | null,
): PendingRequest =>
  requestId === null
    ? uncorrelatedPendingRequest(context, activity)
    : activity.kind === "approval.requested"
      ? approvalPendingRequest(context, activity, requestId)
      : inputPendingRequest(context, activity, requestId);

/**
 * Build the current thread-scoped request view from one synchronized snapshot.
 * A nullable turn correlation never changes the native request identity.
 */
export const pendingRequestsFromActivities = (
  thread: ThreadReference,
  activities: ReadonlyArray<ObservedThreadActivity>,
  historyLimited = false,
): ReadonlyArray<PendingRequest> => {
  const requested = activities.filter(isRequestActivity).slice().sort(compareActivities);
  const context: PendingRequestContext = {
    thread,
    resolvedRequestIds: collectResolvedRequestIds(activities),
    historyLimited,
  };
  const seenRequestIds = new Set<string>();
  const requests: Array<PendingRequest> = [];
  for (const activity of requested.slice().reverse()) {
    const rawRequestId = activityPayloadRecord(activity).requestId;
    const requestId = isRequestId(rawRequestId) ? rawRequestId : null;
    if (requestId !== null && seenRequestIds.has(requestId)) continue;
    if (requestId !== null) seenRequestIds.add(requestId);
    requests.push(requestedPendingRequest(context, activity, requestId));
  }
  requests.reverse();
  return requests;
};

const inputAnswersRuntimeSchema = (
  form: Extract<PendingRequestForm, { readonly kind: "input" }>,
): Schema.Constraint => {
  const fields: Record<string, Schema.Constraint> = Object.create(null);
  const ids = new Set<string>();
  for (const question of form.questions) {
    ids.add(question.id);
    const labels = question.options.map((option) => option.label);
    if (labels.length === 0) {
      fields[question.id] = Schema.String;
      continue;
    }
    const offeredLabel = Schema.String.check(
      Schema.makeFilter((value) => labels.includes(value), {
        message: "answer must be one of the offered options",
      }),
    );
    fields[question.id] = question.multiSelect
      ? Schema.Array(offeredLabel).check(
          Schema.makeFilter(
            (values) => values.length > 0 && new Set(values).size === values.length,
            { message: "multi-select answers must be non-empty and unique" },
          ),
        )
      : offeredLabel;
  }
  const unknownField = Schema.String.check(
    Schema.makeFilter((key) => !ids.has(key), {
      message: "answers contain a field that is not in the current input form",
    }),
  );
  return Schema.StructWithRest(Schema.Struct(fields), [Schema.Record(unknownField, Schema.Never)]);
};

const validateInputResponseAnswers = (
  form: Extract<PendingRequestForm, { readonly kind: "input" }>,
  answers: unknown,
): answers is InputRespondAnswers =>
  serializedByteLength(answers) <= MAX_PENDING_INPUT_ANSWERS_BYTES &&
  Schema.is(inputAnswersRuntimeSchema(form))(answers);

type InputResponseValidation =
  | { readonly kind: "valid"; readonly answers: InputRespondAnswers }
  | {
      readonly kind: "invalid";
      readonly code: "invalid_argument" | "pending_request_not_current";
      readonly message: string;
    };

export const validateObservedInputResponse = (options: {
  readonly thread: ThreadReference;
  readonly activities: ReadonlyArray<ObservedThreadActivity>;
  readonly pendingRequestId: string;
  readonly answers: unknown;
  readonly historyLimited?: boolean;
}): InputResponseValidation => {
  const requests = pendingRequestsFromActivities(
    options.thread,
    options.activities,
    options.historyLimited ?? false,
  );
  const request = requests.find((item) => item.pendingRequestId === options.pendingRequestId);
  if (request === undefined) {
    return {
      kind: "invalid",
      code: "pending_request_not_current",
      message: INPUT_REQUEST_NOT_CURRENT_REASON,
    };
  }
  if (request.state === "resolved") {
    return {
      kind: "invalid",
      code: "pending_request_not_current",
      message: RESOLVED_REQUEST_REASON,
    };
  }
  if (!request.actionable) {
    return {
      kind: "invalid",
      code: "pending_request_not_current",
      message: request.unavailableReason,
    };
  }
  if (request.form.kind !== "input") {
    return {
      kind: "invalid",
      code: "pending_request_not_current",
      message: "The native request is an approval request, not an input form.",
    };
  }
  if (!validateInputResponseAnswers(request.form, options.answers)) {
    return {
      kind: "invalid",
      code: "invalid_argument",
      message:
        "Answers must include each required field and match the free-text, offered-option, and multi-select rules in the current input form.",
    };
  }
  return { kind: "valid", answers: options.answers };
};
