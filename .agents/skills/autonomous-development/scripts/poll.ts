#!/usr/bin/env -S pnpm exec tsx

import { NodeRuntime, NodeServices } from "@effect/platform-node";
import {
  Clock,
  Console,
  Data,
  Duration,
  Effect,
  Match,
  Option,
  Result,
  Runtime,
  Schema,
  Stream,
} from "effect";
import { Command, Flag } from "effect/unstable/cli";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

const defaultIntervalSeconds = 10;
const defaultTimeoutMinutes = 15;
const headValidationAttempts = 3;
const headValidationIntervalSeconds = 10;
const commandTimeoutSeconds = 30;
const rateEvidenceIntervalSeconds = 60;
const transientGithubFailureLimit = 3;
// This checkout has one CI job named `checks`. The local ci-polling worker and
// required-checks-pass.sh remain authoritative for branch protection and any
// additional required checks; this list only supplies the repository's base
// context to the general Effect poller.
const requiredCiContexts: ReadonlyArray<string> = ["checks"];

class PollError extends Data.TaggedError("PollError")<{
  readonly message: string;
}> {}

type CapturedCommand = {
  readonly stdout: string;
  readonly stderr: string;
  readonly exitCode: number;
};

type PollInput = {
  readonly pr: string;
  readonly head: string;
  readonly repo: string;
  readonly intervalSeconds: number;
  readonly timeoutMinutes: number;
};

type Report = {
  readonly exitCode: number;
  readonly lines: ReadonlyArray<string>;
};

const NullableString = Schema.NullOr(Schema.String);

const PullRequestSchema = Schema.Struct({
  number: Schema.Number,
  headRefOid: Schema.String,
});

const CheckSchema = Schema.Struct({
  name: Schema.String,
  state: Schema.String,
  bucket: Schema.String,
  workflow: NullableString,
  event: NullableString,
  link: NullableString,
  startedAt: NullableString,
  completedAt: NullableString,
  description: NullableString,
  headSha: Schema.String,
  checkSuiteId: Schema.Number,
});

const RequiredCheckNameSchema = Schema.Struct({
  name: Schema.String,
});

const RequiredCheckNamesSchema = Schema.Array(RequiredCheckNameSchema);

const CheckRunSchema = Schema.Struct({
  id: Schema.Number,
  name: Schema.String,
  status: Schema.String,
  conclusion: NullableString,
  head_sha: Schema.String,
  html_url: NullableString,
  started_at: NullableString,
  completed_at: NullableString,
  check_suite: Schema.Struct({
    id: Schema.Number,
  }),
  output: Schema.Struct({
    summary: Schema.optionalKey(NullableString),
  }),
});

const CheckRunPagesSchema = Schema.Array(
  Schema.Struct({
    check_runs: Schema.Array(CheckRunSchema),
  }),
);

const CommitStatusSchema = Schema.Struct({
  context: Schema.String,
  state: Schema.String,
  description: NullableString,
  created_at: Schema.String,
  updated_at: Schema.String,
});

const CommitStatusResponseSchema = Schema.Struct({
  statuses: Schema.Array(CommitStatusSchema),
});

const UserSchema = Schema.Struct({
  login: Schema.String,
});

const IssueCommentSchema = Schema.Struct({
  id: Schema.Number,
  user: Schema.NullOr(UserSchema),
  body: Schema.String,
  created_at: Schema.String,
  updated_at: Schema.String,
});

const ReviewSchema = Schema.Struct({
  id: Schema.Number,
  user: Schema.NullOr(UserSchema),
  state: Schema.String,
  body: NullableString,
  commit_id: Schema.NullOr(Schema.String),
  submitted_at: NullableString,
});

const PullCommentSchema = Schema.Struct({
  id: Schema.Number,
  user: Schema.NullOr(UserSchema),
  body: Schema.String,
  commit_id: Schema.NullOr(Schema.String),
  path: Schema.NullOr(Schema.String),
  line: Schema.NullOr(Schema.Number),
  in_reply_to_id: Schema.optionalKey(Schema.NullOr(Schema.Number)),
});

const CodeRabbitPromptEventSchema = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("prompt"),
    source: Schema.String,
    pullRequestUrl: Schema.String,
    prompt: Schema.String,
  }),
  Schema.Struct({
    type: Schema.Literal("error"),
    errorType: Schema.String,
    message: Schema.String,
    recoverable: Schema.Boolean,
  }),
]);

const IssueCommentsSchema = Schema.Array(Schema.Array(IssueCommentSchema));
const ReviewsSchema = Schema.Array(Schema.Array(ReviewSchema));
const PullCommentsSchema = Schema.Array(Schema.Array(PullCommentSchema));

type PullRequest = Schema.Schema.Type<typeof PullRequestSchema>;
type Check = Schema.Schema.Type<typeof CheckSchema>;
type CheckRun = Schema.Schema.Type<typeof CheckRunSchema>;
type ChecksResult = {
  readonly checks: ReadonlyArray<Check>;
  readonly requiredNames: ReadonlyArray<string>;
};
type CommitStatus = Schema.Schema.Type<typeof CommitStatusSchema>;
type IssueComment = Schema.Schema.Type<typeof IssueCommentSchema>;
type Review = Schema.Schema.Type<typeof ReviewSchema>;
type PullComment = Schema.Schema.Type<typeof PullCommentSchema>;

const oneLine = (value: string, limit = 800): string => {
  const normalized = value.replace(/\s+/g, " ").trim();
  return normalized.length > limit ? `${normalized.slice(0, limit - 1)}…` : normalized;
};

const commandText = (executable: string, args: ReadonlyArray<string>): string =>
  [executable, ...args].map((arg) => (arg.includes(" ") ? JSON.stringify(arg) : arg)).join(" ");

const runProcess = (
  executable: string,
  args: ReadonlyArray<string>,
  options: { readonly timeoutSeconds?: number } = {},
): Effect.Effect<CapturedCommand, PollError, ChildProcessSpawner.ChildProcessSpawner> =>
  Effect.scoped(
    Effect.gen(function* () {
      const timeoutSeconds = options.timeoutSeconds ?? commandTimeoutSeconds;
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const handle = yield* spawner.spawn(
        ChildProcess.make(executable, args, {
          stdin: "ignore",
          stdout: "pipe",
          stderr: "pipe",
        }),
      );
      const output = yield* Effect.timeoutOption(
        Effect.all(
          {
            stdout: Stream.mkString(handle.stdout.pipe(Stream.decodeText)),
            stderr: Stream.mkString(handle.stderr.pipe(Stream.decodeText)),
            exitCode: handle.exitCode,
          },
          { concurrency: "unbounded" },
        ),
        Duration.seconds(timeoutSeconds),
      );
      if (Option.isNone(output)) {
        // Interrupting the output collection does not terminate the spawned
        // process. Kill it before leaving the scope so a hung GitHub CLI call
        // cannot prevent the poller's outer timeout from being observed.
        yield* handle.kill().pipe(Effect.ignore);
        return yield* new PollError({
          message: `${commandText(executable, args)} timed out after ${timeoutSeconds}s`,
        });
      }
      return {
        stdout: output.value.stdout,
        stderr: output.value.stderr,
        exitCode: Number(output.value.exitCode),
      };
    }).pipe(
      Effect.catch((cause) =>
        Effect.fail(
          cause instanceof PollError
            ? cause
            : new PollError({
                message: `${commandText(executable, args)} could not be executed: ${oneLine(String(cause))}`,
              }),
        ),
      ),
    ),
  );

const runGh = (
  args: ReadonlyArray<string>,
): Effect.Effect<CapturedCommand, PollError, ChildProcessSpawner.ChildProcessSpawner> =>
  runProcess("gh", args);

const decodeJson = <A>(
  schema: Schema.Decoder<A>,
  captured: CapturedCommand,
  label: string,
  options: {
    readonly allowedExitCodes?: ReadonlyArray<number>;
    readonly onEmptyStdout?: () => A;
    readonly allowEmptyStdout?: (captured: CapturedCommand) => boolean;
  } = {},
): Effect.Effect<A, PollError> =>
  Effect.gen(function* () {
    const allowedExitCode = options.allowedExitCodes?.includes(captured.exitCode) ?? false;
    if (captured.exitCode !== 0 && !allowedExitCode) {
      const details = [captured.stderr, captured.stdout]
        .map((value) => value.trim())
        .filter((value) => value.length > 0)
        .map(oneLine)
        .join(" | ");
      return yield* new PollError({
        message: `${label} failed with exit code ${captured.exitCode}: ${details || "no output"}`,
      });
    }
    if (captured.stdout.trim().length === 0) {
      if (
        allowedExitCode &&
        options.onEmptyStdout !== undefined &&
        (options.allowEmptyStdout?.(captured) ?? true)
      ) {
        return options.onEmptyStdout();
      }
      const detail = captured.stderr.trim().length > 0 ? oneLine(captured.stderr) : "no output";
      return yield* new PollError({
        message: `${label} failed with exit code ${captured.exitCode}: ${detail}`,
      });
    }
    const parsed = yield* Effect.try({
      try: () => JSON.parse(captured.stdout),
      catch: (cause) =>
        new PollError({ message: `${label} returned invalid JSON: ${oneLine(String(cause))}` }),
    });
    return yield* Effect.try({
      try: () => Schema.decodeUnknownSync(schema)(parsed),
      catch: (cause) =>
        new PollError({
          message: `${label} returned an unexpected JSON shape: ${oneLine(String(cause))}`,
        }),
    });
  });

const runGhJson = <A>(
  args: ReadonlyArray<string>,
  schema: Schema.Decoder<A>,
  label: string,
  options: {
    readonly allowedExitCodes?: ReadonlyArray<number>;
    readonly onEmptyStdout?: () => A;
    readonly allowEmptyStdout?: (captured: CapturedCommand) => boolean;
  } = {},
): Effect.Effect<A, PollError, ChildProcessSpawner.ChildProcessSpawner> =>
  Effect.gen(function* () {
    const captured = yield* runGh(args);
    return yield* decodeJson(schema, captured, label, options);
  });

const runGhText = (
  args: ReadonlyArray<string>,
  label: string,
): Effect.Effect<string, PollError, ChildProcessSpawner.ChildProcessSpawner> =>
  Effect.gen(function* () {
    const captured = yield* runGh(args);
    if (captured.exitCode !== 0) {
      const detail =
        captured.stderr.trim().length > 0 ? oneLine(captured.stderr) : "no error output";
      return yield* new PollError({
        message: `${label} failed with exit code ${captured.exitCode}: ${detail}`,
      });
    }
    const value = captured.stdout.trim();
    if (value.length === 0) {
      return yield* new PollError({ message: `${label} returned no repository name` });
    }
    return value;
  });

const resolveRepo = (
  repo: Option.Option<string>,
): Effect.Effect<string, PollError, ChildProcessSpawner.ChildProcessSpawner> => {
  if (Option.isSome(repo)) {
    const value = repo.value.trim();
    return value.length > 0
      ? Effect.succeed(value)
      : Effect.fail(new PollError({ message: "--repo must not be empty" }));
  }
  return runGhText(
    ["repo", "view", "--json", "nameWithOwner", "--jq", ".nameWithOwner"],
    "gh repo view",
  );
};

const readPullRequest = (
  input: PollInput,
): Effect.Effect<PullRequest, PollError, ChildProcessSpawner.ChildProcessSpawner> =>
  runGhJson(
    ["pr", "view", input.pr, "--repo", input.repo, "--json", "number,headRefOid"],
    PullRequestSchema,
    `PR ${input.pr} metadata`,
  );
const checkRunToCheck = (run: CheckRun): Check => {
  const status = run.status.toLowerCase();
  const conclusion = run.conclusion?.toLowerCase() ?? status;
  const bucket =
    status !== "completed"
      ? "pending"
      : conclusion === "success" || conclusion === "skipped" || conclusion === "neutral"
        ? "pass"
        : conclusion === "cancelled" || conclusion === "canceled"
          ? "cancel"
          : "fail";
  return {
    name: run.name,
    state: conclusion,
    bucket,
    workflow: null,
    event: null,
    link: run.html_url,
    startedAt: run.started_at,
    completedAt: run.completed_at,
    description: run.output.summary ?? null,
    headSha: run.head_sha,
    checkSuiteId: run.check_suite.id,
  };
};

const checkRunStartedAt = (run: CheckRun): number => {
  if (run.started_at === null) return 0;
  const parsed = Date.parse(run.started_at);
  return Number.isNaN(parsed) ? 0 : parsed;
};

const isNewerCheckRun = (run: CheckRun, current: CheckRun): boolean => {
  const startedAt = checkRunStartedAt(run);
  const currentStartedAt = checkRunStartedAt(current);
  return startedAt > currentStartedAt || (startedAt === currentStartedAt && run.id > current.id);
};
const commitStatusToCheck = (status: CommitStatus, head: string): Check => {
  const state = status.state.toLowerCase();
  const bucket =
    state === "success" ? "pass" : state === "failure" || state === "error" ? "fail" : "pending";
  return {
    name: status.context,
    state,
    bucket,
    workflow: null,
    event: null,
    link: null,
    startedAt: status.created_at,
    completedAt: bucket === "pending" ? null : status.updated_at,
    description: status.description,
    headSha: head,
    checkSuiteId: 0,
  };
};

const readChecks = (
  input: PollInput,
  options: { readonly requiredOnly?: boolean } = {},
): Effect.Effect<ChecksResult, PollError, ChildProcessSpawner.ChildProcessSpawner> =>
  Effect.gen(function* () {
    const requiredChecksEffect = runGhJson(
      [
        "pr",
        "checks",
        input.pr,
        "--repo",
        input.repo,
        ...(options.requiredOnly === false ? [] : ["--required"]),
        "--json",
        "name",
      ],
      RequiredCheckNamesSchema,
      `PR ${input.pr} required check names`,
      {
        allowedExitCodes: [1, 8],
        onEmptyStdout: () => [],
        allowEmptyStdout: (captured) =>
          captured.exitCode === 8 ||
          /no (?:required )?checks (?:reported|found|registered|available)/i.test(captured.stderr),
      },
    );
    const checkRunPagesEffect = runGhJson(
      [
        "api",
        "--paginate",
        "--slurp",
        `repos/${input.repo}/commits/${input.head}/check-runs?per_page=100`,
      ],
      CheckRunPagesSchema,
      `PR ${input.pr} check runs for ${input.head}`,
    );
    const { requiredChecks, checkRunPages, statuses } = yield* Effect.all(
      {
        requiredChecks: requiredChecksEffect,
        checkRunPages: checkRunPagesEffect,
        statuses: readCommitStatuses(input, `PR ${input.pr} commit statuses for ${input.head}`),
      },
      { concurrency: "unbounded" },
    );
    const requiredNames = [
      ...new Set([...requiredCiContexts, ...requiredChecks.map((check) => check.name)]),
    ];
    const latestRuns = new Map<string, CheckRun>();
    for (const run of checkRunPages.flatMap((page) => page.check_runs)) {
      if (run.head_sha !== input.head || !requiredNames.includes(run.name)) continue;
      const current = latestRuns.get(run.name);
      // Keep only the newest attempt before chooseCheck/evaluateChecks sees
      // the record.
      if (current === undefined || isNewerCheckRun(run, current)) {
        latestRuns.set(run.name, run);
      }
    }
    const statusChecks = statuses
      .filter((status) => requiredNames.includes(status.context))
      .map((status) => commitStatusToCheck(status, input.head));
    return {
      checks: [...latestRuns.values()].map(checkRunToCheck).concat(statusChecks),
      requiredNames,
    };
  });

const readCommitStatuses = (
  input: PollInput,
  label = `CodeRabbit status for ${input.head}`,
): Effect.Effect<ReadonlyArray<CommitStatus>, PollError, ChildProcessSpawner.ChildProcessSpawner> =>
  runGhJson(
    ["api", `repos/${input.repo}/commits/${input.head}/status?per_page=100`],
    CommitStatusResponseSchema,
    label,
  ).pipe(Effect.map((value) => value.statuses));

const readIssueComments = (
  input: PollInput,
  prNumber: number,
): Effect.Effect<ReadonlyArray<IssueComment>, PollError, ChildProcessSpawner.ChildProcessSpawner> =>
  runGhJson(
    [
      "api",
      "--paginate",
      "--slurp",
      `repos/${input.repo}/issues/${prNumber}/comments?per_page=100`,
    ],
    IssueCommentsSchema,
    `PR ${prNumber} issue comments`,
  ).pipe(Effect.map((pages) => pages.flat()));

const readRateLimitEvidence = (
  input: PollInput,
  prNumber: number,
): Effect.Effect<
  { readonly checks: ReadonlyArray<Check>; readonly comments: ReadonlyArray<IssueComment> },
  PollError,
  ChildProcessSpawner.ChildProcessSpawner
> =>
  Effect.all(
    {
      checks: readChecks(input, { requiredOnly: false }).pipe(Effect.map((result) => result.checks)),
      comments: readIssueComments(input, prNumber),
    },
    { concurrency: "unbounded" },
  );

type CodeRabbitPromptResult =
  | { readonly _tag: "prompt"; readonly prompt: string }
  | { readonly _tag: "not-found"; readonly message: string }
  | { readonly _tag: "error"; readonly message: string };

const readCodeRabbitPrompt = (
  input: PollInput,
  prNumber: number,
): Effect.Effect<CodeRabbitPromptResult, PollError, ChildProcessSpawner.ChildProcessSpawner> =>
  Effect.gen(function* () {
    const pullRequestUrl = `https://github.com/${input.repo}/pull/${prNumber}`;
    const args = ["pullrequest", pullRequestUrl, "--show-prompts", "--agent"];
    const captured = yield* runProcess("coderabbit", args, { timeoutSeconds: 120 });
    const lines = captured.stdout
      .split("\n")
      .map((value) => value.trim())
      .filter((value) => value.length > 0);
    if (lines.length === 0) {
      return yield* new PollError({
        message: `${commandText("coderabbit", args)} failed with exit code ${captured.exitCode}: ${oneLine(captured.stderr || "no output")}`,
      });
    }
    let lastError = "no decodable CodeRabbit event";
    for (const line of lines) {
      const event = yield* Effect.result(
        Effect.try({
          try: () => Schema.decodeUnknownSync(CodeRabbitPromptEventSchema)(JSON.parse(line)),
          catch: (cause) =>
            new PollError({
              message: `coderabbit pullrequest returned an unexpected event: ${oneLine(String(cause))}`,
            }),
        }),
      );
      if (Result.isFailure(event)) {
        lastError = event.failure.message;
        continue;
      }
      const eventResult = Match.value(event.success).pipe(
        Match.when({ type: "prompt" }, ({ prompt }) => ({ _tag: "prompt" as const, prompt })),
        Match.when({ type: "error" }, ({ message, recoverable }) =>
          /no coderabbit all-comments prompt was found/i.test(message)
            ? { _tag: "not-found" as const, message }
            : recoverable
              ? { _tag: "recoverable" as const, message }
              : { _tag: "error" as const, message },
        ),
        Match.exhaustive,
      );
      if (eventResult._tag === "recoverable") {
        lastError = eventResult.message;
        continue;
      }
      return eventResult;
    }
    return yield* new PollError({ message: lastError });
  });

const readGithubReviewEvidence = (
  input: PollInput,
  prNumber: number,
): Effect.Effect<
  {
    readonly issueComments: ReadonlyArray<IssueComment>;
    readonly reviews: ReadonlyArray<Review>;
    readonly inlineComments: ReadonlyArray<PullComment>;
  },
  PollError,
  ChildProcessSpawner.ChildProcessSpawner
> =>
  Effect.all(
    {
      issueComments: readIssueComments(input, prNumber),
      reviews: runGhJson(
        [
          "api",
          "--paginate",
          "--slurp",
          `repos/${input.repo}/pulls/${prNumber}/reviews?per_page=100`,
        ],
        ReviewsSchema,
        `PR ${prNumber} reviews`,
      ).pipe(Effect.map((pages) => pages.flat())),
      inlineComments: runGhJson(
        [
          "api",
          "--paginate",
          "--slurp",
          `repos/${input.repo}/pulls/${prNumber}/comments?per_page=100`,
        ],
        PullCommentsSchema,
        `PR ${prNumber} inline comments`,
      ).pipe(Effect.map((pages) => pages.flat())),
    },
    { concurrency: "unbounded" },
  );

const timestamp = (value: string | null): number => {
  if (value === null) return 0;
  const parsed = Date.parse(value);
  return Number.isNaN(parsed) ? 0 : parsed;
};

const newest = <A>(
  values: ReadonlyArray<A>,
  getTimestamp: (value: A) => string | null,
): A | undefined =>
  [...values]
    .sort((left, right) => timestamp(getTimestamp(left)) - timestamp(getTimestamp(right)))
    .at(-1);

const chooseCheck = (checks: ReadonlyArray<Check>): Check | undefined => {
  return newest(checks, (check) => check.completedAt ?? check.startedAt);
};

type CheckEvaluation =
  | { readonly _tag: "pass"; readonly selected: ReadonlyArray<Check> }
  | {
      readonly _tag: "wait";
      readonly selected: ReadonlyArray<Check>;
      readonly pending: ReadonlyArray<string>;
    }
  | {
      readonly _tag: "missing";
      readonly missing: ReadonlyArray<string>;
      readonly pending: ReadonlyArray<string>;
    }
  | {
      readonly _tag: "blocked";
      readonly selected: ReadonlyArray<Check>;
      readonly names: ReadonlyArray<string>;
      readonly reason: "failed" | "cancelled";
    };
const checkState = (check: Check): "pass" | "pending" | "fail" | "cancel" => {
  const bucket = check.bucket.toLowerCase();
  const state = check.state.toLowerCase();
  if (bucket === "pass" || state === "success") return "pass";
  // GitHub treats skipped required jobs as successful checks.
  if (bucket === "skipping" || state === "skipped" || state === "neutral") return "pass";
  if (bucket === "cancel" || state === "cancelled" || state === "canceled") return "cancel";
  if (bucket === "fail" || state === "failure" || state === "error") return "fail";
  return "pending";
};
const evaluateChecks = (
  checks: ReadonlyArray<Check>,
  requiredNames: ReadonlyArray<string> = [],
): CheckEvaluation => {
  const selected: Array<Check> = [];
  const missing: Array<string> = [];
  const requiredContexts = [...new Set([...requiredNames, ...checks.map((check) => check.name)])];
  for (const context of requiredContexts) {
    const check = chooseCheck(checks.filter((candidate) => candidate.name === context));
    if (check === undefined) missing.push(context);
    else selected.push(check);
  }
  const failed = selected.filter((check) => checkState(check) === "fail");
  if (failed.length > 0) {
    return {
      _tag: "blocked",
      selected,
      names: failed.map((check) => check.name),
      reason: "failed",
    };
  }
  const cancelled = selected.filter((check) => checkState(check) === "cancel");
  if (cancelled.length > 0) {
    return {
      _tag: "blocked",
      selected,
      names: cancelled.map((check) => check.name),
      reason: "cancelled",
    };
  }
  const pending = selected.filter((check) => checkState(check) === "pending");
  if (missing.length > 0) {
    return { _tag: "missing", missing, pending: pending.map((check) => check.name) };
  }
  if (pending.length > 0) {
    return { _tag: "wait", selected, pending: pending.map((check) => check.name) };
  }
  return { _tag: "pass", selected };
};

type HeadObservation =
  | { readonly _tag: "valid"; readonly observedHead: string }
  | { readonly _tag: "stale"; readonly observedHead: string }
  | { readonly _tag: "error"; readonly message: string };

const observeHead = (
  input: PollInput,
): Effect.Effect<HeadObservation, never, ChildProcessSpawner.ChildProcessSpawner> =>
  Effect.gen(function* () {
    let lastError = "head query did not return a result";
    for (let attempt = 0; attempt < headValidationAttempts; attempt += 1) {
      const result = yield* Effect.result(readPullRequest(input));
      if (Result.isSuccess(result)) {
        return result.success.headRefOid === input.head
          ? { _tag: "valid", observedHead: result.success.headRefOid }
          : { _tag: "stale", observedHead: result.success.headRefOid };
      }
      lastError = result.failure.message;
      if (attempt < headValidationAttempts - 1) {
        yield* Effect.sleep(Duration.seconds(headValidationIntervalSeconds));
      }
    }
    return { _tag: "error", message: lastError };
  });

type Terminal = {
  readonly state: string;
  readonly exitCode: number;
  readonly details: ReadonlyArray<string>;
};

const reportForObservation = (
  input: PollInput,
  kind: string,
  terminal: Terminal,
  observation: HeadObservation,
): Report => {
  const prefix = (status: "PASS" | "BLOCKED") => [
    `${kind} polling: ${status}`,
    `Repository: ${input.repo}`,
    `PR: ${input.pr}`,
    `Submitted head: ${input.head}`,
  ];
  return Match.value(observation).pipe(
    Match.when({ _tag: "error" }, ({ message }) => ({
      exitCode: 1,
      lines: [
        ...prefix("BLOCKED"),
        "Terminal state: head-validation-blocked",
        `Head validation error: ${message}`,
        `Original terminal state: ${terminal.state}`,
        ...terminal.details,
      ],
    })),
    Match.when({ _tag: "stale" }, ({ observedHead }) => ({
      exitCode: 1,
      lines: [
        ...prefix("BLOCKED"),
        "Terminal state: stale-head",
        `Observed head: ${observedHead}`,
        "The submitted head changed while polling; restart the gate for the new head.",
      ],
    })),
    Match.when({ _tag: "valid" }, ({ observedHead }) => ({
      exitCode: terminal.exitCode,
      lines: [
        ...prefix(terminal.exitCode === 0 ? "PASS" : "BLOCKED"),
        `Observed head: ${observedHead}`,
        `Terminal state: ${terminal.state}`,
        ...terminal.details,
      ],
    })),
    Match.exhaustive,
  );
};

const finalize = (
  input: PollInput,
  kind: string,
  terminal: Terminal,
): Effect.Effect<Report, never, ChildProcessSpawner.ChildProcessSpawner> =>
  observeHead(input).pipe(
    Effect.map((observation) => reportForObservation(input, kind, terminal, observation)),
  );

type HeadPoll =
  | { readonly _tag: "ready"; readonly metadata: PullRequest }
  | { readonly _tag: "done"; readonly report: Report }
  | { readonly _tag: "error"; readonly message: string };

const inspectHead = (
  input: PollInput,
  kind: string,
): Effect.Effect<HeadPoll, never, ChildProcessSpawner.ChildProcessSpawner> =>
  Effect.gen(function* () {
    const metadataResult = yield* Effect.result(readPullRequest(input));
    if (Result.isFailure(metadataResult)) {
      return {
        _tag: "error",
        message: metadataResult.failure.message,
      } as const;
    }
    if (metadataResult.success.headRefOid !== input.head) {
      return {
        _tag: "done",
        report: reportForObservation(
          input,
          kind,
          { state: "stale-head", exitCode: 1, details: [] },
          { _tag: "stale", observedHead: metadataResult.success.headRefOid },
        ),
      } as const;
    }
    return { _tag: "ready", metadata: metadataResult.success } as const;
  });

const errorReport = (
  kind: string,
  repo: string | undefined,
  pr: string,
  message: string,
): Report => ({
  exitCode: 1,
  lines: [
    `${kind} polling: BLOCKED`,
    ...(repo === undefined ? [] : [`Repository: ${repo}`]),
    `PR: ${pr}`,
    "Terminal state: github-error",
    `GitHub error: ${message}`,
  ],
});

const checkDetails = (selected: ReadonlyArray<Check>): ReadonlyArray<string> => [
  "Required checks:",
  ...selected.map((check) => {
    const state = checkState(check);
    const suffix = check.link === null || check.link.length === 0 ? "" : ` (${check.link})`;
    return `- ${check.name}: ${state}${suffix}`;
  }),
];

const pollCi = (
  input: PollInput,
): Effect.Effect<Report, PollError, ChildProcessSpawner.ChildProcessSpawner> =>
  Effect.gen(function* () {
    const deadline = (yield* Clock.currentTimeMillis) + input.timeoutMinutes * 60_000;
    let lastMissing: ReadonlyArray<string> = requiredCiContexts;
    let lastPending: ReadonlyArray<string> = [];
    let consecutiveGithubFailures = 0;
    while (true) {
      const head = yield* inspectHead(input, "CI");
      if (head._tag === "done") {
        return head.report;
      }
      if (head._tag === "error") {
        const failure = classifyGithubFailure(consecutiveGithubFailures, head.message);
        if (
          failure._tag === "terminal" ||
          (yield* Clock.currentTimeMillis) >= deadline
        ) {
          return yield* finalize(input, "CI", {
            state: "github-error",
            exitCode: 1,
            details: [githubFailureDetail(failure)],
          });
        }
        consecutiveGithubFailures = failure.count;
        yield* Effect.sleep(Duration.seconds(input.intervalSeconds));
        continue;
      }

      const checksResult = yield* Effect.result(readChecks(input));
      if (Result.isFailure(checksResult)) {
        const failure = classifyGithubFailure(
          consecutiveGithubFailures,
          checksResult.failure.message,
        );
        if (
          failure._tag === "terminal" ||
          (yield* Clock.currentTimeMillis) >= deadline
        ) {
          return yield* finalize(input, "CI", {
            state: "github-error",
            exitCode: 1,
            details: [githubFailureDetail(failure)],
          });
        }
        consecutiveGithubFailures = failure.count;
        yield* Effect.sleep(Duration.seconds(input.intervalSeconds));
        continue;
      }
      consecutiveGithubFailures = 0;
      const evaluation = evaluateChecks(
        checksResult.success.checks,
        checksResult.success.requiredNames,
      );
      if (evaluation._tag === "pass") {
        return yield* finalize(input, "CI", {
          state: "checks-passed",
          exitCode: 0,
          details: checkDetails(evaluation.selected),
        });
      }
      if (evaluation._tag === "blocked") {
        return yield* finalize(input, "CI", {
          state: `checks-${evaluation.reason}`,
          exitCode: 1,
          details: [
            `Blocked checks (${evaluation.reason}): ${evaluation.names.join(", ")}`,
            ...checkDetails(evaluation.selected),
          ],
        });
      }
      if (evaluation._tag === "missing") {
        lastMissing = evaluation.missing;
        lastPending = evaluation.pending;
      } else {
        lastMissing = [];
        lastPending = evaluation.pending;
      }

      if ((yield* Clock.currentTimeMillis) >= deadline) {
        return yield* finalize(input, "CI", {
          state: "timeout",
          exitCode: 1,
          details: [
            ...(lastMissing.length > 0
              ? [`Missing required contexts (requiredCiContexts): ${lastMissing.join(", ")}`]
              : []),
            ...(lastPending.length > 0 ? [`Pending checks: ${lastPending.join(", ")}`] : []),
          ],
        });
      }
      yield* Effect.sleep(Duration.seconds(input.intervalSeconds));
    }
  });

const promptFindingSection = (prompt: string): string | undefined =>
  prompt.match(
    /(?:^|\n)#{0,6}\s*(?:outside diff comments|findings|review findings|actionable comments)\s*:?\s*\n([\s\S]*?)(?=\n{2,}(?:after applying|next steps|review info|review details)\b|$)/i,
  )?.[1];

const promptFindingLines = (prompt: string): ReadonlyArray<string> => {
  const section = promptFindingSection(prompt);
  if (section === undefined) return [];
  return section
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.startsWith("- "))
    .filter(
      (line) => !/^-(?:\s+)?(?:still-valid|preference-only|concrete|fix|skip)\s*[:?-]/i.test(line),
    );
};

const promptDetails = (prompt: string): ReadonlyArray<string> => {
  const findingLines = promptFindingLines(prompt);
  if (findingLines.length === 0) return ["Findings surfaced: none"];
  return [
    `Findings surfaced: ${findingLines.length}`,
    ...findingLines.slice(0, 20).map((line) => `- ${oneLine(line.slice(2), 240)}`),
    ...(findingLines.length > 20 ? [`…and ${findingLines.length - 20} more prompt findings`] : []),
  ];
};

const isCodeRabbitUser = (user: { readonly login: string } | null): boolean =>
  user?.login.toLowerCase() === "coderabbitai[bot]";

const isPromptRateLimited = (message: string): boolean =>
  /rate\s*limit|quota|allowance/i.test(message);

const isTransientGithubError = (message: string): boolean =>
  /\bHTTP\s+5\d\d\b|\b5\d\d\s+(?:bad gateway|gateway timeout|internal|service unavailable)|(?:connection|network)\s+(?:closed|refused|reset|unavailable)|fetch failed|temporarily unavailable|timed? out|timeout/i.test(
    message,
  );

type GithubFailure =
  | { readonly _tag: "retry"; readonly count: number; readonly message: string }
  | { readonly _tag: "terminal"; readonly count: number; readonly message: string };

const classifyGithubFailure = (consecutiveFailures: number, message: string): GithubFailure => {
  const count = consecutiveFailures + 1;
  return isTransientGithubError(message) && count < transientGithubFailureLimit
    ? { _tag: "retry", count, message }
    : { _tag: "terminal", count, message };
};

const githubFailureDetail = (failure: GithubFailure): string =>
  `GitHub error after ${failure.count} consecutive observation failure${failure.count === 1 ? "" : "s"}: ${failure.message}`;

const parseActionableCount = (body: string): number =>
  [...body.matchAll(/Actionable comments posted:\s*(\d+)/gi)].reduce(
    (total, match) => total + Number(match[1] ?? 0),
    0,
  );

const parseReviewFindingCount = (body: string): number => {
  const severityFindings = [
    ...body.matchAll(
      /<summary><em>[^<]*(?:critical|major|minor|trivial|warning)[^<]*<\/em>/gi,
    ),
  ].length;
  const outsideDiffFindings = [...body.matchAll(/outside diff range comments\s*\((\d+)\)/gi)].reduce(
    (total, match) => total + Number(match[1] ?? 0),
    0,
  );
  return Math.max(parseActionableCount(body), severityFindings, outsideDiffFindings);
};

const isCompletedReviewIssueComment = (comment: IssueComment, head: string): boolean =>
  comment.body.includes(`change_assessment_commit:"${head}"`) &&
  /recent_review_start|no actionable comments were generated/i.test(comment.body);

type GithubReviewAssessment = {
  readonly currentReviews: ReadonlyArray<Review>;
  readonly currentInlineComments: ReadonlyArray<PullComment>;
  readonly currentIssueComments: ReadonlyArray<IssueComment>;
  readonly findingCount: number;
  readonly completed: boolean;
};

const assessGithubReviewEvidence = (
  evidence: {
    readonly issueComments: ReadonlyArray<IssueComment>;
    readonly reviews: ReadonlyArray<Review>;
    readonly inlineComments: ReadonlyArray<PullComment>;
  },
  head: string,
): GithubReviewAssessment => {
  const currentReviews = evidence.reviews.filter(
    (review) => isCodeRabbitUser(review.user) && review.commit_id === head,
  );
  const currentInlineComments = evidence.inlineComments.filter(
    (comment) =>
      isCodeRabbitUser(comment.user) &&
      comment.commit_id === head &&
      (comment.in_reply_to_id === null || comment.in_reply_to_id === undefined),
  );
  const currentIssueComments = evidence.issueComments.filter(
    (comment) => isCodeRabbitUser(comment.user) && comment.body.includes(head),
  );
  const declaredFindings = Math.max(
    ...currentReviews.map((review) => parseReviewFindingCount(review.body ?? "")),
    ...currentIssueComments.map((comment) => parseReviewFindingCount(comment.body)),
    0,
  );
  return {
    currentReviews,
    currentInlineComments,
    currentIssueComments,
    findingCount: Math.max(declaredFindings, currentInlineComments.length),
    completed:
      currentReviews.some((review) => review.state.toLowerCase() !== "pending") ||
      currentIssueComments.some((comment) => isCompletedReviewIssueComment(comment, head)),
  };
};

const githubReviewDetails = (
  evidence: {
    readonly issueComments: ReadonlyArray<IssueComment>;
    readonly reviews: ReadonlyArray<Review>;
    readonly inlineComments: ReadonlyArray<PullComment>;
  },
  head: string,
): ReadonlyArray<string> => {
  const assessment = assessGithubReviewEvidence(evidence, head);
  const { currentReviews, currentInlineComments, currentIssueComments, findingCount } = assessment;
  const findingLines = currentInlineComments.slice(0, 20).map((comment) => {
    const location =
      comment.path === null
        ? "unknown location"
        : `${comment.path}${comment.line === null ? "" : `:${comment.line}`}`;
    const summary =
      comment.body
        .split("\n")
        .map((line) => line.trim())
        .find((line) => line.length > 0 && !line.startsWith("<")) ?? "CodeRabbit comment";
    return `- #${comment.id} ${location}: ${oneLine(summary, 240)}`;
  });
  return [
    `GitHub review threads inspected: ${currentReviews.length} reviews, ${currentInlineComments.length} inline findings, ${currentIssueComments.length} issue comments for the submitted head`,
    `Findings surfaced: ${findingCount}`,
    ...findingLines,
    ...(currentInlineComments.length > 20
      ? [`…and ${currentInlineComments.length - 20} more inline findings`]
      : []),
    ...(findingCount === 0
      ? ["No head-scoped findings were found; check unresolved older GitHub threads before green."]
      : []),
  ];
};

const githubFallbackReport = (
  input: PollInput,
  prNumber: number,
  lastState: string,
  reason: string,
): Effect.Effect<Report, never, ChildProcessSpawner.ChildProcessSpawner> =>
  Effect.gen(function* () {
    const evidence = yield* Effect.result(readGithubReviewEvidence(input, prNumber));
    if (Result.isFailure(evidence)) {
      return yield* finalize(input, "CodeRabbit", {
        state: "github-error",
        exitCode: 1,
        details: [`GitHub error while inspecting review threads: ${evidence.failure.message}`],
      });
    }
    const assessment = assessGithubReviewEvidence(evidence.success, input.head);
    const details = githubReviewDetails(evidence.success, input.head);
    if (!assessment.completed || assessment.findingCount > 0) {
      return yield* finalize(input, "CodeRabbit", {
        state: "review-unresolved",
        exitCode: 1,
        details: [
          `CodeRabbit status: ${lastState}`,
          `CodeRabbit CLI prompt unavailable: ${oneLine(reason)}`,
          `Current-head review completed: ${assessment.completed ? "yes" : "no"}`,
          `Actionable findings from GitHub evidence: ${assessment.findingCount}`,
          ...details,
        ],
      });
    }
    return yield* finalize(input, "CodeRabbit", {
      state: "review-completed",
      exitCode: 0,
      details: [
        `CodeRabbit status: ${lastState}`,
        `CodeRabbit CLI prompt unavailable: ${oneLine(reason)}`,
        ...details,
      ],
    });
  });

const pollCodeRabbit = (
  input: PollInput,
): Effect.Effect<Report, PollError, ChildProcessSpawner.ChildProcessSpawner> =>
  Effect.gen(function* () {
    const deadline = (yield* Clock.currentTimeMillis) + input.timeoutMinutes * 60_000;
    let lastState = "CodeRabbit review has not reported a terminal state";
    let lastRateEvidenceAt = 0;
    let consecutiveGithubFailures = 0;
    while (true) {
      const head = yield* inspectHead(input, "CodeRabbit");
      if (head._tag === "done") {
        return head.report;
      }
      if (head._tag === "error") {
        const failure = classifyGithubFailure(consecutiveGithubFailures, head.message);
        if (
          failure._tag === "terminal" ||
          (yield* Clock.currentTimeMillis) >= deadline
        ) {
          return yield* finalize(input, "CodeRabbit", {
            state: "github-error",
            exitCode: 1,
            details: [githubFailureDetail(failure)],
          });
        }
        consecutiveGithubFailures = failure.count;
        yield* Effect.sleep(Duration.seconds(input.intervalSeconds));
        continue;
      }

      const statuses = yield* Effect.result(readCommitStatuses(input));
      if (Result.isFailure(statuses)) {
        const failure = classifyGithubFailure(consecutiveGithubFailures, statuses.failure.message);
        if (
          failure._tag === "terminal" ||
          (yield* Clock.currentTimeMillis) >= deadline
        ) {
          return yield* finalize(input, "CodeRabbit", {
            state: "github-error",
            exitCode: 1,
            details: [githubFailureDetail(failure)],
          });
        }
        consecutiveGithubFailures = failure.count;
        yield* Effect.sleep(Duration.seconds(input.intervalSeconds));
        continue;
      }
      const codeRabbitStatus = newest(
        statuses.success.filter((status) => status.context.toLowerCase() === "coderabbit"),
        (status) => status.updated_at,
      );
      const description = codeRabbitStatus?.description ?? "";
      const state = codeRabbitStatus?.state.toLowerCase() ?? "missing";
      lastState =
        codeRabbitStatus === undefined
          ? "CodeRabbit status is missing"
          : `${state}: ${description || "no description"}`;

      if (/rate\s*limited|quota exceeded/i.test(description)) {
        return yield* finalize(input, "CodeRabbit", {
          state: "rate-limited",
          exitCode: 1,
          details: [`CodeRabbit status: ${lastState}`],
        });
      }

      const now = yield* Clock.currentTimeMillis;
      const rateEvidenceDue =
        lastRateEvidenceAt === 0 || now - lastRateEvidenceAt >= rateEvidenceIntervalSeconds * 1000;
      const shouldReadRateEvidence =
        rateEvidenceDue && (/review skipped/i.test(description) || state !== "success");
      if (shouldReadRateEvidence) {
        const rateEvidence = yield* Effect.result(
          readRateLimitEvidence(input, head.metadata.number),
        );
        if (Result.isFailure(rateEvidence)) {
          lastRateEvidenceAt = 0;
          const failure = classifyGithubFailure(
            consecutiveGithubFailures,
            rateEvidence.failure.message,
          );
          if (
            failure._tag === "terminal" ||
            (yield* Clock.currentTimeMillis) >= deadline
          ) {
            return yield* finalize(input, "CodeRabbit", {
              state: "github-error",
              exitCode: 1,
              details: [
                `GitHub error while checking rate-limit evidence: ${githubFailureDetail(failure)}`,
              ],
            });
          }
          consecutiveGithubFailures = failure.count;
          yield* Effect.sleep(Duration.seconds(input.intervalSeconds));
          continue;
        }
        lastRateEvidenceAt = now;
        const rateLimitedCheck = rateEvidence.success.checks.some(
          (check) =>
            checkState(check) === "pass" &&
            (/review rate limited/i.test(check.name) ||
              /review rate limited/i.test(check.description ?? "")),
        );
        const statusTime =
          codeRabbitStatus === undefined ? 0 : timestamp(codeRabbitStatus.created_at);
        const rateLimitedComment =
          codeRabbitStatus !== undefined &&
          rateEvidence.success.comments.some(
            (comment) =>
              isCodeRabbitUser(comment.user) &&
              /review\s+rate\s+limited/i.test(comment.body) &&
              timestamp(comment.updated_at) >= statusTime,
          );
        if (rateLimitedCheck || rateLimitedComment) {
          return yield* finalize(input, "CodeRabbit", {
            state: "rate-limited",
            exitCode: 1,
            details: [
              `CodeRabbit status: ${lastState}`,
              "CodeRabbit reported: Review rate limited.",
            ],
          });
        }
      }
      consecutiveGithubFailures = 0;

      if (state === "failure" || state === "error") {
        return yield* finalize(input, "CodeRabbit", {
          state: "review-failed",
          exitCode: 1,
          details: [`CodeRabbit status: ${lastState}`],
        });
      }
      if (state === "success" && /review skipped/i.test(description)) {
        return yield* finalize(input, "CodeRabbit", {
          state: "review-skipped",
          exitCode: 1,
          details: [`CodeRabbit status: ${lastState}`],
        });
      }

      if (state === "success") {
        const prompt = yield* Effect.result(readCodeRabbitPrompt(input, head.metadata.number));
        if (Result.isFailure(prompt)) {
          return isPromptRateLimited(prompt.failure.message)
            ? yield* finalize(input, "CodeRabbit", {
                state: "rate-limited",
                exitCode: 1,
                details: [`CodeRabbit CLI/API error: ${prompt.failure.message}`],
              })
            : yield* githubFallbackReport(
                input,
                head.metadata.number,
                lastState,
                prompt.failure.message,
              );
        }
        const terminal = Match.value(prompt.success).pipe(
          Match.when({ _tag: "error" }, ({ message }) =>
            isPromptRateLimited(message)
              ? {
                  state: "rate-limited",
                  exitCode: 1,
                  details: [`CodeRabbit CLI/API error: ${oneLine(message)}`],
                }
              : { state: "prompt-unavailable", exitCode: 0, details: [message] },
          ),
          Match.when({ _tag: "prompt" }, ({ prompt: promptText }) => {
            if (promptFindingSection(promptText) === undefined) {
              return {
                state: "prompt-unavailable",
                exitCode: 0,
                details: ["CodeRabbit prompt did not contain a recognizable findings section."],
              };
            }
            const findingCount = promptFindingLines(promptText).length;
            if (findingCount === 0) {
              return {
                state: "prompt-unavailable",
                exitCode: 0,
                details: [
                  "CodeRabbit prompt contained no parsed findings; corroborating with GitHub evidence.",
                ],
              };
            }
            return {
              state: "review-findings",
              exitCode: 1,
              details: [`CodeRabbit status: ${lastState}`, ...promptDetails(promptText)],
            };
          }),
          Match.when({ _tag: "not-found" }, ({ message }) => ({
            state: "prompt-unavailable",
            exitCode: 0,
            details: [message],
          })),
          Match.exhaustive,
        );
        if (terminal.state === "prompt-unavailable") {
          return yield* githubFallbackReport(
            input,
            head.metadata.number,
            lastState,
            terminal.details[0] ?? "CodeRabbit did not return an agent prompt",
          );
        }
        return yield* finalize(input, "CodeRabbit", terminal);
      }

      if ((yield* Clock.currentTimeMillis) >= deadline) {
        return yield* finalize(input, "CodeRabbit", {
          state: "timeout",
          exitCode: 1,
          details: [`Last CodeRabbit state: ${lastState}`],
        });
      }
      yield* Effect.sleep(Duration.seconds(input.intervalSeconds));
    }
  });

const pollFlags = {
  pr: Flag.String("pr").pipe(Flag.withDescription("Pull request number or URL")),
  head: Flag.String("head").pipe(
    Flag.filter(
      (value) => /^[0-9a-f]{40}$/i.test(value.trim()),
      () => "--head must be a full 40-character commit SHA",
    ),
    Flag.map((value) => value.trim()),
    Flag.withDescription("Submitted pull request head SHA"),
  ),
  repo: Flag.String("repo").pipe(
    Flag.optional,
    Flag.withDescription("OWNER/REPO; defaults to the current repository"),
  ),
  intervalSeconds: Flag.Int("interval-seconds").pipe(
    Flag.filter(
      (value) => value >= 1 && value <= 300,
      (value) => `--interval-seconds must be between 1 and 300, got ${value}`,
    ),
    Flag.withDefault(defaultIntervalSeconds),
    Flag.withDescription("Seconds between silent observations"),
  ),
  timeoutMinutes: Flag.Int("timeout-minutes").pipe(
    Flag.filter(
      (value) => value >= 1 && value <= 120,
      (value) => `--timeout-minutes must be between 1 and 120, got ${value}`,
    ),
    Flag.withDefault(defaultTimeoutMinutes),
    Flag.withDescription("Maximum polling duration"),
  ),
};

type PollFlagValues = {
  readonly pr: string;
  readonly head: string;
  readonly repo: Option.Option<string>;
  readonly intervalSeconds: number;
  readonly timeoutMinutes: number;
};

const runPoll = (
  kind: "CI" | "CodeRabbit",
  config: PollFlagValues,
): Effect.Effect<void, never, ChildProcessSpawner.ChildProcessSpawner> =>
  Effect.gen(function* () {
    const repo = yield* Effect.result(resolveRepo(config.repo));
    if (Result.isFailure(repo)) {
      process.exitCode = 1;
      yield* Console.log(
        errorReport(kind, undefined, config.pr, repo.failure.message).lines.join("\n"),
      );
      return;
    }

    const input: PollInput = {
      pr: config.pr,
      head: config.head,
      repo: repo.success,
      intervalSeconds: config.intervalSeconds,
      timeoutMinutes: config.timeoutMinutes,
    };
    const result = yield* Effect.result(kind === "CI" ? pollCi(input) : pollCodeRabbit(input));
    const report = Result.isSuccess(result)
      ? result.success
      : errorReport(kind, input.repo, input.pr, result.failure.message);
    process.exitCode = report.exitCode;
    yield* Console.log(report.lines.join("\n"));
  });

const ciCommand = Command.make("ci", pollFlags, (config) => runPoll("CI", config));
const codeRabbitCommand = Command.make("coderabbit", pollFlags, (config) =>
  runPoll("CodeRabbit", config),
);

const command = Command.make("autonomous-development-poll").pipe(
  Command.withDescription("Silently poll autonomous-development CI and CodeRabbit gates"),
  Command.withSubcommands([ciCommand, codeRabbitCommand]),
);

const main = Command.run(command, { version: "0.0.0" }).pipe(
  Effect.provide(NodeServices.layer),
  Effect.catchCause((cause) =>
    Effect.sync(() => {
      process.exitCode = 1;
      process.stdout.write(
        `autonomous-development-poll: BLOCKED\nTerminal state: internal-error\n${oneLine(String(cause))}\n`,
      );
    }),
  ),
);

const preserveProcessExitCode: Runtime.Teardown = (_exit, onExit) => {
  onExit(typeof process.exitCode === "number" ? process.exitCode : 0);
};

NodeRuntime.runMain(main, { teardown: preserveProcessExitCode });
