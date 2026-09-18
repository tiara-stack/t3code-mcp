# GitHub review operations

Use these operations to inspect CodeRabbit feedback on the current PR head.
The main agent performs the analysis, fixes, replies, submissions, and label
changes.

## Polling handoff

For CodeRabbit review-status polling, attach [GitHub review polling worker]
(github-review-polling.md) to a separate read-only explorer. Pass the PR
identity, current head SHA, and required terminal criterion. Give the explorer
the polling reference only, not this review-handling reference. After it
reports, the main agent analyzes findings and handles every action.

## Inspect the current head

Set `PR` to the pull request number or URL. Resolve the repository and PR
identity before querying comments after the polling worker reports a completed
review:

```bash
REPO=$(gh repo view --json nameWithOwner --jq '.nameWithOwner')
PR_JSON=$(gh pr view "$PR" --json number,url,headRefOid,isDraft)
PR_NUMBER=$(printf '%s' "$PR_JSON" | jq -r '.number')
PR_URL=$(printf '%s' "$PR_JSON" | jq -r '.url')
HEAD_SHA=$(printf '%s' "$PR_JSON" | jq -r '.headRefOid')

coderabbit pullrequest "$PR_URL"
gh api --paginate "repos/$REPO/pulls/$PR_NUMBER/comments?per_page=100"
gh api --paginate "repos/$REPO/pulls/$PR_NUMBER/reviews?per_page=100"
gh api --paginate "repos/$REPO/issues/$PR_NUMBER/comments?per_page=100"
```

Identify CodeRabbit by the review/comment author rather than treating every
human review as bot feedback. Inspect both new feedback on `HEAD_SHA` and
unresolved older feedback that still applies to the current diff. An old
comment is not stale merely because the PR has a newer commit; check whether
the code or behavior it names still exists.

## Classify and answer

Classify every finding as follows:

- Fix concrete in-scope defects.
- Fix preference findings when the improvement is worthwhile and low risk.
- Reply to a skipped finding with evidence, the relevant repository or domain
  constraint, and the reason the requested change is not being made.

For an inline review comment, reply in its thread when the API supports it:

```bash
gh api --method POST \
  "repos/$REPO/pulls/$PR_NUMBER/comments/$COMMENT_ID/replies" \
  -f body="$REPLY"
```

For an issue-level PR comment, use `gh pr comment` and identify the original
comment. A review body without a reply endpoint gets a PR comment that names
the review and finding. Keep replies concise and evidence-based, for example:

```text
This does not apply because <specific evidence>. The current behavior is
intentional because <repository or product constraint>.
```

Do not silently delete, ignore, or dismiss a finding. A reply is required for
an invalid or intentionally skipped finding.

## Green criterion

Treat the GitHub review gate as pending while CodeRabbit has not completed a
review for the current head. Treat it as blocked when the bot cannot run due
to authentication, permissions, quota, or service failure. It is green only
when the current head has a completed CodeRabbit review, no actionable valid
finding remains, and each skipped finding has a substantive reply.
