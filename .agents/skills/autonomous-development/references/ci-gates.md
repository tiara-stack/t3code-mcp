# CI gates

Use this reference to keep a pull request in draft while checking CI before
removing its draft state. The `pre-undraft` phase ends with all required checks
green and leaves the PR draft unchanged.
When the file is attached to a read-only explorer, inspect and report the
failure only. The main agent performs repairs, commits, submissions, and
conflict resolution.

## Polling handoff

For CI status polling, attach [CI polling worker](ci-polling.md) to a separate
read-only explorer. Pass the PR identity, submitted head SHA, and required
terminal criterion. Give the explorer the polling reference only, not this
repair reference. After it reports, the main agent handles diagnosis and every
repair.

## Repair a failure

1. Rerun a clearly transient or infrastructure-only failure once. Repair a
   reproducible repository failure in the code, configuration, generated
   output, or test that caused it.
2. Run `pnpm dlx fallow@2.88.2 audit` for ordinary Fallow findings. For
   `fallow_baseline`, use the exact baseline command from
   `.github/workflows/ci.yml`.
3. Fix a baseline finding before changing the baseline. Update a baseline only
   when the code change intentionally and legitimately changes the accepted
   result, after local verification, and commit the update as an understandable
   change. Never regenerate a baseline solely to suppress a failure.
4. If Git reports merge conflicts, fetch the target and resolve each conflict
   while preserving both sides when their intent is clear. Validate the result.
   Report a precise blocker when the intended resolution is ambiguous.
5. Run the local CodeRabbit review command, repair any valid findings, commit
   repairs with Graphite, and submit the new head. Return to the polling
   handoff for that new head:

   ```bash
   coderabbit review --agent --base master --include-untracked
   gt submit --no-interactive
   ```

Inspect failed workflow and step logs with `gh run view <run-id>
--log-failed` or the matching GitHub Actions detail after the polling worker
reports a failed check. A different required check failing is also a release
blocker; use the same repair loop when it is repository-owned and its intent is
clear.
