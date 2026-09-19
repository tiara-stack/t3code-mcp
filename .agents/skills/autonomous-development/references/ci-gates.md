# CI gates

Use this reference to keep a pull request in draft while checking CI before
removing its draft state. The `pre-undraft` phase requires this repository's
`checks` job and every additional required check to pass for the submitted
head, and leaves the PR draft unchanged. The `checks` job in
`.github/workflows/ci.yml` runs the repository's validation commands, including
Fallow.
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
2. Reproduce the failed step using the command and dependency versions in
   `.github/workflows/ci.yml`. Before rerunning Fallow, fetch the Git base
   reference used by that workflow step so it is available for comparison.
3. Fix the finding before changing an audit baseline or configuration. Change
   an accepted result only when the code intentionally changes it, after local
   verification, and commit that change with its reason.
4. If Git reports merge conflicts, fetch the target and resolve each conflict
   while preserving both sides when their intent is clear. Validate the result.
   Report a precise blocker when the intended resolution is ambiguous.
5. Run the local CodeRabbit review command, repair any valid findings, commit
   repairs with Graphite, and submit the new head. Return to the polling
   handoff for that new head:

   ```bash
   TRUNK=$(gt trunk) || exit 1
   test -n "$TRUNK" || exit 1
   coderabbit review --agent --base "$TRUNK" --include-untracked
   gt submit --no-interactive
   ```

Inspect failed workflow and step logs with `gh run view <run-id>
--log-failed` or the matching GitHub Actions detail after the polling worker
reports a failed check. A different required check failing is also a release
blocker; use the same repair loop when it is repository-owned and its intent is
clear.
