# Local CodeRabbit loop

Use this procedure for the local CodeRabbit review mode and for any workflow
that runs a local CodeRabbit review before submitting changes.

## Review and repair

Run from the repository root against the complete branch diff, including new
files:

```bash
TRUNK=master
coderabbit review --agent --base "$TRUNK" --include-untracked
```

Repeat the following until the command succeeds with no new valid findings:

1. Read every finding. Treat a concrete correctness, security, reliability,
   test, or maintainability defect in the requested scope as valid. Fix a
   preference finding when it provides meaningful value at low risk; skip a
   preference-only change when it would not. Do not let CodeRabbit severity
   labels replace this judgment.
2. For each valid finding or coherent group, make the smallest safe fix. Run
   the smallest relevant validation, inspect the diff, and commit the fix:

   ```bash
   gt add -v -- <pathspec>...
   gt modify -c -m "fix(scope): describe the repair"
   ```

3. Rerun the review against the current branch head. A previous finding is not
   green merely because a commit exists; verify that it no longer applies.
4. Treat a failed review command as a blocker, not a clean review. If the same
   valid finding remains after a genuine repair attempt, try an alternative
   fix. Report a blocker when no safe fix exists instead of looping forever.

The loop is complete only after a successful run reports no new valid
findings. For a review-only invocation, report that result and stop without PR
operations.
