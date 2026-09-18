# Commit and submit

Use this procedure throughout implementation and repair work. Commit each
coherent, validated slice as it becomes ready, then submit the completed stack.
Staging, committing, and submitting mutate the repository, so keep this phase
with the main agent.

## Establish the working set

1. Run `git status -s` and mark the paths belonging to the requested work.
2. Leave unrelated new files untracked and unrelated tracked edits untouched
   and unstaged. Never use `git add .`, `git add -A`, or a global unstage to
   sweep unrelated work into the commit.
3. If a path contains both related and unrelated edits, stage only the relevant
   hunks or stop for clarification before changing that path.

The working set is complete only when every intended path is known and every
unrelated path is outside the index.

## Commit each slice

1. Finish one coherent purpose, package, or modification concern. Run the
   smallest relevant validation before committing it.
2. Confirm that the branch selected for this work is checked out and is not the
   trunk. Use a conventional commit message and stage only that slice:

   ```bash
   gt add -v -- <pathspec>...
   gt modify -c -m "<conventional commit subject>"
   ```

   Use the package name as the conventional-commit scope when one is clear.
   Add further `-m` arguments for additional paragraphs; pass real arguments
   rather than embedding escaped `\n` text.
3. Inspect `git status -s` and the commit immediately. Continue with the next
   slice; unrelated paths must remain outside the index.

Never commit directly to `master`. If branch setup somehow left the agent on
trunk, create the correctly named branch with Graphite before committing.
Avoid destructive reset or clean operations while staging or grouping.

## Submit

After all intended slices are committed and local review is clean, submit the
current stack:

```bash
gt submit --no-interactive
```

Record the PR number and URL from Graphite or `gh pr view`. Submission is
complete only when the intended branch has been pushed and the PR identity is
known.
