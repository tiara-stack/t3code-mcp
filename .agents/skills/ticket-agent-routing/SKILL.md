---
name: ticket-agent-routing
description: Add configurable coding-agent and reasoning recommendations to Linear tickets produced with $wayfinder or $to-tickets.
---

# Ticket Agent Routing

Use this skill together with `$wayfinder` or `$to-tickets` when publishing
Linear issues. It adds a recommendation for the person or workflow choosing
the agent for the ticket: the developer for ordinary implementation tickets,
or the Wayfinder agent when dispatching a research ticket. It does not assign
the issue or cause the chosen agent to spawn another agent.

## Configuration

Read `.agents/ticket-routing.yaml` from the repository root before classifying
any ticket. The file is the source of truth for task levels, model names, and
reasoning effort. Validate that every level used by this skill has a non-empty
`model` and `reasoning_effort`; stop and report the configuration error instead
of inventing a fallback.

The configuration may be changed by maintainers as model availability and
usage characteristics change. Treat model names as opaque identifiers and
preserve the exact configured spelling. The repository defaults reflect the
linked DeepSWE results and cost: use Luna medium for quick work, Luna high for
focused work, and Luna max for normal work; use Astra medium for complex or
cross-package work; and reserve Astra xhigh for system-shaping work. Benchmark
and usage reports are signals, not guarantees.

## Route each ticket

1. Start with the approved ticket set from `$wayfinder` or `$to-tickets`.
   Preserve titles, acceptance criteria, parentage, labels, and blocking
   relationships.
2. Assign exactly one configured task level to each ticket. Judge the level by
   scope, risk, breadth, and ambiguity. For a Wayfinder research ticket, assess
   the investigation rather than implementation size:

   - `quick`: a tiny, narrowly bounded implementation or factual investigation.
   - `focused`: a localized fix, feature, or investigation with a clear scope.
   - `standard`: a normal vertical slice or investigation spanning several
     layers, sources, or packages.
   - `complex`: cross-package, high-risk, or investigation-heavy work needing
     a stronger base model.
   - `architectural`: frontier-level, uncertain, or system-shaping work whose
     consequences span the repository and justify the highest effort.

   Choose from the configured levels rather than creating a new level in the
   issue. If a ticket sits between levels, choose the higher level and note the
   reason in the recommendation block.
3. Look up that level's `model` and `reasoning_effort` in the configuration.
4. Prepend the following block to the issue body, before `## Parent`, `## What
   to build`, or any other content:

   ```markdown
   > **Coding agent recommendation**
   >
   > **Coding agent:** `Codex`  
   > **Task level:** `<level>`  
   > **Model:** `<configured model>`  
   > **Reasoning:** `<configured reasoning_effort>`
   >
   > This is routing metadata for the developer or workflow choosing an
   > appropriate agent. It is not an instruction for the assigned agent to
   > spawn a subagent with this model or reasoning level. The developer may
   > override it.
   ```

   Add one concise sentence after the block only when the classification is
   not self-evident; explain the scope or risk that drove the level.
5. Publish the issues through the repository's Linear workflow. Keep the
   recommendation block at the top after any later body edits, and verify the
   created issue body contains the exact configured model and reasoning effort.

When used during Wayfinder charting, apply this skill to each newly created
`wayfinder:research` child before Wayfinder fires its research worker. Classify
the ticket by investigation breadth and ambiguity, then put the recommendation
block at the top of the research question. Leave Wayfinder's child issue,
blocking, claim, branch, and research-worker mechanics unchanged. The block is
read by the Wayfinder agent only at the research-dispatch step, where it selects
the model and reasoning effort for that one spawned research worker. This is
the only Wayfinder-specific use of the block. For the spawned research worker,
the block is metadata and is never an instruction to spawn further subagents or
to change Wayfinder's workflow.

## Wayfinder maps

For a `$wayfinder` map, route child decision tickets that a coding agent may
implement or investigate. The map issue itself normally describes the
destination rather than executable work, so leave it unrouted unless its body
also asks an agent to perform a concrete task. Preserve the `wayfinder:*`
labels and native Linear blocking relationships.

## Completion

The skill is complete only when every in-scope published issue has exactly one
recommendation block at the top, the block names a configured level, model,
and reasoning effort, and the issue text says the recommendation is not a
subagent-spawning instruction and may be overridden by the developer.
