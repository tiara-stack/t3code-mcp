# Domain Docs

How the engineering skills should consume this repo's domain documentation.

## Before exploring, read these

- **`CONTEXT.md`** at the repo root.
- **`docs/adr/`** for decisions that touch the area being changed.

If an expected file does not exist, proceed without calling out its absence.

## File structure

This is a single-context repo:

```text
/
├── CONTEXT.md
├── docs/adr/
└── src/
```

`CONTEXT.md` is the glossary for domain language. ADRs record only decisions
that are hard to reverse, surprising without context, and based on a real
trade-off.

## Use the glossary's vocabulary

When an issue, proposal, hypothesis, or test names a domain concept, use the
term defined in `CONTEXT.md`. If the needed concept is missing, flag the gap
for domain modeling instead of inventing a competing synonym.

## Flag ADR conflicts

If proposed work contradicts an existing ADR, surface the conflict explicitly
before proceeding.
