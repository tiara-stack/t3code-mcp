# Model-routing evidence: DeepSWE and Terminal-Bench 4.0

Checked 2026-09-28.

## Artificial Analysis Coding Agent Index v1.5

The Artificial Analysis comparison reports Codex-harness runs at `max` effort. Its v1.5 index equally weights DeepSWE v1.1, Terminal-Bench 4.0, and SWE-Atlas-QnA; each benchmark uses three attempts per task.

| Model, effort      | Index | DeepSWE v1.1 | Terminal-Bench 4.0 | SWE-Atlas-QnA | Cost per task | Time per task |
| ------------------ | ----: | -----------: | -----------------: | ------------: | ------------: | ------------: |
| GPT-6 Luna, `max`  |    41 |          64% |                15% |           44% |         $0.18 |         21.4m |
| GPT-6 Sol, `max`   |    57 |          69% |                43% |           58% |         $2.99 |         22.3m |
| GPT-6 Astra, `max` |    62 |          68% |                56% |           62% |         $7.47 |         29.4m |

The reported cost is the average API cost per task across the full three-benchmark suite, not a cost for one benchmark. The measurements include the Codex harness and do not isolate model weights from agent behavior.

Compared with Luna, Sol scores 16 points higher on the composite, with +5 percentage points on DeepSWE and +28 on Terminal-Bench; its reported average task cost is about 16.6 times higher. Compared with Sol, Astra scores 5 points higher on the composite and 13 points higher on Terminal-Bench, but costs about 2.5 times more. Sol is one point higher on DeepSWE in this run.

Sources: [Codex model comparison](https://artificialanalysis.ai/agents/coding-agents/comparisons/claude-code-vs-codex), [Coding Agent Index methodology](https://artificialanalysis.ai/methodology/coding-agents-benchmarking).

## Artificial Analysis standalone Terminal-Bench 4.0

Artificial Analysis also runs all 66 Terminal-Bench 4.0 tasks with `mini-swe-agent`, reporting pass@1 averaged over three repeats per task. Its max-effort model comparisons show Luna at 13%, Sol at 44%, and Astra at 59%.

These are independent runs under a different harness from the Codex-harness rows above. Keep the two result sets separate.

Sources: [Artificial Analysis Terminal-Bench 4.0 evaluation](https://artificialanalysis.ai/evaluations/terminalbench-4-0), [model comparisons](https://artificialanalysis.ai/models/comparisons/gpt-6-luna-vs-gpt-6-sol).

## Benchmark-owner leaderboard cross-check

The DeepSWE v1.1 owner leaderboard lists GPT-6 Astra `xhigh` at 74% ±3%, with an average task cost of $4.43, but no GPT-6 Sol or Luna entries. Its published runs use `mini-swe-agent`, so they are not directly comparable to Artificial Analysis's Codex-harness table.

The Terminal-Bench 4.0 owner leaderboard lists GPT-6 Astra with Codex at 58.2% ±2.8%; the owner-board run differs from Artificial Analysis's independent runs. Benchmark scores can change with the agent harness and evaluation run.

Sources: [DeepSWE leaderboard](https://deepswe.datacurve.ai/), [DeepSWE repository and evaluation setup](https://github.com/datacurve-ai/deep-swe), [Terminal-Bench 4.0 owner leaderboard](https://www.tbench.ai/leaderboard/terminal-bench/4.0).

## API pricing and routing implication

Standard short-context API rates per 1M input/output tokens are Luna $0.10/$0.50, Sol $2/$10, and Astra $10/$50. These are API prices, not Codex subscription costs or benchmark-suite costs.

OpenAI positions Luna for focused, high-volume tasks, Sol for complex coding and agentic workflows, and Astra for the hardest end-to-end work. The Artificial Analysis results support the proposed routing: Luna for quick through standard work and Sol for complex and architectural implementation. The benchmarked Sol and Astra rows use `max`; this draft also uses `max` for both Sol levels to match the measured configuration. Astra planning remains user-invoked and is not configured in repository instructions.

Sources: [GPT-6 Luna](https://developers.openai.com/api/docs/models/gpt-6-luna), [GPT-6 Sol](https://developers.openai.com/api/docs/models/gpt-6-sol), [GPT-6 Astra](https://developers.openai.com/api/docs/models/gpt-6-astra).
