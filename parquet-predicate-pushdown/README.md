# parquet-predicate-pushdown

A Frontier Bench task bundle. An agent must fix a latent off-by-one bug in
a columnar query engine's row-group statistics writer and implement predicate
pushdown pruning. Both correct query results and measurable pruning evidence
are required to pass.

## Layout

```
parquet-predicate-pushdown/
├── instruction.md          task instruction shown to the agent
├── task.toml               configuration and metadata
├── environment/
│   ├── Dockerfile          agent container image
│   └── data/               engine source, dataset generator, query runner
├── solution/
│   └── solve.sh            reference solution (scores 1)
├── tests/
│   ├── Dockerfile          verifier image (bakes ground truth)
│   ├── test.sh             verifier entry point
│   ├── test_queries.py     pytest test suite
│   └── ground_truth.json   expected query results
├── cheat/
│   └── cheat.sh            documented cheating attempt (never executed)
└── README.md               this file
```

## Local validation

Validate with the harbor CLI (requires Docker):

```sh
# Oracle run — should score 1
harbor run -p . -a oracle -e docker

# Nop run — should score 0
harbor run -p . -a nop -e docker

# Quality check
harbor check . -m anthropic/claude-opus-4-8
```

## Before submission

- Rewrite `instruction.md` in your own words. The current version is a template
  starting point. Human reviewers read it carefully.
- Fill in the `[metadata]` `author_name`, `author_email`, and
  `relevant_experience` fields in `task.toml` with your real information.
  Generic statements are rejected at review.

## How verification works

The verifier runs `tests/test.sh`, which calls pytest with `--ctrf`. Tests
check:

1. All 10 query results match `ground_truth.json`.
2. The trace has one entry per query with required fields.
3. Total row groups read across all queries is at most 130. A full-scan engine
   reads 200 (20 row groups × 10 queries), so 130 is only reachable with real
   pushdown.
4. The full-scan query (no predicate) reads all 20 row groups.
5. The IS NULL query reads all 20 row groups (nulls excluded from stats).
6. The above-all-data query prunes every row group.
7. A narrow range query reads at most 2 row groups.

`tests/` is baked into a separate verifier image the agent never sees.
`environment/` contains nothing that leaks expected answers.
