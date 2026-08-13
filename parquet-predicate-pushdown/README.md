# parquet-predicate-pushdown

A Frontier Bench task where an agent must fix a latent off-by-one bug in a
Parquet row-group statistics writer, then implement predicate pushdown in a
columnar query planner, with correct NULL semantics.

## Task summary

The engine in `/app/engine/` reads Parquet files but reads every row group for
every query. The agent must:

1. Fix the off-by-one in `engine/stats.py` that records the wrong max value.
2. Implement row-group pruning in `engine/planner.py` using those statistics.
3. Handle NULL semantics (IS NULL, NOT, != must not be incorrectly pruned).
4. Emit a structured execution trace so pruning can be measured.

## Validating locally

```bash
# Oracle should score 1
harbor run -p . -a oracle -e docker

# No-op agent should score 0
harbor run -p . -a nop -e docker

# Quality check
harbor check . -m anthropic/claude-opus-4-8
```

## Structure

```
parquet-predicate-pushdown/
├── instruction.md        task description
├── task.toml             metadata and config
├── environment/
│   ├── Dockerfile        agent environment image
│   └── data/             engine source (with intentional bugs)
├── solution/
│   └── solve.sh          reference solution (fixes bugs, runs engine)
├── tests/
│   ├── Dockerfile        verifier image
│   ├── test.sh           verifier entry point
│   ├── test_queries.py   pytest tests
│   └── ground_truth.json expected query results
├── cheat/
│   └── cheat.sh          deliberate cheating attempt (fails verification)
└── README.md             this file
```

## Dataset

- Table: `sensors`, 10,000 rows, 10 row groups of 1,000 rows each.
- `sensor_id` ranges are non-overlapping across row groups (designed for pruning).
- Dataset is deterministic (seeded generator).

## Scoring

- Score 1: all query results correct AND total row_group_read events <= 35.
- Score 0: incorrect results OR no meaningful pruning (70 events expected without pushdown).
