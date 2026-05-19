# Pipeline Author Journey Fixtures

These CSV fixtures support `tests/test_pipeline_author_journey.py`.

## Source

- Dataset: `2014_usa_states.csv`
- Repository: `plotly/datasets`
- URL: https://raw.githubusercontent.com/plotly/datasets/master/2014_usa_states.csv
- Repository page: https://github.com/plotly/datasets
- License: MIT License, as published in the `plotly/datasets` repository.
- Accessed: 2026-05-20

## Why This Dataset

The source contains public aggregate state population rows. It is small,
tabular, readable, deterministic and does not include personal data.

The fixture keeps a small representative subset of the public CSV so the test
suite remains fast and fully offline.

## Files

- `source.csv`: valid input CSV derived from the public dataset.
- `expected_target.csv`: expected transformed output.
- `invalid_source.csv`: source-like CSV missing `Population`, used to prove the
  framework fails before transform/load.

## Transformation

The test pipeline:

1. reads `Rank`, `State`, `Postal`, `Population`;
2. emits `state_code` from `Postal`;
3. emits uppercase `state_name` from `State`;
4. casts `Population` to integer `population`;
5. derives `population_band`:
   - `large` when population is at least 10,000,000;
   - `medium` when population is at least 5,000,000;
   - `small` otherwise.

## Scenarios Covered

- happy path with real CSV input and expected target comparison;
- dry-run with validation and no real load;
- early managed failure when the source CSV is structurally invalid.
