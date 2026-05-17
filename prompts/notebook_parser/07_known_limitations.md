# 07 - Known Limitations

The notebook parser workflow is a controlled refactoring aid. It is not an
automatic productionization engine.

## Limitations

- Very exploratory notebooks may not be convertible automatically.
- Cells executed out of order can create hidden dependencies.
- Global variables can make parsing unsafe.
- Business logic cannot be inferred when it is absent.
- Visual outputs do not become production rules.
- Side effects outside load must be reviewed.
- Multiple ETL flows in one notebook should become separate pipelines.
- Multiple destinations require explicit split or manual review.
- Hardcoded credentials must be removed by humans.
- `count`, `collect`, `show` and `toPandas` may be valid exploration but should
  not become implicit production behavior.
- A generated `cfg_*.py` can be a draft even when syntactically valid.

## Residual Risk

Even after parsing, a human reviewer must validate:

- business meaning;
- target ownership;
- overwrite or append semantics;
- idempotency;
- data quality rules;
- Spark cost and partitioning;
- certification against the destination.

If these are not reviewed, the output is not production-ready.
