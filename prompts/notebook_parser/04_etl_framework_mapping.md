# 04 - etl_framework Mapping

The final `cfg_*.py` must respect the official `etl_framework` flow:

```text
extract -> check -> transform -> validate -> load -> certify
```

## Stage Mapping

| Clean notebook section | Target stage | Allowed responsibility |
| --- | --- | --- |
| `04 - Extract Candidates` | `Extract._extract` | Read or collect source data only. |
| `05 - Check Candidates` | `Extract._check` | Initial checks before transformation. |
| `06 - Transform Candidates` | `Transform._transform` | Business transformations. |
| `07 - Validate Candidates` | `Transform._validate` | Structural validation and DQ before load. |
| `08 - Load Candidates` | `Load._load` | Writes and other destination side effects. |
| `09 - Certify Candidates` | `Load._certify` | Evidence from final destination. |

## Rules Per Stage

### extract

- Read source tables, files or prepared inputs.
- Apply source filters and extraction window when clearly present.
- Do not write data.
- Do not perform business transformations unless the notebook makes the split
  obvious.

### check

- Validate initial source assumptions.
- Keep checks cheap by default.
- Any action Spark such as `count` must be explicit and documented.

### transform

- Apply business logic.
- Keep Spark operations visible.
- Do not write data.
- Do not certify output.

### validate

- Use `validate_struct` when target schema is known.
- Add explicit DQ checks only when they are present in the notebook or required
  by project standards.
- Do not infer missing business rules.

### load

- All writes belong here.
- Use staging and idempotent commit for production candidates.
- Do not hide writes in helper functions called from other stages.

### certify

- Validate the final destination when possible.
- Do not certify only the in-memory `DataFrame` when the destination can be read.
- Publish explicit metrics through `context.metrics` when they are already
  calculated by the pipeline.

## Ambiguous Mapping

Fail or mark manual review when:

- a cell mixes read, transform and write;
- there are multiple write targets;
- the notebook depends on out-of-order execution;
- global variables define hidden dependencies;
- a visual output appears to decide production logic.
