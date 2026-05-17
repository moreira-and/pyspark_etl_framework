# 05 - Validation Rules

After generating `cfg_*.py`, validate the output before human review.

## Required Checks

1. The file imports without executing the pipeline.
2. No production execution exists outside `if __name__ == "__main__":`.
3. All required stages exist:
   - extract
   - check
   - transform
   - validate
   - load
   - certify
4. The implementation uses `etl_framework` contracts.
5. `show`, `collect`, `count` and `toPandas` are not used as hidden behavior.
6. Writes exist only in load.
7. Certification reads the final destination when possible.
8. No hardcoded secrets or credentials exist.
9. Configuration is isolated and understandable.
10. Ambiguities are preserved as comments or manual review notes.

## Suggested Static Review

Review the generated file for these patterns:

```text
.show(
.collect(
.count(
.toPandas(
.write
save(
insertInto(
saveAsTable(
```

These patterns are not always forbidden. They must be in the correct stage and
must be explicit. For example, a `count()` in `certify` can be acceptable when
it reads the destination and the cost is documented.

## Failure Criteria

The workflow must fail or require human review when:

1. Extract, transform or load cannot be identified clearly.
2. Writes appear in multiple points.
3. Global variables create implicit cell-order dependencies.
4. The notebook depends on out-of-order execution.
5. Credentials are hardcoded.
6. Mixed logic cannot be separated safely.
7. Multiple destinations exist without clear separation.
8. External data is used without explicit configuration.
9. Exploratory cells appear to affect final output.

Do not resolve these cases by silent inference.

## Review Result

Each generated `cfg_*.py` should be classified as one of:

- `production_candidate`: passes structural checks and has no unresolved
  critical ambiguity.
- `draft_requires_review`: generated, but manual decisions remain.
- `failed`: no safe mapping can be produced.
