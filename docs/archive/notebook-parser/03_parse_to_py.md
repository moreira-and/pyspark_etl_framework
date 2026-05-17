# 03 - Parse To Python

This stage converts `cln_*.ipynb` into a Python file compatible with
`etl_framework`.

## Output

```text
prompts/notebook_parser/notebook_parser_runs/<nome_semantico_notebook>/
  cfg_<nome_semantico_notebook>.py
```

## Required File Shape

The generated file must contain:

1. Imports.
2. Explicit configuration.
3. Classes or structure compatible with `etl_framework`.
4. Required ETL methods:
   - extract
   - check
   - transform
   - validate
   - load
   - certify
5. A protected entrypoint:

```python
if __name__ == "__main__":
    ...
```

6. Short comments for:
   - source notebook;
   - semantic name;
   - mapping decisions;
   - preserved ambiguities.

## Rules

- Do not leave execution in global scope.
- Do not leave production logic outside classes/functions.
- Do not hardcode secrets, tokens or credentials.
- Keep configuration isolated and visible.
- Keep code readable for junior developers.
- Do not introduce external runtime dependencies.
- Do not call `count`, `collect`, `show` or `toPandas` as implicit framework
  behavior.
- Do not write outside the load stage.

## Draft vs Production Candidate

If unresolved ambiguity remains, the generated file must be marked as draft in a
top-level comment and must not be treated as production-ready.

Example marker:

```python
# NOTE: Draft generated from notebook parser.
# Manual review required before production use.
```
