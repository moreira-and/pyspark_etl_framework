# 01 - Raw Notebook

The raw stage preserves the original notebook exactly as received.

## Input

One source `.ipynb` file selected by the operator.

Before copying, the workflow must:

1. Identify the input notebook path.
2. Infer `<nome_semantico_notebook>` in snake_case.
3. Create:

```text
prompts/notebook_parser/notebook_parser_runs/<nome_semantico_notebook>/
```

4. Record the execution plan before applying changes.

## Output

```text
prompts/notebook_parser/notebook_parser_runs/<nome_semantico_notebook>/
  raw_<nome_semantico_notebook>.ipynb
```

## Rules

- Copy bytes/content exactly from the original notebook.
- Do not alter cells.
- Do not reorder cells.
- Do not clear outputs.
- Do not normalize variables.
- Do not modify metadata.
- Do not remove widgets, execution counts or visual outputs.

## Why This Exists

The raw copy is the audit baseline. It allows reviewers to compare:

- original exploratory state;
- cleaned parser-friendly notebook;
- final `etl_framework` pipeline.

If the raw copy cannot be created, stop the workflow. A final `.py` must not be
generated without a preserved raw artifact.
