# Notebook Parser Overview

This workflow converts an unstructured `.ipynb` into a reviewed Python pipeline
compatible with `etl_framework`.

It is intentionally staged:

```text
.ipynb desformatado
-> .ipynb limpo e estruturado
-> .py padronizado compativel com etl_framework
```

It does not promise automatic productionization of any exploratory notebook.
The goal is to create a deterministic, auditable path with intermediate files
that a human can review before production use.

## Standard Artifact Layout

For a notebook with semantic name `<nome_semantico_notebook>`, create:

```text
prompts/notebook_parser/notebook_parser_runs/<nome_semantico_notebook>/
  raw_<nome_semantico_notebook>.ipynb
  cln_<nome_semantico_notebook>.ipynb
  cfg_<nome_semantico_notebook>.py
```

`<nome_semantico_notebook>` must be snake_case, without spaces, accents or
special characters. Infer it from the notebook filename, title, stated purpose
or main dataset. If the inferred name is generic, improve it from notebook
content. If no useful name can be inferred, stop and ask for human input.

## Stages

1. Discovery: identify the input notebook and semantic name.
2. Raw preservation: copy the original notebook exactly.
3. Clean notebook: organize cells into parser-friendly sections.
4. Parse to Python: create `cfg_*.py` using the `etl_framework` contract.
5. Validate output: check imports, stage boundaries and forbidden patterns.
6. Review limitations: record ambiguities and cases that require manual work.

## Mandatory Principles

- Do not change `etl_framework` core for notebook-specific behavior.
- Do not infer missing business rules.
- Do not move logic between ETL stages when intent is ambiguous.
- Mark ambiguity in the cleaned notebook or fail explicitly.
- Keep the workflow understandable for junior developers.
- Preserve intermediate artifacts for human review.
- Keep the generated `.py` deterministic and auditable.

## Documentation Map

- [01_raw_notebook.md](01_raw_notebook.md): raw copy rules.
- [02_clean_notebook.md](02_clean_notebook.md): cleaned notebook structure.
- [03_parse_to_py.md](03_parse_to_py.md): conversion to Python.
- [04_etl_framework_mapping.md](04_etl_framework_mapping.md): stage mapping.
- [05_validation_rules.md](05_validation_rules.md): output validation.
- [06_examples.md](06_examples.md): examples and failure cases.
- [07_known_limitations.md](07_known_limitations.md): known limitations.
