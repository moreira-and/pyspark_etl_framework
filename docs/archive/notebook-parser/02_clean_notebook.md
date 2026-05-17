# 02 - Clean Notebook

The cleaned notebook is an organized version of the original notebook. It is
still a notebook and must not import or implement `etl_framework`.

## Output

```text
prompts/notebook_parser/notebook_parser_runs/<nome_semantico_notebook>/
  cln_<nome_semantico_notebook>.ipynb
```

## Required Sections

The cleaned notebook must use this section structure:

```text
# 00 - Context and Objective
# 01 - Imports
# 02 - Configuration Candidates
# 03 - Definitions
# 04 - Extract Candidates
# 05 - Check Candidates
# 06 - Transform Candidates
# 07 - Validate Candidates
# 08 - Load Candidates
# 09 - Certify Candidates
# 10 - Ambiguities and Manual Review
```

## Cleaning Rules

- Preserve original logic.
- Do not create new business logic.
- Do not add `etl_framework` imports yet.
- Group cells by intent only when there is clear evidence.
- Keep comments explaining ambiguities.
- Rename variables only when readability improves and behavior is unchanged.
- Remove duplication only when it is clearly redundant.
- Do not remove apparently useless code without recording it in manual review.
- Move exploratory `print`, `display`, `show`, charting and ad hoc analysis to
  review/comment sections, not to the production flow.

## Mixed Cells

If one cell mixes reading, transformation and writing:

- split it only when separation is obvious;
- otherwise keep it together and mark it in `10 - Ambiguities and Manual Review`.

The parser must not silently decide stage ownership for ambiguous code.

## Manual Review Notes

The manual review section must list:

- ambiguous cells;
- suspected side effects;
- hardcoded paths or credentials;
- multiple possible destinations;
- cells that depend on execution order;
- exploratory code that might affect final output.

If this section contains unresolved production-critical ambiguity, do not
generate a production-ready `cfg_*.py`. Generate a draft only or fail.
