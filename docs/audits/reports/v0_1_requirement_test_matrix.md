# Matriz Requisito Para Teste v0.1

Ultima atualizacao: 2026-05-18.

Esta matriz rastreia promessa -> contrato -> codigo -> teste -> documentacao.
Ela valida readiness do framework v0.1. Readiness de uma pipeline produtiva
concreta exige checklist proprio de `Load`.

| Requisito | Promessa v0.1? | Contrato/docs | Codigo | Teste/evidencia | Lacuna real | Status |
| --- | --- | --- | --- | --- | --- | --- |
| Ordem oficial `extract -> check -> transform -> validate -> load -> certify` | Sim | `docs/v0.1-contract.md` | `Pipeline.run` | `tests/test_pipeline_contract.py::test_run_executes_official_order` | Nenhuma | Protegido |
| `Extract.run` usa Template Method com `_run_extract` e `_run_check` | Sim | `docs/v0.1-contract.md` | `Extract.run` | `tests/test_stage_contract_template_method.py::test_extract_run_should_execute_extract_then_source_struct_check_automatically` | Nenhuma | Protegido |
| `Transform.run` usa Template Method com `_run_transform` e `_run_validate` | Sim | `docs/v0.1-contract.md` | `Transform.run` | `tests/test_stage_contract_template_method.py::test_transform_run_should_execute_transform_then_target_struct_validation_automatically` | Nenhuma | Protegido |
| `Load.run` e dono de `dry_run` e nao chama load/certify real | Sim | `docs/v0.1-contract.md` | `Load.run` | `tests/test_stage_contract_template_method.py::test_load_run_should_skip_real_load_when_dry_run_is_enabled` | Nenhuma | Protegido |
| `Load.run` valida entrada como `DataFrame` | Sim | `docs/v0.1-contract.md` | `Load._run_load` | `tests/test_stage_contract_template_method.py::test_load_run_should_raise_managed_error_when_input_is_not_dataframe` | Nenhuma | Protegido |
| Integracao minima com Spark local | Sim | `docs/development/testing.md` | Contratos publicos | `tests/test_framework_integration.py::test_inline_framework_integration_runs_full_contract` | Nao cobre destino real | Protegido para framework |
| `source_struct` automatico | Sim | `README.md`, `docs/v0.1-contract.md` | `auto_check_source`, `validate_schema` | `tests/test_auto_contract.py::test_auto_check_fails_when_source_column_is_missing` | Nenhuma | Protegido |
| `auto_check` nao executa acoes Spark | Sim | `docs/v0.1-contract.md` | `validate_schema` | `tests/test_auto_contract.py::test_auto_check_source_does_not_run_spark_actions` | Nenhuma | Protegido |
| `source_struct` ausente falha | Sim | `docs/v0.1-contract.md` | `auto_check_source` | `tests/test_auto_contract.py::test_auto_check_fails_when_source_struct_is_missing` | Falha depois de `_extract`; risco aceito na v0.1 | Protegido com limitacao |
| Coluna extra em source strict gera warning | Sim | `docs/v0.1-contract.md` | `validate_schema` | `tests/test_auto_contract.py::test_auto_check_warns_for_extra_source_column_in_strict_mode` | Nenhuma | Protegido |
| `target_struct` automatico | Sim | `README.md`, `docs/v0.1-contract.md` | `auto_validate_target`, `validate_struct` | `tests/test_auto_contract.py::test_auto_validate_fails_when_target_column_is_missing` | Nenhuma | Protegido |
| Tipo alvo incompativel falha | Sim | `docs/v0.1-contract.md` | `validate_struct` | `tests/test_auto_contract.py::test_auto_validate_fails_on_target_type_mismatch` | Nenhuma | Protegido |
| Check declarativo com `severity=error` bloqueia `load` | Sim | `docs/v0.1-contract.md` | `validate_struct`, `assert_no_invalid_records` | `tests/test_auto_contract.py::test_auto_validate_blocks_invalid_target_record_before_load` | Nenhuma | Protegido |
| `auto_validate` usa verificacao limitada e nao expoe dados | Sim | `README.md`, `docs/v0.1-contract.md` | `assert_no_invalid_records` | `tests/test_auto_contract.py::test_auto_validate_uses_limited_count_without_exposing_records` | Benchmark de volume e por pipeline | Protegido para regressao |
| `is_valid = NULL` e tratado como invalido | Sim | `docs/v0.1-contract.md` | `assert_no_invalid_records` | `tests/test_auto_contract.py::test_auto_validate_treats_existing_is_valid_null_as_invalid` | Nenhuma | Protegido |
| `nullable=False` nao bloqueia nulos sozinho | Sim, como limitacao | `promessas.md`, `docs/v0.1-contract.md` | `validate_struct` | `tests/test_auto_contract.py::test_nullable_false_does_not_block_null_without_explicit_check` | Nenhuma | Protegido |
| Nulos exigem check SQL explicito para bloquear | Sim | `docs/v0.1-contract.md`, `QUICK_START.md` | `validate_struct`, `auto_validate_target` | `tests/test_auto_contract.py::test_nullable_false_requires_explicit_sql_check_to_block_null` | Nenhuma | Protegido |
| `dry_run` pula `_load` e `_certify` | Sim | `docs/v0.1-contract.md` | `Load.run` | `tests/test_dry_run.py::test_dry_run_limits_after_check_and_skips_load` | Leitura antes do limite pode ser cara | Protegido com limitacao |
| `dry_run_show_rows=0` nao chama `show` | Sim | `docs/v0.1-contract.md` | `Load._run_dry_run` | `tests/test_dry_run.py::test_show_is_not_called_in_dry_run_when_show_rows_is_zero` | Nenhuma | Protegido |
| `dry_run_show_rows>0` chama `show` explicitamente | Sim | `docs/v0.1-contract.md` | `Load._run_dry_run` | `tests/test_dry_run.py::test_show_is_called_only_for_dry_run_with_positive_show_rows` | Pode expor dados sensiveis | Protegido com limitacao |
| Logs de sucesso carregam `run_id` e stage | Sim | `docs/v0.1-contract.md`, `docs/audits/reports/v0_1_log_evidence.md` | `log_event`, `stage` | `tests/test_pipeline_contract.py::test_pipeline_run_emits_required_log_events_on_success` | Observabilidade externa fora do escopo | Protegido |
| Logs de falha carregam `run_id` e summary | Sim | `docs/v0.1-contract.md`, `docs/audits/reports/v0_1_log_evidence.md` | `log_event`, `stage` | `tests/test_pipeline_contract.py::test_pipeline_run_emits_failure_log_event_with_context` | Nenhuma para log minimo | Protegido |
| Falha de `certify` nao vira `load_failed` | Sim | `docs/v0.1-contract.md`, `docs/audits/reports/v0_1_log_evidence.md` | `Load._run_certify` | `tests/test_pipeline_contract.py::test_certify_failure_is_not_logged_as_load_failure` | Nenhuma | Protegido |
| Erros gerenciados por etapa | Sim | `docs/v0.1-contract.md` | `etl_framework.infra.errors`, `stage` | `tests/test_pipeline_contract.py::test_stage_failures_are_wrapped_with_context` | Nenhuma | Protegido |
| `Load._load` recebe `is_valid` | Sim | `README.md`, `docs/v0.1-contract.md` | `auto_validate_target`, `Load.run` | `tests/test_pipeline_contract.py::test_run_returns_transformed_dataframe_after_success` | Destino real deve projetar se necessario | Protegido com limitacao |
| Nao expor payload sensivel obvio em erro | Sim | `docs/v0.1-contract.md`, `docs/audits/reports/v0_1_log_evidence.md` | `sanitize_error_message`, `log_event` | `tests/test_logger.py::test_log_event_redacts_sensitive_error_fields` | Nao substitui mascaramento externo | Protegido |
| Gate contratual v0.1 separado de exemplos futuros | Sim, governanca | `docs/development/testing.md` | marcador pytest `v02_example` | `tests/test_safe_load_contract.py` marcado como `v02_example` | Exemplos ainda rodam no gate completo | Separado |
| Safe load/idempotencia/rollback no core | Nao | `promessas.md`, `docs/v0.1-known-limitations.md`, ADR 0001 | Nao implementado no pacote | `tests/test_safe_load_contract.py` apenas como exemplo v0.2 | Pipeline real precisa checklist | Fora do escopo |
| Certificacao real lendo destino no core | Nao | `promessas.md`, `docs/v0.1-contract.md` | Responsabilidade de `Load._certify` concreto | `tests/test_safe_load_contract.py::test_safe_load_certification_reads_destination` marcado v0.2 | Nao e contrato v0.1 | Fora do escopo |
| Baixo custo irrestrito em grandes volumes | Nao | `docs/operation/spark-cost-and-benchmark.md` | Helpers Spark explicitos | Plano de benchmark documentado | Benchmark real por pipeline ainda ausente | Risco residual |
| Compatibilidade fora de Python 3.11/Java 17 | Nao para gate oficial v0.1 | `README.md`, `docs/development/testing.md` | CI atual | Gate local/CI em Python 3.11 | `pyproject.toml` permanece permissivo para instalacao | Risco aceito |
