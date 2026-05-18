# Matriz Requisito Para Teste v0.1

| Requisito | Promessa publica? | Arquivo de teste | Nome do teste | Tipo | Protege regressao? | Status |
| --------- | ----------------- | ---------------- | ------------- | ---- | ------------------ | ------ |
| Ordem oficial `extract -> check -> transform -> validate -> load -> certify` | Sim | `tests/test_pipeline_contract.py` | `test_run_executes_official_order` | contrato | Sim | protegido |
| `Extract.run` usa Template Method com `_run_extract` e `_run_check` | Sim | `tests/test_stage_contract_template_method.py` | `test_extract_run_should_execute_extract_then_source_struct_check_automatically` | contrato | Sim | protegido |
| `Transform.run` usa Template Method com `_run_transform` e `_run_validate` | Sim | `tests/test_stage_contract_template_method.py` | `test_transform_run_should_execute_transform_then_target_struct_validation_automatically` | contrato | Sim | protegido |
| `Load.run` e dono de `dry_run` e nao chama load/certify real | Sim | `tests/test_stage_contract_template_method.py` | `test_load_run_should_skip_real_load_when_dry_run_is_enabled` | contrato | Sim | protegido |
| `Load.run` valida entrada como `DataFrame` | Sim | `tests/test_stage_contract_template_method.py` | `test_load_run_should_raise_managed_error_when_input_is_not_dataframe` | regressao | Sim | protegido |
| Integracao minima com Spark local | Sim | `tests/test_framework_integration.py` | `test_inline_framework_integration_runs_full_contract` | integracao minima | Sim | protegido |
| `source_struct` automatico | Sim | `tests/test_auto_contract.py` | `test_auto_check_fails_when_source_column_is_missing` | regressao | Sim | protegido |
| `source_struct` ausente falha | Sim | `tests/test_auto_contract.py` | `test_auto_check_fails_when_source_struct_is_missing` | contrato | Sim | protegido |
| Coluna extra em source strict gera warning | Sim | `tests/test_auto_contract.py` | `test_auto_check_warns_for_extra_source_column_in_strict_mode` | regressao | Sim | protegido |
| `target_struct` automatico | Sim | `tests/test_auto_contract.py` | `test_auto_validate_fails_when_target_column_is_missing` | regressao | Sim | protegido |
| Tipo alvo incompativel falha | Sim | `tests/test_auto_contract.py` | `test_auto_validate_fails_on_target_type_mismatch` | regressao | Sim | protegido |
| Check declarativo com `severity=error` bloqueia `load` | Sim | `tests/test_auto_contract.py` | `test_auto_validate_blocks_invalid_target_record_before_load` | integracao minima | Sim | protegido |
| `is_valid = NULL` e tratado como invalido | Sim | `tests/test_auto_contract.py` | `test_auto_validate_treats_existing_is_valid_null_as_invalid` | regressao | Sim | protegido |
| `dry_run` pula `_load` e `_certify` | Sim | `tests/test_dry_run.py` | `test_dry_run_limits_after_check_and_skips_load` | contrato | Sim | protegido |
| `dry_run_show_rows=0` nao chama `show` | Sim | `tests/test_dry_run.py` | `test_show_is_not_called_in_dry_run_when_show_rows_is_zero` | regressao | Sim | protegido |
| `dry_run_show_rows>0` chama `show` explicitamente | Sim | `tests/test_dry_run.py` | `test_show_is_called_only_for_dry_run_with_positive_show_rows` | contrato | Sim | protegido |
| Logs de sucesso carregam `run_id` e stage | Sim | `tests/test_pipeline_contract.py` | `test_pipeline_run_emits_required_log_events_on_success` | contrato | Sim | protegido |
| Logs de falha carregam `run_id` e summary | Sim | `tests/test_pipeline_contract.py` | `test_pipeline_run_emits_failure_log_event_with_context` | regressao | Sim | protegido |
| Falha de `certify` nao vira `load_failed` | Sim | `tests/test_pipeline_contract.py` | `test_certify_failure_is_not_logged_as_load_failure` | regressao | Sim | protegido |
| Erros gerenciados por etapa | Sim | `tests/test_pipeline_contract.py` | `test_stage_failures_are_wrapped_with_context` | contrato | Sim | protegido |
| `Load._load` recebe `is_valid` | Sim | `tests/test_pipeline_contract.py` | `test_run_returns_transformed_dataframe_after_success` | integracao minima | Sim | protegido |
| Nao expor payload sensivel obvio em erro | Sim | `tests/test_logger.py` | `test_log_event_redacts_sensitive_error_fields` | regressao | Sim | protegido |
| Safe load/idempotencia/rollback no core | Nao | `tests/test_safe_load_contract.py` | exemplos de pipeline concreta | documentacao | Nao e promessa v0.1 | promessa removida |
| Certificacao real lendo destino no core | Nao | `tests/test_safe_load_contract.py` | `test_safe_load_certification_reads_destination` | documentacao | Nao e promessa v0.1 | promessa removida |
| Baixo custo irrestrito em grandes volumes | Nao | N/A | N/A | inexistente | Nao | limitacao documentada |
