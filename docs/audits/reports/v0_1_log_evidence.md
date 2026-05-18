# Evidencias De Logs E Erros v0.1

Estas evidencias sao exemplos sanitizados do comportamento coberto por testes
de contrato. Elas validam logs tecnicos minimos do framework, nao uma
plataforma externa de observabilidade.

## Sucesso

Teste de referencia:
`tests/test_pipeline_contract.py::test_pipeline_run_emits_required_log_events_on_success`

Campos obrigatorios observados em todos os eventos:

```text
event_schema_version=1.0
pipeline_name=orders_pipeline_log_success
run_id=run-pipeline-contract
stage=<run|extract|check|transform|validate|load|certify>
status=<started|succeeded>
mode=prod
```

Sequencia esperada:

```text
run_started
extract_started
extract_succeeded
check_started
check_succeeded
transform_started
transform_succeeded
validate_started
validate_succeeded
load_started
load_succeeded
certify_started
certify_succeeded
run_succeeded
execution_summary
```

## Falha De Validacao

Teste de referencia:
`tests/test_pipeline_contract.py::test_pipeline_run_emits_failure_log_event_with_context`

Eventos de falha esperados:

```text
validate_failed: stage=validate, status=failed, error_type=ValidateError
run_failed: stage=run, status=failed, error_type=ValidateError
execution_summary: stage=run, status=failed, error_type=ValidateError
```

O teste tambem valida `pipeline_name` e `run_id` em todos os payloads.

## Falha De Certificacao

Teste de referencia:
`tests/test_pipeline_contract.py::test_certify_failure_is_not_logged_as_load_failure`

Eventos de falha esperados:

```text
load_succeeded
certify_failed: stage=certify, status=failed, error_type=CertifyError
run_failed: stage=run, status=failed, error_type=CertifyError
execution_summary: stage=run, status=failed, error_type=CertifyError
```

O teste valida que `certify_failed` nao e registrado como `load_failed`.

## Sanitizacao

Teste de referencia:
`tests/test_logger.py::test_log_event_redacts_sensitive_error_fields`

Campos de erro conhecidos sao sanitizados antes de ir para o payload de log.
Isso reduz exposicao acidental de tokens, senhas e segredos, mas nao substitui
politica externa de observabilidade ou mascaramento de dados da pipeline.
