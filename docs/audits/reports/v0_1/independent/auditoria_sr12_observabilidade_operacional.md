# Auditoria SR12 - Observabilidade Operacional Best-Effort

## Escopo e metodo

Papel executado: especialista em observabilidade operacional de pipelines de dados.

Arquivos avaliados:

- `etl_framework/infra/observability.py`
- `etl_framework/utils/observability_events.py`
- `etl_framework/utils/stage_metadata.py`
- `etl_framework/models/context.py`
- `etl_framework/infra/stage.py`
- `etl_framework/contracts/pipeline.py`
- `etl_framework/contracts/extract.py`
- `etl_framework/contracts/transform.py`
- `etl_framework/contracts/load.py`
- `tests/test_observability.py`
- `tests/test_stage.py`
- `tests/test_pipeline_contract.py`
- `tests/test_pipeline_author_journey.py`
- `docs/observability/logging.md`
- `docs/v0.1-contract.md`
- `docs/v0.1-known-limitations.md`

Restricao obedecida: nao foram lidos relatorios de outras auditorias nem arquivos em `docs/audits/reports/v0_1/independent`.

Verificacao local: `pytest tests/test_observability.py tests/test_stage.py -q` iniciou e exibiu 14 testes passando (`..............`), mas estourou timeout de 120s antes de concluir. Resultado usado apenas como evidencia parcial, nao como validacao completa.

## Veredito

Status: aprovado com ressalvas operacionais severas para v0.1.

A promessa central da v0.1 e verdadeira no codigo: eventos sao emitidos pelo runtime, contem `run_id`, `pipeline_name`, etapa, status e timestamps, e falha do sink nao quebra a ETL. A implementacao tambem evita materializar DataFrames apenas para observar, o que e correto para custo operacional.

O problema e que a observabilidade ainda e principalmente uma trilha de controle de fluxo. Ela ajuda a localizar a etapa e distinguir sucesso/falha, mas nao entrega diagnostico operacional suficiente para investigacao de volume, causa raiz de negocio, reprocessamento, destino efetivamente escrito ou perda de evento. Isso e coerente com o escopo best-effort, mas cria risco real de leitura excessivamente otimista dos logs.

## Respostas obrigatorias

- Os eventos possuem campos prometidos? Sim para campos canonicos construidos por `build_observability_event`: `event_schema_version`, `event`, `pipeline_name`, `run_id`, `started_at`, `event_at`, `mode`, destino declarado e `write_mode`. `stage`, `status`, `level` e `elapsed_ms` dependem dos decorators/servico, nao do builder sozinho.
- O sink best-effort e seguro e previsivel? Seguro para a ETL, pois excecoes do sink sao engolidas. Pouco previsivel operacionalmente, pois a perda do evento e silenciosa e nao ha contador local, fallback ou ultimo erro observavel.
- Falha de observabilidade nao quebra a ETL conforme prometido? Sim. `ObservabilityService.emit` captura `Exception` e retorna `None`; testes cobrem sink falhando sem falhar etapa.
- Os eventos ajudam a identificar etapa e causa tecnica? Parcialmente. A etapa e clara; em falha ha `error_type` e `error_message` sanitizado. Em sucesso, os eventos nao carregam causa tecnica, volume, input/output, politica aplicada ou contexto de decisao alem de extras pontuais.
- Existe risco de log inutil, duplicado ou enganoso? Sim. Em falha de etapa interna, ha evento de falha da etapa e `run_failed`, ambos com mesmo erro; isso e util para agregacao, mas pode ser contado duas vezes. Em dry-run ha `dry_run_evidence` e `dry_run_load_completed`, o que pode ser lido como carga concluida se o consumidor olhar so eventos de load.
- Existem pontos cegos operacionais relevantes para v0.1? Sim: ausencia de garantia de entrega, ausencia de auditoria de perda de evento, metricas dependem de preenchimento manual, destino declarado nao prova escrita, certificacao default nao prova nada, e nao ha correlacao externa alem de `run_id`.

## Matriz evento -> campo -> evidencia

| Evento / origem | Campos presentes | Campos ausentes ou condicionais | Evidencia |
| --- | --- | --- | --- |
| Qualquer payload via `build_observability_event` | `event_schema_version`, `event`, `pipeline_name`, `run_id`, `started_at`, `event_at`, `mode`, `target_schema`, `target_table`, `target_path`, `target`, `write_mode` | `stage`, `status`, `level`, `elapsed_ms`, `error_type`, `error_message` so aparecem via extras/servico | `etl_framework/utils/observability_events.py:27` monta campos canonicos; `tests/test_observability.py:67` valida subconjunto obrigatorio |
| `*_started` via `stage` | campos canonicos, `level=info`, `stage`, `status=started` | sem `elapsed_ms`; sem metricas se `context.metrics` vazio | `etl_framework/infra/stage.py:48` chama `stage_started`; `etl_framework/infra/observability.py:134` usa `_stage_payload(stage, "started")`; `tests/test_stage.py:100` espera `extract_started` |
| `*_succeeded` via `stage` | campos canonicos, `level=info`, `stage`, `status=succeeded`, `elapsed_ms` | sem volume, sem contagem, sem detalhes do resultado | `etl_framework/infra/stage.py:90` calcula duracao; `etl_framework/infra/observability.py:150` inclui `elapsed_ms`; `tests/test_stage.py:109` verifica `elapsed_ms` |
| `*_failed` via `stage` | campos canonicos, `level=error`, `stage`, `status=failed`, `elapsed_ms`, `error_type`, `error_message` sanitizado | sem stack trace, sem causa estruturada alem de tipo/mensagem; sem flag de erro gerenciado vs original | `etl_framework/infra/stage.py:58` embrulha erro; `etl_framework/infra/observability.py:200` emite falha; `tests/test_stage.py:147` valida `validate_failed` |
| `execution_summary` | campos canonicos, `stage=run`, `status=succeeded/failed`, `elapsed_ms`; em falha inclui erro | so existe quando `emit_summary=True`; atualmente aplicado a `Pipeline.run` | `etl_framework/contracts/pipeline.py:35` usa `@stage("run", emit_summary=True)`; `etl_framework/infra/stage.py:73` e `:99`; `tests/test_pipeline_contract.py:425` valida summary |
| Sequencia completa em sucesso | `run_started`, eventos de inicio/sucesso para `extract`, `check`, `transform`, `validate`, `load`, `certify`, `run_succeeded`, `execution_summary` | nao inclui metricas de linhas por default; nao prova persistencia no destino | `tests/test_pipeline_contract.py:399` lista a sequencia esperada |
| Sequencia em falha de validate | `validate_failed`, `run_failed`, `execution_summary` com `error_type=ValidateError` | pode duplicar contagem de uma mesma falha em dashboards ingenuos | `tests/test_pipeline_contract.py:498` agrega eventos `*_failed`; `:514` valida summary final |
| Dry-run load | `dry_run_evidence` com `stage=load`, `status=skipped`, `dry_run=True`, `dry_run_limit`, `dry_run_show_rows`; `dry_run_load_completed` com `status=skipped` | nome `dry_run_load_completed` pode induzir leitura incorreta se `status` for ignorado | `etl_framework/contracts/load.py:70` customiza evento/status; `:76` emite evidencia; `etl_framework/utils/stage_metadata.py:13` define metadata |
| `dry_run_sample_requested` | `stage=load`, `status=sample_requested`, `dry_run_show_rows` | emitido antes do `show`; se `show` falhar, o evento de pedido existe mesmo sem amostra concluida | `etl_framework/contracts/load.py:99` usa `timing="started"`; `etl_framework/infra/stage.py:159` emite antes da funcao |
| Metricas explicitas | `metrics` aparece quando `context.metrics` ou extra `metrics` contem escalares serializaveis | nao ha computo automatico de linhas, invalidos, escritos, bytes, particoes ou freshness | `etl_framework/utils/observability_events.py:23` combina metricas; `tests/test_observability.py:236` valida metricas explicitas |
| Sink customizado | recebe dict do evento com `level` default se ausente | falha e descartada silenciosamente; sem retry, DLQ, contador ou log secundario | `etl_framework/infra/observability.py:63` captura tudo; `tests/test_observability.py:123` valida tolerancia a falha |
| Stdout/default logging | logger nomeado por `pipeline_name`, nivel resolvido por `level`, mensagem e o dict | nao e formatter JSON oficial; depende de configuracao logging da aplicacao/processo | `etl_framework/infra/observability.py:259`; limites declarados em `docs/v0.1-known-limitations.md` |

## Campos presentes e ausentes

Campos efetivamente fortes:

- `run_id`: criado e validado como string nao vazia em `EtlExecutionContext`; propagado ao payload.
- `pipeline_name`: vem de `EtlRunConfig` e nomeia tambem o logger default.
- `stage` e `status`: aplicados pelos decorators de stage e runtime_event.
- `event_at` e `started_at`: timestamps UTC/ISO suficientes para ordenacao basica.
- `elapsed_ms`: presente em sucesso/falha de stage; ausente em inicio e em runtime_event simples.
- `error_type` e `error_message`: presentes em falha, com sanitizacao de campos sensiveis conhecidos.
- `metrics`: somente quando explicitamente preenchido.

Campos ausentes relevantes:

- `attempt`, `retry_count`, `parent_run_id`, `correlation_id` externo.
- `source`, `input_path`, `source_table` ou identificador da origem.
- `rows_read`, `rows_valid`, `rows_invalid`, `rows_written` automaticos.
- `spark_application_id`, job/stage/task ids, cluster ou executor.
- `sink_emit_failed`, `sink_name`, `event_delivery_status`.
- `schema_policy`, `strict_schema`, `extra_columns_policy` nos eventos de check/validate.
- `technical_columns_kept`, `dry_run_show_rows_executed`, `load_target_effective`.

## Pontos cegos operacionais

1. Perda silenciosa de evento. O comportamento best-effort e correto para nao quebrar ETL, mas a implementacao engole qualquer excecao do sink sem rastro. Em incidente, o operador nao consegue distinguir "pipeline nao emitiu" de "sink caiu".

2. Metricas nao sao garantidas. O builder inclui `metrics` somente se `context.metrics` ou extras forem preenchidos. Isso evita acoes Spark caras, mas deixa investigacao de volume dependente de disciplina da pipeline.

3. Destino declarado nao e destino comprovado. `target_schema`, `target_table`, `target_path` e `target` sao configuracao. Eles nao provam que `_load` escreveu exatamente ali, nem que a certificacao leu o destino.

4. Certificacao pode ser vazia. `Load._certify` default retorna `None`; mesmo assim `certify_succeeded` pode ser emitido, pois sucesso significa que o hook terminou, nao que houve verificacao real de persistencia.

5. Causa tecnica e curta. `error_type` e mensagem sanitizada ajudam, mas sem stack resumido, stage interno de auto_quality, coluna afetada estruturada ou regra quebrada em campo proprio, a investigacao depende do texto do erro.

6. Dry-run pode parecer carga concluida. O evento `dry_run_load_completed` tem `status=skipped`, mas consumidores que agrupam por nome podem interpretar incorretamente como load feito.

7. Eventos de runtime_event nao tem duracao. Evidencias como `dry_run_evidence` e `dry_run_sample_requested` mostram decisao operacional, mas nao duracao ou conclusao especifica da acao secundaria.

8. Ausencia de identidade Spark. Sem `spark.application.id` ou identificador do job, a ponte entre log do framework e logs Spark/cluster e manual.

## Riscos de falsa observabilidade

- Falso senso de auditoria: os logs sao uteis para debug, mas nao entregam trilha regulatoria, entrega garantida ou replay. A documentacao declara isso, mas o payload estruturado pode parecer mais forte do que e.
- Falso sucesso de destino: `load_succeeded` e `certify_succeeded` significam que metodos retornaram sem excecao, nao que houve commit atomico, rollback possivel, idempotencia ou leitura real do destino.
- Falsa completude de metricas: a presenca opcional de `metrics` pode levar dashboards a comparar runs com granularidades diferentes.
- Dupla contagem de falhas: uma falha em `validate` gera `validate_failed`, `run_failed` e summary com falha. Isso e bom para rastreabilidade, mas ruim para alertas que contam eventos sem deduplicar por `run_id` e erro.
- Falso diagnostico por mensagem sanitizada: sanitizacao e correta, mas pode remover detalhes uteis como caminhos e tokens de correlacao; sem campos estruturados alternativos, a mensagem pode ficar pouco acionavel.
- Falso contrato de JSON: os eventos sao dicts no logger default, mas a v0.1 declara explicitamente que nao ha formatter JSON oficial.

## Aderencia ao escopo v0.1

Adequado:

- Nao promete observabilidade externa.
- Nao falha a ETL por falha do sink.
- Mantem API publica pequena.
- Centraliza emissao no runtime.
- Evita executar acoes Spark para observar resultado.
- Documenta fora de escopo: OpenTelemetry, vendors, entrega garantida, replay, strict mode e formatter JSON oficial.

Fragil:

- O nivel "best-effort" protege o processamento, mas elimina visibilidade sobre perda de observabilidade.
- Os nomes de eventos sao legiveis, mas ainda nao ha esquema operacional suficiente para dashboards confiaveis sem convencoes adicionais.
- O diagnostico de causa depende demais de erro textual.

## Oportunidades

### P1

1. Adicionar mecanismo minimo de autodiagnostico do sink sem quebrar ETL: contador em memoria, ultimo erro sanitizado ou log local em logger interno com rate limit. Nao deve virar entrega garantida; deve apenas tornar perda visivel.

2. Renomear ou documentar com mais dureza `dry_run_load_completed`. Sugestao: preferir `dry_run_load_skipped` para reduzir ambiguidade operacional.

3. Separar semanticamente `certify_succeeded` de certificacao real. Quando `_certify` default nao faz nada, emitir extra como `certification_type="default_noop"` ou `certification_performed=False`.

4. Incluir `extra_columns_policy`, `strict_schema` e `keep_technical_columns` nos eventos relevantes. Sao decisoes operacionais que explicam por que uma etapa passou, alertou ou falhou.

### P2

1. Padronizar metricas recomendadas, mantendo preenchimento explicito: `rows_read`, `rows_valid`, `rows_invalid`, `rows_written`, `rows_rejected`, `metric_missing_reason`.

2. Adicionar `spark_application_id` quando disponivel sem acao Spark. Isso melhora correlacao com logs de cluster sem custo material.

3. Criar helper de assertion de contrato de evento por tipo, para reduzir risco de regressao em campos por etapa.

4. Acrescentar teste de sink falhando durante `Pipeline.run()` completo, nao apenas stage isolado e `ObservabilityService.emit`.

5. Adicionar teste especifico garantindo que `dry_run_evidence` ocorre antes/depois conforme semantica desejada e que falha em `show` gera evento de falha coerente.

### P3

1. Definir tabela documentada de nomes de eventos, status validos e significado operacional.

2. Adicionar `event_id` local para deduplicacao simples em consumidores.

3. Expor exemplo de sink que serializa JSON line a partir do dict, deixando claro que e exemplo, nao contrato oficial.

4. Documentar estrategia de agregacao: contar falhas por `run_id` e `stage`, nao por quantidade bruta de eventos `*_failed`.

## Conclusao

A v0.1 cumpre o contrato minimo de observabilidade operacional best-effort: eventos existem, carregam rastreabilidade basica, nao quebram a ETL quando o sink falha e sao suficientemente baratos para o runtime. Contudo, a utilidade para troubleshooting ainda e limitada. A severidade principal nao esta em bugs de emissao, mas em ambiguidade operacional: sucesso de hook pode ser confundido com sucesso de destino, ausencia de evento pode ser confundida com ausencia de execucao, e metricas opcionais podem ser confundidas com cobertura operacional.

Para v0.1, isso e aceitavel somente se a documentacao continuar explicita de que os eventos ajudam debug, mas nao sao observabilidade externa, auditoria regulatoria, garantia de entrega nem prova de carga produtiva.
