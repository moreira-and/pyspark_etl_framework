# Auditoria SR08 - Magia, Indirecao e Comportamento Escondido

## Escopo e metodo

Auditoria independente executada sobre `etl_framework/contracts/`, `etl_framework/infra/`, `etl_framework/utils/` e testes de fluxo, erros, dry-run e observabilidade. Nao foram lidos relatorios de outras auditorias nem arquivos em `docs/audits/reports/v0_1/independent`.

Papel aplicado: especialista em magia, indirecao e carga cognitiva. Criterio: comportamento relevante que acontece sem chamada explicita do autor da pipeline, especialmente quando altera dados, erros, logs, metricas, contexto, execucao Spark ou ordem do pipeline.

## Sumario executivo

O framework v0.1 usa bastante indirecao deliberada: template methods nos contratos, decorators para observabilidade/erros e helpers de validacao declarativa. Parte disso e aceitavel porque reduz codigo repetitivo do autor da pipeline e os testes documentam a ordem oficial. O risco principal nao e a existencia de template method; e a combinacao de side effects automaticos com falhas silenciosas em observabilidade/metadados e a presenca de acoes Spark dentro de validacao chamada como etapa "automatica".

Classificacao geral: **fragil**. Ha um achado **perigoso**: eventos operacionais podem ser perdidos silenciosamente, inclusive por falha de sink ou falha de construcao de metadata extra, sem qualquer sinal secundario para o operador.

## Inventario de comportamentos escondidos

| ID | Comportamento escondido | Classificacao |
| --- | --- | --- |
| SR08-01 | `@stage` emite eventos, mede tempo e converte excecoes automaticamente | aceitavel com fragilidade |
| SR08-02 | `@runtime_event` emite eventos ao redor de chamadas e engole falha de metadata extra | perigoso |
| SR08-03 | Contratos `Extract`/`Transform` executam check/validate automaticos apos hooks do autor | aceitavel |
| SR08-04 | `auto_validate_target` adiciona `is_valid` e executa acoes Spark para bloquear invalidos | fragil |
| SR08-05 | `Load.run` troca load real por dry-run e pula certify sem chamada explicita no autor | aceitavel com fragilidade |
| SR08-06 | `_persistable_df` remove colunas tecnicas antes de load/certify por configuracao | fragil |
| SR08-07 | Observabilidade global e best-effort descarta eventos em falha de sink | perigoso |
| SR08-08 | `build_observability_event` mistura metricas do contexto com metricas extras automaticamente | fragil |
| SR08-09 | Preflight duplica validacoes de configuracao e falha antes de extract | aceitavel |

## Achados

### SR08-01 - Decorator `@stage` controla observabilidade, tempo e wrapping de erro

Classificacao: **aceitavel com fragilidade**.

Definicao: `etl_framework/infra/stage.py:27` define `stage`; o wrapper resolve `config`/`context`, emite `stage_started` em `stage.py:50`, captura excecoes em `stage.py:58`, chama `stage_failed` em `stage.py:66`, gera `execution_summary` quando habilitado em `stage.py:73` e emite sucesso em `stage.py:91`.

Acionamento: aplicado em `Pipeline.run` (`etl_framework/contracts/pipeline.py:44`), `Extract._run_extract` e `_run_check` (`extract.py:54`, `extract.py:66`), `Transform._run_transform` e `_run_validate` (`transform.py:47`, `transform.py:60`), `Load._run_load`, `_run_dry_run` e `_run_certify` (`load.py:57`, `load.py:70`, `load.py:116`).

Efeito observavel: a pipeline emite eventos sem que componentes concretos chamem logger. Os testes exigem eventos de sucesso `run_started` ate `execution_summary` em `tests/test_pipeline_contract.py:382` e wrapping de falhas por etapa em `tests/test_pipeline_contract.py:791`.

Beneficio pretendido: padroniza trace, duracao e familia de erros sem poluir codigo do autor da pipeline.

Custo cognitivo/operacional: o ponto real de observabilidade e erro nao esta no metodo chamado pelo autor, mas em decorator externo. Um junior que le apenas `Pipeline.run` ve `self.extract(); self.transform(); self.load()`, mas nao ve medicao de tempo, eventos, wrapping de excecao nem summary. A fragilidade aumenta porque `_resolve_config`/`_resolve_context` dependem implicitamente de `self.config`/`self.context` ou kwargs (`stage.py:226`, `stage.py:238`).

### SR08-02 - `@runtime_event` oculta falha de metadata extra

Classificacao: **perigoso**.

Definicao: `etl_framework/infra/stage.py:139` define `runtime_event`; `_resolve_extra` executa callable de metadata e, em qualquer excecao, retorna `{}` em `stage.py:212` a `stage.py:223`.

Acionamento: `Extract._limit_dry_run_extract` usa metadata de dry-run em `etl_framework/contracts/extract.py:85`; `Load._run_dry_run` usa `dry_run_evidence_metadata` em `etl_framework/contracts/load.py:76`; `_show_dry_run_sample` usa metadata em `load.py:99`.

Efeito observavel: se `dry_run_evidence_metadata(config)` ou outro callable quebrar, o evento ainda e emitido sem os campos extras esperados. Nao ha erro, warning ou metric de perda. O teste valida a presenca de `dry_run_limit`/`dry_run_show_rows` no caminho feliz em `tests/test_stage_contract_template_method.py:299`, mas nao cobre falha do builder.

Beneficio pretendido: observabilidade runtime nao deve derrubar ETL por falha auxiliar.

Custo cognitivo/operacional: falha de instrumentacao vira perda silenciosa de evidencia. Para auditoria e operacao, isso e pior que um evento degradado explicito: o operador recebe um evento aparentemente valido, mas incompleto.

### SR08-03 - Template methods executam check/validate automaticos

Classificacao: **aceitavel**.

Definicao: `Extract.run` chama `_run_extract`, depois `_run_check`, e aplica limit em dry-run (`etl_framework/contracts/extract.py:27` a `extract.py:49`). `Transform.run` chama `_run_transform`, depois `_run_validate` (`etl_framework/contracts/transform.py:26` a `transform.py:39`).

Acionamento: `Pipeline.extract` chama `extract_step.run` (`etl_framework/contracts/pipeline.py:86`); `Pipeline.transform` chama `transform_step.run` (`pipeline.py:94`). O autor implementa `_extract`/`_transform`, mas as etapas `check` e `validate` rodam mesmo sem chamada explicita no codigo concreto.

Efeito observavel: testes confirmam `Extract.run` executando `extract` e depois `custom_check`, com eventos `extract_started`, `extract_succeeded`, `check_started`, `check_succeeded` (`tests/test_stage_contract_template_method.py:181` a `test_stage_contract_template_method.py:199`). Para transform, o resultado ganha `is_valid` automaticamente e eventos de validate (`tests/test_stage_contract_template_method.py:219` a `test_stage_contract_template_method.py:245`).

Beneficio pretendido: garante ordem oficial e validacao minima para pipelines de v0.1; o teste de jornada enfatiza que o autor escreve apenas ETL e recebe garantias "for free" (`tests/test_pipeline_author_journey.py:220` a `test_pipeline_author_journey.py:245`).

Custo cognitivo/operacional: ha comportamento relevante fora dos hooks do autor. Neste caso o custo e aceitavel porque os nomes `_run_check`, `_run_validate`, `_custom_check` e `_custom_validate` explicitam o contrato. Ainda assim, junior precisa aprender que `_extract` nunca e "so extract" no fluxo oficial; ela sempre sera seguida por check estrutural.

### SR08-04 - Validacao automatica adiciona coluna e executa Spark actions

Classificacao: **fragil**.

Definicao: `auto_validate_target` em `etl_framework/utils/auto_quality.py:34` chama `validate_struct`, que adiciona `is_valid` em `etl_framework/utils/validate_struct.py:50` e `_add_is_valid_column` em `validate_struct.py:200`. Depois `auto_validate_target` chama `_raise_with_validation_diagnostics`, que executa `invalid_df.limit(1).count()` (`auto_quality.py:78`), `invalid_df.count()` (`auto_quality.py:81`) e, em caso de falha, `summarize_struct_checks`/`collect()` (`auto_quality.py:91` a `auto_quality.py:99`; `validate_struct.py:305`).

Acionamento: todo `Transform.run` aciona `_run_validate` (`etl_framework/contracts/transform.py:60`), que chama `auto_validate_target` (`transform.py:64`).

Efeito observavel: `Transform.run` retorna DataFrame com coluna tecnica `is_valid` sem o autor adiciona-la. Teste confirma colunas `["id", "name_upper", "is_valid"]` em `tests/test_stage_contract_template_method.py:236`. Quando ha invalido, a validacao pode disparar multiplas acoes Spark antes do load.

Beneficio pretendido: bloquear registros invalidos antes de escrita e produzir diagnostico com contagem/checks falhos.

Custo cognitivo/operacional: a documentacao de `stage` promete que o decorator nao materializa DataFrames, mas a etapa automatica de validate pode materializar. O teste `test_pipeline_normal_mode_does_not_trigger_show_or_collect` (`tests/test_pipeline_contract.py:571`) nao bloqueia `count`, e o proprio comentario admite que auto validate pode executar uma pequena contagem. Para dados grandes, a carga operacional aparece dentro de uma validacao implicita, nao no codigo de negocio. Isso dificulta prever custo, reprocessamento e latencia.

### SR08-05 - Dry-run altera ordem e pula load/certify automaticamente

Classificacao: **aceitavel com fragilidade**.

Definicao: `Load.run` retorna apos `_run_dry_run` quando `config.dry_run` e verdadeiro (`etl_framework/contracts/load.py:27` a `load.py:43`). `_run_dry_run` e instrumentado como stage `load`, mas com `succeeded_event="dry_run_load_completed"` e status `skipped` (`load.py:70` a `load.py:82`). `Extract.run` tambem limita o DataFrame em dry-run apos check (`etl_framework/contracts/extract.py:45` a `extract.py:99`).

Acionamento: qualquer `Pipeline.run` com `EtlRunConfig(dry_run=True)` (`etl_framework/models/config.py:36`) aciona este caminho sem alteracao nos componentes concretos.

Efeito observavel: teste confirma que dry-run limita depois do check, antes do transform, pula load/certify e nao chama `_load` nem `_certify` (`tests/test_dry_run.py:161` a `test_dry_run.py:181`). Outro teste confirma que `events == []` no `Load` concreto e os eventos sao `load_started`, `dry_run_evidence`, `dry_run_load_completed` (`tests/test_stage_contract_template_method.py:270` a `test_stage_contract_template_method.py:302`).

Beneficio pretendido: prevenir escrita acidental e preservar validacao em modo de ensaio.

Custo cognitivo/operacional: o evento usa stage `load` mesmo quando nenhum load real acontece. O status `skipped` reduz ambiguidade, mas o nome `load_started` antes de dry-run pode confundir tracing. A ordem tambem muda volume: check ve todos os dados, transform/validate ve dados limitados. Isso e bom para seguranca, mas precisa estar muito visivel para autores e operadores.

### SR08-06 - `_persistable_df` remove colunas tecnicas por configuracao

Classificacao: **fragil**.

Definicao: `Load._persistable_df` valida DataFrame e, quando `config.keep_technical_columns` e falso, remove colunas cujo nome lower-case esta em `config.TECHNICAL_COLUMNS` (`etl_framework/contracts/load.py:129` a `load.py:140`; `etl_framework/models/config.py:41` e `config.py:52` a `config.py:60`).

Acionamento: `_run_load`, `_run_dry_run` e `_run_certify` chamam `_persistable_df` (`etl_framework/contracts/load.py:67`, `load.py:91`, `load.py:126`).

Efeito observavel: teste confirma que `is_valid` e removido antes de load/certify quando configurado (`tests/test_stage_contract_template_method.py:331` a `test_stage_contract_template_method.py:350`).

Beneficio pretendido: permitir que colunas runtime nao vazem para destino fisico.

Custo cognitivo/operacional: altera shape do DataFrame que o autor recebe no hook `_load` sem uma chamada explicita no hook. Pode quebrar certificacoes que esperam `is_valid`, e o criterio por `column.lower()` pode remover uma coluna de negocio homonima caso passe por outra via. A configuracao default `keep_technical_columns=True` reduz risco, mas o side effect e forte.

### SR08-07 - Observabilidade global best-effort descarta eventos

Classificacao: **perigoso**.

Definicao: `ObservabilityService.emit` envolve a chamada do sink em `try/except` e retorna `None` em qualquer excecao (`etl_framework/infra/observability.py:56` a `observability.py:68`). `NoOpObservabilitySink` descarta eventos (`observability.py:29` a `observability.py:34`). O servico global e process-wide (`observability.py:272`, `observability.py:304`), e `configure_observability_sink` troca o sink global (`observability.py:307` a `observability.py:309`).

Acionamento: todos os decorators de stage/runtime_event usam `get_observability_service()` indiretamente (`etl_framework/infra/stage.py:49`, `stage.py:199`). Teste confirma que sink com falha nao propaga erro (`tests/test_observability.py:123` a `test_observability.py:130`).

Efeito observavel: em falha de sink, a pipeline continua e o evento desaparece. Em execucoes paralelas no mesmo processo, troca global de sink pode afetar outra pipeline ou teste se nao houver isolamento.

Beneficio pretendido: falha de log nao derruba ETL.

Custo cognitivo/operacional: o sistema pode perder justamente eventos de falha sem contramedida. Para uma auditoria de previsibilidade, essa e a maior magia operacional: o contrato promete observabilidade runtime, mas a camada que entrega eventos e silenciosamente best-effort. Um junior nao conseguiria inferir perda de evento olhando apenas o fluxo da pipeline.

### SR08-08 - Metricas sao mescladas automaticamente no payload

Classificacao: **fragil**.

Definicao: `build_observability_event` copia `context.metrics` e, se `extra` contem `metrics`, faz `metrics.update(...)` (`etl_framework/utils/observability_events.py:15` a `observability_events.py:25`). A serializacao descarta chaves nao string e converte valores nao escalares para string (`observability_events.py:52` a `observability_events.py:64`).

Acionamento: qualquer evento emitido por `ObservabilityService._emit_event` (`etl_framework/infra/observability.py:235`) inclui essas metricas. Teste confirma merge de metricas explicitas em `tests/test_observability.py:236` e coercao de nao escalares em `tests/test_observability.py:261`.

Efeito observavel: metricas extras podem sobrescrever metricas de contexto com mesmo nome; metricas nao escalares mudam de tipo para string; chaves invalidas somem.

Beneficio pretendido: payload simples e JSON-friendly, sem acoes Spark automaticas.

Custo cognitivo/operacional: a origem de uma metrica no evento nao fica rastreavel. Em auditoria de producao, `rows_written` pode vir do contexto ou de `extra`, e a ultima origem ganha silenciosamente. Isso dificulta troubleshooting quando certificacao e load discordam.

### SR08-09 - Preflight falha antes de extract

Classificacao: **aceitavel**.

Definicao: `Pipeline.run` chama `_preflight()` antes de extract (`etl_framework/contracts/pipeline.py:45` a `pipeline.py:48`). `_preflight` valida structs obrigatorios, chaves e dry-run show rows (`pipeline.py:53` a `pipeline.py:84`).

Acionamento: todo `Pipeline.run`.

Efeito observavel: testes confirmam que falha antes de qualquer evento de negocio quando `source_struct`/`target_struct` estao ausentes ou chave e invalida (`tests/test_pipeline_contract.py:341`, `test_pipeline_contract.py:362`, `test_pipeline_contract.py:379`).

Beneficio pretendido: falhar cedo antes de ler fonte ou escrever destino.

Custo cognitivo/operacional: duplica parte da validacao de `EtlRunConfig`, inclusive target_key, mas isso e justificavel como barreira contra mutacao forcada em objeto frozen, coberta por teste. Baixo risco.

## Respostas as perguntas obrigatorias

**Que comportamento relevante acontece sem chamada explicita do autor da pipeline?** Eventos, tempos, wrapping de erro, summary de execucao, check de source, validate de target, criacao de `is_valid`, dry-run limit, skip de load/certify, remocao opcional de colunas tecnicas e merge de metricas.

**A indirecao melhora simplicidade ou dificulta tracing?** Melhora simplicidade do autor nos contratos principais, especialmente em SR08-01, SR08-03 e SR08-05. Dificulta tracing em SR08-02, SR08-04, SR08-07 e SR08-08 porque falhas ou custos aparecem fora do codigo escrito pelo autor.

**Existem decorators ou helpers que ocultam falhas?** Sim. `_resolve_extra` oculta falhas de metadata extra (`etl_framework/infra/stage.py:212` a `stage.py:223`) e `ObservabilityService.emit` oculta falhas de sink (`etl_framework/infra/observability.py:56` a `observability.py:68`).

**Existe acoplamento implicito entre modulos?** Sim. `@stage` depende de `EtlRunConfig`/`EtlExecutionContext` presentes em `self` ou kwargs; contratos dependem de `config.source_struct`, `config.target_struct`, `strict_schema`, `extra_columns_policy`, `dry_run` e `keep_technical_columns`; observabilidade depende de singleton global.

**Um junior conseguiria prever side effects?** Parcialmente. A jornada de testes mostra o objetivo "garantias for free", mas a previsao exige conhecer decorators, template methods e helpers. Um junior provavelmente preveria a ordem oficial apos ler os contratos, mas nao preveria perda silenciosa de eventos, execucoes `count()` dentro de validate automatico ou sobrescrita de metricas por `extra`.

## Oportunidades

### P1

1. Tornar falha de observabilidade degradada, nao invisivel. `ObservabilityService.emit` deve pelo menos incrementar contador interno, emitir fallback em stderr/logger raiz, ou expor hook de diagnostico quando sink falhar. Em v0.1, manter ETL vivo e aceitavel; perder evento sem rastro nao e aceitavel.
2. Remover `except Exception: return {}` de `_resolve_extra` ou registrar evento/warning `runtime_metadata_failed` com `event`, `stage`, `status` e tipo da excecao. Metadata incompleta deve ser distinguivel de metadata vazia.
3. Documentar e testar explicitamente as acoes Spark de `auto_validate_target`: quantidade esperada, condicoes de `count()`/`collect()` e custo em invalidos. Hoje o comportamento e real, mas escondido sob "automatic target validation".

### P2

1. Renomear ou complementar eventos de dry-run para reduzir ambiguidade: evitar que `load_started` pareca tentativa real de escrita quando o caminho e dry-run. Alternativa: adicionar campo obrigatorio `write_attempted=false` em `dry_run_load_completed`.
2. Tornar merge de metricas rastreavel. Exemplo: recusar colisao entre `context.metrics` e `extra["metrics"]`, ou adicionar `metrics_source` por chave relevante.
3. Explicitar no contrato de `Load._load` que o DataFrame pode ter colunas tecnicas removidas por `keep_technical_columns=False`, com teste para coluna de negocio colidente/reservada quando aplicavel.

### P3

1. Melhorar nomes/documentacao dos template methods para autores: separar em doc curta a ordem real `extract -> check -> dry-run limit -> transform -> validate -> dry-run load evidence|load -> certify`.
2. Adicionar teste de falha de callable `extra` em `@runtime_event` para fixar o comportamento desejado, seja falhar aberto com diagnostico ou propagar.
3. Adicionar teste para colisao de metricas em `build_observability_event`, porque hoje a precedencia de `extra["metrics"]` sobre `context.metrics` e comportamento implicito.

## Veredito

A arquitetura de template method e decorators e defensavel para v0.1, mas precisa de guardrails onde a magia vira perda de rastreabilidade. O framework esta no caminho certo quando a indirecao centraliza ordem, erro e observabilidade; esta errado quando "best effort" significa "silenciosamente perdido" ou quando validacao automatica esconde custo Spark. A prioridade deve ser tornar falhas auxiliares observaveis e custos automaticos explicitos, sem devolver boilerplate ao autor da pipeline.
