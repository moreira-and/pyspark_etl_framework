# Auditoria Independente SR11 - Erros Gerenciados e Rastreabilidade de Falhas

Escopo executado conforme `prompts/_v0.1/auditoria/auditoria_sr11_erros_rastreabilidade.md`. Nao foram consultados relatorios de outras auditorias nem arquivos em `docs/audits/reports/v0_1/independent`.

## Veredito

A v0.1 tem uma base boa de erros gerenciados para o fluxo oficial `Pipeline.run()`: `extract`, `check`, `transform`, `validate`, `load` e `certify` sao embrulhados por erro especifico de etapa, com `pipeline_name`, `run_id`, `stage`, tipo da causa e mensagem sanitizada. A causa raiz tambem fica preservada via atributo `cause` e via encadeamento `raise error from exc`.

Mesmo assim, a rastreabilidade ainda nao e suficiente para operacao pequena em todos os incidentes reais. As falhas de construcao de `EtlRunConfig` e `EtlExecutionContext` ocorrem antes do runtime e saem como `ValueError` generico, sem `pipeline_name`, sem `run_id`, sem etapa oficial e sem evento de falha. Alem disso, a sanitizacao remove `path=...`, `source_path=...` e `target_path=...` da mensagem de erro, o que protege segredo, mas tambem apaga evidencia operacional essencial para troubleshooting. Ha tambem erros auxiliares fora dos contratos que continuam genericos demais quando chamados diretamente.

## Matriz etapa -> falha -> erro -> contexto

| Etapa | Cenario de falha | Erro esperado | Erro real | Mensagem real | Contexto presente/ausente | Teste existente ou necessario |
| --- | --- | --- | --- | --- | --- | --- |
| `preflight` | `Pipeline.run()` com `source_struct=None` | `PreflightError` antes de `_extract` | `PreflightError` | `Preflight failed before extract: source_struct is required before extract (pipeline_name=..., run_id=..., stage=preflight)` | Presente: `pipeline_name`, `run_id`, `stage`. Ausente: N/A no fluxo orquestrado. | Coberto por `tests/test_pipeline_contract.py:322` e `tests/test_auto_contract.py:286`. |
| `preflight` | `Pipeline.run()` com `target_struct=None` | `PreflightError` antes de `_extract` | `PreflightError` | `Preflight failed before extract: target_struct is required before extract (...)` | Presente: `pipeline_name`, `run_id`, `stage`. | Coberto por `tests/test_pipeline_contract.py:345` e `tests/test_auto_contract.py:336`. |
| `config-init` | Criar `EtlRunConfig(pipeline_name="", ...)` ou `target_key` invalido | Erro gerenciado com contexto de configuracao ou preflight | `ValueError` direto em `EtlRunConfig.__post_init__` | `pipeline_name cannot be empty`, `target_key must contain only non-empty strings`, `target_key contains columns not present in target_struct: [...]` | Ausente: `run_id`, etapa oficial, erro gerenciado, evento. Parcial: alguns campos existem no objeto em construcao, mas nao sao carregados no erro. | Necessario teste novo de rastreabilidade para falha no construtor; hoje apenas ha validacao direta em `etl_framework/models/config.py:61`, `:82`, `:155`. |
| `context-init` | Criar `EtlExecutionContext(run_id="")` ou `metrics=[]` | Erro gerenciado ou erro de contexto rastreavel | `ValueError` direto | `run_id must be a non-empty string`; `metrics must be a dict` | Ausente: `pipeline_name`, etapa, evento; `run_id` invalido nao e recuperavel na mensagem. | Necessario teste novo; implementacao em `etl_framework/models/context.py:28` e `:30`. |
| `extract` | `_extract` levanta `RuntimeError("extract failed")` | `ExtractError` | `ExtractError` | `extract failed (pipeline_name=..., run_id=..., stage=extract, cause_type=RuntimeError, cause_message=extract failed)` | Presente: `pipeline_name`, `run_id`, `stage`, `cause`, `__cause__`. | Coberto por `tests/test_pipeline_contract.py:791`; wrapper em `etl_framework/contracts/extract.py:54` e `etl_framework/infra/stage.py:88`. |
| `extract` | `_extract` retorna lista em vez de `DataFrame` | `ExtractError` com causa `TypeError` | `ExtractError` | `extract must return pyspark.sql.DataFrame, got list (...)` | Presente: contexto completo; causa preservada. | Coberto por `tests/test_pipeline_contract.py:840`; validacao em `etl_framework/contracts/extract.py:64` e `etl_framework/utils/dataframe_checks.py`. |
| `check` | Coluna obrigatoria ausente no source | `CheckError` | `CheckError` | `Schema mismatch:\n  - ERROR: Missing column 'name' (...)` | Presente: contexto completo via wrapper. | Coberto por `tests/test_stage_contract_template_method.py` e `tests/test_auto_contract.py:319`; origem em `etl_framework/utils/validate_struct.py:132`. |
| `check` | `auto_check_source(df, None)` chamado diretamente fora de `Pipeline.run()` | Idealmente erro de uso com etapa/check e contexto opcional | `ValueError` direto | `source_struct is required for automatic source check in v0.1` | Ausente: `pipeline_name`, `run_id`, `stage=check`. | Necessario teste novo para uso direto do utilitario; implementacao em `etl_framework/utils/auto_quality.py:24`. |
| `transform` | `_transform` levanta `RuntimeError("transform failed")` | `TransformError` | `TransformError` | `transform failed (pipeline_name=..., run_id=..., stage=transform, cause_type=RuntimeError, cause_message=transform failed)` | Presente: contexto completo; causa preservada. | Coberto por `tests/test_pipeline_contract.py:791`; wrapper em `etl_framework/contracts/transform.py:47`. |
| `transform` | `_transform` retorna tupla em vez de `DataFrame` | `TransformError` com causa `TypeError` | `TransformError` | `transform must return pyspark.sql.DataFrame, got tuple (...)` | Presente: contexto completo. | Coberto por `tests/test_pipeline_contract.py:840`; validacao em `etl_framework/contracts/transform.py:58`. |
| `validate` | `target_struct` sem coluna retornada pelo transform | `ValidateError` | `ValidateError` | `Schema mismatch:\n  - ERROR: Missing column 'name_upper' (...)` | Presente: contexto completo. | Coberto por `tests/test_auto_contract.py:350`. |
| `validate` | Tipo incompatavel no target | `ValidateError` | `ValidateError` | `Schema mismatch:\n  - ERROR: 'name_upper': expected string, got int (...)` | Presente: contexto completo. | Coberto por `tests/test_auto_contract.py:366`. |
| `validate` | Check declarativo falha em dados invalidos | `ValidateError` | `ValidateError` | `Invalid records found during target validation: invalid_count=1; failed_checks=[name_upper.name_upper_required: failed_count=1] (...)` | Presente: contexto completo; nao expoe linhas. | Coberto por `tests/test_auto_contract.py:394`; diagnostico em `etl_framework/utils/auto_quality.py:85`. |
| `validate` | Regra SQL declarativa malformada | `ValidateError` no fluxo; `ValueError` no utilitario direto | No fluxo, `ValidateError`; direto, `ValueError` | `Invalid SQL check rule 'bad_amount_rule' for field 'amount': ... Available columns: [...] Spark error: ...` | No fluxo: contexto completo. Direto: sem `pipeline_name`, `run_id`, `stage`. | Coberto direto por `tests/test_validate_struct.py:387`; falta teste no fluxo garantindo contexto. |
| `load` | `_load` levanta `RuntimeError("load failed")` | `LoadError` | `LoadError` | `load failed (pipeline_name=..., run_id=..., stage=load, cause_type=RuntimeError, cause_message=load failed)` | Presente: contexto completo. | Coberto por `tests/test_pipeline_contract.py:791`; wrapper em `etl_framework/contracts/load.py:57`. |
| `load` dry-run | `df.show()` falha em `_show_dry_run_sample` | `LoadError` | `LoadError`, porque `_run_dry_run` esta decorado e chama `_show_dry_run_sample` | Mensagem preserva erro Spark sanitizado | Presente: contexto completo no wrapper externo. | Falta teste especifico para falha de sample em dry-run; caminho em `etl_framework/contracts/load.py:82` e `:106`. |
| `certify` | `_certify` levanta `RuntimeError("certify failed")` | `CertifyError`, nao `LoadError` | `CertifyError` | `certify failed (pipeline_name=..., run_id=..., stage=certify, cause_type=RuntimeError, cause_message=certify failed)` | Presente: contexto completo. | Coberto por `tests/test_pipeline_contract.py:440` e `:791`; wrapper em `etl_framework/contracts/load.py:116`. |
| `run` | Qualquer erro gerenciado de etapa sobe para `Pipeline.run()` | Deve preservar erro original de etapa | Preserva o mesmo erro se ja tem contexto | Mensagem da etapa original | Presente: contexto da etapa original. | Coberto por `tests/test_pipeline_contract.py:396`; preservacao em `etl_framework/infra/errors.py:98` e `etl_framework/infra/stage.py:86`. |

## Lacunas de rastreabilidade

### P1 - Falhas de configuracao e contexto antes do runtime nao sao gerenciadas

Incidente reproduzivel: um autor junior instancia `EtlRunConfig` com `pipeline_name=""`, `dry_run_show_rows=10` com `dry_run=False`, `target_key` fora do `target_struct`, ou `target_struct` contendo coluna tecnica reservada. A execucao nem chega em `Pipeline.run()`. O erro real e `ValueError` direto a partir de `etl_framework/models/config.py:61`, `:82`, `:155`, `:167` ou `:181`.

Erro esperado: um erro gerenciado de configuracao/preflight com `pipeline_name` quando disponivel, etapa operacional clara (`config` ou `preflight`), e recomendacao objetiva de correcao.

Erro real e mensagem real: `ValueError("pipeline_name cannot be empty")`, `ValueError("dry_run_show_rows can be greater than zero only when dry_run=True")`, `ValueError("target_key contains columns not present in target_struct: [...]")`.

Campos ausentes: `run_id`, `stage`, familia `EtlError`, evento de falha e causa encadeada.

Risco operacional: operador recebe um erro correto tecnicamente, mas sem vinculo ao run e sem etapa oficial. Em incidentes de bootstrap, CI ou notebook, isso quebra a promessa de "erros gerenciados por etapa" documentada em `docs/v0.1-contract.md:258`.

Teste que deveria cobrir: criar testes para `EtlRunConfig` invalido e `EtlExecutionContext` invalido exigindo erro gerenciado ou uma decisao documentada explicita de que erros de construcao estao fora da rastreabilidade operacional.

### P1 - Sanitizacao apaga caminhos operacionais que sao essenciais para debug

Incidente reproduzivel: `_extract` ou `_load` falha com mensagem `FileNotFoundError("source_path=/mnt/raw/orders/2026-05-20.csv not found")` ou `RuntimeError("target_path=/warehouse/gold/orders write denied")`. Ao construir `ExtractError` ou `LoadError`, `sanitize_error_message` substitui `source_path=...` e `target_path=...` por `<redacted>` por causa de `_PATH_RE` em `etl_framework/utils/sanitization.py:17` e uso em `etl_framework/infra/errors.py:35` e `:48`.

Erro esperado: redacao de credenciais e payload sensivel, mas preservando caminho operacional seguro ou ao menos um identificador diagnosticavel. Para dado sensivel, deveria haver politica diferenciada entre segredo, payload de registro e metadado operacional.

Erro real e mensagem real: `... cause_message=source_path=<redacted> not found` ou `... target_path=<redacted> write denied`.

Campos presentes: `pipeline_name`, `run_id`, `stage`, `cause_type`. Campos efetivamente perdidos: path de origem/destino, que muitas vezes e o unico dado necessario para corrigir permissao, particao inexistente ou parametro de ambiente errado.

Teste existente: `tests/test_errors.py:146` exige redacao de `path=/mnt/prod/customer/private.csv`. Esse teste protege segredo, mas tambem cristaliza a perda de debugabilidade.

Teste que deveria cobrir: caso separado para `source_path`/`target_path` configuracional com decisao de politica: preservar, mascarar parcialmente, ou emitir hash/alias estavel.

### P2 - Utilitarios publicos ou reutilizaveis falham com `ValueError` sem contexto quando usados fora do template method

Incidente reproduzivel: um time usa `auto_validate_target(df, target_struct)` diretamente em um notebook ou teste de componente, conforme os testes indicam que o utilitario e "reusable" em `tests/test_auto_contract.py`. Se `target_struct=None`, se ja existe `is_valid`, se a regra SQL e invalida, ou se ha registros invalidos, o erro real e `ValueError` vindo de `etl_framework/utils/auto_quality.py:43`, `:66`, `:85` ou `etl_framework/utils/validate_struct.py:225`.

Erro esperado: para API reutilizavel, a mensagem deveria aceitar contexto opcional ou expor erro de validacao estruturado com campo/coluna/check de forma programatica.

Erro real e mensagem real: `target_struct is required for automatic target validation in v0.1`, `Invalid records found during target validation: invalid_count=...`, `Invalid SQL check rule ...`.

Campos ausentes: `pipeline_name`, `run_id`, etapa, classe gerenciada. No fluxo `Pipeline.run()`, o decorator corrige isso; no uso direto, nao.

Risco operacional: o mesmo problema tem rastreabilidade boa no pipeline e pobre no utilitario. Isso confunde suporte, porque os testes e a documentacao tratam os utilitarios como reutilizaveis, mas a ergonomia de erro muda radicalmente.

Teste que deveria cobrir: `auto_validate_target(..., context=...)` nao existe hoje; criar teste que explicite a decisao. Se utilitarios continuarem sem contexto por desenho, documentar que so o Template Method oferece rastreabilidade operacional.

### P2 - Mensagem de validacao de registros invalidos nao inclui a mensagem declarativa do check

Incidente reproduzivel: `target_struct` declara check com `message="id is required"` no formato documentado em `docs/v0.1-contract.md:160`. Quando o dado falha, `_failed_check_summaries` retorna apenas `field.check: failed_count=N` em `etl_framework/utils/auto_quality.py:78`.

Erro esperado: mensagem real deveria incluir `check`, `field`, `failed_count` e a mensagem definida pelo autor, porque essa e a parte mais util para junior corrigir o dado ou a regra.

Erro real e mensagem real: `Invalid records found during target validation: invalid_count=1; failed_checks=[name_upper.name_upper_required: failed_count=1]`.

Campos presentes: contagem total, check tecnico, contexto de etapa no wrapper. Campos ausentes: `message` do check, `severity`, regra ou dica de correcao.

Teste existente: `tests/test_auto_contract.py:394` exige apenas `name_upper.name_upper_required`; `tests/test_validate_struct.py` valida summary com `message`, mas o erro operacional nao usa esse campo.

Risco operacional: o autor precisa abrir o schema para entender o significado do check. Para um time pequeno, isso aumenta tempo de diagnostico em falhas comuns de qualidade.

### P2 - Erros de producao auxiliares sao genericos demais quando usados em hooks

Incidente reproduzivel: `_custom_validate` chama `assert_target_key_unique(df, ("id",))` e ha duplicidade. O wrapper transforma em `ValidateError`, mas a mensagem raiz e apenas `Duplicate target key found: ('id',)` em `etl_framework/utils/production_checks.py:47`. Nao ha valor de chave, amostra segura, coluna de escopo, nem contagem.

Erro esperado: erro gerenciado de etapa com diagnostico minimo para acao: quais colunas, quantidade aproximada ou pelo menos `failed_check=target_key_unique`.

Erro real e mensagem real: `Duplicate target key found: ('id',) (pipeline_name=..., run_id=..., stage=validate, cause_type=ValueError, cause_message=Duplicate target key found: ('id',))`.

Campos presentes: contexto via wrapper se usado dentro de hook. Campos ausentes: detalhe acionavel da duplicidade. Se usado fora do hook, tambem ausentes `pipeline_name`, `run_id` e etapa.

Teste que deveria cobrir: teste de helper dentro de `_custom_validate` garantindo que a mensagem final seja suficiente para corrigir duplicidade sem reproduzir manualmente a query.

### P3 - Contexto de erro duplicado ao enriquecer `EtlError` sem contexto

Incidente reproduzivel: uma pipeline levanta manualmente `TransformError("business rule failed")` sem contexto. `ensure_stage_error` cria novo erro com `message=str(exc)` e `cause=exc` em `etl_framework/infra/errors.py:116`. A mensagem final pode conter a mensagem antiga e `cause_message` com texto quase identico.

Erro esperado: enriquecimento limpo, sem eco de contexto ou mensagem duplicada.

Erro real e mensagem real: `business rule failed (pipeline_name=..., run_id=..., stage=transform, cause_type=TransformError, cause_message=business rule failed)`.

Campos presentes: contexto completo. Impacto: baixo, mas aumenta ruido em troubleshooting.

Teste existente: `tests/test_errors.py:112` cobre enriquecimento, mas nao impõe limpeza da mensagem.

## Respostas as perguntas obrigatorias

- Cada etapa possui erro gerenciado coerente? Sim para o fluxo oficial dentro de `Pipeline.run()` e dos contratos `Extract`, `Transform` e `Load`. Nao para inicializacao de config/contexto nem para uso direto de utilitarios.
- A causa raiz e preservada? Sim no fluxo decorado: `stage` reergue `raise error from exc` em `etl_framework/infra/stage.py:88`, e `EtlError.cause` guarda a excecao original. Em falhas pre-runtime, nao ha wrapping nem causa gerenciada.
- A mensagem indica etapa, pipeline e run_id quando prometido? Sim nos erros gerenciados por etapa. Nao nos `ValueError` de `EtlRunConfig`, `EtlExecutionContext` e utilitarios diretos.
- Erros de validacao ajudam o autor da pipeline a corrigir? Parcialmente. Schema mismatch e tipo sao bons. Checks declarativos ainda deveriam incluir `message` e talvez `severity`. Helpers de producao sao pobres para diagnostico.
- Existe erro generico demais? Sim: `ValueError` em config/contexto/utilitarios e mensagens de production checks.
- Existe risco de erro silencioso? Sim no sentido operacional de perda de evidencia: sanitizacao de path apaga dado diagnostico; eventos de observabilidade sao best-effort por contrato, mas isso pertence mais a observabilidade. No escopo de erro, nao ha swallow relevante dentro dos contratos principais.

## Oportunidades

### P1

1. Introduzir erro gerenciado para configuracao/preflight fora de `Pipeline.run()` ou documentar formalmente que construcao de `EtlRunConfig` e `EtlExecutionContext` esta fora da promessa de rastreabilidade. Preferencia operacional: criar `ConfigError`/`ContextError` ou mover validacoes executaveis para `_preflight` quando dependem de pipeline.
2. Revisar sanitizacao de `path`, `source_path`, `target_path` e `file_path`. Redigir credenciais e payloads, mas preservar metadados operacionais seguros ou gerar valor mascarado parcialmente/hash estavel.

### P2

1. Incluir `message` e `severity` dos checks declarativos na mensagem de `ValidateError` para falha de dados.
2. Definir contrato de erro para utilitarios reutilizaveis: aceitar contexto opcional, retornar erro estruturado, ou documentar que so o uso via Template Method fornece rastreabilidade.
3. Melhorar mensagens de `production_checks` com identificador de check e diagnostico minimo sem expor dados sensiveis.
4. Adicionar teste de falha de `_show_dry_run_sample` para garantir que erro de sample em dry-run sai como `LoadError` com contexto.

### P3

1. Evitar duplicacao de mensagem quando `ensure_stage_error` enriquece `EtlError` sem contexto.
2. Padronizar nomes de status de eventos versus mensagens de erro (`success`/`succeeded`) para reduzir ambiguidade de suporte, sem misturar isso com a semantica de excecao.

## Conclusao

O framework cumpre a parte central da promessa para falhas que acontecem dentro do fluxo oficial. Para operar a v0.1 com time pequeno, o maior risco nao esta no wrapping dos seis estagios principais; esta nas falhas que acontecem antes do runtime e na perda de detalhes operacionais por sanitizacao agressiva. Essas lacunas geram incidentes reproduziveis em bootstrap, configuracao e paths de origem/destino, exatamente onde um junior mais precisa de mensagem rastreavel e acionavel.
