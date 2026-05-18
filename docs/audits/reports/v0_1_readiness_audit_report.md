# Auditoria De Prontidao v0.1

## Veredito Inicial

Decisao inicial: APROVADO APENAS PARA PILOTO.

O framework tem contrato pequeno e coerente para v0.1, mas a auditoria encontrou
lacunas que impediam fechar uma v0.1 controlada sem hardening: falha de
`certify` tambem era registrada como `load_failed`, o custo Spark de
`auto_validate` estava documentado de forma contraditoria, `is_valid` no
`Load._load` nao estava declarado explicitamente, e nao havia matriz requisito
para teste. A primeira tentativa com o Python global nao executou a suite Spark;
o gate oficial deve usar Poetry ou a `.venv` do projeto.

## P0

Nenhum P0 confirmado no codigo ativo.

## P1

| ID | Problema | Evidencia | Recomendacao |
| -- | -------- | --------- | ------------ |
| P1-001 | Falha de `certify` podia gerar log `load_failed` enganoso. | `Pipeline._run_load` era decorado como `load` e chamava `Load.run`, que tambem executava `_run_certify`. | Separar observabilidade de `load` e `certify`. |
| P1-002 | Falta regressao pipeline-level provando que check declarativo bloqueia `load`. | Havia testes de helper, mas nao um teste do fluxo `Pipeline.run` com regra `target_struct.metadata["checks"]` invalidando registro. | Criar teste de integracao minima com `ValidateError` e `Load` nao chamado. |
| P1-003 | Documentacao dizia proteger ausencia de `count` automatico, mas `auto_validate` usa `limit(1).count()`. | `docs/development/testing.md` prometia ausencia de `count`; `assert_no_invalid_records` executa `count`. | Documentar o `count` intencional e manter a promessa de bloquear invalidos. |
| P1-004 | Contrato de `is_valid` no `Load._load` era implicito. | `validate_struct` adiciona `is_valid`; `Load._load` recebe o DataFrame validado; docs nao diziam como o load concreto deveria tratar a coluna. | Declarar que `Load._load` recebe `is_valid` e pode projetar/remover conforme destino. |

## P2

| ID | Problema | Evidencia | Recomendacao |
| -- | -------- | --------- | ------------ |
| P2-001 | `tests/test_safe_load_contract.py` podia sugerir `SafeLoad` de framework. | O arquivo implementa `SafeMemoryLoad` apenas dentro do teste; pacote nao exporta `SafeLoad`. | Reclassificar como exemplo de pipeline concreta/v0.2. |
| P2-002 | `dry_run` limita depois de `auto_check`. | `Pipeline._apply_dry_run_limit` roda depois de `Extract.run`. | Manter como limitacao documentada. |
| P2-003 | `source_struct` aceita metadata de checks, mas `auto_check` valida apenas schema. | `auto_check_source` chama `validate_schema`. | Manter fora do escopo v0.1 ou documentar mais forte futuramente. |
| P2-004 | Campos opcionais `source_struct` e `target_struct` falham tarde no fluxo. | Config permite `None`; falha ocorre em `auto_check`/`auto_validate`. | Considerar preflight em backlog curto. |

## P3

| ID | Problema | Evidencia | Recomendacao |
| -- | -------- | --------- | ------------ |
| P3-001 | `dry_run_show_rows > 0` pode expor dados. | `Pipeline.load` chama `df.show(..., truncate=False)`. | Manter documentado e default `0`. |
| P3-002 | Validacoes de tipo em config/context poderiam ter mensagens melhores. | Alguns campos usam `.strip()`/comparacoes diretamente. | Melhorar DX em patch futuro. |

## Promessas Vs Realidade

| Promessa | Onde e vendida | Evidencia no codigo | Evidencia nos testes | Status |
| -------- | -------------- | ------------------- | -------------------- | ------ |
| Fluxo `extract -> check -> transform -> validate -> load -> certify` | README e contrato v0.1 | `Pipeline`, `Extract.run`, `Transform.run`, `Load.run` | `test_run_executes_official_order`, `test_inline_framework_integration_runs_full_contract` | COMPROVADA |
| `source_struct` automatico | README e contrato v0.1 | `Extract._auto_check` | `test_auto_check_fails_when_source_column_is_missing` | COMPROVADA |
| `target_struct` automatico | README e contrato v0.1 | `Transform._auto_validate` | `test_auto_validate_fails_when_target_column_is_missing` | COMPROVADA |
| Bloqueio de invalidos antes de `load` | README e contrato v0.1 | `auto_validate_target` + `assert_no_invalid_records` | Regressao adicionada no hardening | COMPROVADA APOS HARDENING |
| `dry_run` pula load real | README, contrato e Quick Start | `Pipeline.load` | `test_dry_run_limits_after_check_and_skips_load` | COMPROVADA |
| Logs e erros com `run_id` | README e contrato v0.1 | `stage`, `log_event`, `EtlError` | `test_stage_errors_include_trace_context...`, `test_pipeline_run_emits...` | COMPROVADA |
| Load seguro/idempotente/rollback | Limites e ADR dizem fora do escopo | Nao implementado | Exemplo concreto apenas | PROMESSA REMOVIDA DA V0.1 |

## Fluxo Real Encontrado

```text
Pipeline.run
  -> Extract._extract
  -> auto_check(source_struct)
  -> Extract._check opcional
  -> dry_run limit, se habilitado
  -> Transform._transform
  -> auto_validate(target_struct)
  -> Transform._validate opcional
  -> Load._load, exceto dry_run
  -> Load._certify, exceto dry_run
```

Automatico: ordem do fluxo, `auto_check`, `auto_validate`, `is_valid`, bloqueio
de invalidos, logs por etapa, erros gerenciados e `run_id`.

Manual: leitura real, transformacao de negocio, escrita segura, remocao de
colunas tecnicas quando o destino exigir, metricas operacionais e certificacao
real.

## Lacunas De Testes

- Antes do hardening, faltava teste pipeline-level para check declarativo
  bloqueando `load`.
- Antes do hardening, faltava teste garantindo que falha de `certify` nao fosse
  registrada como `load_failed`.
- Nao havia matriz requisito para teste.
- A suite Spark nao foi executada localmente por incompatibilidade de Java.

## Testes Frageis Ou Enganosos

- `tests/test_safe_load_contract.py` era util como exemplo, mas o nome podia
  sugerir contrato de `SafeLoad` no pacote v0.1.
- Alguns testes de exemplo usam `collect`/`count` em `Load` ou `certify`, mas
  isso representa pipeline concreta, nao comportamento automatico do core.

## Documentacao Desalinhada

- `docs/development/testing.md` citava ausencia de `count` automatico, em
  contradicao com o bloqueio automatico de invalidos.
- A documentacao ativa nao explicitava que `Load._load` recebe `is_valid`.

## Riscos Operacionais

- O Python global diverge da `.venv`; os gates oficiais devem usar o ambiente
  do projeto.
- `Load` continua responsabilidade da pipeline concreta.
- `dry_run` nao reduz custo de leitura antes de `_extract`/`auto_check`.
- `auto_validate` executa acao Spark para bloquear invalidos.

## Riscos De Regressao

- Falhas de stage podem degradar diagnostico se os eventos forem alterados sem
  teste.
- Regras declarativas em metadata precisam continuar bloqueando `load` quando
  produzem `is_valid=False`.
- Documentacao pode voltar a vender `SafeLoad` se testes exemplares forem lidos
  como API publica.

## Recomendacao Objetiva

Corrigir P1 antes de marcar v0.1. Depois do hardening, aceitar como v0.1
controlada apenas se a suite completa, cobertura e hooks passarem no ambiente
oficial do projeto ou no CI.
