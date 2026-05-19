# Checklist De Readiness Para Load Concreto

Este checklist e obrigatorio antes de qualquer execucao `dry_run=False` em
ambiente produtivo ou compartilhado.

Ele valida uma pipeline concreta. Ele nao transforma a v0.1 em plataforma de
load seguro, rollback, retry seguro ou idempotencia automatica.

## Decisao De Readiness

Preencha para cada pipeline:

| Campo | Valor |
| --- | --- |
| Pipeline | |
| Dono tecnico senior | |
| Ambiente | |
| Destino fisico | |
| `run_id` avaliado | |
| Commit da pipeline | |
| Data da revisao | |
| Decisao | Aprovada / Bloqueada / Risco aceito |

## Gate Antes De `dry_run=False`

| Item | Criterio minimo | Evidencia obrigatoria |
| --- | --- | --- |
| Escopo de escrita | `append`, `overwrite` ou merge tem escopo explicado e revisado. | Codigo do `Load._load` e plano de escrita. |
| Chave de negocio | `target_key` existe, nao permite nulos e cobre o escopo carregado. | Teste de chave nula e duplicada. |
| Rerun | Reexecucao do mesmo escopo nao duplica nem remove dados fora do escopo. | Teste de rerun ou risco aceito. |
| Falha antes do commit | Falha simulada antes da escrita final nao deixa carga parcial. | Teste ou limitacao formal. |
| Falha depois do commit | Comportamento de falha em `_certify` e retry esta documentado. | Teste ou procedimento manual. |
| Certificacao | `_certify` produz evidencia do destino real quando isso for exigido. | Log, metrica ou consulta de destino. |
| Coluna `is_valid` | Destino aceita `is_valid` ou o load projeta apenas colunas de negocio. | Assert/teste de schema final. |
| Volume | Volume esperado tem minimo, maximo e responsavel por excecoes. | Teste com `assert_volume_between` ou metrica equivalente. |
| Freshness | Janela/freshness esta definida quando aplicavel. | Check de coluna, particao ou watermark. |
| Reconciliacao | Escopo carregado pode ser reconciliado com a origem quando aplicavel. | Check por chave, contagem ou metrica. |
| Custo Spark | Acoes Spark conhecidas foram revisadas para volume esperado. | Plano de benchmark ou aceite de risco. |
| Dados sensiveis | `dry_run_show_rows` fica `0` em ambiente compartilhado. | Config revisada. |
| Logs | Logs de sucesso e falha incluem `pipeline_name`, `run_id`, etapa e status. | Evidencia de execucao ou teste. |

## Aceite Formal De Risco

Quando um item nao puder ser atendido, registre:

| Campo | Valor |
| --- | --- |
| Item do checklist | |
| Risco aceito | |
| Motivo | |
| Mitigacao | |
| Dono | |
| Prazo de revisao | |

Sem checklist preenchido ou aceite formal de risco, a pipeline deve permanecer
em `dry_run=True`.
