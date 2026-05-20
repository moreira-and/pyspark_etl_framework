# Custo Spark E Benchmark Minimo Da v0.1

Este documento registra o modelo de custo conhecido da v0.1 e o benchmark
minimo antes de usar uma pipeline concreta com volume alto.

A v0.1 nao promete performance irrestrita. Ela explicita onde executa acoes
Spark para que a equipe avalie custo por pipeline.

## Modelo De Custo

| Componente | Acao Spark | Custo esperado | Uso correto |
| --- | --- | --- | --- |
| `auto_check_source` | Nenhuma acao; usa schema do `DataFrame`. | Baixo, sem job Spark intencional. | Validar nomes e tipos da origem. |
| `validate_struct(..., compute_summary=False)` | Nenhuma acao intencional. | Baixo ate a proxima acao do usuario. | Criar `is_valid` sem resumo agregado. |
| `auto_validate_target` caminho feliz | `filter(...).limit(1).count()`. | Um job pequeno para detectar pelo menos um invalido. | Bloquear invalidos antes do `load`. |
| `auto_validate_target` com invalidos | `count()` dos invalidos e summary dos checks declarativos. | Mais caro, executado apenas no caminho de falha para diagnostico. | Informar quantidade de invalidos e checks com falha. |
| `assert_no_invalid_records` | `filter(...).limit(1).count()`. | Um job pequeno, dependente de particionamento e predicado. | Gate explicito antes de escrita. |
| `assert_target_key_not_null` | `filter(...).limit(1).count()`. | Um job pequeno para encontrar nulo. | Check de pipeline concreta. |
| `assert_target_key_unique` | `groupBy(...).count()` e `limit(1).count()`. | Pode gerar shuffle. | Usar sob revisao para chaves de load. |
| `assert_volume_between` | `count()`. | Job completo sobre o `DataFrame`. | Medir volume quando a contagem for aceitavel. |
| `assert_freshness_at_least` | `filter(...).limit(1).count()`. | Um job pequeno, dependente de particionamento. | Validar freshness quando houver coluna adequada. |
| `assert_reconciled_by_key` | `distinct`, `exceptAll` e `limit(1).count()`. | Pode gerar shuffle entre chaves. | Reconciliacao de escopo revisado. |
| `validate_struct(..., compute_summary=True)` | Agregacao e `collect()` de metadados de checks. | Pode ser caro com muitos checks. | Diagnostico ou auditoria, nao caminho padrao. |
| `dry_run_show_rows > 0` | `show(..., truncate=False)`. | Acao Spark e exposicao de dados. | Manter `0` em ambiente compartilhado. |

## Benchmark Minimo Para Pipeline Concreta

Execute antes de aprovar volume alto:

| Cenario | Minimo esperado |
| --- | --- |
| Volume representativo | Pelo menos o maior volume diario esperado ou 1M+ linhas. |
| Checks | Todos os checks de `target_struct`; incluir 10+ checks se a pipeline usar muitos. |
| Invalidos | Dataset com invalido no inicio, no fim e sem invalidos. |
| Chaves | Dataset com chave nula, duplicada e escopo valido. |
| Particoes | Registrar numero de particoes antes/depois dos checks. |
| Shuffle | Registrar se `groupBy`, `distinct` ou `exceptAll` aparecem no plano. |
| Tempo | Registrar tempo por etapa: `extract`, `check`, `transform`, `validate`, `load`, `certify`. |
| Jobs Spark | Registrar quantidade de jobs disparados pelos helpers usados. |
| Dados sensiveis | Confirmar `dry_run_show_rows=0`. |

## Evidencia Esperada

O relatorio da pipeline deve registrar:

- commit da pipeline;
- ambiente Spark e versoes de Python, Java e PySpark;
- volume e numero de particoes;
- lista de helpers usados;
- tempo e jobs por etapa;
- resultado dos cenarios invalidos;
- decisao: aprovado, bloqueado ou risco aceito.

Sem essa evidencia, a v0.1 pode ser usada em piloto controlado, mas nao como
prova de readiness produtiva da pipeline concreta.
