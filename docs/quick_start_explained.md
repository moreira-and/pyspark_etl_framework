# Quick Start Explicado

Este guia complementa o [QUICK_START.md](../QUICK_START.md). Use o Quick Start
quando quiser copiar um exemplo minimo e executar. Use este documento quando
quiser entender o que cada parte representa e como evoluir o exemplo sem violar
o contrato do framework.

## Ideia Principal

O `spark-etl-framework` nao tenta esconder Spark. Ele organiza uma pipeline
PySpark em uma ordem fixa:

```text
extract -> check -> transform -> validate -> load -> certify
```

O nucleo do framework coordena essa ordem, injeta `SparkSession`, `EtlRunConfig`
e `EtlExecutionContext`, registra logs e transforma falhas em erros com contexto.
Ele nao sabe ler sua origem real, nao sabe aplicar regra de negocio e nao sabe
escrever no seu destino real. Essas partes ficam nas classes concretas.

## O Que O Exemplo Faz

O exemplo do Quick Start cria uma pipeline pequena com dados inline:

- `QuickExtract` cria um `DataFrame` local.
- `QuickTransform` aplica uma transformacao simples em uma coluna.
- `QuickLoad` escreve em Parquet quando `dry_run=False`.
- `EtlRunConfig` descreve nome da pipeline, destino, chave, schema e modo de
  execucao.
- `Pipeline.run()` executa o contrato completo.

Esse exemplo serve para aprender a forma do framework. Ele nao e um padrao de
load produtivo.

## Como Ler As Classes

### `Extract`

`_extract` e o lugar da leitura. Em uma pipeline real, aqui entraria leitura de
tabela, arquivo, stream batch, API previamente materializada ou outro insumo
Spark.

`_check` roda depois da leitura e antes do `dry_run_limit`. Use para checks
preliminares da origem, por exemplo schema esperado da origem ou filtros basicos
da janela. Evite `count` aqui no caminho produtivo, a menos que a pipeline tenha
decidido explicitamente pagar esse custo.

### `Transform`

`_transform` deve conter a regra de negocio da pipeline. Mantenha o codigo Spark
visivel: `withColumn`, `select`, `join`, `filter`, funcoes de janela e outras
operacoes devem continuar claras para quem vai revisar custo e plano Spark.

`_validate` valida a saida transformada. O utilitario `validate_struct` cobre
contrato estrutural e checks declarativos simples. Validacoes produtivas como
unicidade de chave, volume esperado, freshness e reconciliacao continuam sendo
responsabilidade da pipeline concreta.

### `Load`

`_load` escreve no destino. No Quick Start ele chama `df.write.parquet(...)` para
mostrar o encaixe do contrato. Em producao, implemente staging, commit
idempotente e retry seguro.

`_certify` deve produzir evidencia depois da carga. Em producao, certifique lendo
o destino real. Certificar apenas o `DataFrame` em memoria nao prova que o dado
foi gravado corretamente.

## Configuracao Da Execucao

`EtlRunConfig` e pequeno de proposito. Ele orienta o framework e a pipeline:

- `pipeline_name`: nome usado nos logs.
- `target_schema`, `target_table`, `target_path`: destino esperado.
- `target_key`: chave de negocio para orientar idempotencia e DQ.
- `source_struct` e `target_struct`: schemas opcionais de contrato.
- `dry_run`, `dry_run_limit`, `dry_run_show_rows`: execucao segura de teste.
- `start_window` e `end_window`: janela disponivel para a extracao.
- `write_mode`: modo declarado para o destino.

O framework valida formato basico desses campos, mas nao conhece as convencoes
da sua empresa. Para producao, crie uma factory de config no projeto consumidor
com regras locais para path, schema, tabela, janela e overwrite.

## Dry Run Sem Surpresa

Com `dry_run=True`, o framework:

1. executa `_extract`;
2. executa `_check`;
3. aplica `df.limit(dry_run_limit)`;
4. executa `_transform`;
5. executa `_validate`;
6. pula `_load` e `_certify`;
7. chama `df.show(...)` somente se `dry_run_show_rows > 0`.

O limite acontece depois de `_check`. Isso preserva a verificacao inicial da
origem, mas significa que a leitura ainda pode tocar dados reais. Para reduzir
custo de leitura, use filtros de janela e predicate pushdown dentro de
`_extract`.

Em modo normal (`dry_run=False`), `dry_run_show_rows` deve ser `0`. O framework
rejeita exibicao de linhas fora de dry run para reduzir risco de vazamento.

## Validacao Estrutural Nao E Data Quality Completo

`validate_struct` confirma que colunas e tipos batem com um `StructType`, cria a
coluna tecnica `is_valid` e executa checks SQL simples declarados em metadata.

Isso protege contra drift estrutural e erros basicos. Nao substitui:

- chave nula;
- chave duplicada;
- volume minimo ou maximo esperado;
- freshness;
- reconciliacao com origem ou destino;
- politica de quarantine.

Antes de go-live, use o checklist em
[production_readiness.md](production_readiness.md).

## Logs E Metricas

Os logs do framework ja carregam `pipeline_name`, `run_id`, etapa, status, modo e
destino. A pipeline pode adicionar metricas explicitas em `context.metrics`.

Exemplo:

```python
context.metrics["rows_valid"] = valid_df.count()
context.metrics["rows_invalid"] = invalid_df.count()
```

Esses `count()` sao acoes Spark. O framework nao calcula isso sozinho. A pipeline
deve decidir quando a metrica vale o custo.

## Evoluindo O Exemplo Para Uma Pipeline Real

Uma evolucao segura costuma seguir esta ordem:

1. Troque o `DataFrame` inline de `QuickExtract` pela leitura real com janela.
2. Declare `source_struct` e `target_struct` quando houver contrato conhecido.
3. Escreva transformacoes pequenas e teste o schema de saida.
4. Use `validate_struct` para drift estrutural e checks simples.
5. Adicione checks produtivos de DQ no projeto consumidor.
6. Substitua `QuickLoad` por um load com staging e commit idempotente.
7. Faca `_certify` ler o destino real e publicar metricas explicitas.
8. Rode em `dry_run=True`.
9. Rode em ambiente controlado com `dry_run=False` e `dry_run_show_rows=0`.
10. Revise plano Spark, particionamento, shuffle e custo antes do go-live.

## Erros Comuns

- Colocar regra de negocio generica no nucleo do framework.
- Usar `count`, `collect`, `show` ou `toPandas` para debug e esquecer no caminho
  produtivo.
- Achar que `target_struct` garante qualidade completa dos dados.
- Usar `append` sem chave, janela ou estrategia de deduplicacao.
- Fazer `_certify` apenas com o `DataFrame` recebido, sem ler o destino.
- Ligar `compute_summary=True` em volume grande sem aceitar o custo.
- Sair de `dry_run` sem zerar `dry_run_show_rows`.

## Proximas Leituras

- [QUICK_START.md](../QUICK_START.md): exemplo minimo executavel.
- [production_readiness.md](production_readiness.md): checklist de uso produtivo.
- [MANIFEST.md](../MANIFEST.md): regras para evoluir o nucleo.
