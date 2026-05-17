# Production Readiness

Este guia define o padrao minimo para usar `etl_framework` em pipelines
PySpark produtivas. O nucleo continua pequeno: ele coordena contrato, contexto,
logs e erros. Leitura, transformacao, escrita, metricas caras e data quality
produtivo pertencem a cada pipeline concreta.

## Comando Oficial

Use sempre Poetry para validar o projeto:

```bash
poetry --version
poetry install --with dev
poetry run python -m black --check --diff etl_framework tests
poetry run isort --check-only etl_framework tests
poetry run pytest --cov=etl_framework --cov-report=term-missing
```

Matriz minima suportada:

- Python `>=3.11,<3.14`
- Poetry `2.1.4` (mesma versao que gerou `poetry.lock` e que o CI instala)
- PySpark `>=3.5,<4.0`
- Java 17 no CI; use Java compativel com a distribuicao Spark da sua plataforma.

Nao use o Python global como gate. Um ambiente fora do Poetry pode importar outra
versao de PySpark e mascarar falhas.

O workflow `.github/workflows/ci.yml` fixa `POETRY_VERSION=2.1.4`. Se a equipe
atualizar Poetry localmente, atualize tambem o workflow e regenere `poetry.lock`
com a mesma versao.

## Load Seguro

Um `Load` produtivo deve seguir este padrao:

1. Escrever primeiro em staging isolado por `run_id`.
2. Validar que o staging contem somente o escopo esperado.
3. Fazer commit atomico quando o destino suportar isso.
4. Ser idempotente por `target_key`, janela de extracao ou `run_id`.
5. Ser seguro para retry: rerun nao pode duplicar linhas nem ampliar overwrite.
6. Certificar lendo o destino real, nao apenas o `DataFrame` em memoria.
7. Registrar metricas ja calculadas pela pipeline em `context.metrics`.

Exemplo minimo do contrato:

```python
from etl_framework import Load


class SafeTableLoad(Load):
    def _load(self, df, spark, config, context):
        staging_path = f"{config.target_path}/_staging/{context.run_id}"

        # Acao Spark explicita da pipeline concreta.
        df.write.mode("overwrite").parquet(staging_path)

        # Implemente aqui a semantica real do seu destino:
        # - substituir somente a janela/chaves do run;
        # - publicar staging por rename/transaction/merge atomico quando possivel;
        # - limpar staging somente depois do commit.
        commit_staging_to_target(
            staging_path=staging_path,
            target_path=config.target_path,
            target_key=config.target_key,
            start_window=config.start_window,
            end_window=config.end_window,
            run_id=context.run_id,
        )

    def _certify(self, df, spark, config, context):
        target_df = spark.read.parquet(config.target_path)

        # Acao Spark explicita de certificacao, nao feita pelo framework.
        written_rows = target_df.where("etl_run_id = '{}'".format(context.run_id)).count()
        if written_rows <= 0:
            raise RuntimeError("No rows certified in target for this run_id")

        context.metrics["rows_written"] = written_rows
```

`commit_staging_to_target` e a estrategia de rollback dependem do destino
concreto. Em Delta/Iceberg/Hudi, prefira transacoes/merge nativos. Em arquivos
Parquet simples, trate rename e delete como operacoes potencialmente nao
atomicas no storage remoto e documente o plano de recuperacao.

Exemplo minimo de montagem da pipeline:

```python
pipeline = Pipeline(
    spark=spark,
    config=EtlRunConfig(
        pipeline_name="orders_daily",
        target_schema="silver",
        target_table="orders",
        target_path="/warehouse/silver/orders",
        target_key=("order_id",),
        start_window="2026-05-16",
        end_window="2026-05-17",
        write_mode="overwrite",
    ),
    extract=OrdersExtract(),
    transform=OrdersTransform(),
    load=SafeTableLoad(),
)

pipeline.run()
```

`OrdersExtract` e `OrdersTransform` continuam contendo somente leitura,
transformacao e validacoes da pipeline concreta. O padrao seguro fica no
`SafeTableLoad`.

Testes de referencia:

- `tests/test_safe_load_contract.py`: falha em load, falha em certify, rerun,
  ausencia de duplicidade e certificacao baseada no destino.

## Observabilidade

Os logs do framework incluem:

- `pipeline_name`
- `run_id`
- `stage`
- `status`
- `mode`
- `target_schema`
- `target_table`
- `target_path`
- `target`
- `write_mode`
- `elapsed_ms`, quando a etapa termina ou falha
- `metrics`, quando a pipeline concreta preencher `context.metrics` ou passar
  `metrics=` para `log_event`

O framework nao calcula `count`, `collect` ou `show` para metricas. Se a pipeline
precisar de `rows_read`, `rows_valid`, `rows_invalid` ou `rows_written`, ela deve
calcular explicitamente e registrar:

```python
context.metrics["rows_valid"] = validated_df.filter("is_valid").count()
context.metrics["rows_invalid"] = validated_df.filter("NOT is_valid").count()
```

Calcule essas metricas somente quando forem necessarias para operacao ou
certificacao. Em volumes grandes, planeje cache/persist ou reaproveite metricas
do destino/orquestrador quando disponivel.

## Data Quality Produtivo

`target_struct` valida contrato estrutural: nomes, tipos e checks declarativos
simples. Ele nao garante qualidade produtiva sozinho.

Checklist minimo por pipeline:

- `target_key` sem nulos.
- `target_key` unica no escopo carregado.
- duplicidades tratadas antes do load.
- volume minimo/maximo esperado por janela.
- freshness da origem dentro do SLA.
- reconciliacao simples contra origem ou destino.
- politica clara para `is_valid=False`: quarantine ou bloqueio.

Exemplos pequenos:

```python
from pyspark.sql import functions as F


def assert_not_null_keys(df, keys):
    condition = F.lit(False)
    for key in keys:
        condition = condition | F.col(key).isNull()
    if df.filter(condition).limit(1).count() > 0:
        raise ValueError(f"Null value found in target key: {keys}")


def assert_unique_keys(df, keys):
    duplicated = df.groupBy(*keys).count().filter(F.col("count") > 1)
    if duplicated.limit(1).count() > 0:
        raise ValueError(f"Duplicate target key found: {keys}")


def assert_volume_between(df, min_rows, max_rows):
    row_count = df.count()
    if row_count < min_rows or row_count > max_rows:
        raise ValueError("Row count outside expected range")


def assert_freshness(df, column, min_timestamp):
    stale_rows = df.filter(F.col(column) < F.lit(min_timestamp)).limit(1).count()
    if stale_rows > 0:
        raise ValueError("Freshness check failed")


def assert_simple_reconciliation(source_df, target_df, key):
    source_keys = source_df.select(key).distinct()
    target_keys = target_df.select(key).distinct()
    if source_keys.exceptAll(target_keys).limit(1).count() > 0:
        raise ValueError("Target is missing source keys")
```

Quarantine ou bloqueio:

```python
valid_df = validated_df.filter("is_valid")
quarantine_df = validated_df.filter("NOT is_valid")

if quarantine_df.limit(1).count() > 0:
    # Escolha uma politica por pipeline: gravar quarantine auditavel ou bloquear.
    quarantine_df.write.mode("append").parquet(quarantine_path)
    raise ValueError("Invalid records were quarantined; load blocked")
```

Testes de referencia:

- `tests/test_production_data_quality_examples.py`: chave nula, chave duplicada,
  volume fora do range, quarantine e bloqueio de invalidos.

## `compute_summary=True`

`validate_struct(..., compute_summary=True)` executa agregacoes Spark para contar
falhas por check. Em DataFrames grandes isso pode ser full scan. Com muitos
checks, o framework divide em batches para limitar tamanho de expressao, mas cada
batch ainda pode varrer dados.

Use summary apenas:

- em certificacao controlada;
- em ambiente offline;
- apos `persist`/`cache` planejado pela pipeline;
- quando o custo estiver aceito no SLA.

O default `compute_summary=False` deve continuar sendo usado no caminho normal.

## Checks SQL Declarativos

Checks em metadata devem ser predicados Spark SQL simples:

- prefira uma regra por coluna;
- referencie campos explicitamente, incluindo nested fields como
  `payload.amount > 0`;
- evite UDFs, subqueries e expressoes longas sem revisar o plano;
- limite a quantidade de checks por pipeline;
- teste regras invalidas e colunas inexistentes antes do go-live.

`validate_struct` analisa as expressoes antes de retornar o `DataFrame`, sem
executar job Spark. Erros de sintaxe ou coluna inexistente devem falhar com nome
do check, regra e colunas disponiveis.

Testes de referencia:

- `tests/test_validate_struct.py`: regra invalida, coluna inexistente, nested
  schema e muitos checks com batching.

## `dry_run_show_rows`

`dry_run_show_rows` tem default `0`. Valores positivos sao aceitos somente com
`dry_run=True`.

Use `dry_run_show_rows > 0` apenas em debug controlado e com dados nao sensiveis.
Em modo normal (`dry_run=False`) a configuracao e rejeitada para evitar vazamento
acidental de amostras.

## Configuracao De Destino

O framework valida campos minimos, mas cada consumidor deve ter uma factory de
configuracao produtiva com convencoes locais:

```python
def build_orders_config(env, start_window, end_window):
    if env not in {"dev", "stg", "prod"}:
        raise ValueError("Unknown environment")
    if not start_window or not end_window:
        raise ValueError("Production loads require a bounded window")

    return EtlRunConfig(
        pipeline_name="orders_daily",
        target_schema=f"{env}_silver",
        target_table="orders",
        target_path=f"/warehouse/{env}/silver/orders",
        target_key=("order_id",),
        start_window=start_window,
        end_window=end_window,
        write_mode="overwrite",
        dry_run=False,
    )
```

Overwrite produtivo deve sempre ter escopo: particao, janela, chave ou transacao
do destino. Nao use overwrite amplo sem revisao.
