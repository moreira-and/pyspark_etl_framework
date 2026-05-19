# Quick Start

Este exemplo mostra a menor forma util de montar uma pipeline com
`etl_framework`.

Ele e didatico. O `QuickLoad` abaixo nao e um padrao produtivo e nao deve ser
copiado para cargas reais sem checklist senior.

## 1. Instalar

```bash
poetry install --with dev
```

## 2. Criar Uma Pipeline Minima

Crie um arquivo local, por exemplo `quick_start_pipeline.py`, fora do pacote
`etl_framework`.

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from etl_framework import EtlRunConfig, Extract, Load, Pipeline, Transform


source_struct = StructType(
    [
        StructField("id", LongType(), nullable=False),
        StructField("name", StringType(), nullable=True),
    ]
)

target_struct = StructType(
    [
        StructField(
            "id",
            LongType(),
            nullable=False,
            metadata={
                "checks": [
                    {
                        "name": "id_required",
                        "rule": "id IS NOT NULL",
                        "severity": "error",
                    },
                    {
                        "name": "id_positive",
                        "rule": "id > 0",
                        "severity": "error",
                    },
                ]
            },
        ),
        StructField("name", StringType(), nullable=True),
    ]
)


class QuickExtract(Extract):
    def _extract(self, spark, config, context):
        return spark.createDataFrame(
            [(1, "ana"), (2, "bruno")],
            schema=source_struct,
        )


class QuickTransform(Transform):
    def _transform(self, df, spark, config, context):
        return df.select("id", F.upper("name").alias("name"))


class QuickLoad(Load):
    def _load(self, df, spark, config, context):
        # O DataFrame chega validado e inclui a coluna tecnica is_valid.
        # Este exemplo didatico grava tudo; loads reais podem projetar colunas.
        df.write.mode(config.write_mode or "overwrite").parquet(config.target_path)

    def _certify(self, df, spark, config, context):
        return None


spark = SparkSession.builder.appName("quick-start").getOrCreate()

config = EtlRunConfig(
    pipeline_name="quick_start",
    target_schema="sandbox",
    target_table="people",
    target_path="tmp/quick_start/people",
    target_key=("id",),
    source_struct=source_struct,
    target_struct=target_struct,
    dry_run=True,
    dry_run_limit=10,
    dry_run_show_rows=0,
    write_mode="overwrite",
)

pipeline = Pipeline(
    spark=spark,
    config=config,
    extract=QuickExtract(),
    transform=QuickTransform(),
    load=QuickLoad(),
)

result_df = pipeline.run()
```

## 3. Executar

```bash
poetry run python quick_start_pipeline.py
```

Com `dry_run=True`, o framework:

1. executa `QuickExtract._extract`;
2. roda `auto_check` com `source_struct`;
3. aplica `dry_run_limit`;
4. executa `QuickTransform._transform`;
5. roda `auto_validate` com `target_struct`;
6. `Load.run()` registra evidencia tecnica de dry-run;
7. pula `_load` e `_certify`;
8. retorna o `DataFrame` final.

No caminho normal, `auto_validate` executa `limit(1).count()` para impedir que
registros com `is_valid=False` cheguem ao `Load`.

Para permitir escrita real, use:

```python
dry_run=False
dry_run_show_rows=0
```

Essa troca so e aceitavel em pipeline concreta apos preencher o checklist de
readiness de `Load`.

Nao use `dry_run_show_rows > 0` em ambiente compartilhado. Esse modo chama
`show(..., truncate=False)` e pode expor dados sensiveis.

## 4. O Que Trocar Primeiro

Depois que o exemplo rodar:

1. Troque `QuickExtract._extract` pela leitura real da origem.
2. Mantenha `source_struct` alinhado ao schema lido.
3. Coloque regras de negocio em `QuickTransform._transform`.
4. Mantenha `target_struct` alinhado ao resultado transformado.
5. Adicione checks SQL simples em `target_struct` quando quiser bloquear dados.
6. Substitua `QuickLoad` por uma estrategia revisada antes de qualquer uso real
   com `dry_run=False`.

`nullable=False` no `StructField` documenta intencao de schema, mas nao substitui
um check SQL como `campo IS NOT NULL`.

## 5. Proximas Leituras

- [docs/v0.1-contract.md](docs/v0.1-contract.md)
- [docs/v0.1-known-limitations.md](docs/v0.1-known-limitations.md)
- [docs/operation/load-readiness-checklist.md](docs/operation/load-readiness-checklist.md)
