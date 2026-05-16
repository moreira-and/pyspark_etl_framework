# QUICK_START.md

Primeiros passos para executar uma pipeline mínima com `spark-etl-framework`.

Use este arquivo para começar. Use [README.md](README.md) para visão geral e
[MANIFEST.md](MANIFEST.md) para regras arquiteturais.

## 1. Instale o Ambiente

```bash
poetry install --with dev
```

Verifique se o pacote importa:

```bash
poetry run pytest
```

## 2. Crie uma Pipeline Mínima

Crie um arquivo local de teste, por exemplo `quick_start_pipeline.py`, fora do
pacote `etl_framework`.

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from etl_framework import EtlRunConfig, Extract, Load, Pipeline, Transform
from etl_framework.utils import validate_struct


target_struct = StructType(
    [
        StructField(
            "id",
            LongType(),
            nullable=False,
            metadata={
                "checks": [
                    {
                        "name": "id_positive",
                        "rule": "id > 0",
                        "severity": "error",
                    }
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
            schema=target_struct,
        )

    def _check(self, df, spark, config, context):
        return df


class QuickTransform(Transform):
    def _transform(self, df, spark, config, context):
        return df.withColumn("name", F.upper("name"))

    def _validate(self, df, spark, config, context):
        validated_df, _ = validate_struct(df, config.target_struct)
        return validated_df


class QuickLoad(Load):
    def _load(self, df, spark, config, context):
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
    target_struct=target_struct,
    dry_run=True,
    dry_run_limit=10,
    dry_run_show_rows=5,
    write_mode="overwrite",
)

pipeline = Pipeline(
    spark,
    config,
    extract=QuickExtract(),
    transform=QuickTransform(),
    load=QuickLoad(),
)

result_df = pipeline.run()
```

## 3. Execute

```bash
poetry run python quick_start_pipeline.py
```

Com `dry_run=True`, a pipeline:

- executa `extract`;
- executa `check`;
- limita o `DataFrame` com `dry_run_limit`;
- executa `transform`;
- executa `validate`;
- pula a escrita;
- mostra até `dry_run_show_rows` linhas, se esse valor for maior que zero.

Para executar a escrita real, altere:

```python
dry_run=False
```

## 4. O Que Alterar Primeiro

Depois que o quick start rodar:

1. Troque `QuickExtract._extract` pela leitura real da sua origem.
2. Coloque checks preliminares em `QuickExtract._check`.
3. Coloque regras de transformação em `QuickTransform._transform`.
4. Use `validate_struct` ou validações simples em `QuickTransform._validate`.
5. Ajuste `QuickLoad._load` para o destino real.
6. Mantenha `dry_run=True` até validar o comportamento.

## 5. Onde Ler Depois

- [README.md](README.md): visão geral e estrutura do projeto.
- [MANIFEST.md](MANIFEST.md): regras arquiteturais e limites do framework.
- `etl_framework/contracts/pipeline.py`: coordenação do fluxo.
- `etl_framework/models/config.py`: campos de configuração.
- `etl_framework/utils/validate_struct.py`: validação estrutural.
