# QUICK_START.md

Primeiros passos para executar uma pipeline mínima com `spark-etl-framework`.

Use este arquivo para começar. Para uma leitura mais explicativa do exemplo,
leia [docs/quick_start_explained.md](docs/quick_start_explained.md). Use
[README.md](README.md) para visão geral e [MANIFEST.md](MANIFEST.md) para regras
arquiteturais.

## 1. Instale o Ambiente

Use Poetry `2.1.4`, a mesma versão usada pelo CI e pelo `poetry.lock`.

```bash
poetry --version
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

Para executar a escrita real, altere os dois campos abaixo. `dry_run_show_rows`
deve voltar para `0`, porque o framework rejeita exibicao de linhas em modo
normal para evitar vazamento acidental de dados.

```python
dry_run=False
dry_run_show_rows=0
```

O `QuickLoad` acima é apenas didático. Para produção, implemente staging,
commit idempotente e `_certify` lendo o destino real. Use o padrão em
[docs/production_readiness.md](docs/production_readiness.md) antes de habilitar
uma pipeline com milhões de linhas.

## Notebook Parser: from raw .ipynb to etl_framework pipeline

Use this workflow when you need to convert an exploratory notebook into a
structured ETL pipeline.

The parser works in three controlled artifacts:

1. `raw_*.ipynb` - exact copy of the original notebook for historical tracking.
2. `cln_*.ipynb` - cleaned notebook with sections, subtitles and
   parser-friendly structure.
3. `cfg_*.py` - final Python implementation compatible with `etl_framework`.

The standard artifact directory is:

```text
prompts/notebook_parser/notebook_parser_runs/<nome_semantico_notebook>/
  raw_<nome_semantico_notebook>.ipynb
  cln_<nome_semantico_notebook>.ipynb
  cfg_<nome_semantico_notebook>.py
```

This workflow does not guarantee automatic conversion of any notebook.
Ambiguous notebooks must be reviewed manually before becoming production
pipelines.

For the full workflow, see:

`/prompts/notebook_parser/00_overview.md`

## 4. O Que Alterar Primeiro

Depois que o quick start rodar:

1. Troque `QuickExtract._extract` pela leitura real da sua origem.
2. Coloque checks preliminares em `QuickExtract._check`.
3. Coloque regras de transformação em `QuickTransform._transform`.
4. Use `validate_struct` ou validações simples em `QuickTransform._validate`.
5. Ajuste `QuickLoad._load` para o destino real.
6. Mantenha `dry_run=True` até validar o comportamento.
7. Antes do go-live, aplique o checklist de produção, data quality e load seguro
   em [docs/production_readiness.md](docs/production_readiness.md).

## 5. Onde Ler Depois

- [README.md](README.md): visão geral e estrutura do projeto.
- [docs/quick_start_explained.md](docs/quick_start_explained.md): explicação
  humana do exemplo.
- [prompts/notebook_parser/00_overview.md](prompts/notebook_parser/00_overview.md):
  workflow de parser de notebook para pipeline.
- [MANIFEST.md](MANIFEST.md): regras arquiteturais e limites do framework.
- `etl_framework/contracts/pipeline.py`: coordenação do fluxo.
- `etl_framework/models/config.py`: campos de configuração.
- `etl_framework/utils/validate_struct.py`: validação estrutural.
