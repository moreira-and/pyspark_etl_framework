# spark-etl-framework

Template minimo para pipelines ETL em Python com Spark, baseado em um contrato rigido de execucao:

```text
extract -> check -> transform -> validate -> load
```

O framework controla a ordem, os logs, o tratamento de erro, o dry-run, a configuracao minima e o contexto de execucao. Cada pipeline concreta implementa apenas a regra de negocio de cada etapa.

Dependencia externa obrigatoria: `pyspark`.

Configuracao, contexto, logging e rastreabilidade usam apenas biblioteca padrao do Python.

O contrato e os modelos de execucao ficam em `src/etlstruct/models/`, com reexportacao pelo pacote `etlstruct` para manter o uso simples.

## Instalacao

```bash
poetry install --with dev
```

## Criando uma pipeline

```python
from etlstruct import EtlRunConfig, EtlStruct, ExecutionMode


class CustomerPipeline(EtlStruct):
    def __init__(self, spark, source_path, target_path, config):
        super().__init__(config=config)
        self.spark = spark
        self.source_path = source_path
        self.target_path = target_path

    def _extract(self):
        return self.spark.read.parquet(self.source_path)

    def _check(self, df):
        if "customer_id" not in df.columns:
            raise ValueError("Missing required column: customer_id")
        return df

    def _transform(self, df):
        return df.dropDuplicates(["customer_id"])

    def _validate(self, df):
        if df.limit(1).count() == 0:
            raise ValueError("Pipeline result is empty")
        return df

    def _load(self, df):
        df.write.mode("overwrite").parquet(self.target_path)


pipeline = CustomerPipeline(
    spark=spark,
    source_path="/data/raw/customers",
    target_path="/data/curated/customers",
    config=EtlRunConfig(
        pipeline_name="customer_etl",
        mode=ExecutionMode.DRY_RUN,
        dry_run_limit=100,
        dry_run_show_rows=20,
    )
)
pipeline.run()
```

Os metodos publicos `extract`, `check`, `transform`, `validate`, `load` e `run` pertencem ao framework. Pipelines concretas devem implementar somente os hooks protegidos `_extract`, `_check`, `_transform`, `_validate` e `_load`.

## Dry-run

Quando `mode=ExecutionMode.DRY_RUN`:

- `extract` aplica `limit(dry_run_limit)` quando o objeto retornado suporta `.limit(...)`;
- `check`, `transform` e `validate` continuam executando;
- `load` nao chama `_load`, portanto nao persiste dados;
- o resultado final e exibido com `.show(dry_run_show_rows, truncate=False)` quando disponivel;
- os logs registram que a carga foi pulada.

## Testes

```bash
poetry run pytest
```

O teste de integracao usa dados de entrada em `tests/data/customer_input.csv` e executa a pipeline exemplo com uma sessao Spark local.

Para executar o teste de integracao Spark, Java precisa estar instalado e `JAVA_HOME` deve apontar para a instalacao. Sem Java local, o teste e pulado.
