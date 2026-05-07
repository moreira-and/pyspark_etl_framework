# Definicao Inicial - spark-etl-framework

## Objetivo

O `spark-etl-framework` e um template minimo de ETL em Spark, baseado em um contrato rigido chamado `EtlStruct`.

Ele padroniza o ciclo:

```text
extract -> check -> transform -> validate -> load
```

O metodo `run()` executa exatamente essa sequencia.

A pipeline concreta implementa apenas os hooks internos:

```python
_extract()
_check()
_transform()
_validate()
_load()
```

O framework controla:

```text
ordem de execucao
logging
tratamento de erro
dry-run
contexto de execucao
configuracao minima
```

## Dependencia

O spark-etl-framework deve depender apenas de PySpark como biblioteca externa obrigatoria.

Boas praticas de contrato, configuracao, validacao e rastreabilidade devem ser implementadas com recursos nativos do Python, como `abc`, `dataclasses`, `enum`, `typing`, `logging`, `datetime` e `uuid`.

O objetivo e manter o framework minimo, claro e sustentavel, evitando dependencias adicionais antes que exista uma necessidade operacional concreta.

Dependencia funcional:

```text
pyspark
```

Dependencia de desenvolvimento permitida:

```text
pytest
```

Nao utilizar nesta primeira versao:

```text
pydantic
pydantic-settings
pandera
great-expectations
dynaconf
hydra
click
typer
loguru
```

## Base tecnica

A primeira versao deve ser baseada em:

```text
Python standard library + PySpark
```

Recursos esperados:

| Necessidade          | Solucao             |
| -------------------- | ------------------- |
| Contrato rigido      | `abc`               |
| Configuracao simples | `dataclasses`       |
| Modos de execucao    | `enum.StrEnum`      |
| Tipagem              | `typing`            |
| Logging              | `logging`           |
| Decorator            | `functools.wraps`   |
| Tempo de execucao    | `time.perf_counter` |
| Contexto de execucao | `uuid`, `datetime`  |
| Erros especificos    | classes de excecao  |

## Arquitetura do repositorio

```text
spark-etl-framework/
+-- src/
|   +-- etlstruct/
|       +-- __init__.py
|       +-- models/
|       |   +-- __init__.py
|       |   +-- contracts.py
|       |   +-- config.py
|       |   +-- context.py
|       +-- infra/
|       |   +-- __init__.py
|       |   +-- errors.py
|       |   +-- logger.py
|       +-- utils/
|           +-- __init__.py
|           +-- extract.py
|           +-- check.py
|           +-- transform.py
|           +-- validate.py
|           +-- load.py
+-- pipelines/
|   +-- pipeline_example/
|       +-- __init__.py
|       +-- etl_example.py
+-- tests/
|   +-- unit/
|   +-- integration/
+-- pyproject.toml
+-- README.md
```

`models.py` como arquivo unico nao deve existir nesta versao.

Separacao explicita:

```text
models/contracts.py -> contrato da pipeline
models/config.py    -> configuracao da execucao
models/context.py   -> metadados da execucao
```

## Configuracao

Use `dataclass` com validacao manual em `__post_init__`.

```python
from dataclasses import dataclass
from enum import StrEnum


class ExecutionMode(StrEnum):
    FULL = "full"
    DRY_RUN = "dry_run"


@dataclass(frozen=True)
class EtlRunConfig:
    pipeline_name: str
    mode: ExecutionMode = ExecutionMode.FULL
    dry_run_limit: int = 100
    dry_run_show_rows: int = 20

    def __post_init__(self) -> None:
        if not self.pipeline_name.strip():
            raise ValueError("pipeline_name must not be empty.")

        if self.dry_run_limit <= 0:
            raise ValueError("dry_run_limit must be greater than zero.")

        if self.dry_run_show_rows <= 0:
            raise ValueError("dry_run_show_rows must be greater than zero.")

    @property
    def dry_run(self) -> bool:
        return self.mode == ExecutionMode.DRY_RUN
```

## Contexto de execucao

```python
from dataclasses import dataclass, field
from datetime import datetime, timezone
from uuid import uuid4


@dataclass(frozen=True)
class EtlExecutionContext:
    run_id: str = field(default_factory=lambda: str(uuid4()))
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    triggered_by: str = "manual"
```

Esse objeto resolve:

```text
identificacao da execucao
rastreabilidade
metadados minimos
logging com run_id
auditoria basica
```

## Contrato funcional

Toda pipeline concreta deve herdar de `EtlStruct` e implementar:

```python
class MinhaPipeline(EtlStruct):
    def _extract(self):
        ...

    def _check(self, df):
        ...

    def _transform(self, df):
        ...

    def _validate(self, df):
        ...

    def _load(self, df):
        ...
```

A pipeline concreta nao deve implementar `run`, `extract`, `check`, `transform`, `validate` ou `load`.

Esses metodos pertencem ao framework.

O construtor base deve receber somente objetos do framework, sem duplicar campos internos de configuracao:

```python
super().__init__(
    config=config,
    context=context,
)
```

Campos como `pipeline_name`, `mode`, `dry_run_limit` e `dry_run_show_rows` pertencem a `EtlRunConfig`, nao ao `EtlStruct.__init__`.

## Comportamento do run

```python
df = self.extract()
df = self.check(df)
df = self.transform(df)
df = self.validate(df)
self.load(df)
```

A ordem nao deve ser customizavel nesta primeira versao.

## Dry-run

Quando `mode=ExecutionMode.DRY_RUN`:

1. `extract` aplica limite reduzido quando o DataFrame suporta `.limit(...)`;
2. `check`, `transform` e `validate` executam normalmente;
3. `load` nao persiste dados;
4. o DataFrame final e analisado com `.show(...)` quando disponivel;
5. logs indicam claramente que a carga foi pulada.

## Logging

Todo estagio publico deve emitir logs padronizados:

```text
run
extract
check
transform
validate
load
```

Cada log deve registrar:

```text
pipeline_name
stage
stage_started_at
stage_finished_at
duration_seconds
status
error_type e error_message, quando houver
run_id
triggered_by
mode
```

Eventos desejados:

```text
stage_started
stage_finished
stage_failed
dry_run_load_skipped
```

## Tratamento de erros

Cada etapa converte excecoes genericas em erro especifico:

```text
extract   -> ExtractError
check     -> CheckError
transform -> TransformError
validate  -> ValidateError
load      -> LoadError
```

## Escopo da primeira versao

Incluido:

- contrato `EtlStruct`;
- configuracao com `dataclasses`;
- contexto de execucao com `dataclasses`;
- erros especificos;
- logging padronizado;
- modo dry-run;
- pipeline exemplo;
- testes unitarios minimos;
- README inicial.

Fora do escopo:

- Airflow;
- Databricks Jobs;
- Delta Lake;
- catalogo externo;
- lineage;
- CLI;
- plugins;
- factories;
- adapters complexos;
- bibliotecas externas de configuracao ou validacao.

## Criterios de aceite

1. existir classe base `EtlStruct`;
2. `run()` executar `extract -> check -> transform -> validate -> load`;
3. cada estagio ter logging;
4. cada estagio relancar erro especifico;
5. pipeline concreta herdar de `EtlStruct`;
6. dry-run limitar extracao;
7. dry-run nao persistir dados;
8. existir pipeline exemplo;
9. existir teste unitario validando o fluxo;
10. `README.md` explicar como criar pipeline;
11. dependencia funcional obrigatoria ser apenas `pyspark`;
12. configuracao e contexto nao dependerem de Pydantic.
