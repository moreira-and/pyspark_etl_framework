# etl_framework

`etl_framework` e um framework interno simples para padronizar pipelines ETL em
PySpark.

Ele existe para reduzir carga cognitiva de desenvolvedores junior, tornar o
fluxo ETL previsivel e automatizar controles estruturais basicos antes que dados
cheguem ao destino.

O pacote publicado pelo projeto e `etl_framework`. O nome do repositorio e
`spark-etl-framework`.

## Escopo Real Da v0.1

A v0.1 organiza o fluxo:

```text
extract -> check -> transform -> validate -> load -> certify
```

O foco real da v0.1 esta em:

- padronizar a ordem de execucao;
- injetar `SparkSession`, `EtlRunConfig` e `EtlExecutionContext`;
- executar `auto_check` da origem com `source_struct`;
- executar `auto_validate` do resultado com `target_struct`;
- registrar logs tecnicos por etapa;
- propagar erros gerenciados com `pipeline_name`, `run_id` e etapa;
- oferecer `dry_run` para pular a escrita durante desenvolvimento.

A v0.1 ainda nao entrega load seguro, idempotencia, rollback, transacao,
quarantine persistente ou certificacao real do destino. Esses pontos ficam para
pipelines concretas ou para a v0.2.

## O Que O Junior Implementa

Em uma pipeline comum, o desenvolvedor implementa:

| Classe | Metodo principal | Responsabilidade |
| --- | --- | --- |
| `Extract` | `_extract` | Ler a origem e retornar um `DataFrame`. |
| `Transform` | `_transform` | Aplicar transformacoes de negocio. |
| `Load` | `_load` | Escrever no destino escolhido pela pipeline. |
| `Load` | `_certify` | Registrar uma evidencia simples apos a carga. |

Hooks opcionais existem para casos especificos:

- `Extract._custom_check`
- `Transform._custom_validate`

Na v0.1, o junior nao precisa chamar manualmente `validate_schema` para o schema
da origem nem `validate_struct` para o schema do destino. O framework faz isso
automaticamente a partir de `source_struct` e `target_struct`.

## O Que O Framework Automatiza

`Extract.run()` executa `_run_extract()`, valida que `_extract()` retornou um
`DataFrame`, executa `_run_check()`, aplica `config.source_struct` e so entao
chama `_custom_check()`.

`Transform.run()` executa `_run_transform()`, valida que `_transform()` retornou
um `DataFrame`, executa `_run_validate()`, aplica `config.target_struct` e so
entao chama `_custom_validate()`.

`Load.run()` valida que recebeu um `DataFrame`. Com `dry_run=True`, produz
evidencia tecnica de dry-run e nao chama `_load()` nem `_certify()`. Com
`dry_run=False`, executa `_run_load()` e depois `_run_certify()`.

`auto_validate` adiciona a coluna tecnica `is_valid` e bloqueia registros
invalidos antes de `load`. Regras declarativas podem ser definidas em
`StructField.metadata["checks"]` usando expressoes SQL Spark simples.

Para bloquear invalidos, `auto_validate` executa uma acao Spark pequena
(`limit(1).count()`) depois de criar `is_valid`. Essa acao faz parte do contrato
da v0.1; ela evita escrita de registros invalidos, mas deve ser considerada em
pipelines de grande volume.

`Load._load` recebe o `DataFrame` ja validado, incluindo a coluna tecnica
`is_valid`. Se o destino nao aceitar essa coluna, a propria implementacao de
`Load` deve projetar apenas as colunas de negocio antes da escrita.

## O Que A v0.1 Nao Garante

A v0.1 nao deve ser vendida como plataforma completa de DataOps.

Ela nao garante:

- escrita idempotente;
- staging por `run_id`;
- commit atomico;
- rollback;
- retry seguro;
- protecao completa contra carga duplicada ou parcial;
- certificacao lendo o destino real;
- engine completa de qualidade de dados;
- observabilidade externa;
- compatibilidade produtiva irrestrita para milhoes de linhas.

Essas limitacoes estao detalhadas em
[docs/v0.1-known-limitations.md](docs/v0.1-known-limitations.md).

## Instalar E Testar

Pre-requisitos:

- Python `>=3.11,<3.14`
- Poetry `2.1.4`
- PySpark `>=3.5,<4.0`

Instalacao local:

```bash
poetry install --with dev
```

Comandos oficiais:

```bash
poetry run python -m black --check --diff etl_framework tests
poetry run isort --check-only etl_framework tests
poetry run pytest --cov=etl_framework --cov-report=term-missing
```

Detalhes de teste ficam em
[docs/development/testing.md](docs/development/testing.md).

## Documentacao Ativa

- [QUICK_START.md](QUICK_START.md): exemplo minimo executavel.
- [docs/v0.1-contract.md](docs/v0.1-contract.md): contrato real da v0.1.
- [docs/v0.1-known-limitations.md](docs/v0.1-known-limitations.md): limites
  conhecidos e como interpreta-los.
- [docs/roadmap/v0.2.md](docs/roadmap/v0.2.md): itens futuros, separados da
  realidade atual.
- [docs/adr/0001-defer-safe-load-to-v0.2.md](docs/adr/0001-defer-safe-load-to-v0.2.md):
  decisao de adiar load seguro.
- [docs/development/testing.md](docs/development/testing.md): comandos de
  validacao local e CI.
- [CHANGELOG.md](CHANGELOG.md): historico de mudancas.

Documentos antigos, prompts e auditorias foram arquivados em `docs/archive/`.
Eles nao fazem parte da documentacao ativa da v0.1.

## Licenca

MIT. Consulte [LICENSE](LICENSE).
