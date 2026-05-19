# Testes E Validacao Local

Este documento registra o gate tecnico minimo do `etl_framework`.

Ele valida o framework, nao uma pipeline produtiva concreta.

## Ambiente

Use Poetry `2.1.4`, a mesma versao configurada no CI.

```bash
poetry --version
poetry install --with dev
```

Runtime oficial validado para a v0.1 controlada:

- Python `3.11`
- PySpark `3.5.x`

O CI atual roda Python `3.11` e Java `17`.

## Comandos Oficiais

```bash
poetry run python -m black --check --diff etl_framework tests
poetry run isort --check-only etl_framework tests
poetry run pytest --cov=etl_framework --cov-report=term-missing
```

## O Que A Suite Deve Proteger

- ordem `extract -> check -> transform -> validate -> load -> certify`;
- wrappers Template Method em `Extract.run`, `Transform.run` e `Load.run`;
- retorno invalido em etapas que prometem `DataFrame`;
- falhas por etapa e propagacao de erro;
- `auto_check` com `source_struct`;
- `auto_validate` com `target_struct`;
- bloqueio de registros invalidos antes de `load`;
- comportamento de `dry_run`;
- ausencia de `show` e `collect` automaticos no caminho normal;
- uso intencional de `limit(1).count()` no `auto_validate` para bloquear
  registros invalidos antes de `load`;
- validacao de metadata de checks;
- logs e erros sem payload sensivel obvio;
- helpers opcionais em `etl_framework.utils`.

## O Que Os Testes Do Framework Nao Provam

Os testes do framework nao provam que uma pipeline real esta pronta para
producao.

Cada pipeline concreta ainda precisa testar:

- leitura com janela ou filtro de origem;
- regra de negocio em `Transform._transform`;
- load com staging ou escopo seguro;
- idempotencia em rerun;
- falha antes e depois do commit;
- certificacao lendo o destino real;
- chaves nulas ou duplicadas;
- volume, freshness e reconciliacao quando aplicavel;
- custo de checks e metricas em dados grandes.

## Gate Contratual v0.1

O gate contratual da v0.1 e:

```bash
poetry run pytest --cov=etl_framework --cov-report=term-missing
```

Esse comando valida o framework prometido na v0.1.

## Marcadores

Testes que exercitam uma pipeline Spark local completa devem usar o marcador:

```python
@pytest.mark.integration
```

O marcador existe para deixar claro quando um teste depende de Spark local e
pode ser mais lento que testes unitarios puros.

## Custo Spark E Benchmark

O modelo de custo dos helpers e o benchmark minimo para pipelines concretas
ficam em
[docs/operation/spark-cost-and-benchmark.md](../operation/spark-cost-and-benchmark.md).
