# Relatorio de Execucao - Hardening v0.1

## 1. Objetivo

Transformar achados da auditoria em correcoes pequenas e verificaveis, sem
sofisticar o projeto nem vender maturidade produtiva que a v0.1 ainda nao
entrega.

## 2. Escopo executado

- CI local ajustado para formatacao e ordenacao de imports.
- Preflight leve em `Pipeline.run()` antes de `_extract`.
- Politica explicita `extra_columns_policy`: `ignore`, `warn` ou `fail`.
- Diagnostico minimo de invalidos no caminho de falha de `auto_validate`.
- Preservacao default de colunas tecnicas reservadas antes de `_load` e
  `_certify`, com opcao explicita para remove-las.
- `_certify` tornou-se opcional com default sem efeito.
- Documentacao atualizada para observabilidade best-effort e limites reais.
- Testes comportamentais adicionados para os riscos corrigidos.

## 3. Arquivos alterados

- `.github/workflows/ci.yml`
- `pyproject.toml`
- `README.md`
- `QUICK_START.md`
- `CHANGELOG.md`
- `docs/development/testing.md`
- `docs/observability/logging.md`
- `docs/operation/load-readiness-checklist.md`
- `docs/operation/spark-cost-and-benchmark.md`
- `docs/v0.1-contract.md`
- `docs/v0.1-known-limitations.md`
- `etl_framework/contracts/extract.py`
- `etl_framework/contracts/load.py`
- `etl_framework/contracts/pipeline.py`
- `etl_framework/contracts/transform.py`
- `etl_framework/infra/errors.py`
- `etl_framework/models/config.py`
- `etl_framework/utils/auto_quality.py`
- `etl_framework/utils/validate_struct.py`
- testes de contrato, configuracao, validacao, load, erros e jornada.

## 4. Decisoes tecnicas

- `PreflightError` foi adicionado como erro gerenciado, mas sem criar uma nova
  etapa observavel no fluxo ETL. O preflight roda dentro de `run`, antes de
  qualquer acesso a origem.
- `strict_schema=True` foi mantido por compatibilidade e passa a resolver para
  `extra_columns_policy="warn"` quando a politica explicita permanece `ignore`.
- O diagnostico de invalidos continua barato no caminho feliz. O custo extra
  (`count()` de invalidos e summary dos checks) ocorre apenas no caminho de
  falha.
- `Load.run()` entrega colunas tecnicas reservadas a `_load` e `_certify` por
  default. Pipelines que precisam entregar apenas colunas de negocio usam
  `keep_technical_columns=False`.
- `_certify` deixou de ser abstrato. O fluxo `load -> certify` continua
  existindo, mas pipelines simples nao precisam implementar metodo vazio.
- Observabilidade continuou best-effort. Nao foi criada politica tecnica de
  falha para sink porque isso ampliaria escopo da v0.1.

## 5. Riscos reduzidos

- Configuracao sem `source_struct` ou `target_struct` falha antes de `_extract`.
- Colunas extras agora tem contrato explicito e testado.
- Falhas de validacao informam `invalid_count` e checks declarativos quebrados.
- Colunas internas como `is_valid` chegam ao `_load` por default, quando a
  pipeline decide preserva-las.
- Loads simples nao precisam de `_certify` ritual sem evidencia real.
- Documentacao nao trata eventos/logs como garantia forte.

## 6. Riscos nao resolvidos

- A v0.1 ainda nao entrega load seguro, idempotencia, rollback, staging,
  commit atomico ou retry seguro.
- Observabilidade segue best-effort e usa sink global de processo.
- Diagnostico de invalidos melhora debug, mas nao substitui quarantine,
  amostragem segura ou auditoria regulatoria.
- Checks produtivos de chave, volume, freshness e reconciliacao continuam
  helpers opcionais e responsabilidade da pipeline concreta.
- O comando `poetry run black --check .` sem `--no-cache` travou neste ambiente
  Windows por cache do Black, mesmo com todos os arquivos formatados. O comando
  oficial foi ajustado para `--no-cache`.

## 7. Breaking changes, se houver

- `Load._load` e `Load._certify` recebem colunas tecnicas reservadas por default,
  incluindo `is_valid`. A remocao e opcional via `keep_technical_columns=False`.
- `strict_schema=True` permanece compativel com warning, mas a recomendacao
  passa a ser `extra_columns_policy`.
- `_certify` continua suportado, mas nao e mais obrigatorio.

## 8. Testes adicionados ou alterados

- Preflight antes de extract para `source_struct`, `target_struct` e
  consistencia defensiva de `target_key`.
- Politicas de colunas extras: `ignore`, `warn` e `fail`.
- Diagnostico de validacao com `invalid_count` e check falho.
- Preservacao default de `is_valid` no `_load`, e teste para remover colunas
  tecnicas quando configurado.
- `_certify` opcional.
- Atualizacao de testes de integracao que antes esperavam `is_valid` no load.

## 9. Comandos executados

```bash
poetry run black --check --no-cache .
```

Resultado: passou. `42 files would be left unchanged`.

```bash
poetry run isort --check-only .
```

Resultado: passou. `Skipped 2 files`.

```bash
poetry run pytest -q
```

Resultado: passou. `177 passed, 1 warning`.

Observacao: `poetry run black --check .` sem `--no-cache` foi testado e travou
por timeout local mesmo depois da formatacao estar correta. `poetry run black
--check --diff .` e `poetry run python -m black --check --diff .` passaram.

## 10. Veredito pos-execucao

- O projeto esta mais simples? Sim, principalmente porque `_certify` deixou de
  exigir metodo vazio e o load nao depende mais de limpeza manual.
- O projeto esta mais debugavel? Sim, preflight falha cedo e validacao invalida
  agora informa contagem e checks quebrados.
- O projeto ficou mais honesto? Sim, observabilidade foi documentada como
  best-effort e os limites produtivos continuam explicitos.
- O projeto continua aderente a filosofia de micro-framework? Sim. Nao foram
  adicionadas dependencias runtime nem subsistemas novos.
- O projeto ainda tem risco de overengineering? Sim. Decorators, Template
  Method e testes arquiteturais ainda exigem vigilancia para nao virarem culto
  de framework.
- O projeto pode ser usado em piloto controlado? Sim.
- O projeto pode ser vendido como producao robusta? Nao.
