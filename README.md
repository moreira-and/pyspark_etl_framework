# spark-etl-framework

Framework interno para padronizar pipelines ETL em PySpark com um contrato
linear, explícito e fácil de depurar.

## Mapa da Documentação

- [README.md](README.md): visão geral, estrutura e onboarding do projeto.
- [QUICK_START.md](QUICK_START.md): primeiros passos para executar uma pipeline
  mínima.
- [MANIFEST.md](MANIFEST.md): contrato arquitetural, regras de evolução e
  governança técnica.
- [docs/README.md](docs/README.md): índice dos documentos operacionais e de
  auditoria.
- [docs/production_readiness.md](docs/production_readiness.md): checklist de
  produção, load seguro, data quality, observabilidade e CI.

## Visão Geral

O pacote principal é `etl_framework`.

O framework não implementa leitura, transformação ou escrita específicas de
negócio. Ele fornece contratos para que cada pipeline implemente essas partes e
mantém o fluxo principal previsível.

O núcleo cuida de:

- ordem de execução;
- configuração da execução;
- contexto com `run_id`;
- logging técnico;
- erros gerenciados por etapa;
- validação estrutural opcional;
- `dry_run` para reduzir risco durante desenvolvimento.

## Contrato da Pipeline

Fluxo oficial:

```text
extract -> check -> transform -> validate -> load -> certify
```

| Etapa | Método | Responsabilidade |
| --- | --- | --- |
| `extract` | `Extract._extract` | Ler dados de origem. |
| `check` | `Extract._check` | Verificar dados antes da transformação. |
| `transform` | `Transform._transform` | Aplicar transformação. |
| `validate` | `Transform._validate` | Validar saída transformada. |
| `load` | `Load._load` | Persistir resultado. |
| `certify` | `Load._certify` | Registrar evidência simples da carga. |

`Pipeline.run()` coordena a sequência. As implementações concretas entram por
injeção de `Extract`, `Transform` e `Load`.

## Estrutura do Projeto

```text
spark-etl-framework/
├── .github/workflows/ci.yml
├── docs/
│   ├── README.md
│   └── production_readiness.md
├── etl_framework/
│   ├── contracts/      # Pipeline, Extract, Transform, Load e type checks
│   ├── infra/          # logging e erros gerenciados
│   ├── models/         # EtlRunConfig e EtlExecutionContext
│   └── utils/          # validate_struct e schema metadata
├── tests/
├── MANIFEST.md
├── QUICK_START.md
├── README.md
├── pyproject.toml
└── poetry.lock
```

## Componentes Principais

- `Pipeline`: coordena a execução.
- `Extract`: contrato de extração e checks iniciais.
- `Transform`: contrato de transformação e validação final.
- `Load`: contrato de carga e certificação.
- `EtlRunConfig`: configuração imutável de uma execução.
- `EtlExecutionContext`: contexto técnico com `run_id` e `started_at`.
- `validate_struct`: valida `DataFrame` contra `StructType`.
- `EtlError` e erros por etapa: padronizam falhas rastreáveis.

## Instalação

Pré-requisitos:

- Python `>=3.11,<3.14`
- Poetry `2.1.4`
- PySpark `>=3.5,<4.0`

Instalação local:

```bash
poetry install --with dev
```

O CI usa Poetry `2.1.4`, a mesma versão que gerou `poetry.lock`.

Dependência externa de runtime declarada:

- `pyspark`

Dependências de desenvolvimento:

- `pytest`
- `pytest-cov`
- `black`
- `isort`
- `commitizen`
- `pre-commit`

## Primeiros Passos

Siga [QUICK_START.md](QUICK_START.md) para criar e executar uma pipeline mínima.

O quick start cobre:

- criação de `EtlRunConfig`;
- implementação mínima de `Extract`, `Transform` e `Load`;
- execução com `Pipeline.run()`;
- uso de `dry_run`;
- validação estrutural com `validate_struct`.

## Desenvolvimento

Comandos oficiais de validação, alinhados ao CI:

```bash
poetry run python -m black --check --diff etl_framework tests
poetry run isort --check-only etl_framework tests
poetry run pytest --cov=etl_framework --cov-report=term-missing
```

Configurações relevantes estão em `pyproject.toml`:

- `black`: line length `88`;
- `isort`: profile `black`;
- `pytest`: testes em `tests`;
- `coverage`: fonte em `etl_framework`, com mínimo obrigatório configurado.

## Limitações Atuais

- Não há CLI própria.
- Não há diretório de exemplos dedicado.
- A pasta `docs` contém o guia operacional de prontidão produtiva.
- A suíte de testes atual valida contratos do framework com Spark local.
- `dry_run` limita dados após `_extract` e `_check`, não antes da leitura.
- Uso produtivo exige seguir o checklist em
  [docs/production_readiness.md](docs/production_readiness.md), especialmente
  para idempotência de `Load`, data quality e métricas explícitas.

## Referência Arquitetural

Use [MANIFEST.md](MANIFEST.md) para decisões sobre:

- escopo permitido e proibido;
- dependências aceitas;
- critérios de evolução;
- regras de simplicidade;
- governança técnica do núcleo.

## Licença

Este projeto usa licença MIT. Consulte [LICENSE](LICENSE).
