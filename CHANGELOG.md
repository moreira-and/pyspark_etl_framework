# Changelog

Todas as mudancas relevantes deste projeto devem ser registradas aqui.

O projeto usa SemVer pragmatico:

- `MAJOR`: quebra contrato publico ou comportamento esperado.
- `MINOR`: adiciona capacidade compativel.
- `PATCH`: correcao, documentacao ou hardening compativel.

## [Unreleased]

### Documentation

- Reorganiza a documentacao ativa para deixar claro que a v0.1 e um framework
  interno simples, nao uma plataforma completa de DataOps.
- Declara `prompts/_v0.1/` como insumo interno de auditoria, ancorado em
  `promessas.md`.
- Adiciona contrato explicito da v0.1 em `docs/v0.1-contract.md`.
- Move limitacoes conhecidas para `docs/v0.1-known-limitations.md`.
- Adiciona guia enxuto de testes em `docs/development/testing.md`.
- Adiciona checklist de readiness para `Load` concreto e modelo de custo Spark.
- Documenta que `auto_validate` executa uma acao Spark pequena para bloquear
  invalidos e que `Load._load` recebe a coluna tecnica `is_valid`.

### Changed

- Separa a observabilidade de `load` e `certify` para que falhas de certificacao
  nao sejam registradas como falhas de escrita.
- Padroniza `Extract`, `Transform` e `Load` como Template Method, com
  `_run_*` controlado pelo framework e hooks opcionais `_custom_*`.
- Marca exemplos de `SafeLoad`/v0.2 como `v02_example`, fora do gate
  contratual da v0.1.

## [0.1.0] - Base interna

### Added

- Contratos `Pipeline`, `Extract`, `Transform` e `Load`.
- Modelos `EtlRunConfig` e `EtlExecutionContext`.
- Check estrutural automatico via `source_struct`.
- Validate estrutural automatico via `target_struct`.
- Coluna tecnica `is_valid` no resultado validado.
- Logging tecnico por etapa.
- Erros gerenciados por etapa.
- `dry_run` para pular escrita durante desenvolvimento.
- `validate_struct` para schema e checks SQL declarativos simples.
- Helpers opcionais de qualidade operacional em `etl_framework.utils`.
- CI com black, isort, pytest e cobertura minima.
