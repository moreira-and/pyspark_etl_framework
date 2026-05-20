# Auditoria SR05 - Load e Certify na v0.1

## Papel do agente

Atue como arquiteto de dados com foco em escrita, certificacao e riscos de
producao.

## Objetivo unico

Avaliar se `Load` e `certify` estao corretamente limitados ao escopo da v0.1,
sem vender seguranca produtiva que o framework nao entrega.

## Entradas obrigatorias

- `etl_framework/contracts/load.py`
- `etl_framework/contracts/pipeline.py`
- `etl_framework/models/config.py`
- testes de load, dry-run e integracao
- `README.md`
- `QUICK_START.md`
- `docs/v0.1-contract.md`
- `docs/v0.1-known-limitations.md`
- `docs/operation/load-readiness-checklist.md`

## Escopo

Foque em comportamento de `Load.run()`, `_load`, `_certify`, `dry_run`,
`keep_technical_columns`, evidencias tecnicas e limites de responsabilidade da
pipeline concreta.

Nao proponha implementacao de load seguro generico, staging, transacao ou
rollback. Esses itens sao fora do escopo declarado da v0.1.

## Perguntas obrigatorias

- O codigo deixa claro que load seguro e responsabilidade da pipeline concreta?
- `dry_run=True` realmente impede `_load` e `_certify`?
- A evidencia de dry-run pode ser confundida com certificacao real?
- `keep_technical_columns` e previsivel para o autor da pipeline?
- O Quick Start deixa claro que o load de exemplo nao e produtivo?
- Existe risco de uso indevido com `dry_run=False`?

## Evidencias obrigatorias

- caminhos de execucao em `Load.run()`;
- mensagens, eventos ou erros relacionados;
- testes que provam dry-run e carga real;
- documentacao que limita a promessa;
- lacunas ou ambiguidades que poderiam induzir mau uso.

## Criterios de avaliacao

- aderencia ao escopo v0.1;
- robustez operacional;
- rastreabilidade;
- facilidade para junior;
- clareza documental;
- baixo risco de falsa seguranca.

## Saida esperada

Gere:

1. veredito sobre load/certify na v0.1;
2. matriz risco -> evidencia -> impacto;
3. oportunidades P1/P2/P3.

## Criterio de sucesso

A auditoria deve diferenciar falha real da v0.1 de recurso intencionalmente fora
do escopo.
