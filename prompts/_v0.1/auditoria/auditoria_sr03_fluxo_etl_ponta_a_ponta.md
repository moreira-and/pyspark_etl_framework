# Auditoria SR03 - Fluxo ETL Ponta a Ponta

## Papel do agente

Atue como especialista em engenharia de dados e arquitetura ETL.

## Objetivo unico

Validar se o fluxo oficial `extract -> check -> transform -> validate -> load
-> certify` e claro, previsivel e implementado conforme a promessa da v0.1.

## Entradas obrigatorias

- `etl_framework/contracts/pipeline.py`
- `etl_framework/contracts/extract.py`
- `etl_framework/contracts/transform.py`
- `etl_framework/contracts/load.py`
- `etl_framework/models/config.py`
- `etl_framework/models/context.py`
- testes relacionados a pipeline, dry-run, contratos e integracao
- `docs/v0.1-contract.md`

## Escopo

Analise ordem de execucao, responsabilidades por etapa, entrada/saida de cada
contrato e pontos onde responsabilidades vazam para o autor da pipeline.

Nao aprofunde observabilidade, DX ou testes alem do necessario para provar o
fluxo.

## Perguntas obrigatorias

- A ordem oficial e realmente respeitada?
- O desenvolvedor implementa apenas `_extract`, `_transform`, `_load` e
  opcionalmente hooks declarados?
- `check` e `validate` ficam de fato no framework?
- O `dry_run` altera o fluxo de forma previsivel?
- Alguma etapa executa comportamento critico de forma escondida?
- Ha duplicacao ou vazamento de responsabilidade entre contratos?

## Evidencias obrigatorias

- mapa de chamadas do fluxo real;
- arquivo/metodo responsavel por cada etapa;
- comportamento esperado por etapa;
- testes que demonstram a ordem;
- divergencias entre contrato documentado e implementacao.

## Criterios de avaliacao

- aderencia a filosofia;
- simplicidade do fluxo;
- previsibilidade;
- separacao de responsabilidades;
- facilidade para junior;
- robustez operacional;
- rastreabilidade de falhas.

## Saida esperada

Gere:

1. mapa do fluxo real;
2. matriz etapa -> dono -> responsabilidade -> evidencia;
3. pontos de vazamento ou ambiguidade;
4. oportunidades P1/P2/P3.

## Criterio de sucesso

Um leitor deve conseguir entender a execucao ponta a ponta sem abrir todos os
arquivos do framework.
