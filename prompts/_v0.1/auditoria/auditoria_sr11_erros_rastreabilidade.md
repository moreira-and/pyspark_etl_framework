# Auditoria SR11 - Erros Gerenciados e Rastreabilidade de Falhas

## Papel do agente

Atue como especialista em suporte, troubleshooting e confiabilidade operacional
de pipelines de dados.

## Objetivo unico

Avaliar se falhas por etapa sao rastreaveis, diagnosticas e uteis para um time
pequeno operar a v0.1.

## Entradas obrigatorias

- `etl_framework/infra/errors.py`
- `etl_framework/contracts/`
- `etl_framework/models/context.py`
- testes de erros, pipeline, observabilidade e validacao
- `docs/v0.1-contract.md`

## Escopo

Foque em tipos de erro, wrapping, preservacao da causa raiz, `pipeline_name`,
`run_id`, etapa, mensagens e stacktrace util.

Nao avalie logs/eventos em profundidade; isso pertence a auditoria de
observabilidade operacional.

## Perguntas obrigatorias

- Cada etapa possui erro gerenciado coerente?
- A causa raiz e preservada?
- A mensagem indica etapa, pipeline e run_id quando prometido?
- Erros de validacao ajudam o autor da pipeline a corrigir?
- Existe erro generico demais?
- Existe risco de erro silencioso?

## Evidencias obrigatorias

- cenario de falha;
- erro esperado;
- erro real;
- mensagem real;
- campos de contexto presentes ou ausentes;
- teste que cobre ou deveria cobrir.

## Criterios de avaliacao

- rastreabilidade de falhas;
- robustez operacional;
- facilidade para junior;
- simplicidade;
- aderencia promessa-codigo;
- manutencao.

## Saida esperada

Gere:

1. matriz etapa -> falha -> erro -> contexto;
2. lacunas de rastreabilidade;
3. oportunidades P1/P2/P3.

## Criterio de sucesso

Cada risco operacional deve ser descrito como um incidente ou falha reproduzivel.
