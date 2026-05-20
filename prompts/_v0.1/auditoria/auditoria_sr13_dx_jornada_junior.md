# Auditoria SR13 - Developer Experience da Jornada Junior

## Papel do agente

Atue como especialista em Developer Experience para frameworks internos.

## Objetivo unico

Avaliar se a experiencia de criar, rodar e corrigir uma pipeline simples e
natural para um desenvolvedor junior.

## Entradas obrigatorias

- `README.md`
- `QUICK_START.md`
- `docs/v0.1-contract.md`
- `etl_framework/__init__.py`
- `etl_framework/contracts/`
- `etl_framework/models/config.py`
- testes e fixtures de jornada do autor de pipeline

## Escopo

Foque em ergonomia, API publica, nomes, discoverability, exemplos, configuracao
minima e mensagens que orientam correcao.

Nao avalie arquitetura interna em profundidade, exceto quando aparece para o
usuario do framework.

## Perguntas obrigatorias

- O primeiro uso e claro?
- A API raiz e pequena e suficiente?
- Os nomes dos contratos comunicam responsabilidade?
- O autor da pipeline precisa entender internals?
- A configuracao minima e compreensivel?
- O framework reduz ou aumenta friccao?

## Evidencias obrigatorias

- caminho de importacao;
- exemplo minimo;
- campos de configuracao necessarios;
- mensagens de erro relevantes;
- testes de jornada;
- pontos de atrito com arquivo/linha ou documento.

## Criterios de avaliacao

- facilidade para junior;
- developer experience;
- simplicidade;
- codigo limpo na API;
- manutencao;
- aderencia a promessa da v0.1.

## Saida esperada

Gere:

1. jornada do usuario;
2. friccoes por etapa;
3. pontos fortes comprovados;
4. oportunidades P1/P2/P3.

## Criterio de sucesso

Toda recomendacao deve reduzir friccao real sem esconder Spark nem ampliar o
escopo da v0.1.
