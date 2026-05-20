# Auditoria SR06 - Simplicidade e Tracing Mental

## Papel do agente

Atue como especialista em simplicidade arquitetural e carga cognitiva.

## Objetivo unico

Medir o esforco mental necessario para entender, alterar e debugar o fluxo
principal do framework.

## Entradas obrigatorias

- `etl_framework/contracts/`
- `etl_framework/infra/`
- `etl_framework/utils/`
- `etl_framework/models/`
- testes de integracao e jornada do autor de pipeline
- `README.md`
- `QUICK_START.md`

## Escopo

Analise indirecao, profundidade de chamadas, template methods, decorators,
helpers, acoplamentos implicitos e arquivos necessarios para entender uma
execucao simples.

Nao avalie se a arquitetura esta superengenheirada em abstrato; isso pertence a
auditoria de overengineering. Aqui o foco e custo cognitivo observavel.

## Perguntas obrigatorias

- Quantos arquivos um junior precisa abrir para entender `Pipeline.run()`?
- Quantos saltos existem entre chamada publica e comportamento critico?
- Onde o comportamento importante fica escondido?
- O fluxo e simples de seguir em uma falha?
- A simplicidade e real ou apenas aparente?
- O custo cognitivo cresce linearmente ou exponencialmente?

## Evidencias obrigatorias

- mapa de arquivos por jornada;
- sequencia de chamadas;
- exemplos de indirecao com arquivo/metodo;
- pontos onde contexto implicito e necessario;
- comparacao entre comportamento aparente e comportamento real.

## Criterios de avaliacao

- reducao de carga cognitiva;
- simplicidade arquitetural;
- codigo limpo;
- facilidade de manutencao;
- facilidade para junior;
- rastreabilidade de falhas.

## Saida esperada

Gere:

1. diagnostico de simplicidade real;
2. top ofensores de carga cognitiva;
3. mapa de tracing;
4. oportunidades P1/P2/P3.

## Criterio de sucesso

Cada ponto de complexidade deve explicar por que importa para a v0.1.
