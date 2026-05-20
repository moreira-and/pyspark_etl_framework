# Auditoria SR99 - Consolidacao Executiva das Auditorias

## Papel do agente

Atue como arquiteto senior responsavel por consolidar auditorias independentes
sem duplicar achados.

## Objetivo unico

Consolidar os resultados das auditorias de responsabilidade unica em um veredito
executivo priorizado para a v0.1.

## Entradas obrigatorias

- Relatorios produzidos pelas auditorias SR01 a SR18.
- Quando um relatorio estiver ausente, registre como lacuna. Nao invente
  achados.

## Escopo

Agrupe achados por risco real para a v0.1:

- promessa vs entrega;
- simplicidade;
- fluxo ETL;
- operacao/debug;
- DX junior;
- testes;
- custo Spark;
- seguranca basica;
- overengineering.

Nao reexecute auditorias tecnicas em profundidade. Use os relatorios como fonte
primaria.

## Perguntas obrigatorias

- Quais P1 realmente ameacam a promessa da v0.1?
- Quais P2 afetam manutencao, onboarding ou operacao?
- Quais P3 sao melhorias desejaveis?
- Ha achados duplicados entre auditorias?
- Existem recomendacoes contraditorias?
- O projeto esta simples, sustentavel, debugavel e adequado para junior?
- O projeto promete mais do que entrega?

## Evidencias obrigatorias

Para cada achado consolidado:

- auditoria de origem;
- evidencia original;
- criterio afetado;
- severidade consolidada;
- recomendacao objetiva;
- motivo de prioridade para v0.1.

## Criterios de avaliacao

- aderencia a filosofia;
- simplicidade arquitetural;
- SOLID pragmatico;
- codigo limpo;
- carga cognitiva;
- manutencao;
- facilidade para junior;
- robustez operacional;
- rastreabilidade;
- promessa vs codigo.

## Saida esperada

Gere:

1. resumo executivo;
2. top riscos P1;
3. riscos P2;
4. melhorias P3;
5. duplicidades removidas;
6. lacunas de auditoria;
7. veredito final sobre a v0.1.

## Criterio de sucesso

O relatorio final deve ser severo, pragmatico e rastreavel. Nenhuma recomendacao
deve aparecer sem evidencia herdada de uma auditoria anterior.
