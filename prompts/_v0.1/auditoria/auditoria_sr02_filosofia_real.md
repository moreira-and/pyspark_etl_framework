# Auditoria SR02 - Filosofia Real do Projeto

## Papel do agente

Atue como arquiteto senior de software e engenharia de dados. Seu foco e
descobrir a filosofia real do projeto a partir do codigo, nao aceitar a
documentacao como verdade.

## Objetivo unico

Identificar se a filosofia declarada da v0.1 e sustentada pelas decisoes reais
de implementacao.

## Entradas obrigatorias

- `README.md`
- `docs/v0.1-contract.md`
- `docs/v0.1-known-limitations.md`
- `etl_framework/`
- `tests/`

## Escopo

Analise principios, decisoes arquiteturais e coerencia geral. Nao faca uma
auditoria detalhada de fluxo ETL, testes ou operacao; apenas use esses elementos
como evidencias da filosofia real.

## Perguntas obrigatorias

- Qual parece ser a filosofia declarada?
- Qual parece ser a filosofia real observada no codigo?
- O projeto favorece simplicidade ou sofisticacao arquitetural?
- O projeto concentra complexidade no nucleo ou espalha responsabilidades?
- O projeto ajuda um desenvolvedor junior ou exige conhecimento implicito?
- Existem sinais de marketing tecnico exagerado?
- Existem decisoes que traem a promessa da v0.1?

## Evidencias obrigatorias

- documentos que declaram principios;
- modulos/classes/metodos que sustentam ou contradizem esses principios;
- testes que reforcam ou contradizem a filosofia;
- lacunas onde nao ha evidencia suficiente.

## Criterios de avaliacao

- aderencia a filosofia do projeto;
- simplicidade arquitetural;
- SOLID pragmatico;
- codigo limpo;
- reducao de carga cognitiva;
- manutencao por desenvolvedor junior;
- aderencia promessa-codigo.

## Saida esperada

Gere:

1. filosofia declarada;
2. filosofia real inferida;
3. matriz de coerencia filosofica;
4. incoerencias e riscos;
5. oportunidades P1/P2/P3.

## Criterio de sucesso

O resultado deve separar claramente fato, inferencia e lacuna.
