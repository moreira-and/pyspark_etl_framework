# Auditoria SR10 - SOLID Pragmatico e Codigo Limpo

## Papel do agente

Atue como arquiteto senior Python avaliando SOLID sem dogmatismo.

## Objetivo unico

Avaliar se os principios SOLID e codigo limpo estao ajudando a v0.1 ou viraram
ornamento arquitetural.

## Entradas obrigatorias

- `etl_framework/contracts/`
- `etl_framework/infra/`
- `etl_framework/utils/`
- `etl_framework/models/`
- testes relevantes

## Escopo

Foque em responsabilidade unica, coesao, acoplamento, nomes, tamanho de metodos,
duplicacao, clareza de excecoes e separacao pragmatica de responsabilidades.

Nao faca uma auditoria ampla de overengineering. Aqui o foco e qualidade de
design no codigo existente.

## Perguntas obrigatorias

- Cada classe tem responsabilidade clara?
- Existem responsabilidades que deveriam estar juntas mas foram separadas por
  estetica?
- Existem responsabilidades misturadas que dificultam manutencao?
- O codigo favorece extensao real ou cria complexidade artificial?
- Os nomes comunicam comportamento?
- As dependencias apontam em direcao coerente?

## Evidencias obrigatorias

- classe/metodo analisado;
- principio afetado;
- impacto pratico;
- evidencia em testes ou uso;
- recomendacao objetiva.

## Criterios de avaliacao

- SOLID pragmatico;
- codigo limpo;
- simplicidade;
- manutencao;
- facilidade para junior;
- aderencia a filosofia.

## Saida esperada

Gere:

1. diagnostico de SOLID pragmatico;
2. pontos onde design ajuda;
3. pontos onde design atrapalha;
4. oportunidades P1/P2/P3.

## Criterio de sucesso

O relatorio deve evitar dogma e sempre conectar o achado a manutencao ou risco
real da v0.1.
