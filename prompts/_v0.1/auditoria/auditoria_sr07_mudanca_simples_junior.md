# Auditoria SR07 - Mudanca Simples por Desenvolvedor Junior

## Papel do agente

Atue como lider tecnico avaliando manutencao por desenvolvedores junior.

## Objetivo unico

Simular mudancas simples e medir a friccao para um desenvolvedor junior
trabalhar com o framework sem conhecimento do autor principal.

## Entradas obrigatorias

- `README.md`
- `QUICK_START.md`
- `docs/v0.1-contract.md`
- `etl_framework/`
- `tests/fixtures/pipeline_author_journey/`
- testes de jornada do autor de pipeline

## Escopo

Avalie tarefas comuns:

- criar pipeline minima;
- adicionar regra simples no `target_struct`;
- trocar politica de colunas extras;
- usar `dry_run`;
- entender falha de schema;
- evitar persistir colunas tecnicas no destino.

Nao faca auditoria geral de DX. O foco e manutencao e autonomia de junior em
mudancas pequenas.

## Perguntas obrigatorias

- O caminho feliz e facil de seguir?
- O junior precisa conhecer internals para fazer mudanca simples?
- A documentacao aponta para o comportamento certo?
- Erros ajudam a corrigir o problema?
- Ha convencoes implicitas demais?
- A fixture de jornada ensina o uso real?

## Evidencias obrigatorias

- passos necessarios por tarefa;
- arquivos que o junior precisaria abrir;
- pontos de duvida ou ambiguidade;
- mensagens de erro relevantes;
- teste ou exemplo que demonstra a tarefa.

## Criterios de avaliacao

- facilidade para junior;
- developer experience;
- manutencao;
- simplicidade;
- aderencia a filosofia;
- rastreabilidade de falhas.

## Saida esperada

Gere:

1. matriz tarefa -> friccao -> evidencia;
2. riscos de dependencia do autor principal;
3. oportunidades P1/P2/P3.

## Criterio de sucesso

A auditoria deve distinguir dificuldade natural de PySpark de dificuldade criada
pelo framework.
