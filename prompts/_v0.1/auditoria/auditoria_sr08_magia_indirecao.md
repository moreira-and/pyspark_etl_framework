# Auditoria SR08 - Magia, Indirecao e Comportamento Escondido

## Papel do agente

Atue como arquiteto pragmatico com foco em comportamento previsivel.

## Objetivo unico

Identificar pontos onde o framework esconde comportamento critico atras de
decorators, template methods, helpers ou convencoes implicitas.

## Entradas obrigatorias

- `etl_framework/contracts/`
- `etl_framework/infra/`
- `etl_framework/utils/`
- testes que cobrem observabilidade, erros e fluxo

## Escopo

Procure comportamento automatico que altera dados, erros, logs, metricas,
contexto, execucao Spark ou ordem do pipeline.

Nao classifique automaticamente toda indirecao como ruim. Avalie beneficio e
custo para a v0.1.

## Perguntas obrigatorias

- Que comportamento relevante acontece sem chamada explicita do autor da
  pipeline?
- A indirecao melhora simplicidade ou dificulta tracing?
- Existem decorators ou helpers que ocultam falhas?
- Existe acoplamento implicito entre modulos?
- Um junior conseguiria prever side effects?

## Evidencias obrigatorias

- arquivo/metodo onde o comportamento e definido;
- ponto onde o comportamento e acionado;
- efeito observavel;
- beneficio pretendido;
- custo cognitivo ou operacional.

## Criterios de avaliacao

- previsibilidade;
- simplicidade;
- rastreabilidade de falhas;
- codigo limpo;
- manutencao;
- facilidade para junior.

## Saida esperada

Gere:

1. inventario de comportamentos escondidos;
2. classificacao: aceitavel, fragil ou perigoso;
3. oportunidades P1/P2/P3.

## Criterio de sucesso

Cada achado deve demonstrar side effect real ou risco plausivel com evidencia.
