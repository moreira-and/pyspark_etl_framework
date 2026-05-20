# Auditoria SR17 - Performance e Custo Spark

## Papel do agente

Atue como especialista em PySpark, custo operacional e desenho de pipelines de
dados.

## Objetivo unico

Identificar acoes Spark, custos conhecidos e riscos de performance que impactam
a promessa da v0.1.

## Entradas obrigatorias

- `etl_framework/`
- `tests/`
- `docs/operation/spark-cost-and-benchmark.md`
- `docs/v0.1-contract.md`
- `docs/v0.1-known-limitations.md`
- `QUICK_START.md`

## Escopo

Foque em `count`, `show`, `collect`, `limit`, agregacoes, summaries,
diagnosticos de invalidos, dry-run e helpers operacionais que podem disparar
acoes Spark.

Nao exija garantia de baixo custo irrestrito; a v0.1 declara limites.

## Perguntas obrigatorias

- Quais acoes Spark o framework executa intencionalmente?
- Essas acoes estao documentadas?
- O custo e previsivel para o autor da pipeline?
- `dry_run` pode ser confundido com baixo custo de leitura?
- Existem helpers que parecem baratos mas executam jobs?
- Os testes ou docs ajudam a evitar uso caro acidental?

## Evidencias obrigatorias

- arquivo/metodo que executa acao Spark;
- condicao em que a acao roda;
- documentacao correspondente;
- risco operacional;
- teste ou lacuna.

## Criterios de avaliacao

- custo operacional;
- robustez;
- aderencia promessa-codigo;
- facilidade para junior;
- simplicidade;
- manutencao.

## Saida esperada

Gere:

1. inventario de acoes Spark;
2. matriz acao -> condicao -> custo -> documentacao;
3. riscos P1/P2/P3;
4. recomendacoes objetivas.

## Criterio de sucesso

O relatorio deve diferenciar custo necessario para o contrato v0.1 de custo
acidental ou pouco visivel.
