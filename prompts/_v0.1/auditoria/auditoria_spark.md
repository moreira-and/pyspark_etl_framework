# Auditoria Spark v0.1

Voce atuara como arquiteto senior de Spark em producao.

## Escopo Correto

A v0.1 e um framework interno simples para padronizar pipelines PySpark. Ela
nao promete producao irrestrita, `SafeLoad`, rollback, retry seguro,
idempotencia automatica, observabilidade externa nem benchmark universal.

Use `prompts/_v0.1/promessas.md` para separar promessa real de expectativa
produtiva de uma pipeline concreta.

## Avaliar

- `auto_check_source` e ausencia de acoes Spark.
- `auto_validate_target` e uso intencional de `limit(1).count()`.
- helpers que executam `count`, `collect`, `show`, `groupBy`, `distinct` ou
  `exceptAll`.
- risco de shuffle, skew, particionamento e small files em pipelines concretas.
- `dry_run` e seu limite apos `_extract`/`auto_check`.
- exposicao de dados por `dry_run_show_rows > 0`.
- necessidade de benchmark por pipeline antes de alto volume.

## Nao Fazer

Nao classifique falta de `SafeLoad`, rollback ou idempotencia automatica como
bug P0 do framework v0.1. Classifique como risco operacional de pipeline ou
roadmap v0.2, exceto quando houver promessa ativa contraditoria.

## Saida

1. Veredito Spark para o framework v0.1.
2. Veredito separado para pipeline produtiva concreta.
3. Tabela de acoes Spark conhecidas e custo.
4. Benchmarks minimos necessarios.
5. Testes de regressao obrigatorios.
6. Riscos residuais e mitigacoes.
7. Backlog priorizado com criterio de aceite.
