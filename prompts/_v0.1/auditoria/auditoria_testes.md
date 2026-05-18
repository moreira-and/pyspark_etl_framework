# Auditoria De Testes v0.1

Voce atuara como especialista em estrategia de testes para frameworks de
engenharia de dados com Apache Spark.

## Fontes Obrigatorias

- `prompts/_v0.1/promessas.md`
- `docs/v0.1-contract.md`
- `docs/development/testing.md`
- `docs/audits/reports/v0_1_requirement_test_matrix.md`
- suite `tests/`

## Regras

- Avalie testes contra o que a v0.1 promete, nao contra uma plataforma completa
  de producao.
- Separe testes contratuais v0.1 de exemplos didaticos ou v0.2.
- Nao exigir testes de `SafeLoad`, rollback, retry seguro ou idempotencia
  automatica como gate da v0.1.
- Quando uma lacuna for real, proponha teste minimo com nome, comportamento e
  criterio de aceite.

## Avaliar

- ordem oficial do fluxo;
- Template Method dos contratos;
- `auto_check` sem acoes Spark;
- `auto_validate` com `limit(1).count()` limitado e sem exposicao de dados;
- `nullable=False` como intencao, nao bloqueio automatico;
- checks SQL declarativos;
- erros gerenciados por etapa;
- logs sem payload sensivel obvio;
- `dry_run`;
- marcadores pytest e separacao de exemplos `v02_example`;
- compatibilidade entre suporte declarado e CI executado.

## Saida

1. Veredito da suite para framework v0.1.
2. Tabela requisito -> teste -> assert -> lacuna.
3. Testes faltantes com prioridade.
4. Itens fora de escopo v0.1.
5. Comandos de gate e resultado esperado.
6. Riscos residuais aceitos.
