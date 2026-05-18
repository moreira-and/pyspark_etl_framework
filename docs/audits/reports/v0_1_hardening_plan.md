# Plano De Implementacao v0.1 Hardening

## Objetivo

Fechar uma v0.1 controlada, honesta e pequena, sem implementar v1. A entrega
deve provar os contratos criticos de fluxo, `dry_run`, `run_id`, logs, erros,
`source_struct`, `target_struct` e bloqueio de invalidos antes de `load`.

## Escopo Permitido

- Separar observabilidade de `load` e `certify`.
- Adicionar regressao para regra declarativa que bloqueia `load`.
- Documentar o custo Spark intencional de `auto_validate`.
- Documentar que `Load._load` recebe `is_valid`.
- Reclassificar safe load como exemplo de pipeline concreta/v0.2.
- Criar relatorios de governanca e matriz requisito -> teste.

## Escopo Proibido

- Implementar `SafeLoad`, `LoadStrategy`, rollback, idempotencia, commit atomico
  ou certify real.
- Adicionar dependencia nova.
- Alterar a API publica raiz.
- Refatorar amplamente contratos ou helpers fora do P1.

## Arquivos A Alterar

- `etl_framework/contracts/load.py`
- `etl_framework/contracts/pipeline.py`
- `tests/test_pipeline_contract.py`
- `tests/test_auto_contract.py`
- `tests/test_safe_load_contract.py`
- `README.md`
- `QUICK_START.md`
- `docs/v0.1-contract.md`
- `docs/v0.1-known-limitations.md`
- `docs/development/testing.md`
- `CHANGELOG.md`
- `docs/audits/reports/*.md`

## Ordem De Implementacao

1. Ajustar `Load.run` para executar `_load` via stage `load` proprio e
   `_certify` via stage `certify`.
2. Remover o wrapper `load` externo de `Pipeline._run_load`.
3. Atualizar testes de ordem de eventos e adicionar regressao de certify.
4. Adicionar teste de invalidos bloqueando load via `target_struct`.
5. Reclassificar safe load exemplar.
6. Atualizar documentacao ativa.
7. Criar matriz requisito -> teste e relatorio final.
8. Executar gates possiveis e registrar bloqueios de ambiente.

## Riscos

- A ordem de logs muda para `load_succeeded` antes de `certify_started`; isso e
  intencional porque `load` e `certify` sao etapas distintas.
- Testes Spark dependem de Java 17. Ambiente local com Java 8 nao valida a suite.
- `auto_validate` segue executando `count`; o custo fica documentado.

## Rollback Via Git

Reverter os arquivos alterados neste hardening com um commit reverso ou
`git restore` seletivo dos caminhos listados acima. Nao usar `git reset --hard`
porque a worktree ja continha mudancas de arquivo arquivado antes deste trabalho.

## Criterios De Aceite Final

- P1-001 a P1-004 resolvidos.
- Matriz requisito -> teste criada.
- Documentacao ativa nao promete `SafeLoad`, rollback, idempotencia ou certify
  real.
- Suite completa passa em Java 17/CI.
- Se a suite nao puder rodar localmente, o relatorio final registra a limitacao
  e nao força aprovacao plena.
