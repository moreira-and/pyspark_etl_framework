# Backlog De Hardening v0.1

| ID | Prioridade | Problema | Evidencia | Decisao | Arquivos provaveis | Teste obrigatorio | Criterio de aceite |
| -- | ---------- | -------- | --------- | ------- | ------------------ | ----------------- | ------------------ |
| P1-001 | P1 | Falha de `certify` tambem podia gerar `load_failed`. | `Pipeline._run_load` era o stage `load` externo sobre `Load.run`. | corrigir agora | `etl_framework/contracts/load.py`, `etl_framework/contracts/pipeline.py`, `tests/test_pipeline_contract.py` | `test_certify_failure_is_not_logged_as_load_failure` | Falha em `_certify` gera `CertifyError`, `certify_failed`, `run_failed` e nenhum `load_failed`. |
| P1-002 | P1 | Check declarativo invalido nao tinha regressao pipeline-level bloqueando `load`. | Testes cobriam helper, nao o fluxo completo. | testar agora | `tests/test_auto_contract.py` | `test_auto_validate_blocks_invalid_target_record_before_load` | `ValidateError` inclui `run_id` e `Load` nao e chamado. |
| P1-003 | P1 | Contrato documental de acoes Spark era contraditorio. | Docs prometiam ausencia de `count`; codigo usa `limit(1).count()`. | documentar limitacao | `README.md`, `docs/v0.1-contract.md`, `docs/v0.1-known-limitations.md`, `docs/development/testing.md` | Matriz requisito -> teste | Docs declaram o `count` intencional do `auto_validate`. |
| P1-004 | P1 | `is_valid` no `Load._load` era implicito. | `auto_validate` adiciona `is_valid`; docs nao declaravam a responsabilidade do `Load`. | documentar limitacao | `README.md`, `QUICK_START.md`, `docs/v0.1-contract.md`, `docs/v0.1-known-limitations.md` | Matriz requisito -> teste | Docs dizem que o load concreto deve projetar/remover a coluna se necessario. |
| P2-001 | P2 | Teste de safe load podia sugerir API publica inexistente. | `tests/test_safe_load_contract.py` implementa `SafeMemoryLoad` local. | documentar limitacao | `tests/test_safe_load_contract.py`, `docs/roadmap/v0.2.md` | N/A | Arquivo deixa claro que e exemplo de pipeline concreta/v0.2. |
| P2-002 | P2 | `dry_run` nao reduz custo de leitura. | Limite aplicado apos `auto_check`. | manter como debito | `docs/v0.1-known-limitations.md` | Ja coberto por `test_dry_run_limits_after_check_and_skips_load` | Limite permanece documentado. |
| P2-003 | P2 | Preflight de `source_struct`/`target_struct` falha tarde. | Campos opcionais no config. | manter como debito | `etl_framework/models/config.py`, `etl_framework/contracts/pipeline.py` | Futuro teste de preflight | Avaliar sem ampliar API v0.1. |
| P3-001 | P3 | Validacoes de config/context podem ter mensagens melhores. | Erros Python genericos em tipos errados. | manter como debito | `etl_framework/models/config.py`, `etl_framework/models/context.py` | Unit tests baratos | Melhorar DX em patch futuro. |

## Regra De Priorizacao

P0 nao foi identificado. Todos os P1 foram selecionados para correcao, teste ou
documentacao antes do fechamento. P2/P3 permanecem como debito documentado por
nao violarem `dry_run`, rastreabilidade, contrato principal ou uso controlado da
v0.1.
