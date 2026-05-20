# Auditoria SR18 - Seguranca Basica e Dados Sensiveis

## Papel do agente

Atue como especialista em seguranca pragmatica para pipelines de dados.

## Objetivo unico

Avaliar riscos basicos de exposicao de dados sensiveis, principalmente em logs,
erros, dry-run e exemplos.

## Entradas obrigatorias

- `etl_framework/`
- `README.md`
- `QUICK_START.md`
- `docs/v0.1-contract.md`
- `docs/v0.1-known-limitations.md`
- `docs/observability/logging.md`
- testes relacionados a dry-run, erros e observabilidade

## Escopo

Foque em exposicao por `dry_run_show_rows`, mensagens de erro, eventos, metricas,
diagnosticos de invalidos e exemplos. Nao exija modelo completo de seguranca,
criptografia ou governanca regulatoria, pois isso nao esta prometido pela v0.1.

## Perguntas obrigatorias

- O framework pode imprimir dados por padrao?
- `dry_run_show_rows` esta documentado como risco?
- Erros e logs evitam coletar linhas de dados?
- Diagnosticos de invalidos expõem valores sensiveis?
- Exemplos podem induzir uso inseguro?
- Existem lacunas que importam mesmo para uma v0.1 interna?

## Evidencias obrigatorias

- local onde dados podem ser exibidos ou coletados;
- configuracao necessaria;
- documentacao de risco;
- teste que prova ou lacuna;
- impacto operacional.

## Criterios de avaliacao

- robustez operacional;
- rastreabilidade sem vazamento;
- aderencia ao escopo v0.1;
- facilidade para junior;
- baixo risco de mau uso.

## Saida esperada

Gere:

1. matriz fonte de exposicao -> condicao -> mitigacao;
2. riscos P1/P2/P3;
3. recomendacoes objetivas.

## Criterio de sucesso

Toda recomendacao deve ser proporcional a uma v0.1 interna e nao transformar a
biblioteca em plataforma de seguranca.
