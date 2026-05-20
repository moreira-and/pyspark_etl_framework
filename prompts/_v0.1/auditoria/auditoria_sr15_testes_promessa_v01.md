# Auditoria SR15 - Testes por Promessa da v0.1

## Papel do agente

Atue como especialista rigoroso em arquitetura de testes.

## Objetivo unico

Mapear se as promessas centrais da v0.1 estao protegidas por testes de
comportamento.

## Entradas obrigatorias

- `tests/`
- `README.md`
- `docs/v0.1-contract.md`
- `docs/v0.1-known-limitations.md`
- `etl_framework/`

## Escopo

Foque em cobertura comportamental das promessas: fluxo oficial, preflight,
auto_check, auto_validate, dry-run, erros gerenciados, observabilidade
best-effort e API publica.

Nao avalie estilo ou fragilidade dos testes em profundidade; isso pertence a
auditoria de qualidade dos testes.

## Perguntas obrigatorias

- Cada promessa critica possui pelo menos um teste?
- Os testes validam comportamento observavel ou detalhe interno?
- Existem promessas sem teste?
- Existem testes que protegem limites declarados da v0.1?
- Existem cenarios P1 sem teste de falha?

## Evidencias obrigatorias

- promessa;
- arquivo de teste;
- comportamento testado;
- caminho feliz ou falha;
- lacuna quando nao existir teste.

## Criterios de avaliacao

- aderencia promessa-codigo;
- robustez operacional;
- rastreabilidade;
- manutencao;
- facilidade de evolucao.

## Saida esperada

Gere:

1. matriz promessa -> teste;
2. promessas sem protecao;
3. testes desalinhados da promessa;
4. oportunidades P1/P2/P3.

## Criterio de sucesso

Nao use percentual de cobertura como criterio principal. O criterio e valor de
protecao comportamental.
