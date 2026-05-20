# Auditoria SR16 - Qualidade, Manutencao e Valor dos Testes

## Papel do agente

Atue como especialista em testes Python para frameworks internos.

## Objetivo unico

Avaliar se os testes ajudam evolucao, refatoracao e onboarding ou se viraram
burocracia fragil.

## Entradas obrigatorias

- `tests/`
- `tests/fixtures/`
- `docs/development/testing.md`
- `pyproject.toml`

## Escopo

Foque em clareza, dependencia de implementacao, fragilidade, excesso de mocking,
fixtures, duplicacao, granularidade e legibilidade.

Nao reavalie se cada promessa tem teste; isso pertence a auditoria SR15.

## Perguntas obrigatorias

- Os testes contam a historia do framework?
- Um junior aprenderia o uso pelos testes?
- Existem testes cosmeticos ou redundantes?
- Existem testes acoplados demais a detalhes internos?
- As fixtures ajudam ou escondem complexidade?
- Os testes dao confianca para refatorar?

## Evidencias obrigatorias

- arquivos de teste analisados;
- exemplos de assert de alto ou baixo valor;
- fixtures problematicas ou boas;
- duplicacoes relevantes;
- lacunas de legibilidade.

## Criterios de avaliacao

- manutencao;
- facilidade para junior;
- codigo limpo nos testes;
- simplicidade;
- evolucao segura;
- aderencia a filosofia.

## Saida esperada

Gere:

1. diagnostico da suite;
2. testes de alto valor;
3. testes frageis ou burocraticos;
4. oportunidades P1/P2/P3.

## Criterio de sucesso

Cada critica deve explicar como afeta evolucao ou confianca na v0.1.
