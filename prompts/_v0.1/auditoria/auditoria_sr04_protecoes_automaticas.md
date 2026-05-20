# Auditoria SR04 - Protecoes Automaticas da v0.1

## Papel do agente

Atue como especialista em contratos de dados e qualidade estrutural em PySpark.

## Objetivo unico

Validar se as protecoes automaticas prometidas pela v0.1 realmente existem,
falham cedo quando necessario e reduzem erro humano.

## Entradas obrigatorias

- `etl_framework/contracts/`
- `etl_framework/utils/validate_struct.py`
- `etl_framework/utils/dataframe_checks.py`
- `etl_framework/utils/auto_quality.py`
- `etl_framework/utils/production_checks.py`
- `etl_framework/models/config.py`
- testes de auto_check, auto_validate, preflight, dry-run e schema
- `docs/v0.1-contract.md`
- `docs/v0.1-known-limitations.md`

## Escopo

Foque em preflight, `source_struct`, `target_struct`, `auto_check`,
`auto_validate`, `extra_columns_policy`, `strict_schema`,
`keep_technical_columns` e checks declarativos em metadata.

Nao audite performance em profundidade, exceto quando uma acao Spark fizer
parte da protecao.

## Perguntas obrigatorias

- A execucao falha antes de acessar a origem quando falta contrato obrigatorio?
- `auto_check` valida schema da origem conforme documentado?
- `auto_validate` valida o resultado e bloqueia invalidos antes do load?
- Checks SQL declarativos sao simples, previsiveis e rastreaveis?
- As politicas de colunas extras sao coerentes?
- As protecoes reduzem erro humano ou criam falsa seguranca?

## Evidencias obrigatorias

- comportamento por configuracao;
- erros esperados e reais;
- testes que provam caminho feliz e caminho de falha;
- acoes Spark executadas intencionalmente;
- lacunas entre protecao prometida e implementada.

## Criterios de avaliacao

- aderencia a promessa v0.1;
- robustez operacional;
- rastreabilidade de falhas;
- simplicidade;
- manutencao;
- facilidade para junior.

## Saida esperada

Gere:

1. matriz protecao -> promessa -> implementacao -> teste;
2. riscos de falsa seguranca;
3. lacunas P1/P2/P3;
4. recomendacoes objetivas com evidencia.

## Criterio de sucesso

Cada protecao deve ter status claro: confiavel, parcial, fragil ou ausente.
