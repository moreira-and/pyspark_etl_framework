# Promessas Da v0.1

Resumo sucinto do que foi prometido para a v0.1 do `etl_framework`.

## Promessa Central

A v0.1 promete ser um framework interno simples para padronizar pipelines ETL em
PySpark, reduzir carga cognitiva de desenvolvedores junior e automatizar
controles estruturais basicos sem esconder Spark.

## O Que Foi Prometido

- Fluxo oficial e previsivel:
  `extract -> check -> transform -> validate -> load -> certify`.
- Injecao de `SparkSession`, `EtlRunConfig` e `EtlExecutionContext` nas etapas.
- Contratos `Extract`, `Transform` e `Load` organizados por Template Method.
- `Extract.run()` executa `_extract`, valida retorno como `DataFrame` e roda
  `auto_check` com `source_struct`.
- `Transform.run()` executa `_transform`, valida retorno como `DataFrame` e roda
  `auto_validate` com `target_struct`.
- `Load.run()` valida entrada como `DataFrame`, executa `dry_run` quando
  configurado e, fora de `dry_run`, chama `_load` e `_certify`.
- `source_struct` protege nomes e tipos da origem.
- `target_struct` protege nomes, tipos e checks declarativos do resultado.
- Checks declarativos em `StructField.metadata["checks"]` podem bloquear
  registros invalidos antes do `load`.
- `auto_validate` adiciona a coluna tecnica `is_valid`.
- Registros com `is_valid=False` ou regra booleana nula sao bloqueados antes do
  `load`.
- `dry_run=True` pula `_load` e `_certify`, registra evidencia tecnica e so
  chama `show` quando `dry_run_show_rows > 0`.
- Logs tecnicos por etapa com `pipeline_name`, `run_id`, stage, status e
  contexto relevante.
- Erros gerenciados por etapa: `ExtractError`, `CheckError`, `TransformError`,
  `ValidateError`, `LoadError` e `CertifyError`.
- API publica pequena: `Pipeline`, `EtlRunConfig`, `EtlExecutionContext`,
  `Extract`, `Transform` e `Load`.
- Testes de contrato protegendo o comportamento prometido.

## O Que Nao Foi Prometido

A v0.1 nao promete:

- load seguro universal;
- `SafeLoad`;
- `LoadStrategy`;
- idempotencia automatica;
- staging padronizado;
- commit atomico;
- rollback;
- retry seguro;
- protecao completa contra duplicidade ou carga parcial;
- quarantine persistente;
- certificacao real lendo o destino;
- engine completa de qualidade de dados;
- observabilidade externa;
- producao irrestrita em grandes volumes sem revisao senior.

## Observacoes Importantes

- `dry_run` reduz risco de escrita, mas nao garante baixo custo de leitura.
- `auto_validate` executa `limit(1).count()` para detectar invalidos antes do
  `load`.
- `Load._load` recebe o `DataFrame` ja validado com a coluna `is_valid`; se o
  destino nao aceitar essa coluna, o load concreto deve remove-la ou projetar
  apenas colunas de negocio.
- `nullable=False` documenta intencao de schema, mas nulos devem ser bloqueados
  com checks SQL explicitos.

## Fontes Ativas

- `README.md`
- `docs/v0.1-contract.md`
- `docs/v0.1-known-limitations.md`
- `docs/audits/reports/v0_1_requirement_test_matrix.md`
