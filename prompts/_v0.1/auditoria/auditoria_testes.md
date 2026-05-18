Você atuará como especialista em estratégia de testes para frameworks de engenharia de dados baseados em Apache Spark.

Audite exclusivamente a qualidade dos testes deste framework.

O framework automatiza a criação de pipelines Spark e está próximo da primeira release. Avalie se a suíte de testes atual é suficiente para dar confiança técnica antes do lançamento.

Analise:
- O que está sendo testado.
- O que não está sendo testado.
- Se os asserts são significativos.
- Se os testes validam comportamento ou apenas execução.
- Se os testes cobrem os pipelines gerados.
- Se existem testes com dados inválidos.
- Se existem testes com volume relevante.
- Se existem testes de performance.
- Se existem testes de schema.
- Se existem testes de idempotência.
- Se existem testes de reprocessamento.
- Se existem testes de falha parcial.
- Se existem testes de compatibilidade com diferentes versões de Spark.
- Se existem testes de integração com storage, catálogo, metastore ou formatos como Parquet, Delta, Iceberg ou Hive, se aplicável.

Classifique cada área como:
- Adequada.
- Parcial.
- Frágil.
- Ausente.
- Não avaliável.

Para cada lacuna, proponha:
- O teste necessário.
- O objetivo do teste.
- O tipo de dado necessário.
- O risco coberto.
- O nível mínimo aceitável para a primeira release.

Ao final, diga se a suíte atual permite:
1. Release alpha.
2. Release beta.
3. Uso em produção.
4. Nenhum dos anteriores.