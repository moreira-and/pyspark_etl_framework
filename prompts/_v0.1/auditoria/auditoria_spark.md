Você atuará como arquiteto sênior de plataforma de dados especializado em Apache Spark em produção.

Audite se este framework, que automatiza a criação de pipelines Spark, é tecnicamente viável para processar milhões de linhas em ambiente produtivo.

Não avalie apenas se o código funciona. Avalie se ele é operacionalmente seguro, previsível e sustentável.

Considere:
- Estratégia de particionamento.
- Data skew.
- Shuffle.
- Broadcast joins.
- Joins grandes.
- Escrita particionada.
- Small files.
- Uso de cache/persist.
- Uso indevido de collect(), count(), toPandas() ou ações desnecessárias.
- Controle de memória.
- Configurações Spark.
- Reprocessamento.
- Idempotência.
- Atomicidade de escrita.
- Tratamento de falhas.
- Retentativas.
- Observabilidade.
- Métricas.
- Logs.
- Alertas.
- Lineage.
- Validação de schema.
- Data quality.
- Evolução de schema.
- Compatibilidade com diferentes ambientes.
- Custo de execução.
- Debuggability dos pipelines gerados.

Sua resposta deve conter:
1. Veredito de production readiness.
2. Riscos de escala.
3. Riscos de confiabilidade.
4. Riscos de operação.
5. Riscos de manutenção.
6. Evidências encontradas.
7. Evidências ausentes.
8. Benchmarks necessários.
9. Testes de carga mínimos.
10. Critérios objetivos para aprovar produção.

Sempre diferencie:
- Funcionar em ambiente local.
- Funcionar em dados pequenos.
- Funcionar em batch real.
- Funcionar em produção com milhões de linhas.