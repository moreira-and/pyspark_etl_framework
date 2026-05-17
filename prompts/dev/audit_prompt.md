Você é um auditor sênior de engenharia de dados, PySpark e produção. Audite o repositório `spark-etl-framework`, com foco no pacote `etl_framework/`, para decidir se ele está pronto para ser usado em produção processando milhões de linhas por execução, operado por uma equipe enxuta e majoritariamente júnior.

Contexto já observado:
- O pacote `etl_framework` é uma biblioteca interna de contratos ETL em PySpark.
- O fluxo oficial é `extract -> check -> transform -> validate -> load -> certify`.
- O framework coordena execução, logging, erros gerenciados, contexto com `run_id`, configuração e validação estrutural.
- Ele não implementa leitura, transformação ou escrita de negócio; isso fica nas classes concretas `Extract`, `Transform` e `Load`.
- `dry_run` limita dados depois de `_extract` e `_check`, antes de `transform`.
- O caminho normal de produção tenta evitar ações Spark automáticas como `count`, `collect` e `show`.
- `validate_struct` valida schema, adiciona `is_valid`, executa checks declarativos por expressão SQL Spark e só calcula resumo quando `compute_summary=True`.
- Runtime declarado: Python 3.11+ e PySpark 3.5.x, sem dependências externas adicionais além de PySpark.

Arquivos prioritários:
- `etl_framework/contracts/pipeline.py`
- `etl_framework/contracts/extract.py`
- `etl_framework/contracts/transform.py`
- `etl_framework/contracts/load.py`
- `etl_framework/models/config.py`
- `etl_framework/models/context.py`
- `etl_framework/infra/logger.py`
- `etl_framework/infra/errors.py`
- `etl_framework/utils/validate_struct.py`
- `etl_framework/utils/schema_metadata.py`
- `README.md`
- `MANIFEST.md`
- `QUICK_START.md`
- `pyproject.toml`
- `tests/`

Objetivo da auditoria:
Validar se o framework está pronto para adoção em produção com milhões de linhas, considerando simplicidade operacional, previsibilidade, custo Spark, rastreabilidade, segurança, testabilidade e manutenção por desenvolvedores juniores.

Responda com uma avaliação objetiva, não promocional. Diferencie claramente:
1. O que o núcleo do framework já garante.
2. O que depende obrigatoriamente das pipelines concretas.
3. O que impede ou condiciona uso em produção.
4. O que deve ser corrigido antes do go-live.
5. O que pode ser aceito como risco documentado.

Critérios obrigatórios de auditoria:

1. Contrato e previsibilidade
- A ordem `extract -> check -> transform -> validate -> load -> certify` está clara, testada e difícil de violar?
- O framework impede retornos inválidos nas etapas que prometem `DataFrame`?
- Falhas param etapas downstream?
- O contrato é simples o suficiente para uma equipe júnior manter?

2. Escala Spark e custo
- Há ações Spark automáticas no caminho normal de produção?
- Há risco de `count`, `collect`, `show`, summaries, logs ou validações explodirem custo em milhões de linhas?
- `validate_struct` é seguro por padrão?
- `compute_summary=True` está suficientemente sinalizado como caro?
- Há riscos de expressões SQL declarativas causarem planos Spark grandes, lentos ou difíceis de depurar?
- Falta algum guardrail sobre particionamento, cache, persist, checkpoint, skew, broadcast ou shuffle?

3. Escrita, idempotência e recuperação
- `Load` delega escrita concreta. Isso é aceitável?
- O framework deveria impor ou documentar padrões mínimos para idempotência, overwrite/append, deduplicação, atomicidade, retry, rollback ou reprocessamento?
- `target_key`, `write_mode`, `target_path`, `target_schema` e `target_table` são suficientes para orientar produção?
- Há risco de carga parcial sem certificação confiável?

4. Data quality e schema drift
- `source_struct` e `target_struct` são suficientes para drift estrutural?
- Checks declarativos em metadata são seguros, claros e testáveis?
- `is_valid` é bem controlado?
- Faltam validações importantes para produção, como nulos em campos obrigatórios, unicidade de chave, duplicidade, volume esperado, freshness, reconciliação e quarantine?
- O framework deixa claro o que ele não cobre?

5. Observabilidade e suporte
- Logs têm `pipeline_name`, `run_id`, estágio, status, modo e duração?
- Logs são úteis para depuração por time júnior?
- Há risco de vazar dados sensíveis em `error_message`, `df.show` ou mensagens de exceção?
- O framework expõe métricas suficientes para operação: linhas lidas, linhas válidas, linhas inválidas, linhas gravadas, duração por etapa, destino, modo de escrita?
- A ausência de integração nativa com Prometheus, Datadog, CloudWatch, OpenTelemetry ou orquestrador é aceitável ou precisa de extensão?

6. Segurança operacional
- `dry_run` reduz risco de escrita?
- `dry_run_show_rows` pode expor dados sensíveis?
- Há validação de paths, secrets, credenciais ou configurações perigosas?
- O framework evita configuração global implícita?

7. Testes e qualidade
- A suíte cobre contrato, logging, erros, dry run, import público, config, dependências e integração Spark local?
- Há testes de escala ou testes que simulam milhões de linhas?
- Há testes para planos Spark, ausência de ações, comportamento com muitos checks, schemas nested, falhas em escrita e cargas parciais?
- Os testes atuais seriam suficientes como gate de produção?
- Quais testes devem ser adicionados antes do go-live?

8. Packaging, deploy e compatibilidade
- `pyproject.toml` está adequado para empacotamento interno?
- Pin de PySpark `>=3.5,<4.0` é seguro?
- Falta CI, versionamento, publicação em registry interno, changelog, pre-commit ou cobertura mínima obrigatória?
- A API pública em `etl_framework.__init__` está pequena e estável?

9. Documentação e operação por time júnior
- README, QUICK_START e MANIFEST ensinam o uso real?
- Existem exemplos suficientes de pipeline real, load seguro, validação, dry run e tratamento de falhas?
- Faltam runbooks, checklist de produção, troubleshooting, padrões de implementação concreta e exemplos anti-pattern?
- O framework é simples sem ser permissivo demais?

Formato obrigatório da resposta:

## Veredito
Classifique como uma das opções:
- `READY`
- `READY WITH CONDITIONS`
- `NOT READY`

Inclua uma justificativa curta.

## Principais Riscos
Liste achados por severidade:
- `P0`: bloqueia produção
- `P1`: deve corrigir antes do go-live
- `P2`: risco aceitável com mitigação
- `P3`: melhoria futura

Para cada achado, informe:
- Severidade
- Evidência com arquivo e linha aproximada
- Impacto em milhões de linhas
- Impacto para equipe júnior
- Recomendação objetiva
- Teste ou evidência necessária para fechar o risco

## O Que Já Está Bom
Liste garantias reais já presentes no código e nos testes.

## Lacunas Antes de Produção
Liste somente lacunas acionáveis.

## Checklist de Go-Live
Monte uma checklist objetiva para aprovar uma pipeline concreta usando esse framework.

## Plano Mínimo de Correção
Proponha um plano pragmático em fases:
1. Antes do primeiro deploy produtivo
2. Primeiros 30 dias
3. Evolução posterior

Restrições:
- Não proponha transformar o framework em plataforma genérica.
- Não recomende dependências pesadas sem justificativa forte.
- Preserve simplicidade para equipe júnior.
- Separe responsabilidades do framework das responsabilidades das pipelines concretas.
- Prefira recomendações pequenas, testáveis e operacionais.