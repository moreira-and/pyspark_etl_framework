# Auditoria SR02 - Filosofia Real do Projeto

## Escopo e restricoes

Fato: esta auditoria usou o prompt `prompts/_v0.1/auditoria/auditoria_sr02_filosofia_real.md`, as entradas obrigatorias `README.md`, `docs/v0.1-contract.md`, `docs/v0.1-known-limitations.md`, `etl_framework/` e `tests/`, alem de `QUICK_START.md` e `pyproject.toml` como documentacao/codigo de suporte diretamente relacionados a filosofia de uso e dependencia.

Fato: nao foram lidos relatórios de outras auditorias nem arquivos em `docs/audits/reports/v0_1/independent`.

Lacuna: esta auditoria nao executou a suite. A analise e estatica e baseada em codigo, documentacao e testes existentes como evidencia de intencao.

## 1. Filosofia declarada

Fato: a declaracao central e que `etl_framework` e um framework interno simples para padronizar pipelines ETL em PySpark, reduzir carga cognitiva de desenvolvedores junior e tornar o fluxo previsivel (`README.md:3`, `README.md:6`).

Fato: a v0.1 declara escopo estreito: `extract -> check -> transform -> validate -> load -> certify`, com foco em ordem de execucao, injecao de `SparkSession`, `EtlRunConfig` e `EtlExecutionContext`, preflight, `auto_check`, `auto_validate`, eventos best-effort, erros gerenciados e `dry_run` (`README.md:13`, `README.md:25`, `README.md:28`).

Fato: o contrato rejeita a identidade de plataforma: nao e orquestrador, catalogo, plataforma de observabilidade, motor completo de data quality nem camada transacional (`docs/v0.1-contract.md:6`, `docs/v0.1-contract.md:11`).

Fato: a documentacao explicita o Template Method como mecanismo oficial dos contratos (`docs/v0.1-contract.md:30`) e define API raiz pequena: `Pipeline`, `EtlRunConfig`, `EtlExecutionContext`, `Extract`, `Transform`, `Load` (`docs/v0.1-contract.md:283`).

Fato: as limitacoes sao declaradas com clareza: sem load seguro generico, staging, commit atomico, rollback, retry seguro, idempotencia automatica, certificacao real de destino, observabilidade externa ou entrega garantida de eventos (`README.md:97`, `README.md:99`, `docs/v0.1-known-limitations.md:91`, `docs/v0.1-known-limitations.md:95`).

Inferencia: a filosofia declarada nao e "framework rico"; e "contrato linear com automacao estrutural minima, barato de entender, honesto sobre limites e sem esconder Spark".

## 2. Filosofia real inferida

Fato: a API publica raiz e pequena e coincide com o contrato documentado (`etl_framework/__init__.py`; `docs/v0.1-contract.md:283`).

Fato: `Pipeline.run()` e linear e curto: preflight, extract, transform, load, retorno do DataFrame (`etl_framework/contracts/pipeline.py:45`). O preflight falha antes da origem quando faltam `source_struct` ou `target_struct` (`etl_framework/contracts/pipeline.py:53`, `etl_framework/contracts/pipeline.py:57`, `etl_framework/contracts/pipeline.py:60`).

Fato: `Extract.run()` executa `_extract`, `auto_check_source`, `_custom_check` e aplica limite de dry-run depois do check (`etl_framework/contracts/extract.py:27`, `etl_framework/contracts/extract.py:76`, `etl_framework/contracts/extract.py:82`, `etl_framework/contracts/extract.py:91`).

Fato: `Transform.run()` executa `_transform`, `auto_validate_target` e `_custom_validate` (`etl_framework/contracts/transform.py:26`, `etl_framework/contracts/transform.py:70`, `etl_framework/contracts/transform.py:76`).

Fato: `Load.run()` separa `dry_run` de escrita real, pula `_load` e `_certify` quando `dry_run=True`, e permite remover colunas tecnicas via `keep_technical_columns` (`etl_framework/contracts/load.py:27`, `etl_framework/contracts/load.py:35`, `etl_framework/contracts/load.py:67`, `etl_framework/contracts/load.py:129`, `etl_framework/contracts/load.py:132`).

Fato: a implementacao evita dependencias de runtime alem de Python e PySpark (`pyproject.toml:11`, `pyproject.toml:12`), e ha testes para impedir dependencias/frameworks proibidos (`tests/test_dependencies.py:51`, `tests/test_dependencies.py:72`, `tests/test_dependencies.py:90`).

Inferencia: a filosofia real e majoritariamente a declarada: um micro-framework de contrato, nao uma plataforma. A estrutura central e pequena, usa abstrações conhecidas e concentra a sequencia oficial no framework.

Inferencia severa: a simplicidade real vale mais para quem consome o caminho feliz do que para quem precisa criar schemas, declarar checks, entender `is_valid`, interpretar eventos e diagnosticar falhas. A v0.1 reduz codigo repetitivo, mas nao reduz igualmente o modelo mental.

## 3. Matriz de coerencia filosofica

| Principio declarado | Evidencia factual | Inferencia | Lacuna | Veredito |
| --- | --- | --- | --- | --- |
| Fluxo linear previsivel | `Pipeline.run()` chama `_preflight`, `extract`, `transform`, `load` em ordem (`etl_framework/contracts/pipeline.py:45`); teste de ordem oficial (`tests/test_pipeline_contract.py:295`). | Sustentado no core. | Nao avaliado comportamento de pipelines reais fora dos testes. | Coerente. |
| API pequena | `__all__` exporta apenas seis simbolos; contrato exige API raiz pequena (`docs/v0.1-contract.md:283`). | A superficie cotidiana e enxuta. | Helpers avancados continuam importaveis por caminhos internos. | Coerente, com borda avancada exposta. |
| Junior implementa so ETL | Teste afirma ausencia de `logger`, `observability`, `event_logger` nas classes da jornada (`tests/test_pipeline_author_journey.py:182`, `tests/test_pipeline_author_journey.py:187`). | O junior nao precisa instrumentar manualmente. | O junior ainda precisa escrever subclasses, `StructType`, metadata SQL e configurar load/dry-run. | Parcialmente coerente. |
| Checks estruturais automaticos | `auto_check_source` exige `source_struct` (`etl_framework/utils/auto_quality.py:15`, `etl_framework/utils/auto_quality.py:24`); `auto_validate_target` exige `target_struct` (`etl_framework/utils/auto_quality.py:34`, `etl_framework/utils/auto_quality.py:44`). | Automacao real, nao apenas documentada. | Nao ha DSL propria; regras dependem de SQL Spark em metadata. | Coerente, mas cognitivo. |
| Nao esconder Spark | Documentacao afirma facilitar sem esconder Spark (`docs/v0.1-contract.md:6`); Quick Start usa `SparkSession`, `functions`, `StructType`, `DataFrame.write` (`QUICK_START.md:25`, `QUICK_START.md:61`, `QUICK_START.md:74`). | Honestidade tecnica preservada. | O limite entre "nao esconder Spark" e "exigir Spark demais do junior" nao esta formalizado. | Coerente com risco de adoção. |
| Nao ser plataforma DataOps | Limitacoes negam load seguro e garantias produtivas (`README.md:97`, `docs/v0.1-known-limitations.md:91`). | A documentacao evita marketing tecnico exagerado. | `production_checks` e observabilidade estruturada podem sugerir maturidade maior que a v0.1 entrega. | Coerente, mas com sinal de expansao. |
| Eventos best-effort | ObservabilityService engole falhas de sink (`etl_framework/infra/observability.py:37`, `etl_framework/infra/observability.py:41`, `etl_framework/infra/observability.py:56`); contrato diz best-effort (`docs/v0.1-contract.md:241`, `docs/v0.1-contract.md:268`). | Alinhado com promessa e limite. | Falha silenciosa pode dificultar suporte se usuario espera trilha confiavel. | Coerente, com risco operacional conhecido. |
| Simplicidade arquitetural | Contratos usam Template Method; testes bloqueiam import direto de observability no pacote de contratos (`tests/test_runtime_observability_architecture.py:15`, `tests/test_runtime_observability_architecture.py:27`). | Ha disciplina arquitetural. | Decorators escondem parte do fluxo de erro/evento fora dos contratos. | Parcialmente coerente. |
| Baixo custo Spark no core | `auto_check_source` nao executa acoes; teste protege isso (`tests/test_auto_contract.py:141`). | Intencao de custo explicito existe. | `auto_validate_target` executa `limit(1).count`, `count` e `collect` em diagnostico (`etl_framework/utils/auto_quality.py:62`, `etl_framework/utils/auto_quality.py:81`, `etl_framework/utils/auto_quality.py:99`). | Coerente com contrato, mas nao simples para volume. |
| Load nao seguro generico | `Load._load` e abstrato; Quick Start avisa que `QuickLoad` e didatico (`QUICK_START.md:6`, `etl_framework/contracts/load.py:140`). | O core nao vende escrita segura. | `write_mode` aceita `append`/`overwrite` sem revisao automatica (`etl_framework/models/config.py:146`). | Coerente, mas perigoso se usado sem maturidade externa. |

## 4. Incoerencias e riscos

### Risco 1 - A promessa de baixa carga cognitiva e mais forte que a experiencia minima

Fato: a documentacao diz reduzir carga cognitiva de desenvolvedores junior (`README.md:6`) e que o junior nao precisa chamar manualmente validadores (`README.md:52`).

Fato: o Quick Start minimo exige imports PySpark, `StructType`, `StructField`, metadata de checks SQL, tres subclasses, `EtlRunConfig`, `write_mode`, `dry_run`, `dry_run_limit` e entendimento de `nullable=False` versus check SQL (`QUICK_START.md:25`, `QUICK_START.md:40`, `QUICK_START.md:61`, `QUICK_START.md:69`, `QUICK_START.md:74`, `QUICK_START.md:82`, `QUICK_START.md:90`, `QUICK_START.md:156`).

Inferencia: o projeto reduz carga operacional repetitiva, mas nao entrega ainda uma jornada verdadeiramente simples para junior. A promessa "junior-friendly" depende de treinamento implicito em PySpark, schema metadata e semantica de validacao.

Risco: junior consegue seguir exemplo, mas pode errar silenciosamente em pontos semanticos: achar que `nullable=False` bloqueia nulo, persistir `is_valid` sem querer, usar `overwrite` direto, ou assumir que dry-run reduz custo de leitura.

### Risco 2 - Simplicidade do core convive com sofisticacao de runtime escondida

Fato: `stage` resolve config/context por introspeccao de `self` ou kwargs, emite eventos, embrulha excecoes, mede duracao e pode emitir resumo (`etl_framework/infra/stage.py:27`, `etl_framework/infra/stage.py:45`, `etl_framework/infra/stage.py:49`, `etl_framework/infra/stage.py:58`, `etl_framework/infra/stage.py:73`).

Fato: contratos nao importam observabilidade diretamente, e testes protegem essa separacao (`tests/test_runtime_observability_architecture.py:15`, `tests/test_runtime_observability_architecture.py:27`, `tests/test_runtime_observability_architecture.py:42`).

Inferencia: a arquitetura e limpa para o autor da pipeline, mas a depuracao real exige conhecer decorators, servico global de observabilidade, erro gerenciado e payloads. Isso e sofisticacao arquitetural, ainda que bem isolada.

Risco: a filosofia declarada de simplicidade pode degradar quando uma falha ocorre fora do caminho feliz, especialmente em erro de sink de observabilidade, erro sanitizado ou exception wrapping.

### Risco 3 - Complexidade de qualidade esta concentrada no framework, mas a semantica fica espalhada

Fato: `validate_struct` valida schema, extrai checks recursivos, valida regras SQL, adiciona `is_valid`, opcionalmente sumariza, e suporta batching de summaries (`etl_framework/utils/validate_struct.py:18`, `etl_framework/utils/validate_struct.py:47`, `etl_framework/utils/validate_struct.py:50`, `etl_framework/utils/validate_struct.py:53`, `etl_framework/utils/validate_struct.py:326`).

Fato: `SchemaMetadataValidator` valida metadata declarativa e alerta quando a regra nao referencia o campo (`etl_framework/utils/schema_metadata.py:12`, `etl_framework/utils/schema_metadata.py:54`, `etl_framework/utils/schema_metadata.py:72`, `etl_framework/utils/schema_metadata.py:112`).

Fato: `auto_validate_target` adiciona diagnostico e executa acoes Spark para invalidos (`etl_framework/utils/auto_quality.py:78`, `etl_framework/utils/auto_quality.py:81`, `etl_framework/utils/auto_quality.py:92`, `etl_framework/utils/auto_quality.py:99`).

Inferencia: a decisao de qualidade nao esta espalhada em pipelines, o que e positivo. Mas a semantica completa esta dividida entre config, metadata validator, validate_struct, auto_quality, production_checks, docs e testes.

Risco: para manutencao por junior, a regra real "o que bloqueia load" nao esta em um unico lugar mental. Ela depende de `severity`, SQL Spark, `is_valid`, `auto_validate`, e do fato documentado de que `nullable=False` nao bloqueia nulo (`docs/v0.1-contract.md:180`; `tests/test_auto_contract.py:237`, `tests/test_auto_contract.py:256`).

### Risco 4 - `production_checks` aproxima o projeto de plataforma sem assumir o contrato de plataforma

Fato: a v0.1 declara que nao entrega engine completa de qualidade nem garantia produtiva irrestrita (`docs/v0.1-known-limitations.md:91`).

Fato: `production_checks` oferece checagens de chave nula, unicidade, volume, freshness, reconciliacao, invalidos e metricas operacionais, varias com acoes Spark (`etl_framework/utils/production_checks.py:19`, `etl_framework/utils/production_checks.py:37`, `etl_framework/utils/production_checks.py:52`, `etl_framework/utils/production_checks.py:86`, `etl_framework/utils/production_checks.py:105`, `etl_framework/utils/production_checks.py:163`).

Inferencia: esses helpers sao pragmaticos, mas puxam a percepcao do framework para "quase pronto para producao". A documentacao tenta conter isso chamando-os de opcionais/operacionais, mas a existencia deles aumenta a ambiguidade filosofica.

Risco: usuarios podem tratar helpers como selo produtivo, apesar de o proprio projeto negar staging, rollback, idempotencia e certificacao real.

### Risco 5 - O contrato aceita configuracoes perigosas porque delega seguranca produtiva para fora

Fato: `EtlRunConfig.write_mode` aceita `append` e `overwrite` (`etl_framework/models/config.py:146`), e o Quick Start mostra escrita parquet com `mode(config.write_mode or "overwrite")` dentro de `QuickLoad` (`QUICK_START.md:74`).

Fato: a documentacao avisa que o exemplo e didatico e que escrita real exige checklist senior (`QUICK_START.md:6`, `QUICK_START.md:131`, `QUICK_START.md:154`).

Inferencia: isso e coerente com "nao oferecer load seguro generico", mas cria uma armadilha de aderencia real: o framework padroniza caminho ate o load e depois deixa a decisao mais perigosa no hook junior/senior.

Risco: em organizacoes sem disciplina externa forte, a promessa de padronizacao pode dar falsa seguranca justamente no ponto de maior impacto operacional.

### Risco 6 - Marketing tecnico exagerado esta controlado, mas nao ausente

Fato: README e contrato recusam explicitamente DataOps completo, observabilidade externa, entrega garantida, idempotencia e transacao (`README.md:97`, `docs/v0.1-contract.md:301`).

Fato: os testes usam nomes como "gets operational guarantees for free" (`tests/test_pipeline_author_journey.py:220`), enquanto a documentacao limita essas garantias a checks estruturais, dry-run e eventos best-effort.

Inferencia: a documentacao principal e honesta. O risco de marketing esta mais nos nomes e na narrativa de testes do que no contrato formal. "Operational guarantees" e uma expressao forte demais para best-effort logs, schema checks e skip de load em dry-run.

## 5. Oportunidades P1/P2/P3

### P1 - Rebaixar a promessa "junior-friendly" para garantias objetivas ou criar uma API de autoria mais guiada

Fato: hoje a experiencia minima exige conhecimento direto de PySpark, metadata SQL e subclasses (`QUICK_START.md:25`, `QUICK_START.md:40`, `QUICK_START.md:61`, `QUICK_START.md:82`).

Recomendacao: escolher uma das duas linhas e aplicar de forma consistente:

- ajustar linguagem para "reduz boilerplate e padroniza ordem para pipelines PySpark simples", sem sugerir que junior opera com baixo contexto;
- ou criar uma camada opcional de autoria guiada, com factories/check builders simples para casos comuns, sem substituir a API raiz.

Impacto esperado: reduz promessa excessiva e diminui erro semantico em `nullable`, checks SQL, `is_valid` e colunas tecnicas.

### P1 - Tornar o risco de escrita real mais dificil de ignorar

Fato: o projeto avisa que load seguro esta fora do core, mas aceita `dry_run=False` e `write_mode` perigoso sem barreira tecnica alem da implementacao concreta (`etl_framework/contracts/load.py:35`, `etl_framework/models/config.py:146`).

Recomendacao: adicionar um mecanismo explicito de reconhecimento de risco para `dry_run=False`, por exemplo `load_reviewed=True`, `unsafe_load_ack=True` ou um hook `Load._preflight_load_readiness()` default que falha quando `dry_run=False` e a pipeline nao declara revisao. Se isso for considerado pesado para v0.1, ao menos mover o aviso para erro opcional de config.

Impacto esperado: alinha a filosofia real com a negativa de load seguro e evita que o caminho feliz ensine escrita produtiva por imitacao.

### P2 - Consolidar a semantica de validacao em um documento/objeto de contrato unico

Fato: a semantica esta dispersa entre `EtlRunConfig`, `SchemaMetadataValidator`, `validate_struct`, `auto_quality`, `production_checks`, README, contrato e Quick Start.

Recomendacao: criar uma pagina curta ou objeto de referencia "Validation semantics v0.1" com regras fechadas: o que valida schema, o que cria `is_valid`, o que bloqueia load, quando Spark action acontece, o que `nullable=False` nao faz, e como `severity` participa.

Impacto esperado: reduz conhecimento implicito e melhora manutencao por desenvolvedor junior sem mexer no core.

### P2 - Separar melhor "core v0.1" de "helpers produtivos avancados"

Fato: `production_checks` contem checagens uteis, mas elas tensionam a identidade de nao ser plataforma (`etl_framework/utils/production_checks.py:19`, `etl_framework/utils/production_checks.py:52`, `etl_framework/utils/production_checks.py:105`).

Recomendacao: documentar e nomear esses helpers como "advanced explicit checks" e manter fora do caminho feliz. Evitar termos que soem como prontidao produtiva automatica.

Impacto esperado: preserva a honestidade do contrato e evita que helpers sejam confundidos com garantia de go-live.

### P2 - Reduzir magia de decorators para quem depura

Fato: `stage` e `runtime_event` concentram erro, evento, tempo, resumo e resolucao de config/context (`etl_framework/infra/stage.py:27`, `etl_framework/infra/stage.py:139`, `etl_framework/infra/stage.py:226`, `etl_framework/infra/stage.py:238`).

Recomendacao: manter os decorators, mas adicionar documentacao de troubleshooting com mapa "metodo chamado -> decorator -> evento -> erro gerenciado". Alternativamente, expor nomes de eventos e wrappers em constantes tipadas para reduzir strings implicitas.

Impacto esperado: melhora suporte sem contaminar contratos com observabilidade.

### P3 - Ajustar linguagem de testes/documentacao para nao inflar garantias

Fato: a documentacao principal e cuidadosa, mas nomes de teste como "operational guarantees for free" podem cristalizar uma promessa mais ampla que o contrato (`tests/test_pipeline_author_journey.py:220`).

Recomendacao: renomear para "operational scaffolding" ou "framework-managed safeguards" e reservar "guarantee" para propriedades realmente fortes.

Impacto esperado: melhora aderencia promessa-codigo e reduz marketing tecnico acidental.

### P3 - Preservar a austeridade de dependencias como principio explicito

Fato: runtime depende apenas de Python e PySpark (`pyproject.toml:11`, `pyproject.toml:12`) e os testes impedem frameworks externos (`tests/test_dependencies.py:51`, `tests/test_dependencies.py:72`).

Recomendacao: declarar isso como principio arquitetural formal da v0.1, nao apenas como detalhe de empacotamento.

Impacto esperado: fortalece a filosofia de simplicidade e evita deriva futura para stack pesada.

## Conclusao

Fato: o codigo sustenta o contrato essencial da v0.1: fluxo linear, API raiz pequena, checks automaticos, preflight, dry-run, erros gerenciados, eventos best-effort e ausencia de dependencias pesadas.

Inferencia: a filosofia real e mais "framework de contrato disciplinado para PySpark" do que "framework simples para junior". A arquitetura e pragmaticamente limpa, mas nao e trivial. O core concentra bastante responsabilidade para proteger pipelines, enquanto a autoria ainda exige entendimento implicito de Spark, schemas, metadata, severidade de checks, colunas tecnicas e limites de load.

Lacuna: nao ha evidencia suficiente, sem pipelines reais fora dos testes, para afirmar que a carga cognitiva cai em uso organizacional. Os testes provam o caminho feliz e algumas falhas importantes, mas nao provam manutencao por junior em cenarios reais, nem prontidao produtiva.

Veredito: aderencia filosofica geral boa, com risco medio-alto de promessa excessiva no eixo "junior-friendly" e risco medio no eixo "nao parecer plataforma". A v0.1 e honesta sobre limites na documentacao formal, mas precisa tornar essa honestidade igualmente inevitavel na experiencia de autoria.
