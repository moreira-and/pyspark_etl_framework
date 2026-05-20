# Auditoria SR10 - SOLID pragmatico e design de classes

## Escopo executado

Arquivos analisados: `etl_framework/contracts/`, `etl_framework/infra/`,
`etl_framework/utils/`, `etl_framework/models/`, `README.md`,
`docs/v0.1-contract.md` e testes relevantes em `tests/`.

Restricao respeitada: nenhum relatorio de outras auditorias foi lido.

## 1. Diagnostico de SOLID pragmatico

A v0.1 usa SOLID de forma majoritariamente util, nao ornamental. O desenho
principal por Template Method em `Extract`, `Transform`, `Load` e `Pipeline`
reduz variacao perigosa para autores junior e deixa o fluxo oficial
`extract -> check -> transform -> validate -> load -> certify` verificavel em
teste. Essa decisao favorece manutencao real porque centraliza ordem,
observabilidade e erros no framework, enquanto mantem leitura, regra de negocio
e escrita nas pipelines concretas.

O ponto fraco nao e excesso de classes. O risco esta em alguns objetos e funcoes
que juntam politica de execucao, diagnostico, custo Spark e estado global. Esses
pontos ainda sao toleraveis para v0.1, mas ja criam atrito para evolucao:
adicionar uma politica nova de validacao, trocar comportamento de observabilidade
por execucao ou tornar metricas mais seguras exigira editar funcoes centrais com
alto acoplamento.

Veredito severo: o design ajuda a promessa da v0.1, mas ha concentracoes
concretas de responsabilidade em `auto_quality`, `validate_struct`,
`EtlRunConfig`, `EtlExecutionContext.metrics` e no singleton de observabilidade.
Essas concentracoes devem ser tratadas como divida tecnica controlada, nao como
estilo arquitetural desejavel para as proximas versoes.

## 2. Pontos onde design ajuda

### A1 - Template Method dos contratos reduz variacao acidental

- Classe/metodo analisado: `Extract.run`, `_run_extract`, `_run_check`;
  `Transform.run`, `_run_transform`, `_run_validate`; `Load.run`, `_run_load`,
  `_run_certify`; `Pipeline.run`.
- Principio afetado: SRP, OCP e DIP pragmaticos.
- Impacto pratico: a pipeline concreta implementa apenas `_extract`,
  `_transform` e `_load`; a ordem, wrapping de erro e observabilidade ficam no
  framework. Isso evita que cada pipeline junior recrie o fluxo com pequenas
  diferencas.
- Evidencia em codigo: `Pipeline.run()` coordena preflight, extract, transform e
  load em `etl_framework/contracts/pipeline.py:44`; `Extract._run_check()` chama
  `auto_check_source` antes de `_custom_check` em
  `etl_framework/contracts/extract.py:66`; `Transform._run_validate()` chama
  `auto_validate_target` antes de `_custom_validate` em
  `etl_framework/contracts/transform.py:60`.
- Evidencia em testes ou uso: `tests/test_pipeline_contract.py:295` verifica a
  ordem oficial; `tests/test_stage_contract_template_method.py:181` e
  `tests/test_stage_contract_template_method.py:219` verificam check e validate
  automaticos.
- Recomendacao objetiva: manter o Template Method como espinha dorsal da v0.1.
  Evitar expor alternativas paralelas de execucao enquanto a promessa principal
  for reduzir carga cognitiva.

### A2 - Separacao entre porta de observabilidade e sink concreto e boa

- Classe/metodo analisado: `ObservabilitySink`, `ObservabilityService`,
  `StdoutObservabilitySink`, `configure_observability_sink`.
- Principio afetado: DIP e ISP.
- Impacto pratico: o runtime depende de uma porta minima `emit(event)`, nao de
  logging diretamente. Isso permite teste em memoria e troca de sink sem mexer
  nas pipelines.
- Evidencia em codigo: `ObservabilitySink` e um `Protocol` em
  `etl_framework/infra/observability.py:21`; `ObservabilityService` recebe sink
  opcional em `etl_framework/infra/observability.py:44`.
- Evidencia em testes ou uso: `tests/test_pipeline_contract.py:431` comprova
  uso de sink configurado sem dependencia de logger; `tests/test_observability.py:123`
  comprova tolerancia a falha do sink.
- Recomendacao objetiva: preservar a porta pequena. Se crescer, criar metodos
  de conveniencia fora do protocolo, mantendo `emit` como contrato minimo.

### A3 - Erros gerenciados por etapa melhoram rastreabilidade sem vazar para as pipelines

- Classe/metodo analisado: `stage`, `_resolve_stage_error`, `EtlError`,
  `ensure_stage_error`.
- Principio afetado: SRP e codigo limpo.
- Impacto pratico: os hooks concretos podem levantar erros simples e o framework
  acrescenta `pipeline_name`, `run_id` e `stage`. Isso reduz duplicacao em
  pipelines e melhora suporte operacional.
- Evidencia em codigo: wrapping no decorator em
  `etl_framework/infra/stage.py:43`; normalizacao em
  `etl_framework/infra/errors.py:98`.
- Evidencia em testes ou uso: `tests/test_pipeline_contract.py:791` cobre
  wrapping por etapa; `tests/test_pipeline_contract.py:521` evita registrar falha
  de certify como falha de load.
- Recomendacao objetiva: manter familia de erros por etapa. Evitar criar
  excecoes por subtipo de regra enquanto a etapa e a causa ja forem suficientes.

### A4 - Funcoes pequenas de producao sao explicitas sobre acoes Spark

- Classe/metodo analisado: `assert_target_key_not_null`,
  `assert_target_key_unique`, `assert_volume_between`,
  `assert_no_invalid_records`, `split_valid_invalid`.
- Principio afetado: SRP, clareza de nomes e codigo limpo.
- Impacto pratico: os nomes comunicam se ha assert e os docstrings avisam
  quando ha acao Spark. Para v0.1, isso e mais legivel que uma classe generica
  de rules engine.
- Evidencia em codigo: funcoes em `etl_framework/utils/production_checks.py:19`,
  `etl_framework/utils/production_checks.py:37`,
  `etl_framework/utils/production_checks.py:52` e
  `etl_framework/utils/production_checks.py:146`.
- Evidencia em testes ou uso: a filosofia publica documenta que helpers em
  `etl_framework.utils` devem ser explicitos sobre acoes Spark em
  `docs/v0.1-contract.md`.
- Recomendacao objetiva: manter como funcoes simples. Nao transformar em
  hierarquia de classes ate existir variacao real de politica ou destino.

## 3. Pontos onde design atrapalha

### F1 - `auto_validate_target` mistura validacao, bloqueio operacional, diagnostico e custo Spark

- Classe/metodo analisado: `auto_validate_target`,
  `_raise_with_existing_validation_diagnostics`,
  `_raise_with_validation_diagnostics`, `_failed_check_summaries`.
- Principio afetado: SRP e OCP.
- Impacto pratico: qualquer mudanca pequena em politica de invalidos, nivel de
  diagnostico, limite de custo ou formato de erro passa pela mesma funcao. A
  funcao tambem alterna entre transformacao lazy (`validate_struct`) e acoes
  Spark (`limit(1).count()`, `count()`, `collect()` indireto via summary), o que
  torna dificil para mantenedores junior prever custo por leitura do codigo.
- Evidencia em codigo: `auto_validate_target` em
  `etl_framework/utils/auto_quality.py:34` chama `validate_struct` e em seguida
  `_raise_with_validation_diagnostics`; `_raise_with_validation_diagnostics` faz
  probe e contagem em `etl_framework/utils/auto_quality.py:72`; summaries fazem
  collect em `etl_framework/utils/auto_quality.py:91` e
  `etl_framework/utils/validate_struct.py:299`.
- Evidencia em testes ou uso: `tests/test_auto_contract.py:177` valida
  explicitamente o `limit(1).count()`; `tests/test_auto_contract.py:394` valida
  bloqueio antes do load; `README.md` e `docs/v0.1-contract.md` explicam o custo
  intencional.
- Recomendacao objetiva: separar em duas funcoes internas nomeadas por intencao:
  uma para produzir `validated_df` e outra para aplicar politica de bloqueio e
  diagnostico. Nao criar engine nova; apenas tornar custo e decisao operacional
  isolados e testaveis.
- Prioridade: P1.

### F2 - `validate_struct.py` concentra parser de metadata, validacao de schema, construcao de expressoes e resumo

- Classe/metodo analisado: `validate_struct`, `_extract_all_checks`,
  `_validate_check_rules`, `_add_is_valid_column`, `_build_checks_summary`,
  `_join_with_metadata_python`.
- Principio afetado: SRP, OCP e facilidade para junior.
- Impacto pratico: o arquivo e coeso no tema "validar StructType", mas ja tem
  responsabilidades de camadas diferentes: interpretar contrato, montar
  expressoes Spark, validar resolucao SQL, construir diagnostico agregado e
  adaptar resultado para Python. Isso aumenta risco de regressao quando for
  adicionada nova severidade, novo tipo de check ou novo formato de resumo.
- Evidencia em codigo: entrada publica em
  `etl_framework/utils/validate_struct.py:18`; extracao de checks em
  `etl_framework/utils/validate_struct.py:156`; validacao SQL em
  `etl_framework/utils/validate_struct.py:216`; resumo em
  `etl_framework/utils/validate_struct.py:233`.
- Evidencia em testes ou uso: `tests/test_validate_struct.py:128` cobre
  severidade warning; `tests/test_validate_struct.py:253` cobre summary opt-in;
  `tests/test_validate_struct.py:464` cobre batching de 105 checks.
- Recomendacao objetiva: extrair um pequeno objeto ou modulo interno
  `struct_checks` para representar checks normalizados e expressoes Spark,
  deixando `validate_struct` como fachada publica. Evitar expor nova API publica
  na v0.1.
- Prioridade: P2.

### F3 - `EtlRunConfig` virou agregador de contrato, politica runtime e validacao de schema

- Classe/metodo analisado: `EtlRunConfig.__post_init__`,
  `_validate_basic_config`, `_validate_schemas`,
  `_validate_target_key_in_target_struct`.
- Principio afetado: SRP e coesao.
- Impacto pratico: a config e imutavel e util, mas hoje conhece campos de
  destino, dry-run, politica de schema, janelas, write mode, colunas tecnicas e
  validacao de metadata. Isso torna o construtor um ponto de falha para mudancas
  que nao sao puramente configuracionais e duplica parte do preflight.
- Evidencia em codigo: campos e validacoes em
  `etl_framework/models/config.py:14`, `etl_framework/models/config.py:61`,
  `etl_framework/models/config.py:82` e `etl_framework/models/config.py:155`.
- Evidencia em testes ou uso: `tests/test_pipeline_contract.py:365` precisa
  usar `object.__setattr__` para simular inconsistencia e forcar o preflight,
  sinal de que ha validacao em mais de um nivel; `docs/v0.1-contract.md`
  declara que `source_struct` e `target_struct` sao opcionais no modelo mas
  obrigatorios na execucao padrao.
- Recomendacao objetiva: manter as validacoes basicas no dataclass, mas mover
  validacoes dependentes do fluxo oficial para um preflight dedicado ou
  `RunConfigValidator`. Nao quebrar API; apenas reduzir duplicacao entre modelo
  e `Pipeline._preflight`.
- Prioridade: P2.

### F4 - `EtlExecutionContext.metrics` e um dicionario mutavel generico que atravessa camadas

- Classe/metodo analisado: `EtlExecutionContext.metrics`,
  `assert_volume_between`, `require_operational_metrics`,
  `build_observability_event`.
- Principio afetado: ISP, SRP e encapsulamento.
- Impacto pratico: o contexto documenta que nao deve ser state bag, mas expõe
  `metrics: dict[str, Any]` mutavel. Qualquer pipeline ou helper pode inserir
  chaves e valores arbitrarios; depois a observabilidade tenta serializar e
  ignora chaves invalidas. Isso e pragmatico para v0.1, mas fraco para
  manutencao porque erros de nome de metrica so aparecem como ausencia
  operacional.
- Evidencia em codigo: campo em `etl_framework/models/context.py:10`; escrita
  direta em `etl_framework/utils/production_checks.py:76`; validacao posterior
  em `etl_framework/utils/production_checks.py:163`; serializacao tolerante em
  `etl_framework/utils/observability_events.py:52`.
- Evidencia em testes ou uso: `tests/test_observability.py:236` usa chaves
  explicitas; `tests/test_observability.py:261` mostra chave nao-string sendo
  ignorada.
- Recomendacao objetiva: adicionar metodos pequenos no contexto, como
  `record_metric(name, value)` e `record_missing_metric(name, reason)`, mantendo
  o dict por compatibilidade. Usar esses metodos nos helpers internos.
- Prioridade: P2.

### F5 - Singleton global de observabilidade reduz isolamento de testes e clareza de dependencia

- Classe/metodo analisado: `_OBSERVABILITY_SERVICE`,
  `get_observability_service`, `configure_observability_sink`,
  `reset_observability_sink`, decorators `stage` e `runtime_event`.
- Principio afetado: DIP e acoplamento temporal.
- Impacto pratico: contratos e decorators dependem de estado de processo.
  Funciona para v0.1, mas testes e execucoes concorrentes podem vazar sink entre
  runs se nao houver reset disciplinado. A dependencia real de observabilidade
  fica escondida atras do decorator, dificultando isolamento em suites maiores.
- Evidencia em codigo: singleton em `etl_framework/infra/observability.py:272`;
  acesso global em `etl_framework/infra/stage.py:43` e
  `etl_framework/infra/stage.py:189`.
- Evidencia em testes ou uso: `tests/test_observability.py:172` precisa
  configurar sink global e chamar reset no `finally`; `tests/test_pipeline_contract.py:431`
  depende de configuracao global antes de rodar o pipeline.
- Recomendacao objetiva: manter singleton como default, mas permitir injetar
  `ObservabilityService` em `Pipeline` ou `EtlExecutionContext` para testes e
  execucoes isoladas. A injecao deve ser opcional para nao aumentar a carga do
  usuario junior.
- Prioridade: P2.

### F6 - `Load` mistura contrato de escrita, politica de colunas tecnicas e evidencia de dry-run

- Classe/metodo analisado: `Load.run`, `_run_dry_run`, `_show_dry_run_sample`,
  `_persistable_df`, `_run_load`, `_run_certify`.
- Principio afetado: SRP e coesao.
- Impacto pratico: `Load` e o contrato mais carregado: decide dry-run, emite
  evidencia, mostra amostra, remove colunas tecnicas, executa load e certify.
  Isso ainda e aceitavel porque v0.1 nao entrega load seguro generico, mas o
  metodo `_persistable_df` coloca politica de shape do DataFrame dentro do
  contrato abstrato de escrita.
- Evidencia em codigo: dry-run em `etl_framework/contracts/load.py:27` e
  `etl_framework/contracts/load.py:82`; selecao de colunas em
  `etl_framework/contracts/load.py:129`.
- Evidencia em testes ou uso: `tests/test_stage_contract_template_method.py:270`
  cobre skip de load no dry-run; `tests/test_stage_contract_template_method.py:331`
  cobre remocao de colunas tecnicas.
- Recomendacao objetiva: extrair a politica de colunas tecnicas para helper
  interno puro, por exemplo `prepare_load_dataframe(df, config)`. Manter
  `Load.run` como coordenador do fluxo.
- Prioridade: P3.

### F7 - `Pipeline._preflight` duplica regras ja presentes no modelo

- Classe/metodo analisado: `Pipeline._preflight` e
  `EtlRunConfig._validate_target_key_in_target_struct`.
- Principio afetado: DRY pragmatico e SRP.
- Impacto pratico: a duplicacao nao quebra a v0.1, mas exige lembrar que
  algumas invariantes sao verificadas no construtor e repetidas antes da
  execucao. Isso aumenta risco de mensagens divergentes e testes artificiais
  usando mutacao de dataclass congelado.
- Evidencia em codigo: `Pipeline._preflight` em
  `etl_framework/contracts/pipeline.py:53`; validacao equivalente em
  `etl_framework/models/config.py:170`.
- Evidencia em testes ou uso: `tests/test_pipeline_contract.py:365` altera
  `target_key` via `object.__setattr__` para passar pelo construtor e testar
  preflight.
- Recomendacao objetiva: manter preflight para regras dependentes de execucao
  (`source_struct`/`target_struct` obrigatorios, dry-run show rows), mas remover
  ou centralizar a checagem duplicada de `target_key`.
- Prioridade: P3.

## 4. Oportunidades P1/P2/P3

### P1 - Corrigir concentracao operacional em `auto_validate_target`

- Objetivo: separar validacao lazy de politica de bloqueio/diagnostico.
- Mudanca recomendada: manter `auto_validate_target` como API publica, mas
  decompor internamente em `_validate_target_struct` e
  `_enforce_no_invalid_records`.
- Beneficio: torna custo Spark mais rastreavel, facilita teste de diagnostico e
  reduz risco ao evoluir comportamento de invalidos.
- Risco de nao fazer: proxima regra de qualidade ou modo de diagnostico tende a
  aumentar uma funcao que ja mistura decisao, transformacao e acao Spark.

### P2 - Fatiar `validate_struct.py` sem criar API nova

- Objetivo: preservar fachada simples e reduzir acoplamento interno.
- Mudanca recomendada: criar modulo interno para checks normalizados e
  expressoes Spark; deixar `validate_schema`, `validate_struct` e
  `summarize_struct_checks` como entradas publicas.
- Beneficio: facilita adicionar severidades ou checks sem tocar no fluxo todo.
- Risco de nao fazer: regressao cruzada entre schema, expressoes e summary.

### P2 - Encapsular metricas do contexto

- Objetivo: impedir que `context.metrics` vire state bag informal.
- Mudanca recomendada: adicionar metodos no contexto para registrar metricas e
  motivos de ausencia; migrar helpers internos para esses metodos.
- Beneficio: nomes e tipos de metricas ficam mais previsiveis sem quebrar API.
- Risco de nao fazer: metricas operacionais podem falhar silenciosamente por
  typo ou valor nao serializavel.

### P2 - Reduzir dependencia de estado global na observabilidade

- Objetivo: manter default simples e permitir isolamento.
- Mudanca recomendada: permitir `observability_service` opcional em contexto ou
  pipeline, com fallback para singleton atual.
- Beneficio: testes paralelos e execucoes independentes ficam menos acoplados.
- Risco de nao fazer: vazamento de sink global e comportamento dificil de
  reproduzir em suites maiores.

### P3 - Extrair politica de DataFrame persistivel do `Load`

- Objetivo: deixar `Load` coordenar escrita e certificacao, nao decidir shape de
  colunas.
- Mudanca recomendada: mover `_persistable_df` para helper interno puro.
- Beneficio: reduz responsabilidade do contrato abstrato sem alterar
  comportamento publico.
- Risco de nao fazer: novas politicas de colunas tecnicas incharao `Load`.

### P3 - Centralizar validacoes duplicadas entre config e preflight

- Objetivo: uma unica fonte para invariantes de `target_key`.
- Mudanca recomendada: criar helper/validator usado por `EtlRunConfig` e
  `Pipeline._preflight`, ou restringir preflight a regras que so existem em
  tempo de execucao.
- Beneficio: menos testes artificiais e menos risco de mensagens divergentes.
- Risco de nao fazer: duplicacao pequena, mas recorrente, conforme novas regras
  de config surgirem.

## Respostas diretas as perguntas obrigatorias

- Cada classe tem responsabilidade clara? Em geral sim para `Pipeline`,
  `Extract`, `Transform`, erros e sinks. Parcialmente para `Load`,
  `EtlRunConfig` e `ObservabilityService`. `validate_struct` nao e classe, mas
  e o maior ponto de concentracao funcional.
- Existem responsabilidades que deveriam estar juntas mas foram separadas por
  estetica? Nao ha evidencia forte de separacao estetica. A separacao
  `contracts`/`infra`/`utils` tem uso real nos testes e na documentacao.
- Existem responsabilidades misturadas que dificultam manutencao? Sim:
  `auto_validate_target`, `validate_struct.py`, `EtlRunConfig`,
  `EtlExecutionContext.metrics`, singleton de observabilidade e parte de
  `Load`.
- O codigo favorece extensao real ou cria complexidade artificial? Favorece
  extensao real no fluxo ETL e nos sinks. Cria pouca complexidade artificial; o
  problema principal e concentracao de responsabilidades, nao hierarquia
  excessiva.
- Os nomes comunicam comportamento? Majoritariamente sim. Bons exemplos:
  `assert_volume_between`, `require_dataframe`, `split_valid_invalid`,
  `configure_observability_sink`. Nome a observar: `auto_validate_target` e
  amplo demais para uma funcao que tambem bloqueia invalidos e calcula
  diagnostico.
- As dependencias apontam em direcao coerente? Na maior parte sim:
  `contracts` dependem de `infra`, `models` e `utils`; pipelines concretas
  dependem dos contratos. A excecao pratica e observabilidade via singleton,
  que esconde dependencia global em decorators.

## Conclusao

O design atual e pragmaticamente bom para v0.1: ele protege a ordem oficial,
reduz trabalho manual de pipeline junior e torna erro/observabilidade
rastreaveis. Nao ha sinal dominante de SOLID decorativo.

A severidade da auditoria esta nos pontos que podem virar gargalo na proxima
evolucao. O primeiro ajuste deve ser `auto_validate_target`, porque ali se
misturam validacao, politica operacional, diagnostico e custo Spark no caminho
critico antes do `load`. Depois, fatiar `validate_struct` e encapsular metricas
trazem melhor manutencao sem mudar a filosofia simples da v0.1.
