# Auditoria SR06 - Simplicidade e Tracing Mental

## Escopo e restricoes

Auditoria independente executada sobre o reposito local em
`c:\Repositories\spark-etl-framework`, com papel de especialista em simplicidade
arquitetural e tracing/debug.

Arquivos lidos para esta auditoria:

- `prompts/_v0.1/auditoria/auditoria_sr06_simplicidade_tracing.md`
- `README.md`
- `QUICK_START.md`
- `docs/v0.1-contract.md`
- `docs/observability/logging.md`
- `docs/development/testing.md`
- `etl_framework/contracts/pipeline.py`
- `etl_framework/contracts/extract.py`
- `etl_framework/contracts/transform.py`
- `etl_framework/contracts/load.py`
- `etl_framework/infra/stage.py`
- `etl_framework/infra/errors.py`
- `etl_framework/infra/observability.py`
- `etl_framework/models/config.py`
- `etl_framework/models/context.py`
- `etl_framework/utils/auto_quality.py`
- `etl_framework/utils/validate_struct.py`
- `etl_framework/utils/schema_metadata.py`
- `etl_framework/utils/check_metadata.py`
- `etl_framework/utils/stage_metadata.py`
- `etl_framework/utils/observability_events.py`
- `etl_framework/utils/dataframe_checks.py`
- `etl_framework/utils/production_checks.py`
- `etl_framework/utils/sanitization.py`
- `tests/test_pipeline_author_journey.py`
- `tests/test_framework_integration.py`
- `tests/test_pipeline_contract.py`
- `tests/test_dry_run.py`
- `tests/test_stage_contract_template_method.py`
- `tests/test_auto_contract.py`
- `tests/test_validate_struct.py`

Nao foram lidos relatorios de outras auditorias nem arquivos em
`docs/audits/reports/v0_1/independent`.

## 1. Diagnostico de simplicidade real

Diagnostico: a v0.1 tem API publica simples, mas tracing mental medio-alto. A
simplicidade e real apenas na superficie de autoria de pipeline; no debug, ela
vira simplicidade aparente porque o comportamento critico esta espalhado entre
template methods, decorators, validadores automaticos, config imutavel, sink
global de observabilidade e convencoes de schema.

Para um junior entender `Pipeline.run()` de forma honesta, nao basta abrir
`etl_framework/contracts/pipeline.py`. O metodo publico tem 5 linhas uteis
(`_preflight`, `extract`, `transform`, `load`, `return df`) em
`etl_framework/contracts/pipeline.py:44`, mas cada chamada publica atravessa
contratos instrumentados e helpers com comportamento relevante:

- minimo para entender a execucao feliz: 8 arquivos
  (`pipeline.py`, `extract.py`, `transform.py`, `load.py`, `config.py`,
  `context.py`, `auto_quality.py`, `validate_struct.py`);
- minimo para entender falha e tracing: 12 arquivos, adicionando
  `stage.py`, `errors.py`, `observability.py`, `observability_events.py`;
- minimo para entender a jornada documentada e testada: 17+ arquivos,
  adicionando `README.md`, `QUICK_START.md`, `docs/v0.1-contract.md`,
  `tests/test_pipeline_author_journey.py`, `tests/test_pipeline_contract.py` e
  `tests/test_auto_contract.py`.

Isso importa para a v0.1 porque o objetivo declarado e reduzir carga cognitiva
para desenvolvedores junior. O autor de pipeline escreve pouco, mas quando
`run()` falha ele precisa entender mais de uma dezena de pontos para responder
perguntas basicas: quem executou Spark action, quem adicionou `is_valid`, quem
embrulhou a excecao, por que o load foi pulado, por que colunas tecnicas chegaram
ao destino, e onde os logs foram emitidos.

## 2. Respostas objetivas as perguntas obrigatorias

### Quantos arquivos um junior precisa abrir para entender `Pipeline.run()`?

Resposta severa: entre 12 e 17 arquivos, dependendo se "entender" inclui debug
de falha. Para a execucao feliz, 8 arquivos ainda e o minimo realista. Para
falha, tracing e observabilidade, 12 e o minimo. Para reconciliar comportamento
com promessa da v0.1 e testes, passa de 17.

O numero cresce porque `Pipeline.run()` chama metodos aparentemente diretos em
`pipeline.py:45`, mas:

- `_preflight()` valida duplicadamente parte de `EtlRunConfig` em
  `pipeline.py:53` e `config.py:61`;
- `extract()` delega para `Extract.run()` em `pipeline.py:86` e
  `extract.py:27`;
- `Extract.run()` chama `_run_extract`, `_run_check` e possivelmente
  `_limit_dry_run_extract` em `extract.py:27`;
- `Transform.run()` chama `_run_transform` e `_run_validate` em
  `transform.py:26`;
- `Load.run()` bifurca entre dry-run e load real em `load.py:27`;
- todos os estagios criticos passam por `@stage` em `stage.py:27`;
- validacao real passa por `auto_quality.py:15`, `auto_quality.py:34` e
  `validate_struct.py:18`.

### Quantos saltos existem entre chamada publica e comportamento critico?

Contagem no caminho feliz, sem expandir internals de Spark:

1. `Pipeline.run()` -> decorator `@stage("run")` (`pipeline.py:44`,
   `stage.py:43`);
2. wrapper -> `_preflight()` (`pipeline.py:53`);
3. `Pipeline.extract()` -> `Extract.run()` (`pipeline.py:86`, `extract.py:27`);
4. `Extract.run()` -> `_run_extract()` decorado (`extract.py:54`);
5. `_run_extract()` -> `_extract()` concreto (`extract.py:63`);
6. `Extract.run()` -> `_run_check()` decorado (`extract.py:66`);
7. `_run_check()` -> `auto_check_source()` (`extract.py:76`,
   `auto_quality.py:15`);
8. `auto_check_source()` -> `validate_schema()` (`auto_quality.py:24`,
   `validate_struct.py:59`);
9. `_run_check()` -> `_custom_check()` (`extract.py:82`);
10. `Pipeline.transform()` -> `Transform.run()` (`pipeline.py:94`,
    `transform.py:26`);
11. `Transform.run()` -> `_run_transform()` decorado (`transform.py:47`);
12. `_run_transform()` -> `_transform()` concreto (`transform.py:56`);
13. `Transform.run()` -> `_run_validate()` decorado (`transform.py:60`);
14. `_run_validate()` -> `auto_validate_target()` (`transform.py:70`,
    `auto_quality.py:34`);
15. `auto_validate_target()` -> `validate_struct()` (`auto_quality.py:52`,
    `validate_struct.py:18`);
16. `validate_struct()` -> `_validate_schema_match`, `_extract_all_checks`,
    `_validate_check_rules`, `_add_is_valid_column`
    (`validate_struct.py:92`, `validate_struct.py:156`,
    `validate_struct.py:216`, `validate_struct.py:200`);
17. `auto_validate_target()` -> `_raise_with_validation_diagnostics()` com
    `limit(1).count()` mesmo no caminho feliz (`auto_quality.py:76`,
    `auto_quality.py:78`);
18. `_run_validate()` -> `_custom_validate()` (`transform.py:76`);
19. `Pipeline.load()` -> `Load.run()` (`pipeline.py:103`, `load.py:27`);
20. `Load.run()` -> `_run_load()` ou `_run_dry_run()` (`load.py:38`,
    `load.py:30`);
21. `_run_load()` -> `_persistable_df()` -> `_load()` concreto
    (`load.py:57`, `load.py:129`, `load.py:67`);
22. `Load.run()` -> `_run_certify()` -> `_certify()` (`load.py:45`,
    `load.py:116`).

No caminho de falha, cada salto decorado adiciona pelo menos mais 4 saltos
mentais: wrapper captura excecao (`stage.py:58`), resolve erro gerenciado
(`stage.py:116`), emite evento de falha (`stage.py:66`) e re-raise com ou sem
wrapping (`stage.py:83`). Isso faz o tracing de falha passar de linear simples
para uma arvore de causa.

### Onde o comportamento importante fica escondido?

- Decorators em `@stage` e `@runtime_event`: observabilidade, wrapping de erro,
  elapsed time e summary nao aparecem no corpo dos metodos auditados. Evidencia:
  `pipeline.py:44`, `extract.py:54`, `extract.py:66`, `load.py:70`,
  `stage.py:43`, `stage.py:58`, `stage.py:91`.
- Auto-validacao: `Transform._transform()` parece retornar um DataFrame de
  negocio, mas `Transform.run()` adiciona `is_valid` e pode executar Spark
  actions antes do load. Evidencia: `transform.py:70`,
  `auto_quality.py:78`, `validate_struct.py:200`.
- Dry-run: o autor ve `Pipeline.run()`, mas o limite e aplicado dentro de
  `Extract.run()` depois do check, nao no pipeline e nao no load. Evidencia:
  `extract.py:48`, `extract.py:91`, `docs/v0.1-contract.md:206`.
- Colunas tecnicas: `Load._load()` recebe `is_valid` por default, a menos que
  `keep_technical_columns=False`. Isso esta em `_persistable_df`, nao no
  contrato abstrato do hook. Evidencia: `load.py:129`, `README.md:71`.
- Observabilidade global: o sink usado pelos decorators vem de um singleton de
  modulo, nao de uma dependencia explicita em `Pipeline`. Evidencia:
  `observability.py:272`, `observability.py:304`.

### O fluxo e simples de seguir em uma falha?

Parcialmente. A etapa e rastreavel por erro gerenciado (`CheckError`,
`ValidateError`, etc.) e os testes cobrem isso em
`tests/test_pipeline_contract.py:791`. Mas a simplicidade de debug e limitada:

- a stack real aponta para wrappers de decorator antes de apontar para o hook ou
  helper que causou a falha;
- `run_failed` duplica a falha de etapa em nivel de pipeline, exigindo separar
  evento raiz de evento propagado (`tests/test_pipeline_contract.py:471`);
- falhas de validacao podem ter causa em schema, SQL de metadata, coluna
  `is_valid` preexistente, `limit(1).count()`, `count()` ou `collect()` de
  diagnostico (`auto_quality.py:47`, `auto_quality.py:78`,
  `auto_quality.py:81`, `auto_quality.py:99`);
- falhas de observability sink sao engolidas em `observability.py:67`, o que e
  correto para v0.1, mas significa que ausencia de evento nao e sempre falha da
  pipeline.

### A simplicidade e real ou apenas aparente?

E real para autoria inicial e aparente para manutencao/debug. O `QUICK_START.md`
mostra uma experiencia curta de autoria (`QUICK_START.md:113`), mas a mesma
pagina precisa listar nove efeitos automaticos de `dry_run`, incluindo
`auto_check`, `dry_run_limit`, `auto_validate`, evidencia tecnica e skip de
load/certify (`QUICK_START.md:113` a `QUICK_START.md:123`). Isso e um sinal de
que a API parece pequena porque moveu complexidade para o runtime.

### O custo cognitivo cresce linearmente ou exponencialmente?

Para caminho feliz, cresce quase linearmente: cada etapa adiciona um contrato e
um helper. Para falha, cresce de forma combinatoria: etapa x decorator x erro
gerenciado x evento x schema x Spark action. Uma falha em `validate`, por
exemplo, pode exigir cruzar `Transform.run()`, `auto_validate_target`,
`validate_struct`, metadata no `StructType`, `stage()`, `ensure_stage_error`,
sink de observabilidade e teste correspondente.

## 3. Mapa de arquivos por jornada

### Jornada A - Autor junior executa pipeline feliz

Arquivos necessarios:

- `README.md`: promessa e automacoes da v0.1 (`README.md:24` a `README.md:30`).
- `QUICK_START.md`: exemplo minimo com `source_struct`, `target_struct`,
  `dry_run` e `Pipeline.run()` (`QUICK_START.md:28`, `QUICK_START.md:35`,
  `QUICK_START.md:88`, `QUICK_START.md:113`).
- `etl_framework/contracts/pipeline.py`: orquestracao linear
  (`pipeline.py:45`).
- `etl_framework/contracts/extract.py`: template method de extract/check
  (`extract.py:27`, `extract.py:66`).
- `etl_framework/contracts/transform.py`: template method de
  transform/validate (`transform.py:26`, `transform.py:60`).
- `etl_framework/contracts/load.py`: bifurcacao dry-run/load/certify
  (`load.py:27`).
- `etl_framework/models/config.py`: campos que alteram comportamento
  (`config.py:14`, `config.py:39`, `config.py:40`).
- `etl_framework/models/context.py`: `run_id`, `started_at`, `metrics`.
- `tests/test_pipeline_author_journey.py`: teste que representa autoria real
  (`tests/test_pipeline_author_journey.py:220`).

Carga cognitiva: moderada. O autor consegue copiar o exemplo, mas precisa saber
que schema metadata e config controlam comportamento que nao esta no seu codigo.

### Jornada B - Debug de falha em schema de origem

Arquivos necessarios:

- `pipeline.py`: `_preflight()` pode falhar antes de `_extract`
  (`pipeline.py:53`).
- `extract.py`: `_run_check()` e `auto_check_source()` (`extract.py:66`,
  `extract.py:76`).
- `auto_quality.py`: `source_struct` obrigatorio (`auto_quality.py:15`).
- `validate_struct.py`: `_validate_schema_match()` monta erro de missing/type
  (`validate_struct.py:92`).
- `stage.py`: decorator transforma excecao em `CheckError` (`stage.py:58`).
- `errors.py`: `ensure_stage_error()` preserva/adiciona contexto.
- `tests/test_auto_contract.py`: falha de coluna ausente
  (`tests/test_auto_contract.py:318`).

Carga cognitiva: alta para junior porque o erro observado e `CheckError`, mas a
causa esta em `validate_struct`.

### Jornada C - Debug de falha em target/check declarativo

Arquivos necessarios:

- `transform.py`: `_run_validate()` chama `auto_validate_target`
  (`transform.py:60`, `transform.py:70`).
- `auto_quality.py`: rejeita `is_valid` preexistente, bloqueia invalidos,
  calcula diagnostico (`auto_quality.py:47`, `auto_quality.py:78`,
  `auto_quality.py:81`, `auto_quality.py:92`).
- `validate_struct.py`: extrai metadata, valida SQL e adiciona `is_valid`
  (`validate_struct.py:156`, `validate_struct.py:216`,
  `validate_struct.py:200`).
- `check_metadata.py`: normaliza severidade default para warning.
- `schema_metadata.py`: valida shape de metadata no `EtlRunConfig`.
- `stage.py` e `errors.py`: wrapping em `ValidateError`.
- `tests/test_auto_contract.py`: bloqueio antes do load
  (`tests/test_auto_contract.py:394`).

Carga cognitiva: alta. O comportamento aparente e "validar target_struct"; o
comportamento real inclui criar coluna tecnica, executar SQL Spark, contar
invalidos e potencialmente coletar resumo.

### Jornada D - Debug de dry-run

Arquivos necessarios:

- `pipeline.py`: nao mostra dry-run no fluxo principal.
- `extract.py`: aplica `df.limit(config.dry_run_limit)` apos check
  (`extract.py:48`, `extract.py:91`).
- `load.py`: pula `_load()` e `_certify()` em `Load.run()` (`load.py:27`,
  `load.py:82`).
- `stage_metadata.py`: payload de evidencia.
- `stage.py`: `runtime_event()` emite eventos adicionais (`stage.py:139`).
- `docs/v0.1-contract.md`: explicita que dry-run limita depois de extract/check
  (`docs/v0.1-contract.md:194` a `docs/v0.1-contract.md:206`).
- `tests/test_dry_run.py`: prova que o check ve 3 linhas e transform ve 2
  (`tests/test_dry_run.py:161`).

Carga cognitiva: media-alta. O nome `dry_run` sugere baixa execucao, mas a
documentacao admite que nao reduz custo de leitura.

### Jornada E - Debug de observabilidade/tracing

Arquivos necessarios:

- `stage.py`: inicio/sucesso/falha/summary via decorators (`stage.py:50`,
  `stage.py:66`, `stage.py:91`, `stage.py:99`).
- `observability.py`: service global, sink best-effort, swallow de falha
  (`observability.py:37`, `observability.py:67`, `observability.py:272`).
- `observability_events.py`: payload padrao e sanitizacao seletiva
  (`observability_events.py:15`, `observability_events.py:67`).
- `stage_metadata.py`: extras de dry-run.
- `errors.py`: mensagem sanitizada e causa.
- `docs/observability/logging.md`: diz que eventos sao best-effort
  (`docs/observability/logging.md:3`, `docs/observability/logging.md:22`).
- `tests/test_pipeline_contract.py`: sequencia exata de eventos
  (`tests/test_pipeline_contract.py:382`).

Carga cognitiva: alta, principalmente porque a dependencia mais importante para
tracing nao aparece no construtor de `Pipeline`.

## 4. Sequencia de chamadas consolidada

```text
Pipeline.run()  [@stage("run", emit_summary=True)]
  -> _preflight()
  -> extract()
      -> Extract.run()
          -> _run_extract()  [@stage("extract", ExtractError)]
              -> _extract() concreto
              -> require_dataframe()
          -> _run_check()  [@stage("check", CheckError)]
              -> auto_check_source()
                  -> validate_schema()
                      -> _validate_schema_match()
              -> _custom_check()
              -> require_dataframe()
          -> if dry_run:
              -> _limit_dry_run_extract() [@runtime_event]
                  -> df.limit(dry_run_limit)
  -> transform(df)
      -> Transform.run()
          -> _run_transform() [@stage("transform", TransformError)]
              -> _transform() concreto
              -> require_dataframe()
          -> _run_validate() [@stage("validate", ValidateError)]
              -> auto_validate_target()
                  -> reject existing is_valid
                  -> validate_struct()
                      -> validate_schema()
                      -> _extract_all_checks()
                      -> _validate_check_rules()
                      -> _add_is_valid_column()
                  -> _raise_with_validation_diagnostics()
                      -> filter(~is_valid).limit(1).count()
                      -> if invalid:
                          -> invalid_df.count()
                          -> summarize_struct_checks()
                          -> collect()
              -> _custom_validate()
              -> require_dataframe()
  -> load(df)
      -> Load.run()
          -> if dry_run:
              -> _run_dry_run() [@stage("load") + @runtime_event]
                  -> _persistable_df()
                  -> optional df.show()
              -> return
          -> _run_load() [@stage("load", LoadError)]
              -> _persistable_df()
              -> _load() concreto
          -> _run_certify() [@stage("certify", CertifyError)]
              -> _persistable_df()
              -> _certify() concreto/default
  -> return df
```

## 5. Comparacao entre comportamento aparente e comportamento real

| Aparente para o autor | Real no runtime | Evidencia | Impacto v0.1 |
| --- | --- | --- | --- |
| `Pipeline.run()` e linear e trivial. | Cada etapa e decorada, instrumentada, e pode reemitir erro/evento. | `pipeline.py:44`, `stage.py:43`, `stage.py:58` | Debug exige entender runtime invisivel. |
| `Transform._transform()` retorna dados de negocio. | `Transform.run()` adiciona `is_valid` e pode disparar acoes Spark. | `transform.py:70`, `auto_quality.py:78`, `validate_struct.py:200` | Junior pode se surpreender com schema final e custo. |
| `dry_run=True` evita efeitos. | Ainda executa `_extract`, `auto_check`, `_transform`, `auto_validate`; limita depois de check. | `docs/v0.1-contract.md:194`, `docs/v0.1-contract.md:206`, `extract.py:48` | Nome reduz risco de escrita, mas nao custo de leitura. |
| `nullable=False` bloqueia nulos. | Nulos so bloqueiam com check SQL explicito. | `docs/v0.1-contract.md:166`, `tests/test_auto_contract.py:252` | Simplicidade aparente do schema pode deixar dado invalido passar. |
| `Load._load()` recebe target limpo. | Por default recebe colunas tecnicas, incluindo `is_valid`. | `README.md:71`, `load.py:129` | Risco de persistir coluna tecnica sem intencao. |
| Logs rastreiam tudo. | Observabilidade e best-effort e falha de sink e engolida. | `observability.py:67`, `docs/v0.1-contract.md:268` | Ausencia de evento nao e diagnostico conclusivo. |
| `strict_schema=True` e estrito. | Se `extra_columns_policy` continuar `ignore`, vira `warn`. | `config.py:118`, `validate_struct.py:134` | Nome induz leitura errada durante debug de drift. |

## 6. Top ofensores de carga cognitiva

### Ofensor 1 - Decorators concentram comportamento operacional invisivel

`@stage` e `@runtime_event` reduzem repeticao, mas escondem o principal caminho
de debug: inicio/fim/falha, elapsed time, wrapping de erro, summary e eventos de
dry-run. O corpo de `Pipeline.run()` em `pipeline.py:45` nao mostra nenhum
logger, nenhum try/except, nenhum evento. Tudo aparece em `stage.py:43` e
`stage.py:58`.

Por que importa para v0.1: tracing e uma promessa explicita da v0.1
(`README.md:28`). Se o junior nao sabe que a etapa real executada e o wrapper,
ele interpreta stack trace e eventos de forma errada.

Severidade: Alta.

### Ofensor 2 - Template Method em tres contratos multiplica pontos de entrada

`Extract.run()`, `Transform.run()` e `Load.run()` sao simples isoladamente, mas
cada um cria uma mini-DSL de hooks privados: `_extract`, `_custom_check`,
`_transform`, `_custom_validate`, `_load`, `_certify`. A documentacao admite
explicitamente Template Method (`docs/v0.1-contract.md:30`).

Por que importa para v0.1: a ordem real nao esta na pipeline concreta. O junior
ve apenas seus hooks e precisa confiar que o framework chama tudo na ordem certa.
Quando a falha acontece entre hooks, a causa fica no framework, nao no codigo
que ele escreveu.

Severidade: Alta.

### Ofensor 3 - `auto_validate_target` esconde custo Spark e mutacao de schema

`auto_validate_target()` parece helper declarativo, mas rejeita `is_valid`
preexistente, chama `validate_struct`, adiciona `is_valid`, executa
`limit(1).count()` no caminho normal, e no caminho invalido executa `count()` e
`collect()` para resumo (`auto_quality.py:47`, `auto_quality.py:78`,
`auto_quality.py:81`, `auto_quality.py:99`).

Por que importa para v0.1: a v0.1 quer ser simples sem esconder Spark. Aqui o
Spark fica parcialmente escondido. A documentacao menciona a action, mas o
tracing mental ainda e caro porque o custo nasce no validate, nao no codigo do
autor.

Severidade: Alta.

### Ofensor 4 - Config tem nomes que parecem simples, mas semantica nao obvia

`strict_schema=True` vira warning quando `extra_columns_policy` permanece
`ignore` (`config.py:118`). `source_struct` e `target_struct` sao opcionais no
modelo, mas obrigatorios na execucao padrao (`docs/v0.1-contract.md:108`).
`keep_technical_columns=True` faz `Load._load()` receber `is_valid` por default.

Por que importa para v0.1: config e o contrato mental principal do junior. Quando
nomes comuns significam algo especifico ou legado, o custo de debug aumenta.

Severidade: Media-alta.

### Ofensor 5 - Observability global melhora API, mas piora rastreio causal

`Pipeline` nao recebe sink, service ou logger. O runtime usa
`get_observability_service()` e um singleton global (`observability.py:272`,
`observability.py:304`). Isso mantem a API limpa, mas torna o estado operacional
externo ao objeto que esta sendo debugado.

Por que importa para v0.1: se dois testes ou execucoes configuram sinks
diferentes, o entendimento depende de estado global. Os testes usam
`configure_observability_sink()` em varias jornadas, o que comprova o acoplamento
operacional.

Severidade: Media.

### Ofensor 6 - Documentacao e melhor que o codigo para entender o fluxo

`docs/v0.1-contract.md:33` a `docs/v0.1-contract.md:50` traz o melhor mapa do
runtime. O codigo nao possui um mapa unico equivalente; ele distribui a ordem em
quatro classes e decorators.

Por que importa para v0.1: quando a documentacao e a fonte mais facil para
tracing, manutencao depende de sincronizar texto e codigo. Qualquer divergencia
gera debug incorreto.

Severidade: Media.

## 7. Pontos de contexto implicito

- `source_struct` e `target_struct` sao opcionais no dataclass, mas exigidos por
  preflight para a execucao padrao (`config.py:44`, `config.py:45`,
  `pipeline.py:53`).
- `strict_schema` nao e realmente "fail strict"; ele pode significar warning
  (`config.py:118`).
- `nullable=False` no Spark schema e intencao, nao regra de bloqueio
  (`docs/v0.1-contract.md:166`).
- `is_valid` e reservado, criado pelo framework e tambem usado como sentinela de
  validacao previa (`config.py:51`, `auto_quality.py:47`).
- `context.metrics` e apenas logado, nunca computado pelo framework
  (`context.py`, `observability_events.py:20`).
- `dry_run` limita depois de check, portanto a origem ainda pode ser lida em
  volume completo (`docs/v0.1-contract.md:206`).
- Observability sink e best-effort; falha de sink nao falha ETL
  (`observability.py:67`).
- `_certify()` default nao certifica nada; retorna `None` (`load.py:148`).

## 8. Rastreabilidade de falhas

Pontos fortes:

- Cada etapa tem erro gerenciado especifico (`ExtractError`, `CheckError`,
  `TransformError`, `ValidateError`, `LoadError`, `CertifyError`) em
  `errors.py`.
- `ensure_stage_error()` adiciona `pipeline_name`, `run_id` e `stage`.
- Testes cobrem parada downstream e contexto em falha
  (`tests/test_pipeline_contract.py:791`).
- Eventos incluem `stage`, `status`, `pipeline_name`, `run_id` e `elapsed_ms`
  (`tests/test_pipeline_contract.py:382`).

Pontos fracos:

- O mesmo erro aparece como falha da etapa e falha de `run`, exigindo deduplicar
  mentalmente eventos (`tests/test_pipeline_contract.py:471`).
- A causa real pode estar sanitizada, embrulhada e aninhada; a mensagem final
  privilegia seguranca operacional, nao depuracao profunda.
- Falhas em sink somem por design (`observability.py:67`), entao tracing por log
  e incompleto.
- `auto_validate_target` mistura validacao, Spark action e diagnostico, entao a
  etapa `validate` cobre varios subtipos de falha sem subestagio publico.

Conclusao: rastreabilidade por etapa e boa para v0.1; rastreabilidade causal
interna ainda e cara.

## 9. Oportunidades P1/P2/P3

### P1 - Criar um mapa de runtime executavel/estatico no proprio codigo

Adicionar um ponto unico, proximo de `Pipeline.run()`, que documente a sequencia
real de subetapas e seus efeitos: actions Spark, coluna tecnica adicionada,
wrapping de erro e dry-run. Pode ser uma constante, docstring expandida ou helper
interno usado por teste de contrato.

Por que importa para v0.1: reduz o numero de arquivos que um junior precisa
abrir para entender `run()` de 12+ para algo proximo de 5. Tambem reduz risco de
documentacao divergir do runtime.

### P1 - Expor subestagios de validate sem aumentar API do autor

Separar conceitualmente, nos eventos ou mensagens, `validate_schema`,
`validate_checks`, `validate_invalid_probe` e `validate_diagnostics`. Nao precisa
virar API publica; basta aparecer em evento extra ou detalhe estruturado.

Por que importa para v0.1: hoje toda falha vira `ValidateError`. Para debug,
schema ausente, SQL invalido, `is_valid` preexistente e invalido de negocio
parecem a mesma etapa.

### P1 - Renomear ou documentar agressivamente `strict_schema`

`strict_schema` e enganoso porque nao significa falhar com colunas extras. A
v0.1 deveria preferir `extra_columns_policy="fail"` nos exemplos onde bloqueio e
esperado e marcar `strict_schema` como compatibilidade legada no proprio modelo.

Por que importa para v0.1: reduz erro de configuracao em pipelines junior e
evita debug de drift baseado em premissa falsa.

### P2 - Tornar custo Spark visivel no ponto de chamada

Adicionar comentarios ou nomes internos mais explicitos em `auto_quality.py`:
por exemplo `_probe_invalid_records_with_spark_action()` em vez de concentrar
`limit(1).count()` dentro de `_raise_with_validation_diagnostics()`.

Por que importa para v0.1: o contrato promete nao esconder Spark. Nomes que
declaram action reduzem surpresa operacional.

### P2 - Reduzir duplicidade mental entre preflight e `EtlRunConfig`

Hoje parte da consistencia de `target_key` existe em `EtlRunConfig` e e refeita
em `_preflight()` (`config.py:170`, `pipeline.py:53`). Se a duplicidade for
intencional contra mutacao por `object.__setattr__`, documentar isso no codigo.

Por que importa para v0.1: duplicidade sem justificativa parece bug ou
overlap. O teste forca mutacao e preflight revalida, mas o leitor precisa abrir
teste para entender.

### P2 - Melhorar a historia de observability global em testes e docs

Documentar explicitamente que `configure_observability_sink()` altera estado de
processo e deve ser resetado em testes/aplicacoes. A doc atual explica sink, mas
o acoplamento global e o risco de vazamento ficam implicitos.

Por que importa para v0.1: tracing depende de estado global. Sem disciplina,
eventos podem ir para sink errado durante testes ou execucoes embutidas.

### P3 - Padronizar nomes de hooks customizados

`_custom_check` e `_custom_validate` so aparecem depois dos checks automaticos.
Isso deveria estar refletido no nome ou em docstring mais objetiva, por exemplo
`_after_auto_check` e `_after_auto_validate` em uma futura versao compativel.

Por que importa para v0.1: reduz a duvida sobre se o hook substitui ou
complementa validacao automatica.

### P3 - Adicionar tabela curta de "onde debugar"

No `README.md` ou `docs/v0.1-contract.md`, incluir uma tabela:
`PreflightError -> pipeline.py/_preflight`, `CheckError -> extract.py +
validate_schema`, `ValidateError -> transform.py + auto_quality +
validate_struct`, etc.

Por que importa para v0.1: o framework quer ser amigavel a junior; tabela de
debug tem impacto alto e baixo risco.

## 10. Veredito final

A v0.1 esta bem delimitada e evita uma API publica grande. Isso e positivo. Mas
o custo cognitivo real nao e baixo: ele foi deslocado para runtime automatico,
decorators e validadores. A experiencia de escrita e simples; a experiencia de
debug e apenas moderadamente rastreavel.

Veredito: simplicidade operacional aparente com nucleo de tracing medio-alto.
Aceitavel para v0.1 se o objetivo for padronizar uma pipeline simples; perigoso
se for vendido como framework facil de manter por junior sem mapa de runtime e
sem guia de debug por erro.

Prioridade recomendada antes de promover a v0.1: atacar P1 de tracing, nao
adicionar novas features. O maior ganho agora e diminuir o numero de arquivos e
saltos necessarios para responder "por que meu `Pipeline.run()` falhou?".
