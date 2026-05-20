# Auditoria SR09 - Abstracoes sem Pressao Real

## Escopo executado

Arquivos de entrada lidos conforme escopo: `etl_framework/`, `tests/`,
`docs/v0.1-contract.md` e `docs/v0.1-known-limitations.md`.

Nao foram lidos relatorios de outras auditorias nem arquivos existentes em
`docs/audits/reports/v0_1/independent`.

Papel aplicado: arquiteto pragmatico e cetico, avaliando apenas custo e
beneficio de abstracoes, camadas, interfaces e extensibilidade na promessa
v0.1.

## Diagnostico executivo

A arquitetura v0.1 tem um nucleo justificavel: `Pipeline` + contratos
`Extract`, `Transform` e `Load` sustentam explicitamente o fluxo oficial
`extract -> check -> transform -> validate -> load -> certify` documentado em
`docs/v0.1-contract.md`. Essa abstracao preserva a promessa de padronizar ETL
PySpark sem esconder Spark.

O problema principal esta nas bordas: observabilidade, validacao de metadata e
helpers operacionais ja apresentam formato de framework extensivel antes de
haver pressao real de multiplos backends, multiplas politicas de execucao ou
multiplos tipos de pipeline. A v0.1 declara que nao entrega observabilidade
externa, entrega garantida de eventos, load seguro generico, engine completa de
data quality ou producao irrestrita. Mesmo assim, parte do codigo ja carrega
portas, servicos globais, sinks, decoradores genericos e validadores em classes.

Conclusao severa: o framework ainda nao esta em overengineering sistemico no
miolo ETL, mas ja tem sinais claros de arquitetura maior que a pressao real nas
camadas auxiliares. O custo aparece em tracing indireto, arquivos pequenos que
espalham uma unica decisao e APIs publicadas em `utils` que parecem mais
estaveis do que a v0.1 promete.

## Inventario de abstracoes

| Abstracao | Evidencia | Consumidores reais | Implementacoes reais | Beneficio declarado/inferido | Custo de manutencao/tracing | Recomendacao |
| --- | --- | --- | --- | --- | --- | --- |
| `Pipeline` | `etl_framework/contracts/pipeline.py:14`, `run` em `:45`, wrappers `extract/transform/load` em `:86`, `:94`, `:103` | Testes de contrato e jornada: `tests/test_pipeline_contract.py`, `tests/test_pipeline_author_journey.py`, `tests/test_framework_integration.py` | Uma classe concreta | Coordena fluxo oficial, preflight, contexto e propagacao de etapas | Baixo a medio: wrappers adicionam indirecao, mas o fluxo fica rastreavel | Manter; opcionalmente fundir wrappers se nao houver uso publico intencional |
| `Extract` Template Method | `etl_framework/contracts/extract.py:16`, `run` em `:27`, `_run_extract` em `:55`, `_run_check` em `:67`, `_custom_check` em `:111` | Subclasses em testes e jornada: `tests/test_auto_contract.py`, `tests/test_pipeline_author_journey.py`, `tests/test_stage_contract_template_method.py` | Varias subclasses de teste; nenhuma implementacao produtiva no framework | Separa leitura concreta de check estrutural automatico | Medio: `_extract -> _run_extract -> run -> Pipeline.extract` cria cadeia longa para debug | Manter; a pressao e real para padronizar check sem esconder Spark |
| `Transform` Template Method | `etl_framework/contracts/transform.py:15`, `run` em `:26`, `_run_transform` em `:48`, `_run_validate` em `:61`, `_custom_validate` em `:90` | Subclasses em testes e jornada | Varias subclasses de teste; nenhuma implementacao produtiva no framework | Separa regra de negocio da validacao automatica de `target_struct` | Medio: hooks opcionais podem virar segunda camada de regra de negocio | Manter; limitar documentacao dos hooks a uso pequeno, como o contrato ja faz |
| `Load` Template Method | `etl_framework/contracts/load.py:18`, `run` em `:27`, `_run_load` em `:58`, `_run_dry_run` em `:82`, `_run_certify` em `:117`, `_persistable_df` em `:129`, `_certify` em `:153` | Subclasses em testes e jornada | Varias subclasses de teste; nenhuma implementacao segura generica | Padroniza dry-run, skip de escrita, certificacao opcional e colunas tecnicas | Alto relativo: `Load` existe para um ponto que a v0.1 explicitamente nao resolve como seguro | Manter com alerta; nao adicionar strategy/backend de escrita antes de pressao real |
| Hooks `_custom_check`, `_custom_validate`, `_certify` | `extract.py:111`, `transform.py:90`, `load.py:153` | Pouco uso real fora de testes; contrato cita hooks opcionais | Implementacao default no-op; sobrescritas pontuais em testes | Permite regra pequena adicional por pipeline | Medio: cria extensibilidade hipotetica e risco de duplicar auto checks | Simplificar por disciplina, nao por remocao agora; preservar promessa de regras pequenas |
| Decorador `stage` | `etl_framework/infra/stage.py:27`; usado em contratos em `extract.py:55`, `extract.py:67`, `transform.py:48`, `transform.py:61`, `load.py:58`, `load.py:82`, `load.py:117`, `pipeline.py:44` | Contratos e testes de stage | Uma implementacao generica | Centraliza eventos, duracao e wrapping de erro por etapa | Medio/alto: tracing entra por decorador e resolve `config/context` por introspeccao | Manter por enquanto; reduzir generalidade se novas etapas nao surgirem |
| Decorador `runtime_event` | `etl_framework/infra/stage.py:139`; usado em dry-run em `extract.py:85`, `load.py:76`, `load.py:99` | Apenas eventos especificos de dry-run e testes | Uma implementacao generica | Evita chamadas manuais de logger nos contratos | Medio: abstracao generica para tres usos concretos | Simplificar se crescer pouco: chamadas explicitas de service seriam mais rastreaveis |
| `ObservabilitySink` Protocol | `etl_framework/infra/observability.py:21` | `ObservabilityService`, testes com sinks in-memory/failing, docs em `docs/v0.1-contract.md:273` | `NoOpObservabilitySink` e `StdoutObservabilitySink` | Porta avancada para trocar destino de eventos sem tocar contratos | Alto: a v0.1 nao entrega observabilidade externa nem entrega garantida | Manter como porta interna avancada, mas nao expandir; documentar como instavel/interno |
| `ObservabilityService` | `etl_framework/infra/observability.py:37`; metodos `emit/info/warning/error/exception/runtime_event/stage_*` em `:56` a `:200` | `stage.py`, testes de observabilidade e pipeline | Uma classe de servico global | Encapsula payload e best-effort | Alto: muitos metodos para dois padroes reais, com singleton global em `:272` | Simplificar P1/P2: reduzir superficie publica e manter apenas emissoes usadas |
| `NoOpObservabilitySink` e `StdoutObservabilitySink` | `observability.py:29` e `:259` | `ObservabilityService`, reset/configuracao | Duas implementacoes concretas | Default stdout e fallback silencioso | Baixo/medio: implementacoes simples, mas reforcam porta antes de pressao real | Manter `Stdout`; avaliar se `NoOp` precisa classe ou pode ser funcao/None tratado direto |
| Familia `EtlError` | `etl_framework/infra/errors.py:6`, subclasses em `:56` a `:92`, `ensure_stage_error` em `:98` | `stage.py`, contratos, testes de erro | Sete subclasses por etapa | Erros gerenciados com stage, pipeline e run_id | Medio: varias classes quase identicas, mas mapeiam o fluxo oficial | Manter; custo aceitavel porque melhora rastreabilidade prometida |
| `EtlRunConfig` | `etl_framework/models/config.py:14` | Todos os contratos, observabilidade, testes | Uma dataclass imutavel | Contrato de execucao e preflight de campos v0.1 | Medio: acumula campos prospectivos (`write_mode`, janelas, politicas) | Manter; revisar campos sem consumidor funcional antes de promover API |
| `EtlExecutionContext` | `etl_framework/models/context.py:10` | Pipeline, observabilidade, production checks | Uma dataclass | Trace `run_id`, timestamp e metricas explicitas | Baixo: pequeno e diretamente ligado a rastreabilidade | Manter |
| `SchemaMetadataValidator` | `etl_framework/utils/schema_metadata.py:12` | `EtlRunConfig._validate_schemas` e testes diretos | Uma classe; sem subclasses | Valida metadata de `StructType` antes da execucao | Medio: classe sem estado complexo e sem polimorfismo real | Simplificar P2: funcao seria suficiente sem perda da promessa |
| `CheckMetadataValidator` | `schema_metadata.py:72` | `SchemaMetadataValidator` e testes indiretos/diretos | Uma classe; sem subclasses | Valida um check declarativo e erros SQL simples | Medio: classe para uma rotina procedural pequena | Simplificar P2: fundir em funcoes de metadata |
| `normalize_check_metadata` | `etl_framework/utils/check_metadata.py:8` | `validate_struct.py:188` e `schema_metadata.py:90` | Uma funcao compartilhada | Evita duplicacao entre validacao de config e execucao | Baixo: abstracao funcional com dois consumidores reais | Manter |
| `validate_schema`, `validate_struct`, `summarize_struct_checks` | `etl_framework/utils/validate_struct.py:18`, `:59`, `:82` | `auto_quality.py`, testes, `utils.__init__` | Funcoes concretas | Nucleo da promessa de check/validate automatico | Medio: arquivo grande, mas dominio e real | Manter; evitar novas camadas sobre essas funcoes |
| `auto_check_source` e `auto_validate_target` | `etl_framework/utils/auto_quality.py:15`, `:34` | `Extract._run_check`, `Transform._run_validate`, testes diretos | Funcoes concretas | Nomeiam o contrato v0.1 e adicionam diagnostico de invalidos | Medio: camada fina sobre `validate_schema/validate_struct` | Manter enquanto agrega diagnostico; nao criar classes/strategies |
| Helpers `production_checks` | `etl_framework/utils/production_checks.py:19` a `:163` | Testes e export em `utils.__init__`; docs de limitacoes citam helpers opcionais | Funcoes concretas | Guardas explicitos para revisao senior e evidencia operacional | Medio/alto: parecem biblioteca produtiva apesar de v0.1 nao garantir producao irrestrita | Manter como opcionais, mas nao tratar como contrato central v0.1 |
| Builders `stage_metadata` | `etl_framework/utils/stage_metadata.py:6`, `:12`, `:18` | Decoradores de dry-run em `extract.py` e `load.py` | Tres funcoes pequenas | Isolam metadados de eventos de dry-run | Medio: arquivo separado para tres dicts triviais piora tracing | Fundir P2 em `stage.py` ou nos contratos |
| Builder `build_observability_event` | `etl_framework/utils/observability_events.py:15` | `ObservabilityService._emit_event`, testes | Uma funcao concreta | Payload estruturado comum | Baixo/medio: util claro, mas acoplado a servico global | Manter; e mais simples que espalhar payload |

## Matriz beneficio vs custo

| Grupo | Beneficio real v0.1 | Custo atual | Veredito |
| --- | --- | --- | --- |
| `Pipeline` + `Extract/Transform/Load` | Alto: materializa o fluxo oficial e obriga checks automaticos | Medio: Template Method e wrappers adicionam saltos | Manter |
| Hooks opcionais | Baixo/medio: permitem pequenas regras locais | Medio: extensibilidade facil de abusar | Manter com restricao explicita |
| `Load` completo com certify/dry-run/persistable shape | Medio: dry-run e certificacao sao parte do contrato | Alto: load seguro esta fora do escopo v0.1 | Manter minimo; bloquear novas estrategias genericas |
| Observabilidade plugavel | Baixo/medio: logs best-effort sao prometidos | Alto: porta, servico, sinks, singleton e decoradores antes de backend real | Simplificar superficie |
| Decoradores de runtime | Medio: removem logging manual dos contratos | Medio/alto: tracing indireto, introspeccao de kwargs/self | Manter `stage`; reavaliar `runtime_event` |
| Validadores de metadata em classes | Medio: protegem checks declarativos cedo | Medio: classes sem polimorfismo e sem estado relevante | Simplificar para funcoes |
| Helpers de production checks | Baixo/medio: uteis para revisao senior | Medio/alto: sugerem maturidade produtiva que a v0.1 nega | Manter como opcionais, nao expandir |
| Builders pequenos de metadata | Baixo: retornam dicts triviais | Medio: arquivo e chamada indireta para tres payloads | Fundir |

## Candidatos a simplificacao

1. `ObservabilityService` deve encolher.
   - Evidencia: define muitos metodos especializados (`emit`, `info`,
     `warning`, `error`, `exception`, `runtime_event`, `stage_started`,
     `stage_succeeded`, `stage_warning`, `stage_failed`) em
     `etl_framework/infra/observability.py:56` a `:200`.
   - Pressao real: os contratos precisam emitir eventos de etapa e poucos
     eventos de dry-run. Nao ha backend externo real, entrega garantida ou
     formatter JSON oficial na v0.1.
   - Preservacao da promessa v0.1: manter eventos tecnicos best-effort por
     etapa, `run_id`, modo, destino e metricas explicitas. Isso pode existir
     com superficie menor.
   - Recomendacao: consolidar em `emit_event(...)` e `emit_stage(...)` ou
     tornar metodos privados, mantendo `configure_observability_sink` como
     ponto avancado.

2. `runtime_event` e builders `stage_metadata` podem ser fundidos.
   - Evidencia: `runtime_event` em `etl_framework/infra/stage.py:139` serve
     poucos usos de dry-run; builders ficam em `etl_framework/utils/stage_metadata.py:6`,
     `:12`, `:18`.
   - Pressao real: tres payloads triviais, todos internos.
   - Preservacao da promessa v0.1: eventos de dry-run continuam emitidos com
     `dry_run_limit` e `dry_run_show_rows`.
   - Recomendacao: mover esses payloads para perto do ponto de emissao ou
     substituir por chamadas explicitas no metodo. Isso melhora tracing.

3. `SchemaMetadataValidator` e `CheckMetadataValidator` podem virar funcoes.
   - Evidencia: classes em `etl_framework/utils/schema_metadata.py:12` e `:72`,
     sem subclasses, sem configuracao variavel e com um unico caminho de uso
     real via `EtlRunConfig`.
   - Pressao real: validar metadata declarativa, nao prover hierarquia de
     validadores.
   - Preservacao da promessa v0.1: `source_struct` e `target_struct` continuam
     rejeitando metadata malformada, reserved columns e erros SQL simples.
   - Recomendacao: substituir por `validate_schema_metadata(struct, name)` e
     `validate_check_metadata(...)`, mantendo `normalize_check_metadata`.

4. Wrappers `Pipeline.extract`, `Pipeline.transform`, `Pipeline.load` sao
   candidatos fracos a fusao.
   - Evidencia: `etl_framework/contracts/pipeline.py:86`, `:94`, `:103`.
   - Pressao real: eles apenas repassam `spark/config/context` para objetos
     injetados.
   - Preservacao da promessa v0.1: `Pipeline.run()` ainda coordenaria
     `extract -> transform -> load` e retornaria o DataFrame final.
   - Recomendacao: manter se forem API de teste/introspeccao intencional;
     fundir em `run()` se nao forem publicos.

5. `production_checks` deve permanecer opcional e nao virar camada obrigatoria.
   - Evidencia: helpers exportados em `etl_framework/utils/__init__.py` e
     definidos em `etl_framework/utils/production_checks.py:19` a `:163`.
   - Pressao real: docs dizem que producao exige revisao senior e que a v0.1
     nao garante producao irrestrita.
   - Preservacao da promessa v0.1: helpers continuam disponiveis para checks
     explicitos, sem prometer load seguro generico.
   - Recomendacao: nao criar `ProductionValidator`, `ReadinessService`,
     strategy de metricas ou politica obrigatoria na v0.1.

## Oportunidades P1/P2/P3

### P1 - Conter a superficie de observabilidade antes que vire API acidental

- Problema: `ObservabilitySink` e `ObservabilityService` estao documentados
  como porta avancada, mas o modulo expoe uma arquitetura de plugin completa
  (`Protocol`, servico global, NoOp, Stdout, configuracao por env).
- Risco: consumidores passam a depender de detalhes que a v0.1 nao promete,
  elevando custo de mudanca quando surgir um backend real.
- Acao recomendada: declarar explicitamente como infra interna/avancada e
  reduzir metodos publicos do service aos fluxos usados por `stage.py`.
- Promessa preservada: eventos/logs tecnicos best-effort continuam existindo;
  falha de sink continua sem quebrar ETL.

### P1 - Travar expansao de `Load` para estrategias genericas

- Problema: `Load` ja carrega dry-run, persistable shape e certify, mas a
  propria documentacao diz que load seguro generico, staging, rollback,
  idempotencia e commit atomico estao fora da v0.1.
- Risco: a proxima extensao natural seria criar estrategias de escrita sem
  maturidade real, exatamente o tipo de abstracao sem pressao que a v0.1 deve
  evitar.
- Acao recomendada: manter `Load` como contrato minimo e adicionar comentario
  ou doc curta proibindo backends genericos na v0.1.
- Promessa preservada: pipeline ainda executa `load` e `certify`; a escrita
  concreta continua responsabilidade da pipeline.

### P2 - Rebaixar validadores de metadata de classes para funcoes

- Problema: `SchemaMetadataValidator` e `CheckMetadataValidator` parecem
  preparar extensao polimorfica que nao existe.
- Risco: aumenta carga cognitiva para entender uma validacao puramente
  procedural.
- Acao recomendada: consolidar em funcoes no mesmo modulo, preservando os
  mesmos erros e warnings.
- Promessa preservada: configs continuam falhando cedo para metadata invalida.

### P2 - Fundir `stage_metadata`

- Problema: tres funcoes retornam dicionarios triviais e obrigam o leitor a
  saltar de contratos para `utils/stage_metadata.py`.
- Risco: tracing de dry-run fica mais indireto que o beneficio entregue.
- Acao recomendada: mover payloads para constantes/funcoes privadas em
  `infra/stage.py` ou para os proprios contratos.
- Promessa preservada: eventos de dry-run mantem os mesmos campos.

### P2 - Definir fronteira publica de `utils`

- Problema: `utils.__init__` exporta validadores estruturais e helpers
  operacionais. Isso torna utilitarios opcionais parecidos com API estavel de
  producao.
- Risco: extensoes futuras ficam presas a helpers que talvez sejam apenas
  suporte de testes e exemplos.
- Acao recomendada: documentar quais helpers fazem parte da API v0.1 e quais
  sao auxiliares explicitos com custo Spark.
- Promessa preservada: API raiz continua pequena (`Pipeline`, `EtlRunConfig`,
  `EtlExecutionContext`, `Extract`, `Transform`, `Load`), como contratado.

### P3 - Avaliar fusao dos wrappers de `Pipeline`

- Problema: `Pipeline.extract/transform/load` nao reduzem complexidade hoje.
- Risco: baixo; a indirecao e pequena.
- Acao recomendada: so fundir se nao houver intencao de permitir teste ou
  chamada isolada dessas etapas.
- Promessa preservada: `Pipeline.run()` continuaria coordenando o fluxo oficial.

### P3 - Manter hooks opcionais sob vigilancia

- Problema: `_custom_check` e `_custom_validate` podem virar extensibilidade
  hipotetica para qualquer regra.
- Risco: medio no futuro, baixo agora.
- Acao recomendada: nao remover; apenas reforcar que regras estruturais ficam
  no framework e hooks sao para regras pequenas.
- Promessa preservada: pipelines ainda conseguem complementar checks sem
  duplicar o contrato automatico.

## Respostas as perguntas obrigatorias

- Existem interfaces com uma unica implementacao real?
  - Sim. `ObservabilitySink` tem duas implementacoes internas (`NoOp` e
    `Stdout`), mas nenhum backend externo real no framework. `SchemaMetadataValidator`
    e `CheckMetadataValidator` sao classes com um unico uso real e sem
    polimorfismo.

- Existem camadas que nao reduzem complexidade nem isolam risco?
  - Sim. `stage_metadata` nao isola risco significativo; apenas separa tres
    dicts triviais. `Pipeline.extract/transform/load` reduzem pouco e podem
    ser mantidos apenas se forem API intencional.

- Alguma extensibilidade e apenas hipotetica?
  - Sim. A porta de observabilidade e parte dos hooks opcionais sao
    parcialmente hipoteticos na v0.1. Eles sao aceitaveis enquanto contidos,
    mas nao justificam novas camadas.

- Ha sinais de arquitetura enterprise maior que o problema?
  - Sim, principalmente em observabilidade: `Protocol`, service global,
    metodos por nivel, sinks, reset, env config e decorators genericos para uma
    promessa best-effort sem backend externo.

- Alguma abstracao piora tracing ou manutencao?
  - Sim. Decoradores `stage` e `runtime_event` melhoram padronizacao, mas
    escondem emissao de evento e wrapping de erro. `stage_metadata` piora
    tracing do dry-run. Validadores em classes aumentam arquivos e nomes sem
    extensao real.

- Que arquivos poderiam ser fundidos sem perda real da v0.1?
  - `etl_framework/utils/stage_metadata.py` poderia ser fundido nos contratos
    ou em `etl_framework/infra/stage.py`.
  - `etl_framework/utils/check_metadata.py` poderia ser fundido em
    `schema_metadata.py` ou `validate_struct.py`, embora hoje tenha dois
    consumidores reais e seu custo seja baixo.
  - `SchemaMetadataValidator` e `CheckMetadataValidator` poderiam ser funcoes
    no mesmo arquivo, sem trocar arquivo necessariamente.
  - Wrappers de `Pipeline` poderiam ser fundidos em `Pipeline.run()` se nao
    forem superficie publica desejada.

## Recomendacao final

Nao remover o desenho central `Pipeline` + `Extract` + `Transform` + `Load`:
ele preserva diretamente a promessa v0.1 de fluxo linear, checks automaticos,
erros por etapa e dry-run.

Remover ou simplificar primeiro as abstracoes auxiliares que nao carregam a
promessa central: superficie de observabilidade, builders triviais de metadata
e classes validadoras sem polimorfismo. Essa simplificacao preserva a v0.1
porque mantem o comportamento observavel, os checks estruturais e os erros
rastreaveis, apenas reduzindo pontos de extensao antes de haver pressao real.
