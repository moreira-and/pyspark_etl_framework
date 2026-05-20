# Auditoria SR15 - Testes por Promessa da v0.1

## Escopo e restricoes

Papel exercido: especialista rigoroso em arquitetura de testes da promessa v0.1.

Arquivos usados como entrada obrigatoria:

- `prompts/_v0.1/auditoria/auditoria_sr15_testes_promessa_v01.md`
- `README.md`
- `docs/v0.1-contract.md`
- `docs/v0.1-known-limitations.md`
- `tests/`
- `etl_framework/`

Restricao observada: nenhum relatorio de auditoria independente anterior foi lido como fonte de conteudo.

Verificacao local executada:

- `poetry run pytest`: coletou 182 testes e chegou a 91% sem falhas antes de timeout de 120s.
- `poetry run pytest tests/test_validate_struct.py`: 15 passed em 35.74s; houve warning de cache por acesso negado em `.pytest_cache`.

Conclusao executiva: a suite protege bem as promessas centrais da v0.1 por comportamento observavel, especialmente fluxo oficial, preflight, auto_check, auto_validate, dry-run, erros gerenciados e API publica. A fragilidade principal nao e ausencia geral de testes, mas dependencia de cobertura indireta para alguns limites operacionais e ausencia de poucos cenarios integrados P1/P2 que deveriam travar regressao em runtime completo.

## Matriz promessa -> teste

| Promessa | Arquivo de teste | Comportamento testado | Caminho | Avaliacao |
| --- | --- | --- | --- | --- |
| Fluxo oficial `extract -> check -> transform -> validate -> load -> certify` | `tests/test_pipeline_contract.py:295`, `tests/test_framework_integration.py:95`, `tests/test_pipeline_author_journey.py:220` | `Pipeline.run()` executa etapas em ordem, retorna DataFrame final e chama load/certify | Feliz | Forte. Testa superficie publica e efeito observavel, nao apenas chamada interna. |
| Injecao de `SparkSession`, `EtlRunConfig` e `EtlExecutionContext` | `tests/test_pipeline_contract.py:666` | Todas as etapas recebem as mesmas instancias | Feliz | Forte. Protege contrato operacional relevante para autores de pipeline. |
| Preflight falha antes de `_extract` sem `source_struct` | `tests/test_pipeline_contract.py:322`, `tests/test_auto_contract.py:286` | `PreflightError`, etapa `preflight`, nenhum evento de extract/custom logic | Falha | Forte. Valida fail-fast antes de acessar origem. |
| Preflight falha antes de `_extract` sem `target_struct` | `tests/test_pipeline_contract.py:345`, `tests/test_auto_contract.py:336` | `PreflightError`, `events == []`, `_extract` nao chamado | Falha | Forte. Protege promessa critica. |
| Preflight rejeita `target_key` inconsistente | `tests/test_pipeline_contract.py:365`, `tests/test_config.py:176` | Falha antes de extrair quando chave nao existe no `target_struct` | Falha | Boa. Cobre modelo e rechecagem em runtime via mutacao controlada. |
| `auto_check` roda depois de `_extract` e antes de custom check | `tests/test_stage_contract_template_method.py:181`, `tests/test_auto_contract.py:114` | `Extract.run()` chama extract, valida source_struct e depois custom hook | Feliz | Forte. Testa Template Method real. |
| `auto_check` rejeita coluna obrigatoria ausente | `tests/test_stage_contract_template_method.py:203`, `tests/test_auto_contract.py:300`, `tests/test_production_checks.py:27` | `CheckError`/`ValueError` com coluna ausente | Falha | Forte. Protege drift estrutural de origem. |
| `auto_check` rejeita tipo incompativel | `tests/test_production_checks.py:42` | `validate_schema` falha com tipo esperado vs atual | Falha | Parcial. Cobre utilitario, mas nao ha teste integrado `Pipeline.run()` para source type mismatch. |
| `auto_check` nao executa acoes Spark | `tests/test_auto_contract.py:141` | Monkeypatch bloqueia `count`, `collect`, `show`, `toLocalIterator` | Feliz/limite | Forte para custo de origem. |
| Politica de colunas extras `ignore/warn/fail` | `tests/test_validate_struct.py:63`, `tests/test_validate_struct.py:90`, `tests/test_validate_struct.py:104`, `tests/test_validate_struct.py:116`, `tests/test_auto_contract.py:314` | Extras ignoradas, avisadas ou bloqueadas; `strict_schema=True` vira warning | Feliz/falha | Boa no utilitario; integrada apenas para warning no source. Falta `Pipeline.run()` cobrindo `extra_columns_policy="fail"` em source/target. |
| `auto_validate` roda depois de `_transform` e antes de custom validate/load | `tests/test_stage_contract_template_method.py:219`, `tests/test_auto_contract.py:114` | `Transform.run()` adiciona `is_valid` e chama custom validate depois | Feliz | Forte. |
| `auto_validate` rejeita coluna target ausente | `tests/test_stage_contract_template_method.py:247`, `tests/test_auto_contract.py:350` | `ValidateError` com coluna ausente | Falha | Forte. |
| `auto_validate` rejeita tipo target incompativel | `tests/test_auto_contract.py:366`, `tests/test_validate_struct.py:43` | Falha com expected/got | Falha | Forte. |
| `auto_validate` adiciona `is_valid` | `tests/test_auto_contract.py:166`, `tests/test_pipeline_contract.py:295`, `tests/test_framework_integration.py:95` | DataFrame final contem `is_valid` | Feliz | Forte. |
| Checks SQL declarativos `severity=error` participam de `is_valid` | `tests/test_validate_struct.py:175`, `tests/test_auto_contract.py:394` | Regras de erro invalidam linhas e bloqueiam load | Falha | Forte. |
| Checks SQL `severity=warning` nao invalidam | `tests/test_validate_struct.py:128`, `tests/test_validate_struct.py:175` | Warning aparece em summary, mas `is_valid=True` | Feliz/limite | Boa. Cobertura em utilitario, nao em pipeline completa. |
| `NULL` em regra booleana e invalido | `tests/test_auto_contract.py:256`, `tests/test_auto_contract.py:379` | Null explicito via SQL check ou `is_valid` null falha | Falha | Forte. |
| Bloqueio de invalidos antes do `load` | `tests/test_auto_contract.py:394`, `tests/test_production_checks.py:54` | `ValidateError`, load/certify nao chamados | Falha | Forte. Promessa critica protegida. |
| Diagnostico minimo de invalidos inclui `invalid_count` e checks quebrados | `tests/test_auto_contract.py:177`, `tests/test_auto_contract.py:394` | Mensagem contem `invalid_count=1` e nome do check | Falha | Boa. Testa conteudo essencial, mas pouco sensivel a multiplos checks/campos em pipeline integrada. |
| `auto_validate` usa `limit(1).count()` e nao expoe registros | `tests/test_auto_contract.py:177`, `tests/test_pipeline_contract.py:571` | Observa `limit(1)` antes de `count`, bloqueia `show`/`toLocalIterator`; producao nao chama `show`/`collect` no framework | Feliz/falha | Forte para a promessa de custo/privacidade declarada. |
| `nullable=False` nao bloqueia nulo sozinho | `tests/test_auto_contract.py:237` | Nulo passa quando nao ha check SQL explicito | Limite | Forte. Protege limite documentado. |
| Dry-run limita apos extract/check e antes de transform | `tests/test_dry_run.py:161`, `tests/test_pipeline_author_journey.py:220` | Check ve 3 linhas, transform/validate ve 2, resultado tem 2 | Feliz/limite | Forte. Protege semantica exata do dry-run. |
| Dry-run pula `_load` e `_certify` | `tests/test_stage_contract_template_method.py:270`, `tests/test_dry_run.py:161`, `tests/test_auto_contract.py:443` | Load/certify nao chamados; eventos de evidencia emitidos | Feliz | Forte. |
| `dry_run_show_rows` so chama `show` quando explicitamente > 0 | `tests/test_dry_run.py:183`, `tests/test_dry_run.py:201`, `tests/test_dry_run.py:219` | Producao e dry-run zero nao chamam `show`; positivo chama com limite | Feliz/limite | Forte. |
| Load recebe colunas tecnicas por default | `tests/test_stage_contract_template_method.py:305`, `tests/test_production_checks.py:87` | `_load` ve `is_valid` | Feliz | Forte. |
| `keep_technical_columns=False` remove colunas tecnicas | `tests/test_stage_contract_template_method.py:331`, `tests/test_production_checks.py:145` | `_load` recebe apenas colunas de negocio | Feliz | Forte. |
| `_certify` e opcional | `tests/test_stage_contract_template_method.py:378` | Load sem override de certify executa sem erro | Feliz | Boa. |
| Erros gerenciados por etapa com `pipeline_name`, `run_id`, stage e causa | `tests/test_pipeline_contract.py:791`, `tests/test_errors.py:10`, `tests/test_stage.py:133` | Excecoes genericas viram `ExtractError`/`CheckError`/etc. com contexto | Falha | Forte. |
| Falha interrompe etapas downstream | `tests/test_pipeline_contract.py:907`, `tests/test_pipeline_author_journey.py:220` | Validate/check falha e load nao roda | Falha | Forte. |
| Falha em `certify` nao e mascarada como `load` | `tests/test_pipeline_contract.py:521` | `CertifyError`, `load_succeeded`, sem `load_failed` | Falha | Forte. Protege rastreabilidade de etapa. |
| Observabilidade emite eventos tecnicos por etapa | `tests/test_pipeline_contract.py:382`, `tests/test_stage_contract_template_method.py:181`, `tests/test_stage.py:89` | Eventos started/succeeded por etapa com trace fields | Feliz | Forte. |
| Observabilidade de falha inclui contexto e resumo | `tests/test_pipeline_contract.py:471`, `tests/test_stage.py:133` | Eventos `_failed`, `execution_summary`, erro sanitizado | Falha | Forte. |
| Observabilidade best-effort: falha do sink nao quebra ETL | `tests/test_observability.py:123`, `tests/test_stage.py:118` | Service/decorator tolera sink quebrado | Falha de observabilidade | Boa, mas nao integrada em `Pipeline.run()` completo. |
| Observabilidade nao deve ser chamada manualmente por contratos/pipeline | `tests/test_runtime_observability_architecture.py:8` | AST impede imports/chamadas de infra observability nos contratos | Estrutural | Parcial. Protege arquitetura, nao comportamento. |
| API publica raiz pequena | `tests/test_package_import.py:22` | `__all__` contem apenas `Pipeline`, config/context e contratos | Feliz | Forte e simples. |
| Limites de custo de summary/checks | `tests/test_validate_struct.py:253`, `tests/test_validate_struct.py:310`, `tests/test_validate_struct.py:464` | Summary opt-in, default nao coleta, checks em lote | Limite | Boa. Protege limites importantes do contrato de custo. |
| Helpers operacionais opcionais | `tests/test_production_checks.py:200`, `tests/test_production_checks.py:219`, `tests/test_production_checks.py:255`, `tests/test_production_checks.py:272` | Target key, volume, freshness, reconciliacao, split valid/invalid, metricas | Feliz/falha | Boa, mas sao helpers opcionais; nao devem ser confundidos com promessa core. |

## Promessas sem protecao suficiente

1. `Observability best-effort` no fluxo completo de `Pipeline.run()` com sink quebrado.
   - Evidencia existente: `tests/test_observability.py:123` testa `ObservabilityService` diretamente; `tests/test_stage.py:118` testa um metodo decorado isolado.
   - Lacuna: falta um teste integrado configurando `FailingObservabilitySink` e executando `Pipeline.run()` completo com extract/check/transform/validate/load/certify, provando que nenhum evento runtime real da pipeline quebra a ETL.
   - Severidade: P1. A promessa e operacional e transversal; cobertura apenas por unidade/decorator pode deixar regressao de composicao escapar.

2. `extra_columns_policy="fail"` aplicado pelo fluxo oficial.
   - Evidencia existente: `tests/test_validate_struct.py:116` cobre o utilitario; `tests/test_auto_contract.py:314` cobre warning integrado para source.
   - Lacuna: falta `Pipeline.run()` falhando em `check` para coluna extra na origem com policy `fail` e falhando em `validate` para coluna extra no target com policy `fail`.
   - Severidade: P2. A regra e documentada como controle de schema e deveria ser travada na jornada real, nao so no helper.

3. Tipo incompativel na origem via `Pipeline.run()`.
   - Evidencia existente: `tests/test_production_checks.py:42` cobre `validate_schema`; `tests/test_auto_contract.py:300` cobre missing column via pipeline.
   - Lacuna: falta teste integrado em que `_extract` retorna coluna existente com tipo errado e o framework levanta `CheckError` antes de transform/load.
   - Severidade: P2. O helper esta coberto, mas a promessa e `auto_check` no fluxo oficial.

4. Limites negativos de load seguro/idempotencia/rollback/certificacao real.
   - Evidencia existente: `docs/v0.1-known-limitations.md` declara fora de escopo; testes mostram que `Load` e abstrato/concreto do usuario e que `_certify` default nao faz nada (`tests/test_stage_contract_template_method.py:378`).
   - Lacuna: nao ha teste que impeça introducao acidental de promessa falsa, como um sink/load generico "seguro" no pacote publico, estrategia generica de escrita, ou certificacao real simulada como se fosse garantia.
   - Severidade: P3. Menos critico para comportamento atual, mas importante para preservar honestidade da v0.1.

5. Promessa de logs/eventos "best-effort por etapa via runtime" versus detalhes de payload por todas as etapas.
   - Evidencia existente: `tests/test_pipeline_contract.py:382` valida sequencia completa de eventos no sucesso; `tests/test_observability.py:55` valida campos do builder.
   - Lacuna: nem todos os campos declarados em `docs/v0.1-contract.md` sao assertados etapa a etapa no teste integrado, por exemplo destino declarado, nivel operacional e metricas explicitas em eventos reais de pipeline.
   - Severidade: P2. A suite protege nomes e trace fields, mas a rastreabilidade completa do payload ainda depende de testes de builder separados.

## Testes desalinhados da promessa

1. `tests/test_runtime_observability_architecture.py`
   - Tipo de protecao: AST/estrutura interna.
   - Desalinhamento: protege uma decisao arquitetural importante, mas nao e teste comportamental de promessa v0.1. Ele nao prova que eventos sao emitidos, tolerados ou rastreaveis em runtime.
   - Risco: se usado como evidencia principal de observabilidade, inflaria a confianca indevidamente.

2. `tests/test_dependencies.py`
   - Tipo de protecao: dependencia e imports proibidos.
   - Desalinhamento: util para manutencao e simplicidade, mas nao protege diretamente fluxo oficial, preflight, auto_check, auto_validate, dry-run, erros ou API publica.
   - Risco: baixo. Deve permanecer, mas nao deve contar como cobertura de promessa operacional.

3. `tests/test_production_checks.py`
   - Tipo de protecao: helpers opcionais de prontidao operacional.
   - Desalinhamento parcial: o nome sugere protecao produtiva ampla, mas a v0.1 declara que nao garante prontidao produtiva irrestrita. Os testes sao validos para helpers opcionais, nao para promessa core de load seguro.
   - Risco: medio de comunicacao. Relatorios e documentacao de release nao devem vender esses testes como prova de go-live.

4. `tests/test_validate_struct.py`
   - Tipo de protecao: utilitario central.
   - Desalinhamento parcial: muitos cenarios sao robustos, mas alguns comportamentos prometidos como `auto_validate` no fluxo oficial ficam cobertos apenas por helper. Onde a promessa fala "framework antes do load", o teste mais valioso e integrado.
   - Risco: medio. Falhas de adaptacao entre helper e contratos poderiam escapar em casos nao integrados.

## Oportunidades P1/P2/P3

### P1

- Adicionar teste integrado `Pipeline.run()` com `FailingObservabilitySink`, caminho feliz completo e asserts de que o DataFrame retorna, load/certify executam e nenhuma excecao de sink vaza.
- Adicionar teste integrado de `auto_validate` com multiplos checks quebrados em campos diferentes, assertando `invalid_count`, lista de checks e `load.loaded is False`. Isso endurece diagnostico minimo no caminho de falha real.

### P2

- Adicionar `Pipeline.run()` com `extra_columns_policy="fail"` para origem: `_extract` retorna coluna extra, deve levantar `CheckError`, nao chamar transform/load.
- Adicionar `Pipeline.run()` com `extra_columns_policy="fail"` para target: transform retorna coluna extra, deve levantar `ValidateError`, nao chamar load.
- Adicionar `Pipeline.run()` com tipo errado na origem: coluna existe, tipo diverge, deve levantar `CheckError` antes de transform.
- Adicionar teste integrado de payload de observabilidade com metricas em `context.metrics` durante pipeline real, nao apenas `build_observability_event`.
- Adicionar teste integrado de `warning` severity no `target_struct` real de pipeline, provando que load ainda roda com `is_valid=True`.

### P3

- Adicionar teste de guarda da API publica para confirmar que `ObservabilityService`, `ObservabilitySink` e sinks concretos nao entram em `etl_framework.__all__`.
- Adicionar teste de documentacao/contrato simples que falhe se aparecer uma implementacao generica publica de load seguro sem teste explicito de idempotencia/rollback. Isso preserva a fronteira declarada da v0.1.
- Separar ou renomear alguns testes de helpers operacionais para deixar claro que eles nao certificam producao; isso reduz risco de interpretacao errada por usuarios junior.

## Respostas as perguntas obrigatorias

- Cada promessa critica possui pelo menos um teste?
  - Sim para as promessas centrais: fluxo oficial, preflight, auto_check, auto_validate, dry-run, erros gerenciados, observabilidade, API publica e limites principais. A ressalva e que algumas promessas sao protegidas por testes indiretos de helper/decorator, nao por fluxo integrado completo.

- Os testes validam comportamento observavel ou detalhe interno?
  - A maioria valida comportamento observavel: ordem de eventos, erros gerenciados, ausencia de chamadas downstream, DataFrame final, colunas entregues ao load, eventos emitidos e chamadas Spark bloqueadas. Existem testes estruturais por AST em observabilidade e dependencia que sao uteis, mas nao devem ser contados como protecao comportamental da promessa.

- Existem promessas sem teste?
  - Nao ha promessa core completamente sem teste. Existem limites e garantias negativas com protecao fraca ou indireta, principalmente best-effort integrado de observabilidade, policy `fail` no fluxo oficial e fronteira contra falsas promessas de load seguro.

- Existem testes que protegem limites declarados da v0.1?
  - Sim. Exemplos: `nullable=False` nao bloqueia nulo sem check explicito (`tests/test_auto_contract.py:237`), dry-run limita depois de check (`tests/test_dry_run.py:161`), `show` so com `dry_run_show_rows > 0` (`tests/test_dry_run.py:219`), summary e opt-in (`tests/test_validate_struct.py:253`), default nao coleta (`tests/test_validate_struct.py:310`), `_certify` e opcional (`tests/test_stage_contract_template_method.py:378`) e sink de observabilidade e best-effort (`tests/test_stage.py:118`).

- Existem cenarios P1 sem teste de falha?
  - Sim: falha do sink de observabilidade durante `Pipeline.run()` completo. Ha teste do service e do decorator, mas nao do fluxo real completo com multiplas etapas. Tambem recomendo tratar como P1 o diagnostico integrado de multiplos invalidos/checks no caminho de `auto_validate`, porque ele e a principal barreira antes do `load`.

## Parecer final

A suite tem valor real de protecao comportamental e esta acima do minimo aceitavel para uma v0.1 honesta. O caminho oficial e os controles automaticos mais perigosos estao cobertos com testes de integracao Spark e asserts de efeitos observaveis.

Ainda assim, a auditoria nao deve aprovar a suite como "fechada". O risco residual esta em promessas transversais cobertas por pecas isoladas, principalmente observabilidade best-effort, e em politicas de schema cobertas no utilitario mas nao no fluxo oficial. A correcao recomendada nao e aumentar percentual de cobertura; e adicionar poucos testes integrados de alto sinal que comprovem as promessas exatamente onde o usuario da v0.1 as consome: `Pipeline.run()`.
