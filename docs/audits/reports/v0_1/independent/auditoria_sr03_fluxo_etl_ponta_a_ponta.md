# Auditoria SR03 - Fluxo ETL Ponta a Ponta

## Veredito executivo

**Status: aprovado com ressalvas severas para uso v0.1 controlado.**

O framework implementa e testa a ordem oficial `extract -> check -> transform -> validate -> load -> certify` no caminho nominal. A orquestracao esta concentrada em `Pipeline.run()` e os contratos `Extract`, `Transform` e `Load` usam Template Method para separar codigo de negocio de etapas automaticas.

A promessa, porem, nao e plenamente segura como contrato operacional ponta a ponta. Ha tres fragilidades relevantes:

1. `certify` e declarado como etapa oficial, mas pode ser um no-op silencioso por default.
2. `validate` altera o DataFrame ao adicionar `is_valid` antes de `load`, vazando coluna tecnica para a pipeline concreta por default.
3. `dry_run` evita escrita, mas ainda executa `extract`, `check`, `transform`, `validate` e acoes Spark de validacao; portanto nao e um modo previsivelmente barato, apenas um modo sem `_load` e `_certify`.

## Escopo analisado

Arquivos obrigatorios analisados:

- `etl_framework/contracts/pipeline.py`
- `etl_framework/contracts/extract.py`
- `etl_framework/contracts/transform.py`
- `etl_framework/contracts/load.py`
- `etl_framework/models/config.py`
- `etl_framework/models/context.py`
- `docs/v0.1-contract.md`
- testes relacionados a pipeline, dry-run, contratos automaticos e integracao

Tambem foram analisados utilitarios diretamente chamados pelo fluxo:

- `etl_framework/utils/auto_quality.py`
- `etl_framework/utils/validate_struct.py`
- `etl_framework/utils/dataframe_checks.py`
- `etl_framework/utils/stage_metadata.py`
- `etl_framework/infra/stage.py`

## Mapa do fluxo real

Fluxo implementado:

```text
Pipeline.run()
  -> _preflight()
  -> extract()
       -> Extract.run()
            -> _run_extract()
                 -> pipeline concreta: _extract()
                 -> require_dataframe(stage="extract")
            -> _run_check()
                 -> framework: auto_check_source(source_struct)
                 -> pipeline concreta opcional: _custom_check()
                 -> require_dataframe(stage="check")
            -> se dry_run: _limit_dry_run_extract()
  -> transform(df)
       -> Transform.run()
            -> _run_transform()
                 -> pipeline concreta: _transform()
                 -> require_dataframe(stage="transform")
            -> _run_validate()
                 -> framework: auto_validate_target(target_struct)
                 -> pipeline concreta opcional: _custom_validate()
                 -> require_dataframe(stage="validate")
  -> load(df)
       -> Load.run()
            -> se dry_run:
                 -> _run_dry_run()
                      -> _persistable_df()
                      -> opcional: df.show(dry_run_show_rows)
                 -> return
            -> _run_load()
                 -> _persistable_df()
                 -> pipeline concreta: _load()
            -> _run_certify()
                 -> _persistable_df()
                 -> pipeline concreta opcional: _certify()
  -> retorna DataFrame validado
```

Evidencias:

- `Pipeline.run()` chama `_preflight()`, `extract()`, `transform(df)` e `load(df)` nessa ordem em `etl_framework/contracts/pipeline.py:45`.
- `Extract.run()` executa `_run_extract()`, `_run_check()` e aplica limite em `dry_run` depois do check em `etl_framework/contracts/extract.py:27`.
- `Transform.run()` executa `_run_transform()` e depois `_run_validate()` em `etl_framework/contracts/transform.py:26`.
- `Load.run()` decide entre `_run_dry_run()` ou `_run_load()` seguido de `_run_certify()` em `etl_framework/contracts/load.py:27`.
- O contrato documentado descreve o mesmo fluxo em `docs/v0.1-contract.md:31`.

## Matriz etapa -> dono -> responsabilidade -> evidencia

| Etapa | Dono real | Responsabilidade real | Evidencia |
| --- | --- | --- | --- |
| `preflight` | Framework | Falhar antes do extract quando `source_struct`, `target_struct`, `target_key` ou `dry_run_show_rows` tornam a execucao invalida. | `etl_framework/contracts/pipeline.py:53`; testes em `tests/test_pipeline_contract.py:322` e `tests/test_pipeline_contract.py:345`. |
| `extract` | Pipeline concreta, envelopada pelo framework | Ler origem e retornar `DataFrame`; framework valida tipo de retorno. | `etl_framework/contracts/extract.py:50`; `require_dataframe` em `etl_framework/utils/dataframe_checks.py:6`. |
| `check` | Framework + hook opcional da pipeline | Framework valida schema de origem; depois pipeline pode executar `_custom_check`. | `etl_framework/contracts/extract.py:73`; `auto_check_source` em `etl_framework/utils/auto_quality.py:15`. |
| `dry_run limit` | Framework | Limitar dados apos `check` e antes de `transform`. | `etl_framework/contracts/extract.py:96`; teste `tests/test_dry_run.py:161`. |
| `transform` | Pipeline concreta, envelopada pelo framework | Aplicar regra de negocio e retornar `DataFrame`; framework valida tipo de retorno. | `etl_framework/contracts/transform.py:46`; teste de jornada em `tests/test_pipeline_author_journey.py`. |
| `validate` | Framework + hook opcional da pipeline | Framework valida target, adiciona `is_valid`, bloqueia invalidos; depois pipeline pode executar `_custom_validate`. | `etl_framework/contracts/transform.py:66`; `auto_validate_target` em `etl_framework/utils/auto_quality.py:34`; bloqueio testado em `tests/test_auto_contract.py:394`. |
| `load` | Pipeline concreta, com pre-processamento do framework | Framework remove ou preserva colunas tecnicas conforme config; pipeline escreve destino. | `etl_framework/contracts/load.py:52`; `_persistable_df` em `etl_framework/contracts/load.py:119`; teste em `tests/test_stage_contract_template_method.py:305`. |
| `certify` | Pipeline concreta opcional | Produzir evidencia final apos load; default nao faz nada. | `etl_framework/contracts/load.py:135`; teste `tests/test_stage_contract_template_method.py:349`. |

## Respostas as perguntas obrigatorias

### A ordem oficial e realmente respeitada?

Sim no caminho nominal. A ordem e implementada em `Pipeline.run()` e reforcada pelos contratos internos. Os testes `test_run_executes_official_order` e `test_inline_framework_integration_runs_full_contract` validam a sequencia `extract`, `check`, `transform`, `validate`, `load`, `certify` em `tests/test_pipeline_contract.py:295` e `tests/test_framework_integration.py:124`.

Ressalva: a etapa `preflight` nao aparece no fluxo oficial, mas executa comportamento critico antes de `extract`. Ela esta documentada como parte da coordenacao em `docs/v0.1-contract.md:33`, entao nao e uma divergencia grave, mas deve continuar explicita para o autor da pipeline.

### O desenvolvedor implementa apenas `_extract`, `_transform`, `_load` e opcionalmente hooks declarados?

Na API normal, sim. `Extract._extract`, `Transform._transform` e `Load._load` sao abstratos; `_custom_check`, `_custom_validate` e `_certify` sao opcionais. A API raiz tambem expõe apenas `Pipeline`, `EtlRunConfig`, `EtlExecutionContext`, `Extract`, `Transform` e `Load`.

Ressalva severa: `Load._certify` default retorna `None`, entao uma etapa oficial pode ser "executada" sem evidencia real. Isso enfraquece o contrato para leitores que entendem `certify` como garantia operacional.

### `check` e `validate` ficam de fato no framework?

Parcialmente. O check estrutural de origem e a validacao estrutural/declarativa de destino ficam no framework. Entretanto, ambos permitem hooks da pipeline apos a validacao automatica: `_custom_check` e `_custom_validate`. Isso e documentado, mas cria uma area onde responsabilidades podem vazar se os hooks forem usados para regras grandes, duplicadas ou com acoes Spark caras.

### O `dry_run` altera o fluxo de forma previsivel?

Sim quanto a escrita: ele pula `_load` e `_certify`. O teste `test_dry_run_limits_after_check_and_skips_load` demonstra que `check` recebe 3 linhas, `transform` recebe 2 apos `dry_run_limit`, e `load/certify` nao sao chamados em `tests/test_dry_run.py:161`.

Nao totalmente quanto a custo: `dry_run` ainda executa `_extract`, `auto_check`, `_transform`, `auto_validate` e pode executar `show`. Alem disso, `auto_validate_target` executa `limit(1).count()` sempre para detectar invalidos e, em falha, `count()` e `collect()` de resumo em `etl_framework/utils/auto_quality.py:78`, `etl_framework/utils/auto_quality.py:81` e `etl_framework/utils/auto_quality.py:99`.

### Alguma etapa executa comportamento critico de forma escondida?

Sim.

- `validate` adiciona `is_valid` automaticamente e bloqueia invalidos antes do load. Isso e bom operacionalmente, mas muda o shape do DataFrame depois da transformacao.
- `load` chama `_persistable_df()` antes de `_load` e `_certify`, podendo remover colunas tecnicas se `keep_technical_columns=False`.
- `dry_run` registra evidencia e pode chamar `df.show()` dentro de `load` quando `dry_run_show_rows > 0`.
- `preflight` falha antes de qualquer leitura, apesar de nao aparecer no nome do fluxo oficial.

Esses comportamentos estao documentados, mas continuam sendo criticos e precisam aparecer em guias de autor de pipeline, nao apenas no contrato tecnico.

### Ha duplicacao ou vazamento de responsabilidade entre contratos?

Ha vazamento controlado, mas real:

- `Load` recebe por default um DataFrame com coluna tecnica `is_valid`, embora `target_struct` rejeite colunas tecnicas em `EtlRunConfig`. O autor de `_load` precisa saber decidir entre persistir ou remover metadados via `keep_technical_columns`.
- `certify` e etapa oficial, mas sua semantica fica inteiramente na pipeline concreta e pode ser nula.
- `_custom_check` e `_custom_validate` permitem que a pipeline reintroduza regras de qualidade fora do mecanismo declarativo do framework.
- `context.metrics` aceita metricas operacionais arbitrarias; isso nao quebra o fluxo, mas pode virar canal de estado informal se mal usado.

## Divergencias entre contrato documentado e implementacao

| Tema | Documento | Implementacao | Avaliacao |
| --- | --- | --- | --- |
| Ordem oficial | `extract -> check -> transform -> validate -> load -> certify` em `docs/v0.1-contract.md:29`. | Ordem implementada e testada. | Aderente. |
| `preflight` | Documentado como passo de `Pipeline.run()` em `docs/v0.1-contract.md:33`. | Falha antes do extract em `pipeline.py:53`. | Aderente, mas precisa ser tratado como etapa critica. |
| `check` | Framework valida nomes e tipos contra `source_struct`. | `auto_check_source` chama `validate_schema`. | Aderente. |
| `validate` | Framework valida target, cria `is_valid` e bloqueia invalidos. | `auto_validate_target` chama `validate_struct` e diagnosticos. | Aderente. |
| `dry_run` | Limita apos check, pula load/certify, `show` opcional. | Implementado em `Extract.run()` e `Load.run()`. | Aderente, com ressalva de custo. |
| `certify` | Etapa oficial e responsabilidade da pipeline concreta. | Hook default nao faz nada. | Ambiguo: contrato permite, mas o nome da etapa sugere garantia maior que a implementada. |
| Load seguro | Documento afirma que v0.1 nao fornece implementacao segura de load. | `Load` apenas chama `_load`; sem idempotencia, rollback ou staging. | Aderente, mas operacionalmente severo. |

## Pontos de vazamento ou ambiguidade

### Ponto 1 - `certify` oficial pode ser no-op

`Load._certify()` retorna `None` por default. O teste `test_load_certify_hook_is_optional_when_no_evidence_is_needed` valida esse comportamento. Para um fluxo que promete `certify` como etapa oficial, isso e perigoso: o evento `certify_succeeded` pode representar apenas a execucao de um metodo vazio, nao uma certificacao real de destino.

Impacto: falso senso de garantia operacional apos escrita.

Severidade: **P1**.

### Ponto 2 - `validate` injeta `is_valid` e altera contrato entregue ao `load`

`validate_struct` adiciona `is_valid` em `etl_framework/utils/validate_struct.py:200`. O `Load` recebe esse DataFrame por default porque `keep_technical_columns=True` em `EtlRunConfig`. Isso obriga o autor de `_load` a conhecer coluna tecnica do framework ou configurar `keep_technical_columns=False`.

Impacto: responsabilidade de shape final vaza para a pipeline concreta; risco de persistir coluna tecnica indesejada.

Severidade: **P1**.

### Ponto 3 - `dry_run` nao e contrato de baixo custo

O dry-run limita depois de `check`, portanto a leitura e a validacao inicial rodam sobre o resultado extraido. Depois, `validate` ainda pode disparar acoes Spark para bloquear invalidos. A documentacao reconhece isso, mas o nome `dry_run` pode induzir autor junior a esperar baixo custo.

Impacto: jobs grandes podem continuar caros em dry-run; risco de surpresa operacional.

Severidade: **P2**.

### Ponto 4 - Hooks customizados podem duplicar qualidade automatica

`_custom_check` e `_custom_validate` rodam apos os checks automaticos. O contrato diz que devem ser pequenos, mas nada no codigo limita acoes Spark, escrita lateral, duplicacao de schema check ou mudanca agressiva do DataFrame.

Impacto: separacao de responsabilidades depende de disciplina do autor da pipeline.

Severidade: **P2**.

### Ponto 5 - `Load` combina preparo de shape, escrita e certificacao

`Load.run()` decide dry-run, prepara DataFrame persistivel, chama escrita e chama certificacao. Isso mantem o fluxo simples, mas concentra responsabilidades diferentes em um contrato que a pipeline concreta precisa entender bem.

Impacto: autores junior podem colocar idempotencia, staging, validacao final e certificacao em lugares inconsistentes.

Severidade: **P2**.

### Ponto 6 - `target_key` e validado, mas nao orienta execucao de load

`target_key` e validado contra `target_struct`, mas nao ha uso no contrato `Load` para merge, idempotencia, deduplicacao ou escopo de overwrite. O documento declara isso fora do escopo, mas a presenca do campo pode parecer promessa operacional maior.

Impacto: ambiguidade de contrato para quem espera uso automatico da chave.

Severidade: **P3**.

## Testes que demonstram a ordem e o contrato

- `tests/test_pipeline_contract.py:295`: valida ordem completa `extract`, `check`, `transform`, `validate`, `load`, `certify`.
- `tests/test_framework_integration.py:124`: valida integracao inline com fluxo completo.
- `tests/test_stage_contract_template_method.py:270`: valida que `Load.run()` em dry-run nao chama `_load` nem `_certify`.
- `tests/test_stage_contract_template_method.py:305`: valida `load` seguido de `certify` quando `dry_run=False`.
- `tests/test_dry_run.py:161`: valida que o limite de dry-run ocorre apos `check` e antes de `transform`.
- `tests/test_auto_contract.py:394`: valida que registros invalidos no target sao bloqueados antes de `load`.
- `tests/test_pipeline_contract.py:322` e `tests/test_pipeline_contract.py:345`: validam preflight antes de extract quando `source_struct` ou `target_struct` faltam.

Lacunas de teste relevantes:

- Nao ha teste que prove que `certify_succeeded` diferencia certificacao real de `_certify` default no-op.
- Nao ha teste de contrato impedindo `_custom_check` ou `_custom_validate` de executar escrita lateral ou acoes Spark caras.
- Nao ha teste que deixe explicito que `target_key` nao participa do load.
- Nao ha teste ponta a ponta demonstrando `keep_technical_columns=False` em `Pipeline.run()`, apenas em `Load.run()` isolado.

## Oportunidades

### P1 - Tornar `certify` semanticamente honesto

Opcoes aceitaveis:

- renomear evento/etapa default para indicar `certify_skipped` quando `_certify` nao for sobrescrito;
- exigir implementacao explicita de `_certify` em pipelines produtivas;
- adicionar configuracao declarativa `certify_required=True` para falhar quando o hook default for usado.

Critério: `certify_succeeded` nao deve significar "metodo vazio executou".

### P1 - Reduzir vazamento de `is_valid` para `_load`

Reavaliar o default de `keep_technical_columns`. Para o contrato junior e para destinos de negocio, `False` e mais seguro. Se manter `True`, a documentacao de autor de pipeline deve destacar que `_load` recebe coluna tecnica por default.

Critério: o autor de `_load` deve saber exatamente quais colunas chegam sem precisar ler `validate_struct`.

### P2 - Separar dry-run sem escrita de dry-run barato

Documentar e/ou modelar dois conceitos:

- `dry_run=True`: nao escreve;
- futuro modo de amostragem/custo reduzido: limita ou filtra antes de leituras caras quando a origem permitir.

Critério: nenhum usuario deve inferir que `dry_run` reduz custo de leitura.

### P2 - Endurecer contratos dos hooks opcionais

Adicionar guidance e testes negativos para deixar claro que `_custom_check` e `_custom_validate` nao devem escrever, logar manualmente, duplicar schema check ou executar certificacao.

Critério: hooks permanecem pequenos e nao viram mini-pipelines paralelas.

### P2 - Explicitar contrato de `Load`

Criar uma secao curta com "o que `_load` recebe" e "o que `_load` nao recebe":

- recebe DataFrame ja validado;
- pode receber colunas tecnicas;
- nao recebe garantia de idempotencia;
- nao recebe staging automatico;
- nao recebe uso automatico de `target_key`.

Critério: junior consegue implementar `_load` sem assumir comportamento inexistente.

### P3 - Amarrar `target_key` a expectativa operacional

Ou documentar o campo como metadado declarativo sem efeito de escrita, ou preparar interface futura para loads idempotentes. No estado atual, o campo e validado, mas nao executa nada.

Critério: nome e presenca do campo nao devem sugerir merge/upsert automatico.

## Conclusao

O fluxo ponta a ponta esta tecnicamente implementado e testado na ordem prometida. `check` e `validate` sao de fato responsabilidades centrais do framework, e o autor da pipeline consegue implementar o caminho comum com `_extract`, `_transform` e `_load`.

O ponto severo nao e a ordem; e a semantica operacional ao redor dela. `certify` pode ser vazio, `load` pode receber coluna tecnica sem que isso seja obvio no ponto de implementacao, e `dry_run` e previsivel para evitar escrita, mas nao para custo. Para v0.1, isso e aceitavel somente com documentacao explicita e revisao senior antes de producao, exatamente como o contrato ja sugere. Para evoluir alem de v0.1, esses pontos devem ser tratados antes de vender o fluxo como garantia ponta a ponta robusta.
