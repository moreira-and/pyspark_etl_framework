# Auditoria Independente SR05 - Load e Certify v0.1

## Escopo E Metodo

Papel executado: especialista em load/certify para v0.1, com foco em escrita,
certificacao e risco de producao.

Arquivos revisados:

- `etl_framework/contracts/load.py`
- `etl_framework/contracts/pipeline.py`
- `etl_framework/models/config.py`
- `tests/test_stage_contract_template_method.py`
- `tests/test_dry_run.py`
- `tests/test_framework_integration.py`
- `tests/test_production_checks.py`
- `README.md`
- `QUICK_START.md`
- `docs/v0.1-contract.md`
- `docs/v0.1-known-limitations.md`
- `docs/operation/load-readiness-checklist.md`
- utilitarios necessarios de evento/erro: `etl_framework/infra/stage.py` e
  `etl_framework/utils/stage_metadata.py`

Verificacao executavel: tentativas de rodar subconjuntos focados com `pytest`
foram encerradas por timeout de 120s antes de retornar resultado. Portanto, esta
auditoria usa leitura estatica rastreavel e referencia testes existentes, mas nao
declara uma execucao local bem-sucedida.

## Veredito

Veredito: **aprovado com ressalvas P1/P2 para o escopo v0.1, nao aprovado como
garantia produtiva generica**.

O codigo de `Load.run()` esta aderente ao contrato declarado da v0.1: com
`dry_run=True`, executa somente `_run_dry_run()` e retorna antes de `_run_load()`
e `_run_certify()` (`etl_framework/contracts/load.py:35-43`). Com
`dry_run=False`, executa `_run_load()` e depois `_run_certify()`
(`etl_framework/contracts/load.py:44-54`). A responsabilidade de escrita segura
continua corretamente fora do framework generico: `_load()` e abstrato
(`etl_framework/contracts/load.py:142-151`), `_certify()` e opcional e no-op por
default (`etl_framework/contracts/load.py:153-162`), e a documentacao afirma que
a v0.1 nao fornece implementacao segura de load (`docs/v0.1-contract.md:209-237`,
`docs/v0.1-known-limitations.md:24-35`).

O ponto severo: a v0.1 permite `dry_run=False` sem nenhum gate tecnico no
framework alem da configuracao basica. Isso e intencionalmente fora do escopo,
mas cria risco operacional real se um autor junior copiar um `Load` direto com
`append` ou `overwrite`. A mitigacao existe em documentacao e checklist, nao em
enforcement de runtime. Essa diferenca precisa permanecer explicita em qualquer
mensagem de release, exemplo e quick start.

## Respostas Obrigatorias

### O codigo deixa claro que load seguro e responsabilidade da pipeline concreta?

Sim, no contrato. `Pipeline` so coordena o fluxo e delega comportamento concreto
para os contratos injetados (`etl_framework/contracts/pipeline.py:17-21`,
`etl_framework/contracts/pipeline.py:45-50`). `Load._load()` e abstrato e recebe
`df`, `spark`, `config` e `context`, sem estrategia de destino padrao
(`etl_framework/contracts/load.py:142-151`). A documentacao reforca que staging,
escopo de overwrite, idempotencia, retry, rollback e certificacao lendo destino
real sao responsabilidade da pipeline concreta (`docs/v0.1-contract.md:223-237`).

### `dry_run=True` realmente impede `_load` e `_certify`?

Sim. A bifurcacao em `Load.run()` chama `_run_dry_run()` e retorna antes do
caminho real (`etl_framework/contracts/load.py:35-43`). `_run_dry_run()` apenas
aplica `_persistable_df()` e, opcionalmente, `_show_dry_run_sample()`
(`etl_framework/contracts/load.py:82-97`). Nao ha chamada a `_load()` ou
`_certify()` nesse metodo. Os testes existentes cobrem isso: `events == []` e
eventos `load_started`, `dry_run_evidence`, `dry_run_load_completed`
(`tests/test_stage_contract_template_method.py:270-302`); no fluxo de pipeline,
`load.load_called is False` e `load.certify_called is False`
(`tests/test_dry_run.py:161-180`).

### A evidencia de dry-run pode ser confundida com certificacao real?

No codigo, a evidencia e marcada como `stage="load"`, `status="skipped"`,
`mode="dry_run"` e `dry_run=True` via `dry_run_evidence_metadata`
(`etl_framework/contracts/load.py:72-82`,
`etl_framework/utils/stage_metadata.py:18-25`). Isso reduz a chance de confusao.
Porem, o evento chama-se `dry_run_evidence`, nao `dry_run_not_certified`, e a
certificacao real e opcional/no-op por default. O risco remanescente e mais de
interpretacao operacional do que de codigo: consumidores de log precisam entender
que esta evidencia prova ausencia de escrita, nao leitura do destino.
`docs/v0.1-known-limitations.md:82-83` explicita que essa evidencia nao e
certificacao real e nao executa `_certify`.

### `keep_technical_columns` e previsivel para o autor da pipeline?

Parcialmente sim. O default e `True` em `EtlRunConfig`
(`etl_framework/models/config.py:36-41`), a validacao rejeita valor nao booleano
(`etl_framework/models/config.py:105-106`), e `_persistable_df()` remove colunas
cujo nome em lower-case esta em `TECHNICAL_COLUMNS` quando
`keep_technical_columns=False` (`etl_framework/contracts/load.py:129-140`). Os
testes cobrem preservacao default de `is_valid` e exclusao quando configurado
(`tests/test_stage_contract_template_method.py:305-352`,
`tests/test_production_checks.py:87-197`).

Ressalva: a remocao e baseada em nomes reservados, nao em origem da coluna. Uma
coluna de negocio chamada `updated_at`, `inserted_at`, `etl_run_at`,
`etl_run_id` ou `is_valid` sera removida se `keep_technical_columns=False`.
`target_struct` rejeita colunas reservadas (`etl_framework/models/config.py:25-27`,
`etl_framework/models/config.py:166-167`), o que reduz o risco para saida
declarada, mas a regra ainda precisa estar clara para autores que manipulam
schemas fora do caminho ideal.

### O Quick Start deixa claro que o load de exemplo nao e produtivo?

Sim. O arquivo declara no inicio que e didatico e que `QuickLoad` nao e padrao
produtivo (`QUICK_START.md:6-7`). O exemplo usa `dry_run=True` por default
(`QUICK_START.md:88-93`), descreve que `Load.run()` registra evidencia tecnica e
pula `_load`/`_certify` (`QUICK_START.md:113-123`) e diz que trocar para
`dry_run=False` so e aceitavel apos checklist de readiness (`QUICK_START.md:129-136`).
Tambem manda substituir `QuickLoad` antes de uso real com `dry_run=False`
(`QUICK_START.md:151-154`).

### Existe risco de uso indevido com `dry_run=False`?

Sim, alto se o usuario ignorar a documentacao. `dry_run=False` aciona `_load()`
e depois `_certify()` sem exigir checklist, idempotencia, staging, transacao,
rollback, leitura de destino real ou metrica de escrita (`etl_framework/contracts/load.py:44-54`).
`write_mode` aceita apenas `overwrite` ou `append`, mas isso e validacao de valor,
nao seguranca operacional (`etl_framework/models/config.py:146-152`). O checklist
exige revisar escopo de escrita, rerun, falha antes/depois do commit,
certificacao e colunas tecnicas antes de `dry_run=False`
(`docs/operation/load-readiness-checklist.md:24-40`), mas permanece fora do
runtime.

## Caminhos De Execucao Em `Load.run()`

### Caminho `dry_run=True`

1. `Load.run()` verifica `config.dry_run`.
2. Chama `_run_dry_run(df, spark, config, context)`.
3. `_run_dry_run()` e decorado como stage `load`, com evento final
   `dry_run_load_completed` e status `skipped`
   (`etl_framework/contracts/load.py:70-82`).
4. O runtime emite `dry_run_evidence` com `stage=load`, `status=skipped`,
   `dry_run=True`, `dry_run_limit` e `dry_run_show_rows`
   (`etl_framework/contracts/load.py:76-82`,
   `etl_framework/utils/stage_metadata.py:18-25`).
5. `_persistable_df()` valida que recebeu DataFrame e aplica ou remove colunas
   tecnicas conforme `keep_technical_columns`
   (`etl_framework/contracts/load.py:129-140`).
6. Se `dry_run_show_rows > 0`, chama `df.show(..., truncate=False)`
   (`etl_framework/contracts/load.py:92-114`).
7. Retorna sem `_load()` e sem `_certify()`.

### Caminho `dry_run=False`

1. `Load.run()` chama `_run_load()`.
2. `_run_load()` e stage `load`, prepara `load_df` via `_persistable_df()` e
   chama `_load(load_df, spark, config, context)`
   (`etl_framework/contracts/load.py:57-68`).
3. Se `_load()` falhar, o decorator converte/propaga `LoadError` e emite falha
   de stage (`etl_framework/infra/stage.py:57-88`).
4. Apos sucesso, `Load.run()` chama `_run_certify()`.
5. `_run_certify()` e stage `certify`, prepara `certify_df` via
   `_persistable_df()` e chama `_certify(certify_df, spark, config, context)`
   (`etl_framework/contracts/load.py:116-127`).
6. Se `_certify()` falhar, a falha e de `CertifyError`, separada de `LoadError`
   pelo stage proprio (`etl_framework/contracts/load.py:116-127`,
   `etl_framework/infra/stage.py:57-88`).

## Matriz Risco -> Evidencia -> Impacto

| Risco | Evidencia | Impacto |
| --- | --- | --- |
| Uso produtivo indevido com `dry_run=False` | `Load.run()` executa `_run_load()` e `_run_certify()` sem gate operacional (`etl_framework/contracts/load.py:44-54`); checklist e documental (`docs/operation/load-readiness-checklist.md:24-56`). | Carga duplicada, sobrescrita ampla ou parcial se pipeline concreta usar `append`/`overwrite` sem escopo. Falha real fora do framework generico, mas risco alto de operacao. |
| Falsa certificacao por `_certify()` default no-op | `_certify()` retorna `None` por default (`etl_framework/contracts/load.py:153-162`); docs dizem que evidencia real exige sobrescrita (`docs/v0.1-contract.md:219-221`). | Evento `certify_succeeded` pode existir sem consulta ao destino se a pipeline nao implementar evidencia real. Aderente a v0.1, mas perigoso se vendido como garantia produtiva. |
| Evidencia de dry-run interpretada como certificacao | Evento `dry_run_evidence` marca `status=skipped` e `dry_run=True` (`etl_framework/contracts/load.py:72-82`, `etl_framework/utils/stage_metadata.py:18-25`); limitacao documentada (`docs/v0.1-known-limitations.md:82-83`). | Baixo risco no payload, medio risco em dashboards/resumos que ignorem `status` e `mode`. Pode induzir aprovacao operacional indevida. |
| Persistencia involuntaria de colunas tecnicas | Default `keep_technical_columns=True` (`etl_framework/models/config.py:36-41`); `_persistable_df()` preserva tudo nesse modo (`etl_framework/contracts/load.py:129-133`); docs avisam (`README.md:71-93`). | Destino pode receber `is_valid` e metadados tecnicos sem intencao. Previsivel se o autor leu docs; arriscado para junior que copia `_load` direto. |
| Remocao involuntaria de coluna de negocio com nome reservado | Remocao por `column.lower() not in config.TECHNICAL_COLUMNS` (`etl_framework/contracts/load.py:135-140`); reservadas incluem `inserted_at`, `updated_at`, `etl_run_at`, `etl_run_id`, `is_valid` (`etl_framework/models/config.py:51-59`). | Colunas de negocio com nomes reservados podem desaparecer quando `keep_technical_columns=False`. Mitigado por rejeicao em `target_struct`, mas ainda relevante em DataFrames nao declarados. |
| Exposicao de dados sensiveis em dry-run | `_show_dry_run_sample()` chama `show(..., truncate=False)` (`etl_framework/contracts/load.py:99-114`); config impede `dry_run_show_rows > 0` fora de dry-run (`etl_framework/models/config.py:130-135`); Quick Start alerta (`QUICK_START.md:138-139`). | Exposicao local/log de dados sensiveis em ambiente compartilhado. Nao escreve destino, mas pode violar confidencialidade. |
| Custo Spark nao limitado integralmente por dry-run | Documentacao afirma que limite ocorre depois de `_extract` e `auto_check` (`docs/v0.1-contract.md:194-207`, `docs/v0.1-known-limitations.md:80-81`); teste verifica check com 3 registros e transform/validate com 2 (`tests/test_dry_run.py:161-180`). | Dry-run reduz risco de escrita, nao garante baixo custo de leitura. Pode causar custo/latencia em fontes grandes. |
| Falha em `_certify()` apos commit sem rollback automatico | Fluxo chama `_certify()` depois de `_load()` (`etl_framework/contracts/load.py:44-54`); checklist exige documentar falha depois do commit (`docs/operation/load-readiness-checklist.md:31-33`). | Uma carga pode ter sido gravada e a certificacao falhar. O framework reporta erro, mas nao desfaz escrita. Isso e fora do escopo v0.1 e deve ser aceito explicitamente por pipeline. |

## Evidencias De Teste Existentes

- `tests/test_stage_contract_template_method.py:270-302`: `dry_run=True` nao
  chama hooks concretos e emite `dry_run_evidence` e `dry_run_load_completed`.
- `tests/test_stage_contract_template_method.py:305-327`: `dry_run=False`
  executa `load` e `certify`, preservando `is_valid` por default e emitindo
  eventos `load_*` e `certify_*`.
- `tests/test_stage_contract_template_method.py:331-352`: com
  `keep_technical_columns=False`, `_load` e `_certify` recebem apenas colunas de
  negocio.
- `tests/test_stage_contract_template_method.py:378-386`: `_certify` e opcional
  quando nao ha evidencia a produzir.
- `tests/test_dry_run.py:161-180`: dry-run limita depois de check, preserva
  transform/validate e pula load/certify.
- `tests/test_dry_run.py:183-234`: `show` nao roda em producao nem em dry-run
  com zero linhas, e so roda com `dry_run_show_rows > 0`.
- `tests/test_framework_integration.py:124-155`: integracao linear completa
  executa `extract`, `check`, `transform`, `validate`, `load`, `certify`.
- `tests/test_production_checks.py:87-197`: validacao envia `is_valid` ao load
  por default e permite exclusao configurada.

## Lacunas E Ambiguidades

1. **`certify_succeeded` pode significar no-op.** Como `_certify()` default
   retorna `None`, um observador pode interpretar sucesso de certify como prova
   de destino real, quando pode ser apenas ausencia de hook concreto.
2. **Checklist nao e enforcement.** A documentacao exige checklist antes de
   `dry_run=False`, mas o framework nao tem flag, artefato ou preflight que
   comprove aceite de risco. Para v0.1 isso e aceitavel, mas deve ser apresentado
   como responsabilidade operacional externa.
3. **Nomes reservados afetam previsibilidade.** A remocao de colunas tecnicas por
   nome e simples e testada, mas pode surpreender em schemas nao declarados.
4. **Eventos de dry-run sao corretos, mas dependem de consumidores lerem
   `status` e `mode`.** Dashboards que agreguem apenas por nome de stage podem
   mascarar que load foi pulado.

## Oportunidades P1/P2/P3

### P1

- **P1 - Reforcar semanticamente a certificacao no-op.** Documentar junto aos
  eventos/logs que `certify_succeeded` nao prova leitura de destino quando
  `_certify()` nao foi sobrescrito. Opcao compativel com v0.1: adicionar nota
  explicita no contrato e exemplos, sem implementar certificacao generica.
- **P1 - Manter `dry_run=False` sempre acompanhado de aviso operacional.**
  README, Quick Start e contrato ja fazem isso; a oportunidade e bloquear
  qualquer nova documentacao/exemplo que mostre `dry_run=False` sem remeter ao
  checklist e sem dizer que `append`/`overwrite` direto e didatico/legado.

### P2

- **P2 - Tornar a evidencia de dry-run menos ambigua para consumidores de log.**
  A informacao tecnica esta correta (`status=skipped`, `mode=dry_run`), mas uma
  nota de observabilidade deve orientar dashboards a separar `dry_run_evidence`
  de `certify_succeeded`.
- **P2 - Explicitar a lista de colunas removidas por `keep_technical_columns=False`
  nos guias de autor.** A regra por nome e case-insensitive deve aparecer perto
  dos exemplos de `_load`, com alerta para `updated_at`/`inserted_at` quando
  forem nomes de negocio.
- **P2 - Registrar a limitacao de teste desta auditoria.** Como os testes Spark
  nao retornaram no limite local de 120s, a proxima verificacao independente deve
  rodar os testes focados em ambiente CI ou com timeout maior.

### P3

- **P3 - Melhorar exemplos de certificacao concreta.** Sem propor load seguro
  generico, incluir um exemplo pequeno de `_certify()` que registre metrica ou
  evidencia simples do destino quando a pipeline concreta exigir.
- **P3 - Adicionar checklist resumido ao final do Quick Start.** O Quick Start ja
  referencia o checklist; um resumo curto dos itens de maior risco reduziria
  copia acidental de `QuickLoad`.

## Conclusao

A v0.1 diferencia corretamente contrato de framework e responsabilidade de
pipeline concreta. `dry_run=True` e tecnicamente efetivo para impedir escrita e
certificacao, e `keep_technical_columns` tem comportamento implementado e
testado. A principal conclusao severa e que **nada na v0.1 torna `dry_run=False`
seguro por si so**: qualquer seguranca de escrita, idempotencia, rollback,
escopo de overwrite e certificacao real depende da pipeline concreta e do
checklist operacional. Isso nao e falha do escopo v0.1; seria falha apenas se o
projeto vendesse esse contrato como pronto para producao sem essas ressalvas.
