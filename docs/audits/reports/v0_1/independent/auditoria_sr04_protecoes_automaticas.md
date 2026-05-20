# Auditoria Independente SR04 - Protecoes Automaticas e Invariantes Operacionais

## 1. Escopo, papel e metodo

Papel executado: especialista em protecoes automaticas e invariantes operacionais.

Escopo lido:

- Prompt SR04: `prompts/_v0.1/auditoria/auditoria_sr04_protecoes_automaticas.md`
- Contratos e modelos: `etl_framework/contracts/`, `etl_framework/models/config.py`
- Utilitarios: `etl_framework/utils/validate_struct.py`, `etl_framework/utils/dataframe_checks.py`, `etl_framework/utils/auto_quality.py`, `etl_framework/utils/production_checks.py`, `etl_framework/utils/schema_metadata.py`, `etl_framework/utils/check_metadata.py`
- Testes relevantes: `tests/test_auto_contract.py`, `tests/test_validate_struct.py`, `tests/test_dry_run.py`, `tests/test_pipeline_contract.py`, `tests/test_schema_metadata.py`, `tests/test_production_checks.py`
- Documentacao v0.1: `docs/v0.1-contract.md`, `docs/v0.1-known-limitations.md`

Restricoes respeitadas:

- Nao foram lidos relatorios em `docs/audits/reports/v0_1/independent`.
- Este relatorio e o unico arquivo escrito.

Verificacao executada:

- Tentativa 1: `pytest tests/test_auto_contract.py tests/test_validate_struct.py tests/test_dry_run.py tests/test_production_checks.py tests/test_schema_metadata.py`
  - Resultado real: timeout em 120s, ainda durante `tests/test_auto_contract.py`.
- Tentativa 2: recorte minimo com testes criticos de preflight, auto_check, auto_validate, dry-run e extra columns.
  - Resultado real: timeout em 180s sem resultado util.

Conclusao metodologica: a avaliacao abaixo se baseia em leitura estatica rastreavel de implementacao e testes. A suite local nao produziu confirmacao executada nesta sessao, o que deve ser tratado como lacuna operacional de verificabilidade, nao como falha funcional provada.

## 2. Sumario executivo

As protecoes automaticas centrais da v0.1 existem e estao majoritariamente alinhadas ao contrato: o preflight falha antes de `_extract` quando faltam `source_struct` ou `target_struct`; `auto_check` valida schema de origem sem acao Spark; `auto_validate` cria `is_valid`, aplica checks declarativos de severidade `error` e bloqueia invalidos antes do load com `limit(1).count()`.

O status geral, porem, e **parcial**. A implementacao reduz erros estruturais de desenvolvedor junior, mas ainda cria falsa seguranca em pontos importantes: `warning` em metadata praticamente nao gera efeito operacional no fluxo automatico, `nullable=False` nao bloqueia nulos sem check explicito, colunas extras sao ignoradas por padrao, e checks declarativos em `source_struct` sao validados como metadata mas nao executados pelo `auto_check`.

Nao ha P1 funcional evidente no escopo lido. Existem P2 relevantes por rastreabilidade incompleta e semantica que pode induzir uso incorreto.

## 3. Matriz protecao -> promessa -> implementacao -> teste

| Protecao | Promessa v0.1 | Implementacao observada | Teste observado | Status |
| --- | --- | --- | --- | --- |
| Preflight obrigatorio | Sem `source_struct` ou `target_struct`, `Pipeline.run()` falha antes de `_extract` (`docs/v0.1-contract.md:108`) | `Pipeline.run()` chama `_preflight()` antes de `extract()`; `_preflight` acumula erros para `source_struct` e `target_struct` e levanta `PreflightError` (`etl_framework/contracts/pipeline.py:45`, `etl_framework/contracts/pipeline.py:57`, `etl_framework/contracts/pipeline.py:60`, `etl_framework/contracts/pipeline.py:79`) | `tests/test_auto_contract.py:286`, `tests/test_auto_contract.py:336`, `tests/test_pipeline_contract.py:322`, `tests/test_pipeline_contract.py:345` verificam ausencia de chamada ao extract/eventos vazios | Confiavel |
| Preflight de `target_key` | Chave alvo deve existir em `target_struct` | Validado no modelo (`etl_framework/models/config.py:164`, `etl_framework/models/config.py:170`) e rechecado no preflight (`etl_framework/contracts/pipeline.py:63`) | `tests/test_pipeline_contract.py:365` muta config congelada e prova rechecagem antes do extract | Confiavel |
| `auto_check` de origem | Validar coluna obrigatoria ausente, tipo incompativel e coluna extra conforme politica, sem `count`, `collect` ou `show` (`docs/v0.1-contract.md:122`) | `Extract._run_check` chama `auto_check_source` antes de `_custom_check` (`etl_framework/contracts/extract.py:66`, `etl_framework/contracts/extract.py:76`); `auto_check_source` exige `source_struct` e chama `validate_schema` (`etl_framework/utils/auto_quality.py:15`); `validate_schema` valida apenas schema e documenta ausencia de acao Spark (`etl_framework/utils/validate_struct.py:59`) | `tests/test_auto_contract.py:141` monkeypatch de acoes Spark; `tests/test_auto_contract.py:300` coluna ausente; `tests/test_production_checks.py:26` e `tests/test_production_checks.py:41` schema/type | Confiavel para schema; parcial para metadata checks |
| `auto_validate` de destino | Validar nomes, tipos e checks; adicionar `is_valid`; tratar `NULL` booleano como invalido; bloquear antes do load (`docs/v0.1-contract.md:136`) | `Transform._run_validate` chama `auto_validate_target` antes de `_custom_validate` (`etl_framework/contracts/transform.py:60`, `etl_framework/contracts/transform.py:70`); `validate_struct` cria `is_valid` e rejeita `is_valid` preexistente (`etl_framework/utils/validate_struct.py:200`); `_check_rule` usa `coalesce(..., False)` (`etl_framework/utils/validate_struct.py:265`); bloqueio usa `limit(1).count()` (`etl_framework/utils/auto_quality.py:77`) | `tests/test_auto_contract.py:166`, `tests/test_auto_contract.py:177`, `tests/test_auto_contract.py:350`, `tests/test_auto_contract.py:366`, `tests/test_auto_contract.py:394` | Confiavel para severidade `error`; fragil para `warning` |
| Diagnostico de invalidos | Em falha, informar quantidade e checks declarativos quebrados (`docs/v0.1-contract.md:153`) | Em falha, faz `invalid_df.count()` e summary; filtra somente `severity == "error"` (`etl_framework/utils/auto_quality.py:81`, `etl_framework/utils/auto_quality.py:91`, `etl_framework/utils/auto_quality.py:97`) | `tests/test_auto_contract.py:432` espera `invalid_count=1` e nome do check | Parcial |
| Checks declarativos em metadata | Formato simples com `name`, `rule`, `severity`, `message` (`docs/v0.1-contract.md:159`) | `SchemaMetadataValidator` valida lista, dict, campos obrigatorios, severidade e alguns erros SQL comuns (`etl_framework/utils/schema_metadata.py:54`, `etl_framework/utils/schema_metadata.py:88`); `validate_struct` tambem valida resolucao Spark da regra (`etl_framework/utils/validate_struct.py:220`) | `tests/test_schema_metadata.py:12`, `tests/test_schema_metadata.py:69`, `tests/test_schema_metadata.py:91`, `tests/test_validate_struct.py:380` | Parcial |
| `extra_columns_policy` | `ignore`, `warn`, `fail`; `strict_schema=True` vira warning quando politica explicita continua `ignore` (`docs/v0.1-contract.md:112`, `docs/v0.1-contract.md:116`) | Config normaliza politica e converte strict+ignore para warn (`etl_framework/models/config.py:111`, `etl_framework/models/config.py:118`); schema aplica warn/fail/ignore (`etl_framework/utils/validate_struct.py:121`) | `tests/test_validate_struct.py:91`, `tests/test_validate_struct.py:104`, `tests/test_validate_struct.py:116`, `tests/test_auto_contract.py:314` | Confiavel, mas default permissivo |
| `strict_schema` legado | Compatibilidade, equivalente a warning quando politica explicita permanece `ignore` | Config converte `extra_columns_policy` para `warn`, entao a resolucao posterior nao depende mais do booleano (`etl_framework/models/config.py:118`) | `tests/test_auto_contract.py:314`, `tests/test_validate_struct.py:60` | Confiavel |
| `keep_technical_columns` | Load recebe tecnicas por default; pode excluir para destino de negocio (`docs/v0.1-contract.md:212`) | `Load._persistable_df` retorna df completo quando true; remove nomes tecnicos case-insensitive quando false (`etl_framework/contracts/load.py:129`) | `tests/test_production_checks.py:87`, `tests/test_production_checks.py:145` | Confiavel |
| Dry-run | Executa extract, auto_check, limita, transform, auto_validate; pula `_load` e `_certify`; `show` so se solicitado (`docs/v0.1-contract.md:192`) | `Extract.run` limita apos `_run_check` (`etl_framework/contracts/extract.py:39`, `etl_framework/contracts/extract.py:48`); `Load.run` retorna apos `_run_dry_run` (`etl_framework/contracts/load.py:35`); `show` condicionado (`etl_framework/contracts/load.py:92`) | `tests/test_dry_run.py:161`, `tests/test_dry_run.py:183`, `tests/test_dry_run.py:201`, `tests/test_dry_run.py:219` | Confiavel |
| Checks operacionais explicitos | Helpers opcionais podem executar count/agregacoes e nao substituem framework automatico | `production_checks.py` declara acoes Spark em docstrings e implementa checks de chave, volume, freshness, reconciliacao, invalidos e metricas (`etl_framework/utils/production_checks.py:15`) | `tests/test_production_checks.py:191`, `tests/test_production_checks.py:212`, `tests/test_production_checks.py:257`, `tests/test_production_checks.py:269` | Confiavel como opt-in |

## 4. Respostas as perguntas obrigatorias

### A execucao falha antes de acessar a origem quando falta contrato obrigatorio?

Sim. `Pipeline.run()` chama `_preflight()` antes de `extract()` (`etl_framework/contracts/pipeline.py:45`). Ausencia de `source_struct` ou `target_struct` gera `PreflightError` (`etl_framework/contracts/pipeline.py:57`, `etl_framework/contracts/pipeline.py:60`, `etl_framework/contracts/pipeline.py:79`). Os testes afirmam que `_extract` nao foi chamado/eventos ficaram vazios (`tests/test_auto_contract.py:286`, `tests/test_pipeline_contract.py:322`).

Status: **confiavel**.

### `auto_check` valida schema da origem conforme documentado?

Sim para nomes, tipos e extras. `auto_check_source` exige `source_struct` e delega para `validate_schema` (`etl_framework/utils/auto_quality.py:15`). `validate_schema` usa schema do DataFrame e nao executa acoes Spark (`etl_framework/utils/validate_struct.py:59`). Ha teste que monkeypatcha `count`, `collect`, `show` e `toLocalIterator` para falhar se chamados (`tests/test_auto_contract.py:141`).

Limite severo: checks declarativos presentes em `source_struct.metadata["checks"]` sao validados como forma durante criacao de `EtlRunConfig`, mas nao executados por `auto_check`, porque ele chama `validate_schema`, nao `validate_struct`. Isso esta coerente com a documentacao de Auto Check, mas e uma fonte de falsa seguranca para quem ve metadata de checks aceita na origem.

Status: **parcial**.

### `auto_validate` valida o resultado e bloqueia invalidos antes do load?

Sim para checks de severidade `error`. `Transform._run_validate` executa `auto_validate_target` antes de devolver o DataFrame ao load (`etl_framework/contracts/transform.py:60`). O bloqueio ocorre em `_raise_with_validation_diagnostics` com `df.filter(~is_valid).limit(1).count()` (`etl_framework/utils/auto_quality.py:77`) e, se houver invalido, levanta `ValueError` antes do load. O teste `tests/test_auto_contract.py:394` verifica que `load.loaded` e `load.certified` permanecem falsos quando a validacao falha.

Limite: checks `warning` nao invalidam e nao sao reportados no caminho automatico feliz. Em falha, o diagnostico filtra apenas `severity == "error"` (`etl_framework/utils/auto_quality.py:97`).

Status: **confiavel para bloqueio de error; parcial para rastreabilidade total de checks**.

### Checks SQL declarativos sao simples, previsiveis e rastreaveis?

Parcialmente. A forma e simples: metadata deve ser lista/tupla de dicts com `name`, `rule`, severidade opcional e mensagem opcional. O validador captura campos ausentes, severidade invalida, parenteses desbalanceados e operadores comuns errados (`etl_framework/utils/schema_metadata.py:54`, `etl_framework/utils/schema_metadata.py:97`). A execucao tambem tenta resolver a expressao Spark sem job (`etl_framework/utils/validate_struct.py:220`).

Rastreabilidade e incompleta: warnings quebrados nao aparecem no fluxo automatico; so aparecem quando `validate_struct(..., compute_summary=True)` e usado diretamente. Alem disso, regras SQL podem referenciar outras colunas; o validador apenas emite warning se a regra nao referencia o campo (`etl_framework/utils/schema_metadata.py:107`), o que e flexivel mas menos previsivel para junior.

Status: **parcial**.

### As politicas de colunas extras sao coerentes?

Sim, a semantica esta coerente com a documentacao. O default `ignore` e permissivo por contrato; `warn` alerta; `fail` bloqueia. `strict_schema=True` converte `ignore` para `warn` no modelo (`etl_framework/models/config.py:118`). A implementacao aplica a politica em schema validation (`etl_framework/utils/validate_struct.py:121`).

Risco: o default permissivo reduz friccao, mas para producao cria falsa seguranca se o usuario espera schema estrito. O nome `strict_schema` tambem pode induzir expectativa de falha, mas o contrato documenta que e legado e vira warning.

Status: **confiavel, com risco operacional documentado**.

### As protecoes reduzem erro humano ou criam falsa seguranca?

Reduzem erro humano estrutural: ordem fixa, preflight, validação de tipo/nome, bloqueio antes do load, dry-run sem escrita e remocao opcional de colunas tecnicas sao protecoes reais.

Tambem criam falsa seguranca em quatro situacoes:

- `nullable=False` parece bloqueio, mas e apenas intencao; o proprio teste `tests/test_auto_contract.py:237` prova que nulo passa sem check SQL explicito.
- `warning` em check declarativo parece regra de qualidade, mas nao bloqueia nem aparece no fluxo automatico feliz.
- `source_struct` pode carregar metadata de checks validada pelo modelo, mas `auto_check` nao executa esses checks.
- `extra_columns_policy="ignore"` e default, logo drift por coluna extra passa silenciosamente.

Status geral: **parcial**.

## 5. Riscos de falsa seguranca

### FS-01 - Checks declarativos `warning` nao deixam evidencia operacional automatica

Severidade: P2.

Evidencia:

- `warning` nao participa de `is_valid`, por desenho (`etl_framework/utils/validate_struct.py:208`).
- `auto_validate_target` so calcula summary quando ha invalido (`etl_framework/utils/auto_quality.py:78`).
- O diagnostico de falha filtra somente `severity == "error"` (`etl_framework/utils/auto_quality.py:97`).
- `tests/test_validate_struct.py:128` prova que warning falho aparece em summary apenas quando `compute_summary=True`, que nao e usado pelo fluxo automatico.

Impacto: um autor junior pode declarar um check `warning` acreditando que ele sera observavel. No fluxo padrao, ele nao bloqueia e nao gera evidencia automatica.

Recomendacao: registrar warnings quebrados em evento/metricas ou documentar explicitamente que `warning` e apenas metadata/summary manual fora do fluxo automatico. Preferivel: `auto_validate_target` deve aceitar configuracao de summary de warnings ou emitir contagem leve opcional documentada.

### FS-02 - Checks em `source_struct` sao aceitos como metadata, mas nao executados no auto_check

Severidade: P2.

Evidencia:

- `EtlRunConfig._validate_schemas` valida metadata tambem para `source_struct` (`etl_framework/models/config.py:161`).
- `auto_check_source` chama `validate_schema`, que valida apenas nomes/tipos/extras (`etl_framework/utils/auto_quality.py:15`, `etl_framework/utils/validate_struct.py:59`).
- A documentacao promete checks declarativos em `target_struct`, nao em `source_struct`; porem a aceitacao da metadata em source pode induzir falsa expectativa.

Impacto: regras de qualidade declaradas na origem podem parecer ativas, mas nao afetam `check`.

Recomendacao: rejeitar `checks` em `source_struct` na v0.1 ou emitir warning explicito na configuracao dizendo que checks de source nao sao executados pelo auto_check.

### FS-03 - `nullable=False` nao bloqueia nulos

Severidade: P3, porque esta documentado.

Evidencia:

- Documentacao explicita que `nullable=False` documenta intencao e nao bloqueia nulos (`docs/v0.1-contract.md:180`).
- Teste comprova passagem de null sem check SQL (`tests/test_auto_contract.py:237`) e bloqueio apenas com regra `IS NOT NULL` (`tests/test_auto_contract.py:256`).

Impacto: risco alto de interpretacao errada por usuarios PySpark menos experientes.

Recomendacao: mensagens de docs e exemplos devem sempre parear `nullable=False` com check `IS NOT NULL` quando a regra for obrigatoria.

### FS-04 - Colunas extras passam silenciosamente por default

Severidade: P3, porque esta documentado.

Evidencia:

- Default de `extra_columns_policy` e `ignore` (`etl_framework/models/config.py:40`).
- Implementacao ignora extras quando politica resolvida nao e `warn` nem `fail` (`etl_framework/utils/validate_struct.py:124`).
- Teste confirma default permissivo (`tests/test_validate_struct.py:91`).

Impacto: drift de origem ou destino pode nao bloquear pipeline.

Recomendacao: manter default se o contrato exigir compatibilidade, mas exemplos produtivos devem usar `extra_columns_policy="fail"` ou justificar `ignore`.

### FS-05 - Suite Spark nao foi verificavel localmente dentro de timeout razoavel

Severidade: P2 operacional.

Evidencia:

- Execucao direcionada de 57 testes expirou em 120s.
- Recorte minimo de 6 testes expirou em 180s.

Impacto: protecoes automaticas dependem de testes de comportamento Spark; se o feedback local nao fecha, regressões podem sobreviver ate CI ou ambiente de outro mantenedor.

Recomendacao: separar testes unitarios sem Spark de testes Spark; criar marcador ou perfil rapido para SR04 com fixture Spark reutilizavel e tempo previsivel.

## 6. Lacunas por severidade

### P1

Nenhuma lacuna P1 confirmada por leitura estatica no escopo solicitado. O caminho critico "invalidos nao chegam ao load" esta implementado e coberto por teste fonte.

### P2

1. **Warnings declarativos sem efeito operacional automatico.**
   - Evidencia: `etl_framework/utils/auto_quality.py:78`, `etl_framework/utils/auto_quality.py:97`.
   - Risco: check declarado parece monitorado, mas nao bloqueia nem aparece no caminho feliz.

2. **Checks em `source_struct` aceitos mas nao executados.**
   - Evidencia: `etl_framework/models/config.py:161`, `etl_framework/utils/auto_quality.py:15`, `etl_framework/utils/validate_struct.py:59`.
   - Risco: falsa seguranca para autores que reutilizam metadata de checks na origem.

3. **Verificabilidade local ruim dos testes Spark.**
   - Evidencia: timeouts de 120s e 180s nesta auditoria.
   - Risco: regressao em protecoes automaticas demora a ser detectada.

### P3

1. **Default `extra_columns_policy="ignore"` e permissivo.**
   - Evidencia: `etl_framework/models/config.py:40`, `etl_framework/utils/validate_struct.py:124`.
   - Risco: drift por coluna extra nao falha.

2. **`nullable=False` depende de convencao/documentacao, nao de protecao automatica.**
   - Evidencia: `docs/v0.1-contract.md:180`, `tests/test_auto_contract.py:237`.
   - Risco: usuario assume bloqueio inexistente.

3. **`strict_schema` nao e estrito de fato.**
   - Evidencia: strict+ignore vira `warn` (`etl_framework/models/config.py:118`).
   - Risco: nome legado pode confundir, apesar de documentado.

## 7. Erros esperados e reais

| Cenario | Erro esperado | Implementacao real | Evidencia |
| --- | --- | --- | --- |
| `source_struct=None` no fluxo padrao | `PreflightError` antes de `_extract` | `_preflight` adiciona "source_struct is required before extract" e levanta `PreflightError` | `etl_framework/contracts/pipeline.py:57`, `etl_framework/contracts/pipeline.py:79`; `tests/test_auto_contract.py:286` |
| `target_struct=None` no fluxo padrao | `PreflightError` antes de `_extract` | `_preflight` adiciona "target_struct is required before extract" e levanta `PreflightError` | `etl_framework/contracts/pipeline.py:60`, `etl_framework/contracts/pipeline.py:79`; `tests/test_auto_contract.py:336` |
| Coluna obrigatoria ausente na origem | `CheckError` embrulhando schema mismatch | `_run_check` e stage wrapper convertem falha de `auto_check_source` em erro de check | `etl_framework/contracts/extract.py:66`; `tests/test_auto_contract.py:300` |
| Tipo alvo incompativel | `ValidateError` antes do load | `auto_validate_target` chama `validate_struct`; mismatch vira falha de validate stage | `etl_framework/contracts/transform.py:60`; `tests/test_auto_contract.py:366` |
| Registro invalido por check `error` | `ValidateError` com `invalid_count` e sem load | `limit(1).count()` detecta invalido; depois `count()` e summary geram diagnostico; load nao roda | `etl_framework/utils/auto_quality.py:77`; `tests/test_auto_contract.py:394` |
| Coluna extra com politica `fail` | `ValueError` de schema | `_validate_schema_match` adiciona erro para extras | `etl_framework/utils/validate_struct.py:128`; `tests/test_validate_struct.py:116` |
| `dry_run_show_rows > 0` sem dry-run | `ValueError` no config e/ou `PreflightError` | Config ja falha; preflight tambem revalida | `etl_framework/models/config.py:133`, `etl_framework/contracts/pipeline.py:74` |

## 8. Acoes Spark executadas intencionalmente

| Local | Acao | Condicao | Justificativa | Evidencia |
| --- | --- | --- | --- | --- |
| `auto_validate_target` | `limit(1).count()` | Sempre apos construir `is_valid`, para detectar qualquer invalido | Bloquear antes do load com custo menor que contagem total | `etl_framework/utils/auto_quality.py:77`; `docs/v0.1-contract.md:151` |
| `auto_validate_target` | `count()` | Somente quando existe invalido | Diagnostico `invalid_count` | `etl_framework/utils/auto_quality.py:81` |
| `auto_validate_target` | `collect()` em summary agregado | Somente caminho de falha | Diagnostico de checks quebrados sem coletar linhas de dados | `etl_framework/utils/validate_struct.py:253`; `etl_framework/utils/auto_quality.py:99` |
| `validate_struct(..., compute_summary=True)` | agregacao + `collect()` | Opt-in | Summary declarativo | `etl_framework/utils/validate_struct.py:53`, `etl_framework/utils/validate_struct.py:244` |
| `dry_run_show_rows > 0` | `show()` | Apenas dry-run com amostra solicitada | Evidencia visual explicita; risco de exposicao documentado | `etl_framework/contracts/load.py:92`, `etl_framework/contracts/load.py:114` |
| `production_checks` | `count`, agregacao, `limit(1).count()` | Apenas helpers opt-in | Checks operacionais fora do automatico basico | `etl_framework/utils/production_checks.py:15` |

## 9. Status por protecao

| Protecao | Status claro |
| --- | --- |
| Preflight de contratos obrigatorios | Confiavel |
| Preflight de `target_key` | Confiavel |
| `auto_check` de schema da origem | Confiavel |
| `auto_check` de checks declarativos da origem | Ausente por desenho, mas fonte de falsa seguranca |
| `auto_validate` de schema do destino | Confiavel |
| `auto_validate` de checks `error` | Confiavel |
| `auto_validate` de checks `warning` | Fragil |
| Diagnostico de invalidos | Parcial |
| Politica de colunas extras | Confiavel, default permissivo |
| `strict_schema` | Parcial semanticamente; implementacao coerente com legado |
| `keep_technical_columns` | Confiavel |
| Dry-run sem escrita | Confiavel |
| Checks operacionais de producao | Confiaveis como opt-in; nao automaticos |

## 10. Recomendacoes objetivas

1. **Tornar `warning` rastreavel ou rebaixar sua promessa.**
   - Opcao preferida: emitir metrica/evento para checks `warning` quebrados quando configurado.
   - Alternativa: documentar que `warning` nao tem efeito no fluxo automatico e so aparece em summary manual.

2. **Bloquear ou advertir checks em `source_struct`.**
   - Se a v0.1 nao executa checks de origem, `EtlRunConfig` deve rejeitar `metadata["checks"]` em `source_struct` ou emitir warning explicito.

3. **Criar perfil rapido de testes SR04.**
   - Deve executar preflight, auto_check sem acoes, auto_validate invalidante, dry-run sem load e politica `fail` em tempo previsivel.
   - A auditoria nao conseguiu obter resultado local em 180s para 6 testes criticos.

4. **Endurecer exemplos produtivos.**
   - Usar `extra_columns_policy="fail"` nos exemplos de producao.
   - Para nulos obrigatorios, sempre declarar `metadata.checks` com `IS NOT NULL`.

5. **Melhorar mensagens para junior.**
   - Ao validar `target_struct`, quando `nullable=False` existir sem check `IS NOT NULL`, considerar warning educativo.
   - Ao usar `strict_schema=True`, mensagem ou docstring deve reforcar que nao falha em extras.

## 11. Conclusao

A v0.1 entrega protecoes automaticas reais para invariantes estruturais e bloqueio basico antes do load. O fluxo padrao e melhor que depender de disciplina manual do autor da pipeline.

O framework, contudo, nao deve ser vendido como qualidade operacional completa. Seu ponto mais perigoso e a diferenca entre "check declarado" e "check efetivamente bloqueante/rastreavel". Para uso por junior, a protecao e boa em schema e ordem de execucao, mas ainda exige mensagens mais explicitas sobre `warning`, `nullable=False`, checks de origem e politica permissiva de colunas extras.
