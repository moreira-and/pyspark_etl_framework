# Auditoria SR17 - Performance e Custo Spark

## Sumario Executivo

A v0.1 e honesta ao declarar que nao promete baixo custo irrestrito, mas o
framework ainda exige disciplina operacional forte para nao ser interpretado
como barato por padrao. O custo necessario do contrato esta concentrado em
`auto_validate_target`: um `limit(1).count()` para bloquear invalidos antes do
`load` e, no caminho de falha, diagnostico com `count()` e summary. Esse custo
esta documentado e e defensavel para a promessa da v0.1.

O risco severo esta nos pontos que parecem auxiliares: helpers de producao
executam `count`, `groupBy().count()`, `distinct` e `exceptAll`; `dry_run`
limita somente depois de `_extract` e `auto_check`; e `show` pode ser acionado
em dry-run quando `dry_run_show_rows > 0`. A documentacao cobre esses pontos,
mas os nomes dos helpers e a ergonomia para junior ainda podem induzir a uso
caro sem benchmark previo.

Veredito: aprovado apenas para piloto controlado. Para volume alto, a v0.1
depende do benchmark minimo documentado e de revisao senior por pipeline.

## Escopo E Fontes

Fontes lidas:

- `prompts/_v0.1/auditoria/auditoria_sr17_performance_custo_spark.md`
- `etl_framework/`
- `tests/`
- `docs/operation/spark-cost-and-benchmark.md`
- `docs/v0.1-contract.md`
- `docs/v0.1-known-limitations.md`
- `QUICK_START.md`

Nao foram lidos relatorios de outras auditorias nem arquivos em
`docs/audits/reports/v0_1/independent`.

## Inventario De Acoes Spark

1. `Extract._limit_dry_run_extract`
   - Evidencia: `etl_framework/contracts/extract.py:99` retorna
     `df.limit(config.dry_run_limit)`.
   - Condicao: `config.dry_run=True`, depois de `_extract` e `auto_check`.
   - Custo: `limit` e transformacao lazy; nao dispara job sozinho, mas altera o
     plano que sera materializado por acoes posteriores.
   - Documentacao: `docs/v0.1-contract.md:198` e `QUICK_START.md:118`.
   - Teste: `tests/test_dry_run.py:161` confirma que o limite ocorre depois do
     check e antes do transform.

2. `Load._show_dry_run_sample`
   - Evidencia: `etl_framework/contracts/load.py:114` chama
     `df.show(config.dry_run_show_rows, truncate=False)`.
   - Condicao: `config.dry_run=True` e `dry_run_show_rows > 0`.
   - Custo: acao Spark e exposicao de dados sensiveis.
   - Documentacao: `docs/v0.1-contract.md:204`,
     `docs/operation/spark-cost-and-benchmark.md:24`,
     `docs/v0.1-known-limitations.md:84` e `QUICK_START.md:138`.
   - Teste: `tests/test_dry_run.py:219` confirma chamada apenas nesse modo.

3. `auto_validate_target` no caminho feliz
   - Evidencia: `etl_framework/utils/auto_quality.py:78` executa
     `invalid_df.limit(1).count()`.
   - Condicao: sempre que `Transform._run_validate` chama
     `auto_validate_target` e nao existem invalidos.
   - Custo: job Spark para provar ausencia de pelo menos um invalido; pode
     varrer mais do que o nome "limit(1)" sugere, dependendo de particionamento
     e predicado.
   - Documentacao: `docs/v0.1-contract.md:151` e
     `docs/operation/spark-cost-and-benchmark.md:15`.
   - Teste: `tests/test_auto_contract.py:177` espiona `limit(1)` seguido de
     `count()` e bloqueia exposicao de linhas.

4. `auto_validate_target` no caminho de falha
   - Evidencia: `etl_framework/utils/auto_quality.py:81` executa
     `invalid_df.count()`; `etl_framework/utils/auto_quality.py:82` chama
     `_failed_check_summaries`; `etl_framework/utils/auto_quality.py:99` coleta
     o resumo filtrado.
   - Condicao: existe pelo menos um registro invalido apos validacao de
     `target_struct`.
   - Custo: job adicional de contagem completa dos invalidos e agregacao por
     checks; mais caro justamente em incidente.
   - Documentacao: `docs/v0.1-contract.md:157`,
     `docs/operation/spark-cost-and-benchmark.md:16` e
     `docs/v0.1-known-limitations.md:87`.
   - Teste: `tests/test_auto_contract.py:177` cobre diagnostico sem `show`; ha
     lacuna de teste medindo quantidade de jobs ou custo relativo.

5. `auto_validate_target` quando entrada ja contem `is_valid`
   - Evidencia: `etl_framework/utils/auto_quality.py:62` executa
     `invalid_df.limit(1).count()`; `etl_framework/utils/auto_quality.py:65`
     executa `invalid_df.count()` se houver invalidos.
   - Condicao: `df` de entrada ja possui coluna `is_valid`.
   - Custo: mesmo padrao de gate e diagnostico, antes de rejeitar ou aceitar a
     validacao existente.
   - Documentacao: parcialmente coberto pelo contrato de `auto_validate`, mas
     nao aparece como caso separado na matriz de custo.
   - Teste: `tests/test_auto_contract.py` cobre `is_valid` nulo como invalido,
     mas a documentacao nao destaca esse caminho.

6. `validate_struct(..., compute_summary=True)` e `summarize_struct_checks`
   - Evidencia: `etl_framework/utils/validate_struct.py:54` chama
     `_build_checks_summary`; `etl_framework/utils/validate_struct.py:305`
     executa `counts_df.collect()`.
   - Condicao: `compute_summary=True` ou chamada direta a
     `summarize_struct_checks`.
   - Custo: agregacao por check e coleta de metadados; com mais de 100 checks,
     o codigo segmenta em batches, mas ainda executa agregacoes por lote.
   - Documentacao: `docs/operation/spark-cost-and-benchmark.md:23` e
     `docs/v0.1-known-limitations.md:89`.
   - Testes: `tests/test_validate_struct.py:253`,
     `tests/test_validate_struct.py:310` e `tests/test_validate_struct.py:464`.

7. `assert_target_key_not_null`
   - Evidencia: `etl_framework/utils/production_checks.py:31` executa
     `df.filter(condition).limit(1).count()`.
   - Condicao: helper operacional chamado pela pipeline concreta.
   - Custo: job Spark dependente de particionamento e seletividade.
   - Documentacao: `docs/operation/spark-cost-and-benchmark.md:18`.
   - Teste: `tests/test_production_checks.py:200`.

8. `assert_target_key_unique`
   - Evidencia: `etl_framework/utils/production_checks.py:45` usa
     `df.groupBy(*key_tuple).count()` e filtra duplicados.
   - Condicao: helper operacional chamado para validar unicidade.
   - Custo: shuffle potencialmente caro; o `limit(1).count()` posterior nao
     elimina o custo da agregacao.
   - Documentacao: `docs/operation/spark-cost-and-benchmark.md:19`.
   - Teste: `tests/test_production_checks.py:200`.

9. `assert_volume_between`
   - Evidencia: `etl_framework/utils/production_checks.py:74` executa
     `df.count()`.
   - Condicao: helper chamado para validar volume ou preencher metrica.
   - Custo: job completo sobre o DataFrame.
   - Documentacao: `docs/operation/spark-cost-and-benchmark.md:20`.
   - Teste: `tests/test_production_checks.py:219`.

10. `assert_freshness_at_least`
    - Evidencia: `etl_framework/utils/production_checks.py:99` executa
      `stale.limit(1).count()`.
    - Condicao: helper chamado para validar coluna de freshness.
    - Custo: job Spark dependente de predicado e particionamento.
    - Documentacao: `docs/operation/spark-cost-and-benchmark.md:21`.
    - Teste: `tests/test_production_checks.py:219`.

11. `assert_reconciled_by_key`
    - Evidencia: `etl_framework/utils/production_checks.py:119` e
      `etl_framework/utils/production_checks.py:120` calculam `distinct`;
      `etl_framework/utils/production_checks.py:121` usa `exceptAll(...).limit(1).count()`.
    - Condicao: helper chamado para reconciliar chaves entre origem e destino.
    - Custo: shuffle e comparacao entre dois DataFrames; risco alto em chave
      larga ou destino grande.
    - Documentacao: `docs/operation/spark-cost-and-benchmark.md:22`.
    - Teste: `tests/test_production_checks.py:219`.

12. `assert_no_invalid_records`
    - Evidencia: `etl_framework/utils/production_checks.py:157` executa
      `df.filter(~valid_condition).limit(1).count()`.
    - Condicao: helper chamado apos validacao.
    - Custo: job Spark adicional, possivelmente redundante se usado logo apos
      `auto_validate_target`, que ja executou gate equivalente.
    - Documentacao: `docs/operation/spark-cost-and-benchmark.md:17`.
    - Teste: `tests/test_production_checks.py:260` cobre comportamento funcional.

## Matriz Acao -> Condicao -> Custo -> Documentacao

| Acao Spark | Condicao | Custo operacional | Documentacao | Avaliacao |
| --- | --- | --- | --- | --- |
| `df.limit(dry_run_limit)` | `dry_run=True`, apos `_extract` e `auto_check` | Lazy; nao reduz custo de leitura inicial | Contrato e Quick Start | Adequado, mas facil superestimar como protecao de custo |
| `show(..., truncate=False)` | `dry_run_show_rows > 0` | Job Spark e exposicao de dados | Contrato, limitacoes, benchmark, Quick Start | Documentado; default seguro `0` |
| `filter(...).limit(1).count()` em `auto_validate_target` | Toda validacao target | Job necessario para bloquear invalidos | Contrato e benchmark | Custo necessario da v0.1 |
| `invalid_df.count()` | Apenas quando ha invalido | Contagem completa dos invalidos | Contrato e benchmark | Custo de incidente; aceitavel, mas deve aparecer em runbook |
| Summary + `collect()` | Falha de `auto_validate` ou `compute_summary=True` | Agregacao por check e coleta pequena | Contrato, benchmark, limitacoes | Necessario para diagnostico; risco com muitos checks |
| `groupBy().count()` | `assert_target_key_unique` | Shuffle | Benchmark | Alto custo e nome pouco alarmante |
| `df.count()` | `assert_volume_between` | Job completo | Benchmark | Explicito, mas deve ser usado com parcimonia |
| `distinct` + `exceptAll` + `count` | `assert_reconciled_by_key` | Shuffle entre DataFrames | Benchmark | Alto custo; exige benchmark por pipeline |
| `filter(...).limit(1).count()` | `assert_*` de nulidade/freshness/invalidos | Job pequeno, nao gratuito | Benchmark | Documentado; risco de composicao com varios helpers |

## Riscos P1/P2/P3

### P1 - `dry_run` pode ser confundido com modo barato de leitura

Evidencia: `Extract.run` aplica `df.limit(config.dry_run_limit)` somente depois
de `_extract` e `_run_check`; a linha efetiva e `etl_framework/contracts/extract.py:99`.
O contrato declara isso em `docs/v0.1-contract.md:207`, e o Quick Start lista o
limite no passo 4, apos extract e auto_check (`QUICK_START.md:113` a
`QUICK_START.md:118`).

Risco operacional: uma pipeline concreta pode executar uma leitura cara em
`_extract` antes de qualquer limite. Em fontes particionadas ou tabelas remotas,
`dry_run=True` evita escrita, mas nao garante custo baixo de scan, planejamento
ou acesso fisico.

Teste/lacuna: `tests/test_dry_run.py:161` confirma a ordem com `count()` nos
hooks de teste, mas nao existe teste que simule uma leitura de origem cara ou
que force a documentacao do `Extract._extract` a receber filtros de janela.

Severidade: P1 porque o nome `dry_run` e a presenca de `dry_run_limit` podem
gerar decisao operacional errada em volume alto.

### P1 - Helpers de chave e reconciliacao parecem simples, mas podem gerar shuffle caro

Evidencia: `assert_target_key_unique` usa `groupBy(*key_tuple).count()` em
`etl_framework/utils/production_checks.py:45`; `assert_reconciled_by_key` usa
`distinct` e `exceptAll` em `etl_framework/utils/production_checks.py:119` a
`etl_framework/utils/production_checks.py:121`.

Risco operacional: esses helpers podem ser chamados como "checks pequenos", mas
podem embaralhar todo o dataset. Em destino grande, reconciliacao por chave pode
custar mais que a transformacao principal.

Documentacao correspondente: `docs/operation/spark-cost-and-benchmark.md:19` e
`docs/operation/spark-cost-and-benchmark.md:22` avisam que pode haver shuffle.

Teste/lacuna: `tests/test_production_checks.py:200` e
`tests/test_production_checks.py:219` cobrem funcionalidade em datasets pequenos,
sem assertar plano fisico, numero de jobs, particionamento ou comportamento em
escala.

Severidade: P1 porque o custo e alto e a API nao forca benchmark ou aceite
explicito.

### P2 - `auto_validate_target` e correto, mas adiciona acao obrigatoria em toda pipeline

Evidencia: `etl_framework/utils/auto_quality.py:78` executa
`invalid_df.limit(1).count()` no caminho normal. `Transform._run_validate`
sempre chama `auto_validate_target` em `etl_framework/contracts/transform.py`.

Risco operacional: toda execucao, inclusive `dry_run`, paga ao menos um job
para provar que nao ha invalido. Isso e necessario ao contrato de bloquear
invalidos antes do load, mas nao e custo zero.

Documentacao correspondente: `docs/v0.1-contract.md:151` e
`docs/operation/spark-cost-and-benchmark.md:15`.

Teste/lacuna: `tests/test_auto_contract.py:177` confirma que usa `limit(1)` e
nao expõe registros. Falta teste que confirme que `auto_validate_target` nao
executa `count()` completo no caminho feliz.

Severidade: P2 porque o custo e intencional e documentado, mas afeta todas as
pipelines.

### P2 - Diagnostico de invalidos fica mais caro no momento de falha

Evidencia: quando ha invalido, `etl_framework/utils/auto_quality.py:81`
executa `invalid_df.count()`, e `etl_framework/utils/auto_quality.py:99`
coleta summaries filtrados. `validate_struct` executa `collect()` em
`etl_framework/utils/validate_struct.py:305`.

Risco operacional: um incidente de qualidade em lote grande dispara contagem
adicional e agregacoes. Isso melhora a mensagem de erro, mas pode aumentar
tempo e custo justamente quando a execucao ja esta falhando.

Documentacao correspondente: `docs/v0.1-contract.md:157` e
`docs/operation/spark-cost-and-benchmark.md:16`.

Teste/lacuna: os testes cobrem o erro e o texto diagnostico, mas nao ha teste
de quantidade de batches, numero de checks ou limite operacional do diagnostico.

Severidade: P2 porque e custo intencional, porem precisa de limite pratico por
pipeline.

### P2 - `assert_no_invalid_records` pode duplicar custo se usado apos auto-validate

Evidencia: `auto_validate_target` ja executa gate de invalidos; o helper
`assert_no_invalid_records` repete `filter(...).limit(1).count()` em
`etl_framework/utils/production_checks.py:157`.

Risco operacional: um autor junior pode encadear o helper por cautela depois do
framework ja ter validado, criando job redundante.

Documentacao correspondente: o benchmark lista o helper em
`docs/operation/spark-cost-and-benchmark.md:17`, mas nao alerta sobre
redundancia com `auto_validate_target`.

Teste/lacuna: `tests/test_production_checks.py:260` cobre split e bloqueio de
invalidos, mas nao cobre redundancia nem orienta uso recomendado.

Severidade: P2 por custo acidental evitavel.

### P3 - `show` esta bem guardado, mas ainda e acao e vazamento potencial

Evidencia: `etl_framework/contracts/load.py:114` chama `show(..., truncate=False)`.
O modelo de config rejeita `dry_run_show_rows > 0` quando `dry_run=False` em
`etl_framework/models/config.py:133`.

Risco operacional: em ambiente compartilhado, o valor positivo mostra dados sem
truncamento. O custo Spark e menor que shuffles, mas o risco de exposicao e real.

Documentacao correspondente: `QUICK_START.md:138`,
`docs/v0.1-known-limitations.md:84` e
`docs/operation/spark-cost-and-benchmark.md:24`.

Teste/lacuna: `tests/test_dry_run.py:219` cobre chamada condicional. Cobertura
adequada para v0.1.

Severidade: P3 porque default e seguro e ha documentacao clara.

### P3 - Testes funcionais nao medem custo Spark de forma rastreavel

Evidencia: ha testes que espionam chamadas (`tests/test_auto_contract.py:177`) e
testes funcionais dos helpers (`tests/test_production_checks.py:200` e
`tests/test_production_checks.py:219`), mas o benchmark exige registrar jobs por
etapa em `docs/operation/spark-cost-and-benchmark.md:39`.

Risco operacional: a suite evita regressao grosseira de contrato, mas nao prova
numero de jobs, shuffles, particoes ou custo em escala.

Documentacao correspondente: `docs/operation/spark-cost-and-benchmark.md:28` a
`docs/operation/spark-cost-and-benchmark.md:45` pede benchmark por pipeline.

Teste/lacuna: falta fixture ou exemplo operacional que capture plano fisico ou
Spark listener para os helpers caros.

Severidade: P3 porque a v0.1 declara que benchmark por pipeline e necessario,
mas a lacuna reduz protecao para mantenedores.

## Respostas As Perguntas Obrigatorias

### Quais acoes Spark o framework executa intencionalmente?

Executa intencionalmente `limit(1).count()` em `auto_validate_target`,
`count()` e summary no caminho de invalidos, `show()` quando
`dry_run_show_rows > 0`, e as acoes explicitas dos helpers operacionais
(`count`, `groupBy().count()`, `distinct`, `exceptAll`, `collect` de summary).
`df.limit(dry_run_limit)` e intencional, mas lazy.

### Essas acoes estao documentadas?

Sim, a maioria esta documentada em `docs/operation/spark-cost-and-benchmark.md`.
O contrato tambem cobre `auto_validate`, `dry_run` e `show`. A lacuna e de
granularidade: o caminho de `is_valid` preexistente e a redundancia entre
`auto_validate_target` e `assert_no_invalid_records` nao aparecem como riscos
separados.

### O custo e previsivel para o autor da pipeline?

Parcialmente. O autor que le o benchmark consegue prever os custos. O autor que
usa apenas nomes de API pode subestimar `assert_target_key_unique`,
`assert_reconciled_by_key`, `assert_volume_between` e `dry_run_limit`.

### `dry_run` pode ser confundido com baixo custo de leitura?

Sim. A documentacao afirma corretamente que nao garante baixo custo de leitura,
mas a combinacao `dry_run=True` + `dry_run_limit` continua perigosa para junior:
o limite vem depois da leitura concreta e do auto-check.

### Existem helpers que parecem baratos mas executam jobs?

Sim. `assert_target_key_not_null`, `assert_freshness_at_least` e
`assert_no_invalid_records` executam `limit(1).count()`. `assert_volume_between`
executa `count()` completo. `assert_target_key_unique` e
`assert_reconciled_by_key` podem disparar shuffles.

### Os testes ou docs ajudam a evitar uso caro acidental?

Ajudam, mas nao bastam. Os docs avisam o custo e os testes protegem algumas
condicoes. Falta teste/guia que torne visivel numero de jobs, plano fisico e
composicao de helpers em pipeline real.

## Aderencia Aos Criterios

- Custo operacional: aceitavel no contrato central, arriscado nos helpers de
  producao sem benchmark.
- Robustez: boa para bloquear invalidos antes do load; fraca para prevenir uso
  caro acidental por composicao.
- Aderencia promessa-codigo: boa. O codigo executa o que o contrato declara.
- Facilidade para junior: media. O fluxo ajuda, mas nomes como `dry_run` e
  `assert_target_key_unique` escondem custo Spark relevante.
- Simplicidade: boa no contrato; helpers concentram complexidade Spark.
- Manutencao: boa se a matriz de custo continuar atualizada; risco de drift se
  novos helpers forem adicionados sem linha no benchmark.

## Recomendacoes Objetivas

1. Renomear ou documentar inline os helpers caros com linguagem operacional
   mais forte. Exemplo: docstrings de `assert_target_key_unique` e
   `assert_reconciled_by_key` devem mencionar explicitamente "shuffle" e
   "benchmark obrigatorio em volume alto".

2. Adicionar nota no benchmark de que `assert_no_invalid_records` pode ser
   redundante logo apos `auto_validate_target`.

3. Criar teste de contrato de custo para o caminho feliz de `auto_validate`,
   garantindo que nao chama `invalid_df.count()` completo quando
   `limit(1).count()` retorna zero.

4. Criar um exemplo minimo de benchmark operacional que registre quantidade de
   jobs por etapa, como exigido em `docs/operation/spark-cost-and-benchmark.md:39`.

5. Reforcar no Quick Start, perto de `dry_run_limit`, que o limite nao protege
   a leitura de `_extract`; ele apenas reduz o DataFrame entregue ao transform.

6. Para helpers com shuffle, exigir que a pipeline concreta registre volume,
   particoes e plano antes de aprovar `dry_run=False` em ambiente produtivo.

7. Manter `dry_run_show_rows=0` como default e nao flexibilizar a regra que
   proibe `dry_run_show_rows > 0` fora de dry-run.

## Conclusao

O custo necessario para o contrato v0.1 esta razoavelmente explicito e
justificado: bloquear invalidos antes do `load` requer uma acao Spark. O custo
acidental esta nos helpers opcionais e na interpretacao errada de `dry_run`.
Sem benchmark por pipeline, especialmente para unicidade, reconciliacao e
volume, a v0.1 nao deve ser tratada como pronta para producao em datasets
grandes.
