# Auditoria SR13 - Developer Experience da Jornada Junior

## Veredito

A v0.1 entrega uma base utilizavel para um desenvolvedor junior criar e rodar
uma pipeline simples, mas a jornada ainda nao e natural. O primeiro exemplo
funciona como contrato minimo, a API raiz e pequena, e os testes de jornada
provam que o autor escreve apenas extract/transform/load. Ainda assim, o
framework transfere friccao relevante para o junior em quatro pontos: quantidade
de configuracao obrigatoria, nomenclatura de estruturas sem helper guiado,
diagnosticos que dizem o que falhou mas nao indicam a correcao imediata, e
defaults/exemplos que exigem leitura senior para nao criar falsa seguranca.

O risco principal de DX nao e a arquitetura interna. O risco e o usuario novo
copiar o Quick Start, trocar a origem, esquecer uma decisao critica
(`extra_columns_policy`, checks de nulo, `keep_technical_columns` ou readiness
de load) e so descobrir a semantica correta depois de ler varios trechos do
contrato.

## Escopo Lido

- `README.md`
- `QUICK_START.md`
- `docs/v0.1-contract.md`
- `etl_framework/__init__.py`
- `etl_framework/contracts/pipeline.py`
- `etl_framework/contracts/extract.py`
- `etl_framework/contracts/transform.py`
- `etl_framework/contracts/load.py`
- `etl_framework/models/config.py`
- `etl_framework/utils/auto_quality.py`
- `etl_framework/utils/validate_struct.py`
- `etl_framework/utils/dataframe_checks.py`
- `etl_framework/infra/errors.py`
- `tests/test_pipeline_author_journey.py`
- `tests/fixtures/pipeline_author_journey/README.md`

## Respostas Obrigatorias

### O primeiro uso e claro?

Parcialmente. O Quick Start mostra o import correto da API raiz
(`from etl_framework import EtlRunConfig, Extract, Load, Pipeline, Transform`) e
um exemplo completo em um unico arquivo (`QUICK_START.md:25`,
`QUICK_START.md:61`, `QUICK_START.md:69`, `QUICK_START.md:74`,
`QUICK_START.md:82`, `QUICK_START.md:96`). Isso e bom para copiar e executar.

O problema e que o primeiro uso ja obriga o junior a entender `StructType`,
`StructField.metadata["checks"]`, `target_key`, `target_path`, dry-run, modo de
escrita e colunas tecnicas. A documentacao avisa que `QuickLoad` e didatico e
nao deve ser copiado para carga real (`QUICK_START.md:6`), mas nao oferece uma
alternativa segura de desenvolvimento alem de manter `dry_run=True`.

### A API raiz e pequena e suficiente?

Sim. A API publica exportada em `etl_framework/__init__.py` contem somente
`Pipeline`, `EtlRunConfig`, `EtlExecutionContext`, `Extract`, `Transform` e
`Load` (`etl_framework/__init__.py:1`, `etl_framework/__init__.py:8`). Isso
esta alinhado ao contrato de API publica esperada (`docs/v0.1-contract.md:281`).

A suficiencia e real para a jornada simples, mas incompleta para discoverability:
nao ha helper publico para criar configuracao minima, validar localmente o
contrato antes de instanciar `Pipeline`, ou gerar uma mensagem orientada para o
autor da pipeline.

### Os nomes dos contratos comunicam responsabilidade?

Sim para `Extract`, `Transform`, `Load` e `Pipeline`. A tabela do README e
direta sobre o que o junior implementa (`README.md:36`, `README.md:42`).

Parcialmente para `check` e `validate`. O contrato distingue `check` como
validacao da origem e `validate` como validacao do alvo
(`docs/v0.1-contract.md:59`, `docs/v0.1-contract.md:62`), mas o junior pode
nao inferir sozinho que `source_struct` so valida nomes/tipos enquanto
`target_struct` tambem executa checks declarativos. Essa diferenca aparece no
contrato (`docs/v0.1-contract.md:127`, `docs/v0.1-contract.md:141`) e no codigo
(`etl_framework/utils/auto_quality.py:17`, `etl_framework/utils/auto_quality.py:36`),
mas nao fica suficientemente evidente no primeiro exemplo.

### O autor da pipeline precisa entender internals?

Nao para a jornada feliz. O teste integrado prova que o autor implementa classes
concretas de leitura, transformacao e carga (`tests/test_pipeline_author_journey.py:48`,
`tests/test_pipeline_author_journey.py:65`, `tests/test_pipeline_author_journey.py:85`)
e verifica que esses componentes nao carregam `logger`, `observability` ou
`event_logger` (`tests/test_pipeline_author_journey.py:187`). A orquestracao e
feita por `Pipeline.run()` (`etl_framework/contracts/pipeline.py:45`).

Sim em pontos de correcao. Para entender por que um erro de schema virou
`CheckError`, por que `dry_run` ainda le a origem, por que `is_valid` aparece
no load, ou por que `nullable=False` nao bloqueia nulos, o junior precisa
conectar README, contrato e implementacao (`README.md:91`,
`docs/v0.1-contract.md:180`, `docs/v0.1-contract.md:206`,
`etl_framework/contracts/load.py:129`).

### A configuracao minima e compreensivel?

Parcialmente. O modelo e pequeno o bastante para ler, mas a configuracao minima
operacional tem muitos campos obrigatorios para uma primeira pipeline:
`pipeline_name`, `target_schema`, `target_table`, `target_path`, `target_key`,
`source_struct` e `target_struct` aparecem no exemplo (`QUICK_START.md:82`) e
no modelo (`etl_framework/models/config.py:30`). Os defaults existem para
`dry_run`, `dry_run_limit`, `dry_run_show_rows`, `extra_columns_policy` e
`keep_technical_columns` (`etl_framework/models/config.py:36`,
`etl_framework/models/config.py:40`), mas esses defaults tem consequencias de
DX e seguranca.

O maior atrito e que `source_struct` e `target_struct` sao opcionais no tipo,
mas obrigatorios no fluxo padrao. O contrato explica essa diferenca
(`docs/v0.1-contract.md:108`), e o preflight falha cedo
(`etl_framework/contracts/pipeline.py:53`, `etl_framework/contracts/pipeline.py:58`,
`etl_framework/contracts/pipeline.py:61`), mas a assinatura do modelo comunica
"opcional" enquanto a jornada comunica "obrigatorio".

### O framework reduz ou aumenta friccao?

Reduz friccao de orquestracao, ordem de execucao, preflight, dry-run e
observabilidade basica. Aumenta friccao de entrada por exigir conhecimento
Spark/schema/checks cedo demais e por deixar decisoes perigosas como default ou
nota de rodape.

## Jornada Do Usuario

1. O junior abre o README e entende a promessa: fluxo
   `extract -> check -> transform -> validate -> load -> certify`, injecao de
   `SparkSession`, `EtlRunConfig` e `EtlExecutionContext`, e dry-run para
   desenvolvimento (`README.md:24`, `README.md:30`).
2. Ele abre o Quick Start e copia um arquivo unico com import da API raiz
   (`QUICK_START.md:25`).
3. Ele declara `source_struct` e `target_struct` manualmente com tipos Spark e,
   no alvo, metadata de checks (`QUICK_START.md:28`, `QUICK_START.md:35`).
4. Ele implementa tres classes: `_extract`, `_transform` e `_load`
   (`QUICK_START.md:61`, `QUICK_START.md:69`, `QUICK_START.md:74`).
5. Ele instancia `EtlRunConfig` com metadados de destino, structs, dry-run e
   modo de escrita (`QUICK_START.md:82`).
6. Ele monta `Pipeline` com `spark`, `config`, `extract`, `transform` e `load`,
   e chama `pipeline.run()` (`QUICK_START.md:96`, `QUICK_START.md:104`).
7. Se tudo estiver correto, `Pipeline.run()` executa preflight, extract,
   transform, load/dry-run e retorna o `DataFrame` final
   (`etl_framework/contracts/pipeline.py:45`).
8. Em dry-run, `Extract` limita apos o check (`etl_framework/contracts/extract.py:91`)
   e `Load` pula `_load`/`_certify` (`etl_framework/contracts/load.py:82`).
9. Em falha de origem, o erro e embrulhado como `CheckError` com
   `pipeline_name`, `run_id` e `stage` (`etl_framework/infra/errors.py:38`,
   `etl_framework/infra/errors.py:40`, `etl_framework/infra/errors.py:42`).

## Friccoes Por Etapa

### Descoberta Inicial

Friccao: a documentacao ativa esta bem apontada (`README.md:142`), mas o junior
precisa alternar entre README, Quick Start e contrato para entender a diferenca
entre exemplo didatico, contrato de v0.1 e uso real. A mensagem "nao copie
QuickLoad para cargas reais" e correta (`QUICK_START.md:6`), mas aumenta
incerteza sem oferecer um caminho concreto alem do checklist.

Impacto: o primeiro sucesso e possivel, mas a transicao de exemplo para pipeline
real depende de julgamento senior.

### Criacao Do Exemplo Minimo

Friccao: o exemplo minimo e longo para o primeiro contato. Antes de escrever uma
transformacao simples, o autor precisa declarar schemas Spark completos,
metadata de checks, chave, destino e dry-run (`QUICK_START.md:28`,
`QUICK_START.md:35`, `QUICK_START.md:82`).

Impacto: a API publica e pequena, mas a superficie cognitiva da primeira
pipeline nao e pequena.

### Configuracao

Friccao severa: `source_struct` e `target_struct` sao `StructType | None` no
modelo (`etl_framework/models/config.py:43`, `etl_framework/models/config.py:44`),
mas a execucao padrao exige ambos e falha no preflight
(`etl_framework/contracts/pipeline.py:58`, `etl_framework/contracts/pipeline.py:61`).

Impacto: a assinatura sugere que omitir schema pode ser uma opcao valida; a
correcao so aparece ao rodar ou ao ler o contrato.

Friccao adicional: `extra_columns_policy` default e `ignore`
(`etl_framework/models/config.py:40`) e o contrato avisa que extras nao falham
por padrao (`docs/v0.1-contract.md:187`). Para junior, isso reduz ruido no
inicio, mas tambem permite drift silencioso em uma pipeline copiada.

### Implementacao De Extract

Ponto forte: `Extract.run()` executa `_extract`, valida retorno `DataFrame`,
roda `auto_check_source` e so depois libera downstream
(`etl_framework/contracts/extract.py:27`, `etl_framework/contracts/extract.py:67`,
`etl_framework/contracts/extract.py:76`). O teste de jornada cobre CSV real
(`tests/test_pipeline_author_journey.py:48`).

Friccao: em dry-run, o limite ocorre depois da leitura e do check
(`etl_framework/contracts/extract.py:91`; contrato em `docs/v0.1-contract.md:206`).
Isso e correto para v0.1, mas o nome `dry_run_limit` pode induzir o junior a
esperar custo baixo de leitura.

### Implementacao De Transform

Ponto forte: `Transform.run()` executa `_transform` e valida automaticamente o
alvo (`etl_framework/contracts/transform.py:26`, `etl_framework/contracts/transform.py:61`,
`etl_framework/contracts/transform.py:70`). Isso reduz chamadas manuais e
mantem o autor focado em Spark.

Friccao: `nullable=False` nao bloqueia nulos sozinho (`docs/v0.1-contract.md:180`;
`QUICK_START.md:156`). Essa regra e documentada, mas e contraintuitiva para
junior e deveria aparecer como diagnostico ou warning educativo, nao apenas
como texto.

### Implementacao De Load

Friccao severa: `Load._load` recebe colunas tecnicas por default; a remocao
depende de `keep_technical_columns=False` (`README.md:91`,
`docs/v0.1-contract.md:214`, `etl_framework/contracts/load.py:129`). O Quick
Start comenta `is_valid`, mas mantem o default verdadeiro
(`QUICK_START.md:74`). Junior pode persistir `is_valid`, `etl_run_id` ou outras
colunas tecnicas sem intencao.

Impacto: o primeiro load real tem chance alta de vazar shape tecnico para o
destino se o autor copiar o exemplo e trocar apenas `dry_run=False`.

### Correcao De Erros

Ponto forte: erros gerenciados incluem `pipeline_name`, `run_id`, `stage`,
`cause_type` e `cause_message` (`etl_framework/infra/errors.py:38`,
`etl_framework/infra/errors.py:40`, `etl_framework/infra/errors.py:42`,
`etl_framework/infra/errors.py:46`). Falhas de schema mostram coluna ausente,
tipo esperado/obtido e colunas extras (`etl_framework/utils/validate_struct.py:110`,
`etl_framework/utils/validate_struct.py:118`, `etl_framework/utils/validate_struct.py:125`,
`etl_framework/utils/validate_struct.py:132`).

Friccao: as mensagens sao tecnicas, nao orientadas. Por exemplo, `Schema
mismatch` informa "Missing column", mas nao diz "corrija `source_struct` ou a
leitura em `_extract`". `Preflight failed before extract` informa o campo
faltante (`etl_framework/contracts/pipeline.py:58`), mas nao aponta para o
exemplo minimo nem para quais campos sao realmente obrigatorios na v0.1.

## Pontos Fortes Comprovados

- API raiz pequena e coerente com o contrato: seis exports em
  `etl_framework/__init__.py` (`etl_framework/__init__.py:8`) e mesma lista no
  contrato (`docs/v0.1-contract.md:281`).
- Ordem oficial centralizada em `Pipeline.run()`, sem exigir que o autor chame
  check/validate manualmente (`etl_framework/contracts/pipeline.py:45`).
- Contratos comunicam responsabilidades basicas de forma legivel no README
  (`README.md:42`) e no contrato (`docs/v0.1-contract.md:59`).
- Preflight falha antes de `_extract` quando faltam `source_struct` ou
  `target_struct` (`etl_framework/contracts/pipeline.py:53`,
  `etl_framework/contracts/pipeline.py:58`, `etl_framework/contracts/pipeline.py:61`).
- Retorno errado de hook falha com mensagem direta:
  "`stage` must return pyspark.sql.DataFrame" (`etl_framework/utils/dataframe_checks.py:6`).
- Teste de jornada existe e cobre caminho feliz, dry-run e falha antes de
  transform/load (`tests/test_pipeline_author_journey.py:220`,
  `tests/test_pipeline_author_journey.py:267`, `tests/test_pipeline_author_journey.py:314`).
- Fixture de jornada e pequena, offline e documentada
  (`tests/fixtures/pipeline_author_journey/README.md:26`).
- O teste afirma que o autor nao precisa lidar com logger/observability nos
  componentes de negocio (`tests/test_pipeline_author_journey.py:187`).

## Evidencias Obrigatorias

### Caminho De Importacao

O caminho recomendado e:

```python
from etl_framework import EtlRunConfig, Extract, Load, Pipeline, Transform
```

Evidencia: Quick Start (`QUICK_START.md:25`) e teste de jornada
(`tests/test_pipeline_author_journey.py:17`).

### Exemplo Minimo

O exemplo minimo exige:

- subclasses de `Extract`, `Transform` e `Load` (`QUICK_START.md:61`,
  `QUICK_START.md:69`, `QUICK_START.md:74`);
- `EtlRunConfig` com metadados de destino, schemas e dry-run
  (`QUICK_START.md:82`);
- `Pipeline(...).run()` (`QUICK_START.md:96`, `QUICK_START.md:104`).

### Campos De Configuracao Necessarios

Campos sem default no modelo: `pipeline_name`, `target_schema`, `target_table`,
`target_path` (`etl_framework/models/config.py:30`). `target_key` tem default
vazio, mas o exemplo e a jornada usam chave (`etl_framework/models/config.py:34`,
`QUICK_START.md:86`, `tests/test_pipeline_author_journey.py:128`).

Campos tecnicamente opcionais no modelo, mas obrigatorios no fluxo padrao:
`source_struct` e `target_struct` (`etl_framework/models/config.py:43`,
`etl_framework/models/config.py:44`, `etl_framework/contracts/pipeline.py:58`,
`etl_framework/contracts/pipeline.py:61`).

Defaults que alteram DX: `dry_run=False`, `dry_run_limit=100`,
`dry_run_show_rows=0`, `extra_columns_policy="ignore"` e
`keep_technical_columns=True` (`etl_framework/models/config.py:36`,
`etl_framework/models/config.py:40`).

### Mensagens De Erro Relevantes

- Sem `source_struct`: "source_struct is required before extract"
  (`etl_framework/contracts/pipeline.py:58`) ou "source_struct is required for
  automatic source check in v0.1" (`etl_framework/utils/auto_quality.py:24`).
- Sem `target_struct`: "target_struct is required before extract"
  (`etl_framework/contracts/pipeline.py:61`) ou "target_struct is required for
  automatic target validation in v0.1" (`etl_framework/utils/auto_quality.py:44`).
- Retorno errado de hook: "`stage` must return pyspark.sql.DataFrame"
  (`etl_framework/utils/dataframe_checks.py:6`).
- Schema invalido: "Schema mismatch", "Missing column", "expected X, got Y",
  "Extra columns not declared in schema" (`etl_framework/utils/validate_struct.py:110`,
  `etl_framework/utils/validate_struct.py:118`, `etl_framework/utils/validate_struct.py:125`,
  `etl_framework/utils/validate_struct.py:132`).
- Check SQL invalido: "Invalid SQL check rule ... Available columns ..."
  (`etl_framework/utils/validate_struct.py:226`).
- Registros invalidos: "Invalid records found during target validation:
  invalid_count=...; failed_checks=..." (`etl_framework/utils/auto_quality.py:86`).

### Testes De Jornada

`tests/test_pipeline_author_journey.py` cobre:

- caminho feliz com CSV e comparacao de alvo esperado
  (`tests/test_pipeline_author_journey.py:220`, `tests/test_pipeline_author_journey.py:239`);
- garantia de que o autor escreveu apenas ETL, sem observabilidade manual
  (`tests/test_pipeline_author_journey.py:187`);
- dry-run sem chamada real de load/certify (`tests/test_pipeline_author_journey.py:267`,
  `tests/test_pipeline_author_journey.py:286`, `tests/test_pipeline_author_journey.py:289`);
- falha gerenciada em CSV invalido antes de transform/load
  (`tests/test_pipeline_author_journey.py:314`, `tests/test_pipeline_author_journey.py:321`,
  `tests/test_pipeline_author_journey.py:330`).

## Oportunidades P1/P2/P3

### P1 - Tornar a configuracao minima honesta e guiada

Problema: `source_struct` e `target_struct` sao opcionais no tipo, mas
obrigatorios no fluxo padrao. Isso e uma quebra de expectativa para junior.

Recomendacao: adicionar uma factory publica pequena, por exemplo
`EtlRunConfig.standard(...)`, ou uma funcao `make_run_config(...)` que deixe
explicito o conjunto minimo da v0.1 e mantenha o dataclass atual para casos
avancados. Alternativamente, tornar `source_struct` e `target_struct`
obrigatorios no construtor padrao da v0.1.

Reducao de friccao: o erro passa a acontecer no autocomplete/assinatura, nao
apos executar a pipeline.

### P1 - Melhorar mensagens de correcao para erros comuns

Problema: mensagens dizem o que falhou, mas nao orientam a acao do autor.

Recomendacao: enriquecer `PreflightError`, `CheckError` e `ValidateError` com
frases acionaveis:

- campo ausente na origem: "corrija a leitura em `_extract` ou atualize
  `source_struct`";
- campo ausente no alvo: "corrija `_transform` ou atualize `target_struct`";
- `dry_run_show_rows`: "defina `dry_run=True` ou volte `dry_run_show_rows=0`";
- `is_valid` existente: "renomeie a coluna de negocio ou remova antes de
  `auto_validate`".

Reducao de friccao: o junior consegue corrigir sem abrir implementacao interna.

### P1 - Evitar que o primeiro load real persista colunas tecnicas sem intencao

Problema: `keep_technical_columns=True` por default entrega `is_valid` e outras
colunas tecnicas a `_load` (`etl_framework/contracts/load.py:129`). O Quick
Start menciona isso, mas o exemplo mantem o default (`QUICK_START.md:74`).

Recomendacao: no Quick Start, configurar explicitamente
`keep_technical_columns=False` quando `target_path` representa destino de
negocio, ou separar exemplo de inspecao tecnica e exemplo de escrita real.

Reducao de friccao: evita que a primeira pipeline real tenha shape inesperado no
destino.

### P2 - Adicionar warnings educativos para `nullable=False` sem check SQL

Problema: `nullable=False` parece bloqueio, mas e apenas intencao documentada
(`docs/v0.1-contract.md:180`). Junior tende a confiar no schema Spark.

Recomendacao: quando `target_struct` tiver `nullable=False` sem check
`IS NOT NULL`, emitir warning de configuracao ou disponibilizar validador
educativo no Quick Start.

Reducao de friccao: transforma uma regra escondida em feedback imediato.

### P2 - Tornar a diferenca entre `source_struct` e `target_struct` mais visivel

Problema: `source_struct` valida nomes/tipos; `target_struct` valida nomes,
tipos e checks. A diferenca e tecnica e facil de perder.

Recomendacao: adicionar uma tabela curta no Quick Start: "source_struct: schema
da leitura; checks nao bloqueiam regras de negocio aqui" e "target_struct:
schema do resultado; checks `error` bloqueiam load".

Reducao de friccao: evita expectativa errada sobre checks declarativos na origem.

### P2 - Incluir teste de jornada para erro de transformacao do junior

Problema: o teste de jornada cobre falha de origem, mas nao cobre o caso comum
em que `_transform` renomeia uma coluna errado e quebra `target_struct`.

Recomendacao: adicionar cenario em `tests/test_pipeline_author_journey.py` com
`target_struct` esperando uma coluna que `_transform` nao produz, validando que
a mensagem aponta para `_transform`/`target_struct`.

Reducao de friccao: protege a experiencia de correcao mais provavel depois do
primeiro exemplo.

### P3 - Reduzir ruido do Quick Start sem esconder Spark

Problema: o primeiro arquivo mistura schema, checks, ETL, SparkSession,
configuracao e load didatico.

Recomendacao: manter o exemplo completo, mas incluir antes dele um bloco
"menor pipeline que roda em dry-run" sem checks de negocio, seguido de um bloco
incremental "adicione checks para bloquear dados".

Reducao de friccao: preserva Spark e contrato v0.1, mas diminui carga cognitiva
na primeira leitura.

### P3 - Criar checklist de migracao do exemplo para pipeline real

Problema: a documentacao diz o que trocar primeiro (`QUICK_START.md:145`), mas
nao transforma isso em criterio verificavel.

Recomendacao: converter a secao em checklist objetivo: origem real definida,
`source_struct` conferido, `target_struct` conferido, checks de nulo
declarados, politica de extras escolhida, `keep_technical_columns` decidido,
readiness de load preenchido antes de `dry_run=False`.

Reducao de friccao: junior sabe quando pedir revisao senior e o que levar para
a revisao.

## Conclusao

A v0.1 esta no caminho certo para uma API pequena e um fluxo previsivel, mas
a jornada junior ainda depende demais de leitura cuidadosa e interpretacao de
contrato. A recomendacao severa e nao ampliar escopo: nao criar load seguro
generico, nao esconder Spark e nao transformar a v0.1 em plataforma. O ganho de
DX deve vir de assinatura/configuracao mais honesta, mensagens de erro com
proxima acao, exemplos que nao induzam persistencia tecnica acidental e testes
de jornada que cubram tambem erros comuns de transformacao.
