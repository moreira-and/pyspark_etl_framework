# Auditoria SR18 - Seguranca Basica e Dados Sensiveis

## Escopo e metodo

Papel executado: especialista em seguranca pragmatica para pipelines de dados.

Escopo aplicado: exposicao basica de dados sensiveis em dry-run, erros, eventos,
metricas, diagnosticos de invalidos e exemplos. Nao foi exigido modelo completo
de seguranca, criptografia, governanca regulatoria, IAM, mascaramento
corporativo ou DLP, porque isso nao esta prometido para a v0.1 interna.

Fontes lidas: `etl_framework/`, `README.md`, `QUICK_START.md`,
`docs/v0.1-contract.md`, `docs/v0.1-known-limitations.md`,
`docs/observability/logging.md` e testes relacionados a dry-run, erros,
observabilidade, validacao e jornada de autor.

Resultado severo: a v0.1 tem default razoavelmente seguro contra impressao de
linhas e contra vazamento obvio em mensagens de erro gerenciadas. O risco
residual relevante nao esta em uma acao automatica escondida que coleta linhas;
esta em canais explicitamente configuraveis ou livres (`dry_run_show_rows`,
`target_path`, `context.metrics` e eventos/metricas extras), que podem publicar
dado sensivel em log se um autor junior usar esses campos como area de debug.

## Respostas obrigatorias

### O framework pode imprimir dados por padrao?

Nao. O default de `EtlRunConfig.dry_run_show_rows` e `0`
(`etl_framework/models/config.py:36-38`) e `Load._run_dry_run()` so chama
`_show_dry_run_sample()` quando `config.dry_run_show_rows > 0`
(`etl_framework/contracts/load.py:91-97`). A chamada que imprime dados e
explicita: `df.show(config.dry_run_show_rows, truncate=False)`
(`etl_framework/contracts/load.py:106-114`).

Teste de prova: `tests/test_dry_run.py:183-198` garante que producao nao chama
`show`; `tests/test_dry_run.py:201-216` garante que dry-run com
`dry_run_show_rows=0` nao chama `show`; `tests/test_dry_run.py:219-234` prova
que o modo positivo chama `show` com `truncate=False`.

Impacto operacional: o default evita vazamento acidental de linhas, mas quando
habilitado o modo imprime colunas de negocio completas no stdout/log do ambiente.

### `dry_run_show_rows` esta documentado como risco?

Parcialmente. O Quick Start alerta: "Nao use `dry_run_show_rows > 0` em ambiente
compartilhado" e informa que o modo chama `show(..., truncate=False)` e pode
expor dados sensiveis (`QUICK_START.md:138-139`). As limitacoes conhecidas tambem
registram que `dry_run_show_rows > 0` pode expor dados sensiveis
(`docs/v0.1-known-limitations.md:80-85`).

Lacuna: o contrato principal descreve que `dry_run` chama `show` somente se
`dry_run_show_rows > 0`, mas nao repete o aviso de exposicao de dados na propria
secao de Dry Run (`docs/v0.1-contract.md:192-207`). O README explica o fluxo de
dry-run, mas tambem nao destaca o risco de dados sensiveis nessa area
(`README.md:62-75`).

Impacto operacional: um junior que leia apenas o contrato ou README pode
entender a mecanica, mas nao a politica de seguranca esperada.

### Erros e logs evitam coletar linhas de dados?

No caminho gerenciado, sim para linhas de dados. O decorator de stage declara que
nao materializa DataFrames (`etl_framework/infra/stage.py:35-39`) e em falha so
emite evento com erro gerenciado (`etl_framework/infra/stage.py:56-88`). O erro
gerenciado sanitiza mensagem base e `cause_message`
(`etl_framework/infra/errors.py:33-49`). O builder de eventos sanitiza apenas
campos conhecidos de erro: `error`, `error_message`, `exception` e
`cause_message` (`etl_framework/utils/observability_events.py:10-11`,
`etl_framework/utils/observability_events.py:67-73`).

Testes de prova: `tests/test_stage.py:176-190` impede materializacao por
`count`, `collect` ou `show` no decorator; `tests/test_pipeline_contract.py:571-663`
verifica que modo normal nao chama `show` nem `collect`; `tests/test_errors.py:133-181`
prova redacao de token, senha, payload, CPF e path em mensagens de erro.

Lacuna: logs/eventos normais incluem `target_path` como campo canonico sem
sanitizacao (`etl_framework/utils/observability_events.py:27-44`) e metricas
explicitamente preenchidas sao emitidas como vieram, salvo filtro de chave string
e conversao de valores nao escalares para `str`
(`etl_framework/utils/observability_events.py:52-64`). Isso nao coleta linhas
automaticamente, mas e um canal de vazamento se a pipeline publicar valores de
dados em metricas ou metadados.

### Diagnosticos de invalidos expoem valores sensiveis?

Por padrao, nao expoem linhas. Quando ha invalidos, `auto_validate` calcula
`invalid_count` e resumos por check (`etl_framework/utils/auto_quality.py:72-88`).
O summary seleciona apenas `field`, `check` e `failed_count` antes de coletar
(`etl_framework/utils/auto_quality.py:91-101`). A montagem de summary agrega
contagens por regra e nao retorna linhas de dados (`etl_framework/utils/validate_struct.py:244-320`).

Documentacao de prova: o contrato afirma que o diagnostico de invalidos nao
coleta linhas nem chama `show` por padrao (`docs/v0.1-contract.md:156-158`).

Risco residual: nomes de campos, nomes de checks, mensagens e regras SQL entram
no summary interno (`etl_framework/utils/validate_struct.py:307-314`). A excecao
de `auto_validate` hoje inclui somente `field.check: failed_count=...`
(`etl_framework/utils/auto_quality.py:96-101`), o que e adequado. Se no futuro
mensagem ou regra forem incluidos diretamente em erro/log, poderao carregar
termos sensiveis definidos pelo autor.

### Exemplos podem induzir uso inseguro?

O Quick Start e relativamente defensivo: usa `dry_run=True`,
`dry_run_show_rows=0` (`QUICK_START.md:82-93`), explica que dry-run pula `_load`
e `_certify` (`QUICK_START.md:113-123`) e alerta sobre `dry_run_show_rows > 0`
(`QUICK_START.md:138-139`). Tambem diz que o `QuickLoad` e didatico e nao deve
ser copiado para carga real sem checklist senior (`QUICK_START.md:6-7`).

Lacunas: o exemplo ainda mostra escrita direta em `config.target_path`
(`QUICK_START.md:74-77`) e o contrato/logging documenta `target_path` em eventos
(`docs/observability/logging.md:73-95`) sem orientar que paths/URIs nao devem
conter credenciais, identificadores sensiveis ou nomes de datasets restritos.
Para v0.1 isso nao exige vault ou DLP, mas exige aviso simples.

### Existem lacunas que importam mesmo para uma v0.1 interna?

Sim. Tres lacunas importam sem transformar a biblioteca em plataforma de
seguranca:

1. `context.metrics` e campos `metrics` extras sao publicados sem redacao por
politica de nome/valor. A propria classe permite metricas arbitrarias
(`etl_framework/models/context.py:16-23`) e o builder as emite
(`etl_framework/utils/observability_events.py:23-25`, `:52-64`).
2. `target_path` e metadados de destino sao logados como campos canonicos sem
redacao (`etl_framework/utils/observability_events.py:35-39`), enquanto a
sanitizacao de path so existe para mensagens de erro
(`etl_framework/utils/sanitization.py:17-29`).
3. O alerta de `dry_run_show_rows` esta em Quick Start e limitacoes, mas nao
esta no ponto mais autoritativo do contrato de Dry Run
(`docs/v0.1-contract.md:192-207`).

## Matriz fonte de exposicao -> condicao -> mitigacao

| Fonte de exposicao | Condicao necessaria | Dados expostos/coletados | Mitigacao existente | Lacuna residual | Evidencia |
| --- | --- | --- | --- | --- | --- |
| Dry-run sample | `dry_run=True` e `dry_run_show_rows > 0` | Linhas e colunas do DataFrame final via stdout/log, sem truncamento | Default `0`; proibido fora de dry-run; evento `dry_run_sample_requested`; docs alertam em Quick Start e limitacoes | Sem cap adicional, sem allowlist/mascara, aviso ausente no contrato principal | `etl_framework/contracts/load.py:91-114`; `etl_framework/models/config.py:127-136`; `tests/test_dry_run.py:201-234`; `QUICK_START.md:138-139` |
| Eventos de erro | Excecao em stage gerenciado | Tipo de erro e mensagem sanitizada | `EtlError` sanitiza mensagem e causa; payload sanitiza campos de erro conhecidos | Sanitizacao e regex best-effort; campos fora da lista nao sao redigidos | `etl_framework/infra/errors.py:33-49`; `etl_framework/utils/observability_events.py:67-73`; `tests/test_errors.py:133-181` |
| Diagnostico de invalidos | `auto_validate` encontra `is_valid=False` | Contagem de invalidos e `field.check` com failed_count | Nao chama `show`; coleta apenas summary agregado | Se nomes de campos/checks forem sensiveis, aparecem em erro; risco baixo e controlado por contrato de schema | `etl_framework/utils/auto_quality.py:81-101`; `etl_framework/utils/validate_struct.py:299-320`; `docs/v0.1-contract.md:156-158` |
| Metricas operacionais | Pipeline preenche `context.metrics` ou extra `metrics` | Qualquer escalar informado pelo autor; nao escalares viram `str` | So aceita chaves string nao vazias; nao computa metricas automaticamente | Sem bloqueio/redacao para `cpf`, `email`, `sample`, `token`, `path`, payload ou valores de linha | `etl_framework/models/context.py:16-23`; `etl_framework/utils/observability_events.py:23-25`, `:52-64`; `tests/test_observability.py:236-282` |
| Metadados de destino | Qualquer evento runtime | `target_schema`, `target_table`, `target_path`, `target`, `write_mode` | Campos ajudam rastreabilidade operacional | `target_path` pode conter URI com credencial, path sensivel ou identificador de dataset restrito; nao ha redacao normal | `etl_framework/utils/observability_events.py:27-44`; `docs/observability/logging.md:73-95`; `tests/test_observability.py:55-96` |
| Exemplos | Autor copia Quick Start sem revisao | Escrita direta no path configurado | Exemplo usa dry-run e avisa que `QuickLoad` e didatico | Falta aviso sobre segredo em `target_path` e sobre metricas sem dados de linha | `QUICK_START.md:6-7`, `:74-93`, `:138-154` |

## Riscos P1/P2/P3

### P1 - Nenhum achado P1 confirmado

Nao encontrei vazamento automatico de linhas por default, coleta automatica de
linhas em logs, ou persistencia de payload sensivel sem acao explicita do autor.
A v0.1 nao imprime dados por padrao e os testes cobrem esse contrato. Portanto,
nao ha P1 proporcional ao escopo auditado.

### P2 - Metricas e extras de observabilidade podem virar canal de vazamento

Se uma pipeline colocar amostras, IDs pessoais, emails, CPFs, payloads, tokens,
paths internos ou motivos com dado real em `context.metrics`, o framework publica
essas informacoes no payload de observabilidade. O builder combina metricas do
contexto e de extras (`etl_framework/utils/observability_events.py:23-25`) e
serializa escalares diretamente (`etl_framework/utils/observability_events.py:52-64`).
O teste atual confirma inclusao e conversao de metricas, mas nao prova redacao
de conteudo sensivel em metricas (`tests/test_observability.py:236-282`).

Impacto operacional: logs de execucao, stdout local, handlers de logging ou sinks
externos podem receber dados pessoais ou segredos com aparencia de metrica
tecnica. Isso e especialmente provavel em v0.1 porque `metrics` e uma area facil
para debug por junior.

Recomendacao objetiva: manter metricas livres, mas adicionar uma barreira leve:
documentar allowlist recomendada (`rows_*`, `duration_*`, `*_missing_reason`
sem valores de dados), redigir valores de metricas cujas chaves contenham termos
sensíveis obvios (`token`, `secret`, `password`, `cpf`, `email`, `payload`,
`row`, `sample`, `path`) ou, no minimo, adicionar teste que demonstre a politica
escolhida.

### P2 - `target_path` e metadados de destino sao emitidos sem sanitizacao normal

Todo evento estruturado inclui `target_path` diretamente
(`etl_framework/utils/observability_events.py:35-39`). A sanitizacao existente
remove `path=...` apenas quando o path aparece dentro de mensagem de erro
(`etl_framework/utils/sanitization.py:17-29`), nao quando `target_path` e campo
canonico do payload. A documentacao de logging tambem apresenta `target_path`
como campo normal (`docs/observability/logging.md:73-95`).

Impacto operacional: se `target_path` receber URI com credencial, SAS token, path
com nome de cliente, dataset restrito ou identificador sensivel, todos os eventos
do run carregam esse dado. Isso nao e DLP corporativo, mas e uma exposicao basica
em logs.

Recomendacao objetiva: proibir por documentacao e teste caminhos com credenciais
em `target_path`; aplicar sanitizacao de credenciais URI nesse campo antes de
emitir eventos; considerar manter apenas path logico/alias ou mascaramento
parcial quando o path puder ser sensivel.

### P2 - `dry_run_show_rows > 0` imprime dados completos quando habilitado

O comportamento e explicito e testado: `df.show(..., truncate=False)` imprime
amostras completas (`etl_framework/contracts/load.py:106-114`;
`tests/test_dry_run.py:219-234`). A configuracao so e permitida com
`dry_run=True` (`etl_framework/models/config.py:133-136`), e o default e seguro.

Impacto operacional: em notebook compartilhado, CI, terminal capturado, driver
Spark com logs agregados ou sink de stdout, a amostra pode expor qualquer coluna
presente no DataFrame final. O risco e mitigado por opt-in, mas a severidade do
efeito e alta quando alguem habilita.

Recomendacao objetiva: manter o recurso para desenvolvimento local, mas reforcar
no contrato principal e README que ele e proibido em ambiente compartilhado.
Opcional e proporcional: emitir evento com `level="warning"` para
`dry_run_sample_requested`, porque hoje o evento e `info` por default.

### P3 - Avisos de seguranca estao dispersos e nao no contrato autoritativo

O aviso de `dry_run_show_rows` aparece no Quick Start (`QUICK_START.md:138-139`)
e nas limitacoes (`docs/v0.1-known-limitations.md:84`), mas a secao de Dry Run
do contrato principal so descreve o mecanismo (`docs/v0.1-contract.md:192-207`).
O README tambem nao destaca o risco na descricao de dry-run (`README.md:62-75`).

Impacto operacional: a decisao segura depende de leitura cruzada. Para uma v0.1
voltada a junior, o risco deve estar junto do knob perigoso.

Recomendacao objetiva: duplicar uma frase curta no contrato e README: manter
`dry_run_show_rows=0` em ambientes compartilhados; valores positivos chamam
`show(..., truncate=False)` e podem expor dados sensiveis.

### P3 - Falta teste negativo para ausencia de dados sensiveis em metricas e `target_path`

Ha testes bons para erro sanitizado (`tests/test_errors.py:133-181`) e para
`error_message` em payload (`tests/test_observability.py:285-307`), mas nao ha
teste equivalente para `context.metrics` nem para `target_path` canonico.

Impacto operacional: regressao futura pode ampliar vazamento por observabilidade
sem falhar em CI. O custo de teste e baixo.

Recomendacao objetiva: adicionar casos pequenos em `tests/test_observability.py`
para a politica escolhida: redigir credencial em URI de `target_path` e tratar
metricas com nomes sensiveis.

## Pontos positivos comprovados

- Default nao imprime dados: `dry_run_show_rows=0`
  (`etl_framework/models/config.py:36-38`).
- Config rejeita `dry_run_show_rows > 0` quando `dry_run=False`
  (`etl_framework/models/config.py:133-136`) e o preflight reforca a regra
  (`etl_framework/contracts/pipeline.py:74-77`).
- Dry-run pula `_load` e `_certify`
  (`etl_framework/contracts/load.py:35-42`) e os testes de jornada confirmam
  ausencia de load/certify em dry-run (`tests/test_pipeline_author_journey.py:267-300`).
- Decorators nao materializam DataFrame por observabilidade
  (`etl_framework/infra/stage.py:35-39`; `tests/test_stage.py:176-190`).
- Diagnostico de invalidos fica agregado, sem linha de dado por padrao
  (`etl_framework/utils/auto_quality.py:81-101`).
- Mensagens de erro gerenciadas redigem tokens, senhas, payloads e paths em
  campos de erro (`etl_framework/utils/sanitization.py:5-29`;
  `tests/test_errors.py:133-181`).

## Recomendacoes objetivas

1. Replicar o aviso de `dry_run_show_rows > 0` em `docs/v0.1-contract.md` e
   `README.md`, no mesmo paragrafo em que o knob e explicado.
2. Trocar o evento `dry_run_sample_requested` para `level="warning"` ou documentar
   explicitamente que qualquer alerta operacional deve tratar esse evento como
   risco de exposicao.
3. Definir politica minima para `context.metrics`: apenas contagens, duracoes,
   flags e justificativas operacionais; nunca valores de linha, exemplos,
   payloads, identificadores pessoais ou segredos.
4. Sanitizar ou validar `target_path` antes de emitir eventos, pelo menos para
   credenciais em URI. Isso e pequeno e proporcional a v0.1.
5. Adicionar testes unitarios de observabilidade para metricas com chave sensivel
   e `target_path` com credencial em URI, alinhados a politica escolhida.
6. Manter diagnostico de invalidos agregado. Nao incluir `message` ou `rule` em
   excecoes/logs sem revisao, porque esses campos sao definidos pelo autor da
   pipeline.

## Conclusao

A v0.1 esta aceitavel como framework interno simples se a organizacao assumir
que `dry_run_show_rows`, metricas e metadados de destino sao canais de log e nao
areas de debug livre. O risco principal nao e vazamento automatico por padrao; e
mau uso previsivel de configuracoes e campos livres por autores junior.

Status final: aprovado com ressalvas P2. Antes de ampliar uso em ambientes
compartilhados, a biblioteca deve fortalecer documentacao e testes em torno de
metricas, `target_path` e `dry_run_show_rows`.
