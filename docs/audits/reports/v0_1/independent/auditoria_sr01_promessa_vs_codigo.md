# Auditoria SR01 - Promessa vs Codigo

Auditoria independente sobre aderencia entre promessa declarada da v0.1 e
implementacao real no repositorio local.

Escopo observado: `README.md`, `QUICK_START.md`, `docs/v0.1-contract.md`,
`docs/v0.1-known-limitations.md`, `etl_framework/` e `tests/`.

Restricao operacional: nao foram usados relatorios de outras auditorias.

## 1. Resumo executivo

A v0.1 entrega a maior parte do contrato central que declara: fluxo linear,
preflight antes da origem, injecao de `SparkSession`, `EtlRunConfig` e
`EtlExecutionContext`, `auto_check` com `source_struct`, `auto_validate` com
`target_struct`, bloqueio de invalidos antes do load, `dry_run` que pula escrita,
eventos best-effort e erros gerenciados por etapa.

O codigo esta mais forte quando a promessa e sobre coordenacao do fluxo e
validacao estrutural. Ha cobertura direta em testes para preflight, ordem dos
hooks, `is_valid`, bloqueio de invalidos, dry-run, propagacao de contexto,
eventos e politica de colunas extras.

A principal divergencia tecnica esta em `Load._load`: o contrato afirma que o
DataFrame entregue ao load inclui colunas tecnicas reservadas "como `is_valid`,
`etl_run_id` e timestamps tecnicos" (`docs/v0.1-contract.md:214-216`), mas a
implementacao automatica observada cria apenas `is_valid` em
`validate_struct` (`etl_framework/utils/validate_struct.py:200-213`). As demais
colunas aparecem como reservadas em configuracao, nao como colunas adicionadas
automaticamente (`etl_framework/models/config.py:51-57`). Isto e promessa acima
do codigo.

Nao foi encontrada evidencia de que as limitacoes declaradas escondam uma falha
do contrato central. As negativas sobre load seguro, idempotencia, rollback,
certificacao real, observabilidade externa e producao irrestrita sao coerentes
com a implementacao. O problema e de formulacao pontual: a documentacao mistura
"colunas tecnicas reservadas" com "colunas tecnicas efetivamente entregues".

Classificacao geral: parcialmente aderente, com contrato central entregue e uma
divergencia P1 de documentacao/implementacao sobre metadados tecnicos no load.

## 2. Matriz promessa vs codigo vs testes

| ID | Promessa declarada | Evidencia documental | Implementacao observada | Teste observado | Status | Justificativa |
| --- | --- | --- | --- | --- | --- | --- |
| P01 | Fluxo oficial `extract -> check -> transform -> validate -> load -> certify`. | `README.md:18`; `docs/v0.1-contract.md:20`; `docs/v0.1-known-limitations.md:11`. | `Pipeline.run()` chama `_preflight`, `extract`, `transform`, `load` (`etl_framework/contracts/pipeline.py:45-49`); `Extract.run`, `Transform.run` e `Load.run` encadeiam as subetapas (`extract.py:27-49`, `transform.py:26-35`, `load.py:27-50`). | `tests/test_pipeline_contract.py:382` valida eventos na ordem completa; `tests/test_stage_contract_template_method.py:305` valida load antes de certify. | entregue | A sequencia esta codificada em template methods e protegida por testes de eventos/ordem. |
| P02 | Injecao compartilhada de `SparkSession`, `EtlRunConfig` e `EtlExecutionContext`. | `README.md:24`; contrato de metodos em `docs/v0.1-contract.md:68-78`. | Construtor de `Pipeline` recebe spark/config/context e repassa para etapas (`pipeline.py:25-41`, `pipeline.py:85-105`). | `tests/test_pipeline_contract.py:666-757` verifica que as etapas compartilham as mesmas instancias. | entregue | A promessa e implementada por composicao simples e verificada ate certify. |
| P03 | Preflight antes de acessar a origem; sem `source_struct` ou `target_struct`, falha cedo com `PreflightError`. | `README.md:58-60`; `docs/v0.1-contract.md:108-110`; `docs/v0.1-known-limitations.md:95`. | `_preflight()` valida `source_struct`, `target_struct`, `target_key` e `dry_run_show_rows` antes de `extract()` (`pipeline.py:53-83`). | `tests/test_auto_contract.py:293-295` e `343-345`; `tests/test_pipeline_contract.py:334-375` verificam falha antes de extract. | entregue | A origem nao e chamada quando o contrato minimo do fluxo padrao falta. |
| P04 | `auto_check` da origem valida nomes, tipos e politica de colunas extras via `source_struct`, sem criar `is_valid` e sem acoes Spark. | `README.md:26`; `docs/v0.1-contract.md:124-133`; `docs/v0.1-known-limitations.md:12`. | `Extract._run_check()` chama `auto_check_source()` antes de `_custom_check` (`extract.py:67-83`); `auto_check_source()` chama `validate_schema()` (`auto_quality.py:15-31`); `validate_schema()` retorna o DF sem `is_valid` (`validate_struct.py:59-79`). | `tests/test_auto_contract.py:130-159` verifica reutilizacao e ausencia de `count`, `collect`, `show`, `toLocalIterator`; `tests/test_auto_contract.py:300-314` verifica falha por coluna ausente. | entregue | O comportamento corresponde ao contrato estrutural e evita materializacao no check. |
| P05 | `auto_validate` valida nomes, tipos e checks declarativos via `target_struct`, adiciona `is_valid`, trata `NULL` como invalido e bloqueia invalidos antes do load. | `README.md:27`, `78-88`; `docs/v0.1-contract.md:138-158`; `docs/v0.1-known-limitations.md:13-15`, `58-59`. | `Transform._run_validate()` chama `auto_validate_target()` (`transform.py:61-78`); `validate_struct()` cria `is_valid` (`validate_struct.py:18-55`, `200-213`); `_check_rule()` usa `coalesce(..., False)` (`validate_struct.py:265-267`); `_raise_with_validation_diagnostics()` bloqueia invalidos (`auto_quality.py:72-91`). | `tests/test_auto_contract.py:166-174`; `tests/test_auto_contract.py:177-233`; `tests/test_auto_contract.py:379-436`; `tests/test_validate_struct.py:128-248`. | entregue | A promessa de validacao declarativa e bloqueio antes do load esta implementada e testada em caminho feliz e falha. |
| P06 | `auto_validate` executa `limit(1).count()` no caminho de bloqueio e diagnostico adicional em falha, sem expor linhas por padrao. | `README.md:82-88`; `docs/v0.1-contract.md:151-158`; `docs/v0.1-known-limitations.md:85-87`. | `_raise_with_validation_diagnostics()` usa `invalid_df.limit(1).count()`, depois `invalid_df.count()` e summary de checks apenas se houver invalido (`auto_quality.py:76-91`). | `tests/test_auto_contract.py:177-233`; `tests/test_pipeline_contract.py:571-656` garante que nao ha `show`/`collect` automatico no modo normal. | entregue | O custo Spark declarado esta no codigo. A coleta acontece somente no summary de falha de checks (`auto_quality.py:94-100`), que e documentado como diagnostico de falha. |
| P07 | `severity="error"` participa de `is_valid`; `severity="warning"` nao invalida registro. | `docs/v0.1-contract.md:175-176`. | `_add_is_valid_column()` filtra `check["severity"] == "error"` (`validate_struct.py:207-213`). | `tests/test_validate_struct.py:128-172`; `tests/test_validate_struct.py:175-248`. | entregue | O comportamento de severidade e explicito e testado. |
| P08 | `dry_run=True` executa extract/check/transform/validate, aplica `dry_run_limit`, registra evidencia tecnica e pula `_load` e `_certify`; `show` so com `dry_run_show_rows > 0`. | `README.md:30`, `63-75`; `QUICK_START.md:113-123`, `138-140`; `docs/v0.1-contract.md:194-207`; `docs/v0.1-known-limitations.md:80-84`. | `Extract.run()` limita apos `_run_check()` (`extract.py:27-49`, `91-99`); `Load.run()` chama `_run_dry_run()` e retorna (`load.py:27-38`); `_run_dry_run()` so chama show se configurado (`load.py:82-114`). | `tests/test_auto_contract.py:443`; `tests/test_stage_contract_template_method.py:270-303`; `tests/test_dry_run.py:161-227`. | entregue | O dry-run reduz risco de escrita, nao de leitura, conforme documentado. |
| P09 | `Load.run()` valida DataFrame, entrega colunas tecnicas por default e permite remover com `keep_technical_columns=False`. | `README.md:71-75`, `91-93`; `docs/v0.1-contract.md:214-217`; `docs/v0.1-known-limitations.md:76-79`. | `_persistable_df()` valida DataFrame e, se configurado, remove colunas cujo nome aparece em `config.TECHNICAL_COLUMNS` (`load.py:129-140`). | `tests/test_stage_contract_template_method.py:305-352`; `tests/test_production_checks.py:87-194`. | parcial | O mecanismo existe para preservar/remover colunas tecnicas presentes, mas o framework so cria `is_valid`. A promessa documental de `etl_run_id` e timestamps chegando ao load nao tem implementacao automatica correspondente. |
| P10 | Erros gerenciados carregam `pipeline_name`, `run_id` e etapa. | `README.md:29`; `docs/v0.1-contract.md:258-266`; `docs/v0.1-known-limitations.md:18-19`. | Decorator `stage()` embrulha excecoes via `ensure_stage_error()` (`stage.py:31-85`, `errors.py:92-128`); `EtlError` formata pipeline/run/stage (`errors.py:6-43`). | `tests/test_errors.py:22-121`; `tests/test_auto_contract.py:394-436`; `tests/test_pipeline_contract.py:471-520`. | entregue | A rastreabilidade de falha por etapa esta implementada e testada. |
| P11 | Eventos/logs tecnicos best-effort por etapa via runtime, com `pipeline_name`, `run_id`, modo, destino, etapa, status, nivel, duracao e metricas quando presentes. | `README.md:28`; `docs/v0.1-contract.md:241-270`; `docs/v0.1-known-limitations.md:17`, `91-92`. | `stage()` emite started/succeeded/failed e summary (`stage.py:31-85`); `ObservabilityService.emit()` engole falha de sink (`observability.py:37-66`); payload base vem de `build_observability_event()` (`observability_events.py:15-45`). | `tests/test_pipeline_contract.py:382-520`; `tests/test_observability.py:71-167`, `219-231`. | entregue | O contrato best-effort esta alinhado ao codigo; a documentacao tambem limita entrega garantida. |
| P12 | API raiz pequena: `Pipeline`, `EtlRunConfig`, `EtlExecutionContext`, `Extract`, `Transform`, `Load`. | `docs/v0.1-contract.md:283-292`. | `etl_framework/__init__.py` exporta exatamente esses nomes em `__all__` (`etl_framework/__init__.py:1-15`). | `tests/test_package_import.py:24`. | entregue | A API de import raiz esta de acordo com o contrato. |
| P13 | `nullable=False` documenta intencao, mas nao bloqueia nulos sem check SQL explicito. | `QUICK_START.md:156-157`; `docs/v0.1-contract.md:179-186`. | Validacao de schema compara nomes/tipos; bloqueio de nulos depende de checks (`validate_struct.py:83-132`, `200-213`). | `tests/test_auto_contract.py:241-283`. | entregue | A limitacao e honesta e testada. |
| P14 | `extra_columns_policy` controla extras: `ignore`, `warn`, `fail`; `strict_schema=True` vira warning quando politica explicita permanece `ignore`. | `docs/v0.1-contract.md:112-121`, `188-192`. | Resolucao em `EtlRunConfig` (`config.py:108-124`) e em `validate_struct` (`validate_struct.py:135-154`). | `tests/test_config.py:99-118`; `tests/test_validate_struct.py:113-125`; `tests/test_auto_contract.py:314-340`. | entregue | O comportamento declarado esta codificado e testado. |
| P15 | A v0.1 nao entrega load seguro, estrategia generica de escrita, idempotencia, rollback, retry seguro, quarantine persistente, certificacao real do destino ou producao irrestrita. | `README.md:32-33`, `99-111`; `docs/v0.1-contract.md:222-236`, `303-315`; `docs/v0.1-known-limitations.md:24-41`, `99-110`. | `Load` e abstrato para `_load`; `_certify` default nao faz nada (`load.py:18-50`, `143-160`). Nao ha implementacao generica de staging/commit/rollback/idempotencia no pacote. | Evidencia indireta: testes validam que `_load` concreto e responsavel pelo destino (`tests/test_stage_contract_template_method.py:305-329`) e que dry-run pula `_load`/`_certify` (`tests/test_stage_contract_template_method.py:270-303`). | entregue | A ausencia e declarada como limite, nao prometida como feature. Nao ha mascaramento de falha central porque o contrato central nao promete escrita segura generica. |

## 3. Promessas sem evidencia suficiente

### PSE-01 - `etl_run_id` e timestamps tecnicos entregues ao `Load._load`

- Promessa: `docs/v0.1-contract.md:214-216` afirma que, por default, o
  DataFrame recebido por `Load._load` inclui colunas tecnicas reservadas como
  `is_valid`, `etl_run_id` e timestamps tecnicos.
- Codigo esperado: uma etapa automatica entre validate e load deveria adicionar
  `etl_run_id`, `etl_run_at` ou timestamps equivalentes.
- Codigo encontrado: `validate_struct` adiciona apenas `is_valid`
  (`etl_framework/utils/validate_struct.py:200-213`). `EtlRunConfig` lista
  `inserted_at`, `updated_at`, `etl_run_at`, `etl_run_id` e `is_valid` como
  reservadas (`etl_framework/models/config.py:51-57`), mas isso so serve para
  rejeitar/remover quando presentes, nao para criar as colunas.
- Teste encontrado: testes confirmam somente `is_valid` chegando ao load
  (`tests/test_stage_contract_template_method.py:321-322`;
  `tests/test_production_checks.py:87-194`).
- Status: parcial.
- Severidade: P1, porque a documentacao promete metadados de rastreabilidade no
  payload do load que a implementacao nao cria.

### PSE-02 - "certify" como evidencia simples apos carga

- Promessa: contrato de etapa declara `certify` como responsabilidade da
  pipeline concreta para produzir evidencia simples (`docs/v0.1-contract.md:55-64`).
- Codigo encontrado: `Load._certify()` default retorna `None`
  (`etl_framework/contracts/load.py:153-160`), e isso e documentado como
  opcional (`docs/v0.1-contract.md:219-221`).
- Teste encontrado: ha teste de chamada apos load e de pulo em dry-run
  (`tests/test_stage_contract_template_method.py:305-329`,
  `tests/test_stage_contract_template_method.py:270-303`).
- Status: entregue com limite explicito.
- Nota: nao e falha do framework, mas a palavra "certify" pode sugerir mais que
  evidencia simples se lida fora da secao de limites. A documentacao central
  mitiga isso ao negar certificacao real de destino.

## 4. Divergencias entre documentacao e implementacao

### D01 - Documentacao promete colunas tecnicas que o framework nao cria

- Documento: `docs/v0.1-contract.md:214-216`.
- Trecho curto: DataFrame do load inclui colunas tecnicas reservadas como
  `is_valid`, `etl_run_id` e timestamps tecnicos.
- Implementacao real: apenas `is_valid` e criado automaticamente por
  `validate_struct` (`etl_framework/utils/validate_struct.py:200-213`).
- Evidencia adicional: `TECHNICAL_COLUMNS` e uma lista de colunas reservadas
  (`etl_framework/models/config.py:51-57`) usada para rejeicao no
  `target_struct` e remocao opcional no load (`load.py:129-140`), nao para
  materializacao automatica.
- Impacto: um desenvolvedor pode assumir que `etl_run_id` e timestamp de run
  chegam ao destino por default. Isso afeta rastreabilidade operacional e
  reproducibilidade de cargas concretas.
- Classificacao: P1.

### D02 - README e Quick Start sao mais precisos que o contrato formal no ponto de colunas tecnicas

- `README.md:91-93` usa "como `is_valid`" e nao afirma explicitamente que
  `etl_run_id` e timestamps sao criados.
- `QUICK_START.md:76` e `QUICK_START.md:123` tambem so prometem `is_valid`.
- O contrato formal amplia a promessa em `docs/v0.1-contract.md:214-216`.
- Impacto: a fonte normativa mais forte e a que mais promete. Isso cria
  ambiguidade de contrato.
- Classificacao: P2.

### D03 - Limitacoes declaradas nao escondem falha central, mas dependem de leitura rigorosa

- Documentos declaram fora de escopo load seguro, idempotencia, rollback,
  transacao, quarantine persistente e certificacao real (`README.md:32-33`,
  `docs/v0.1-contract.md:303-315`, `docs/v0.1-known-limitations.md:24-41`).
- Codigo confirma isso: `Load` e abstrato e `_certify` default e no-op
  (`etl_framework/contracts/load.py:18-50`, `153-160`).
- Impacto: nao ha divergencia de implementacao, mas o framework continua
  perigoso se uma pipeline concreta usar `append`/`overwrite` sem checklist.
- Classificacao: P2, como risco de uso, nao como falha de promessa.

## 5. Oportunidades P1/P2/P3

### P1 - Corrigir contrato de colunas tecnicas entregues ao load

Escolher uma das duas opcoes e alinhar documentacao, codigo e testes:

1. Se a v0.1 deve entregar somente `is_valid`, alterar
   `docs/v0.1-contract.md:214-216` para dizer que `etl_run_id` e timestamps sao
   apenas nomes reservados, nao materializados automaticamente.
2. Se a v0.1 deve entregar `etl_run_id` e timestamps, implementar a
   materializacao automatica antes de `Load._load`, definir nomes e semantica
   exatos, e adicionar testes de carga e `keep_technical_columns=False`.

Critica: a opcao 1 e mais compativel com a filosofia v0.1 simples e com README/
Quick Start atuais. A opcao 2 aumenta o contrato operacional e deve ser tratada
como mudanca funcional.

### P2 - Separar explicitamente "reservado" de "gerado pelo framework"

Hoje `TECHNICAL_COLUMNS` mistura nomes que o framework reserva com o unico nome
que ele gera automaticamente (`is_valid`). Criar linguagem e talvez constantes
separadas reduziria ambiguidade:

- colunas geradas automaticamente na v0.1: `is_valid`;
- colunas reservadas/proibidas em `target_struct`: `inserted_at`, `updated_at`,
  `etl_run_at`, `etl_run_id`, `is_valid`;
- colunas que a pipeline concreta pode criar sob revisao: definir se permitido
  ou nao.

### P2 - Fortalecer teste negativo sobre ausencia de metadados tecnicos automaticos

Enquanto a decisao P1 nao for tomada, falta um teste que congele o contrato real:
o load recebe `is_valid` por default, mas nao recebe `etl_run_id`/timestamps a
menos que uma etapa concreta os crie. Sem esse teste, a divergencia documental
pode voltar.

### P3 - Incluir matriz de limites no contrato ou no README

As limitacoes existem, mas ficariam mais rastreaveis se fossem ligadas
diretamente a responsabilidades de codigo:

- "nao ha rollback" -> `Load._load` e abstrato;
- "nao ha certificacao real" -> `_certify` default no-op;
- "dry_run nao reduz custo de leitura" -> limite apos `_run_check`;
- "observabilidade e best-effort" -> sink falha sem quebrar ETL.

Isto ajudaria desenvolvedor junior a nao interpretar guard-rails como garantias
produtivas.

## Conclusao

A promessa central da v0.1 esta majoritariamente entregue. O framework realmente
padroniza o fluxo ETL PySpark e automatiza checks estruturais basicos sem
prometer plataforma completa de DataOps.

A falha relevante e contratual, nao arquitetural: `docs/v0.1-contract.md`
promete no load colunas tecnicas que o codigo nao materializa. Enquanto isso nao
for corrigido, a v0.1 nao deve ser descrita como fornecedora automatica de
`etl_run_id` ou timestamps tecnicos no DataFrame carregado.
