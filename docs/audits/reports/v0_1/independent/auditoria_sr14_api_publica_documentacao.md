# Auditoria SR14 - API Publica e Documentacao Ativa

Papel executado: especialista em API publica e documentacao.

Escopo respeitado: foram lidos o prompt SR14, as entradas obrigatorias e codigo/documentacao necessarios para validar exports, instalacao, comandos, links, exemplos e consistencia de nomes. Nao foram lidos relatorios de outras auditorias nem arquivos em `docs/audits/reports/v0_1/independent`.

## Veredito

A API raiz esta pequena e alinhada ao contrato v0.1, mas a documentacao ainda nao separa com rigor suficiente tres camadas que importam para usuarios junior e mantenedores:

- API cotidiana: `Pipeline`, `EtlRunConfig`, `EtlExecutionContext`, `Extract`, `Transform`, `Load`.
- API avancada/opcional: helpers exportados por `etl_framework.utils` e porta de observabilidade em `etl_framework.infra.observability`.
- detalhe interno: builders em `etl_framework.utils.stage_metadata`, `etl_framework.utils.observability_events`, decorators/runtime de stage e sinks concretos.

O maior problema de documentacao ativa e a identidade do pacote: `pyproject.toml` declara a distribuicao Poetry como `spark-etl-framework`, enquanto o README afirma que o "pacote publicado" e `etl_framework`. Isso mistura nome de distribuicao com modulo importavel e prejudica instalacao, suporte e publicacao.

## Matriz API / Documentacao

| Item | Codigo real | Documentacao ativa | Classificacao correta | Avaliacao |
| --- | --- | --- | --- | --- |
| API raiz | `etl_framework/__init__.py` exporta somente `Pipeline`, `EtlRunConfig`, `EtlExecutionContext`, `Extract`, `Transform`, `Load` (`etl_framework/__init__.py:1-15`). | `docs/v0.1-contract.md` lista exatamente esses seis simbolos como API raiz esperada (`docs/v0.1-contract.md:281-290`). `QUICK_START.md` importa exatamente esses simbolos (`QUICK_START.md:25`). | Cotidiana. | Conforme. |
| Contratos de etapa | `etl_framework/contracts/__init__.py` exporta `Extract`, `Transform`, `Load`, `Pipeline` (`etl_framework/contracts/__init__.py:1-10`). | O contrato documenta Template Method, metodos obrigatorios e hooks opcionais (`docs/v0.1-contract.md:31-83`). | Cotidiana quando importada pela raiz; subpacote e atalho tecnico. | Conforme, mas a doc deve preferir sempre import raiz para junior. |
| Modelos | `etl_framework/models/__init__.py` exporta `EtlRunConfig` e `EtlExecutionContext` (`etl_framework/models/__init__.py:1-6`). | A configuracao documenta campos principais e opcionais (`docs/v0.1-contract.md:87-110`). | Cotidiana quando importada pela raiz; subpacote e atalho tecnico. | Conforme. |
| Helpers `utils` | `etl_framework/utils/__init__.py` exporta 11 helpers de qualidade e validacao, incluindo `validate_schema` e `validate_struct` (`etl_framework/utils/__init__.py:1-26`). | A doc diz que helpers em `etl_framework.utils` sao opcionais (`docs/v0.1-contract.md:295-296`) e que o junior nao precisa chamar `validate_schema`/`validate_struct` manualmente (`README.md:52-54`). | Avancada/opcional. | Parcial: exports reais existem, mas falta matriz nominal e custo por helper na pagina de contrato. |
| Observabilidade | `ObservabilitySink`, `ObservabilityService`, sinks e funcoes `configure_*` existem em `etl_framework/infra/observability.py` (`etl_framework/infra/observability.py:21-37`, `etl_framework/infra/observability.py:259-317`). | O contrato diz que `ObservabilitySink` e porta avancada e que `ObservabilityService`/sinks concretos nao sao API cotidiana (`docs/v0.1-contract.md:273-295`). | Avancada para mantenedores; sinks concretos e service runtime sao detalhe interno salvo documentacao explicita. | Parcial: a classificacao existe, mas nao ha uma lista fechada do que pode ser importado por usuarios avancados. |
| Nome publicado vs importavel | `pyproject.toml` declara `name = "spark-etl-framework"` e inclui o pacote Python `etl_framework` (`pyproject.toml:2`, `pyproject.toml:8`). | README afirma: "O pacote publicado pelo projeto e `etl_framework`. O nome do repositorio e `spark-etl-framework`" (`README.md:10-11`). | Distribuicao: `spark-etl-framework`; modulo importavel: `etl_framework`. | Inconsistente. |
| Instalacao local | README, Quick Start e guia de testes usam `poetry install --with dev` (`README.md:128`, `QUICK_START.md:12`, `docs/development/testing.md:13`). | `pyproject.toml` define grupo dev e dependencias correspondentes (`pyproject.toml:14-19`). | Caminho suportado local/dev. | Conforme. |
| Comandos oficiais | README e guia de testes listam `black`, `isort` e `pytest --cov` (`README.md:134-136`, `docs/development/testing.md:26-28`). | `pyproject.toml` possui configuracoes para Black, isort, pytest e coverage (`pyproject.toml:22-49`). | Gate oficial de framework. | Conforme. |
| Links ativos | Links obrigatorios apontam para arquivos existentes: contrato, limitacoes, testing, checklist, benchmark, observability logging e LICENSE. | Links aparecem em README, Quick Start, contrato, limitacoes e testing (`README.md:144-158`, `QUICK_START.md:161-163`, `docs/v0.1-contract.md:237`, `docs/v0.1-contract.md:275`, `docs/v0.1-contract.md:299`). | Documentacao ativa. | Conforme em existencia; ha texto de link com caminho absoluto aparente em alguns documentos, mas destino relativo esta correto. |
| `Load` e colunas tecnicas | `Load._persistable_df` preserva colunas tecnicas por default e remove quando `keep_technical_columns=False` (`etl_framework/contracts/load.py:129-139`). | README, contrato, Quick Start e limitacoes explicam essa regra (`README.md:71-76`, `README.md:91-93`, `QUICK_START.md:151-152`, `docs/v0.1-contract.md:212-218`, `docs/v0.1-known-limitations.md:76-79`). | Cotidiana, porque afeta todo `Load` concreto. | Conforme. |
| Acoes Spark documentadas | `auto_validate_target` executa `limit(1).count()` no caminho feliz e `count()`/`collect()` no diagnostico de falha (`etl_framework/utils/auto_quality.py:62`, `etl_framework/utils/auto_quality.py:78`, `etl_framework/utils/auto_quality.py:84-90`). | README, contrato, limitacoes e testing documentam custo e comportamento (`README.md:84-89`, `docs/v0.1-contract.md:152-167`, `docs/v0.1-known-limitations.md:85-90`, `docs/development/testing.md:42-44`). | Cotidiana no efeito; implementacao interna no mecanismo. | Conforme. |

## Inconsistencias

### SR14-P1-01 - README confunde pacote publicado com modulo importavel

Evidencia:

- `pyproject.toml` declara a distribuicao como `spark-etl-framework` (`pyproject.toml:2`).
- O mesmo arquivo inclui o pacote Python importavel `etl_framework` (`pyproject.toml:8`).
- README afirma que "O pacote publicado pelo projeto e `etl_framework`" (`README.md:10-11`).

Impacto:

Um usuario junior pode tentar instalar, publicar, versionar ou procurar o artefato pelo nome errado. Para uma biblioteca interna, essa diferenca e critica: distribuicao, repositorio e modulo importavel precisam estar nomeados sem ambiguidade. A documentacao atual so fica correta se "pacote publicado" estiver sendo usado como sinonimo informal de modulo Python, mas isso contradiz a semantica de `pyproject.toml`.

Recomendacao:

Trocar o texto para algo equivalente a: distribuicao/projeto Poetry `spark-etl-framework`; modulo importavel `etl_framework`; repositorio `spark-etl-framework`. Se houver pacote publicado em registry interno com outro nome, alinhar `pyproject.toml` ou documentar explicitamente a divergencia.

### SR14-P1-02 - API avancada exportada em `etl_framework.utils` nao tem inventario contratual fechado

Evidencia:

- `etl_framework.utils` exporta helpers nominalmente publicos por `__all__`: `REQUIRED_OPERATIONAL_METRICS`, `assert_freshness_at_least`, `assert_no_invalid_records`, `assert_reconciled_by_key`, `assert_target_key_not_null`, `assert_target_key_unique`, `assert_volume_between`, `require_is_valid_column`, `require_operational_metrics`, `split_valid_invalid`, `validate_schema`, `validate_struct` (`etl_framework/utils/__init__.py:1-26`).
- O contrato apenas diz genericamente que helpers em `etl_framework.utils` sao opcionais e devem continuar explicitos sobre acoes Spark (`docs/v0.1-contract.md:295-296`).
- O README so cita `validate_schema` e `validate_struct` para dizer que o junior nao precisa chama-los manualmente (`README.md:52-54`).

Impacto:

Todo simbolo em `__all__` de um subpacote sem underscore tende a virar API publica de fato. A documentacao nao deixa claro quais helpers sao suportados para usuarios avancados, quais sao experimentais e quais tem custo Spark. Isso aumenta risco de dependencia acidental em helper que a v0.1 talvez nao queira estabilizar.

Recomendacao:

Adicionar no contrato uma tabela "API avancada opcional" com todos os exports de `etl_framework.utils`, finalidade, custo Spark conhecido e estabilidade. Se algum helper nao deve ser publico, remover do `__all__` ou movelo para modulo interno com nome privado.

### SR14-P2-01 - Observabilidade e classificada como avancada, mas a superficie importavel real e maior que a promessa

Evidencia:

- `etl_framework.infra.observability` contem `ObservabilitySink`, `ObservabilityService`, `NoOpObservabilitySink`, `StdoutObservabilitySink`, `get_observability_service`, `configure_observability_sink`, `reset_observability_sink` e `configure_observability_from_env` (`etl_framework/infra/observability.py:21-37`, `etl_framework/infra/observability.py:259-317`).
- O contrato diz que `ObservabilitySink` e porta avancada, mas que `ObservabilityService` e sinks concretos pertencem a infraestrutura interna ou extensao avancada para mantenedores (`docs/v0.1-contract.md:273-295`).

Impacto:

A frase atual mistura "infraestrutura interna" e "extensao avancada para mantenedores" sem fixar import suportado. Quem precisa plugar observabilidade nao sabe se deve importar `ObservabilitySink`, `configure_observability_sink`, ambos, ou nada. Quem nao deveria tocar nisso tambem consegue descobrir uma superficie ampla e sem aviso de estabilidade.

Recomendacao:

Documentar uma unica rota suportada para extensao avancada, por exemplo `from etl_framework.infra.observability import ObservabilitySink, configure_observability_sink`, e marcar explicitamente `ObservabilityService`, sinks concretos e `get_observability_service` como detalhe interno/runtime.

### SR14-P2-02 - Documentacao ativa lista detalhes internos pelo caminho de modulo

Evidencia:

- O contrato cita `etl_framework.utils.stage_metadata` e `etl_framework.utils.observability_events` nominalmente (`docs/v0.1-contract.md:277-279`).
- Esses nomes nao aparecem em `etl_framework.utils.__all__` (`etl_framework/utils/__init__.py:15-26`).

Impacto:

Ao citar caminhos internos em documento contratual, a documentacao torna esses modulos descobertos e aparentemente suportados. Isso conflita com o objetivo de API publica pequena e facilita imports indevidos por pipelines concretas.

Recomendacao:

Remover nomes de modulos internos do contrato principal ou movelos para uma secao "detalhes internos, nao importar em pipelines". O contrato deve descrever o comportamento observado, nao incentivar caminho de import interno.

### SR14-P2-03 - Caminho suportado para instalacao de usuario final nao esta documentado

Evidencia:

- Toda a documentacao ativa ensina instalacao local/dev com `poetry install --with dev` (`README.md:128`, `QUICK_START.md:12`, `docs/development/testing.md:13`).
- `pyproject.toml` define metadados de distribuicao e pacote (`pyproject.toml:2-8`), mas nao ha comando documentado para consumo como dependencia de outra pipeline.

Impacto:

Para desenvolvimento do framework, o caminho esta claro. Para usuario da biblioteca em uma pipeline concreta, a documentacao nao diz se deve usar path dependency Poetry, registry interno, Git tag, wheel local ou outro fluxo. Isso enfraquece "pacote publicado e claro para o usuario", uma pergunta obrigatoria da SR14.

Recomendacao:

Adicionar uma secao curta separando "desenvolver o framework" de "usar em uma pipeline". Mesmo que a v0.1 ainda nao tenha publicacao em registry, documentar o caminho suportado temporario.

## Lacunas

1. Falta tabela oficial com os seis exports de API cotidiana, import recomendado pela raiz e exemplos minimos por simbolo.
2. Falta inventario fechado da API avancada de `etl_framework.utils`, apesar de haver `__all__` publico.
3. Falta politica de estabilidade por camada: raiz, subpacotes `contracts`/`models`, `utils`, `infra`.
4. Falta diferenciar claramente distribuicao Poetry, repositorio e modulo importavel.
5. Falta caminho de consumo da biblioteca por pipelines concretas fora do desenvolvimento local do framework.
6. Falta aviso explicito de "nao importar" para modulos internos citados no contrato, como `stage_metadata` e `observability_events`.
7. Falta exemplo de configuracao de observabilidade avancada, ou decisao explicita de que ela nao e suportada para usuarios da v0.1.

## Respostas As Perguntas Obrigatorias

### A API raiz contem apenas o esperado?

Sim. A API raiz contem apenas `Pipeline`, `EtlRunConfig`, `EtlExecutionContext`, `Extract`, `Transform` e `Load` (`etl_framework/__init__.py:1-15`), exatamente como o contrato espera (`docs/v0.1-contract.md:281-290`).

### A documentacao ensina o caminho suportado?

Parcialmente. Para desenvolvimento local do framework, sim: `poetry install --with dev` e os gates oficiais estao consistentes (`README.md:128-136`, `docs/development/testing.md:13-28`). Para consumo como biblioteca por uma pipeline concreta, nao: a documentacao nao define o mecanismo suportado de dependencia/publicacao.

### Existem docs desatualizadas ou contraditorias?

Sim. A contradicao principal e o nome do pacote publicado: README diz `etl_framework`, mas `pyproject.toml` declara `spark-etl-framework` como nome de distribuicao (`README.md:10-11`, `pyproject.toml:2`).

### Os comandos oficiais batem com o projeto?

Sim. Os comandos oficiais batem com dependencias e configuracoes declaradas em `pyproject.toml` (`README.md:134-136`, `docs/development/testing.md:26-28`, `pyproject.toml:14-49`).

### O pacote publicado e claro para o usuario?

Nao. A distincao entre distribuicao `spark-etl-framework`, modulo importavel `etl_framework` e repositorio `spark-etl-framework` nao esta clara. Esse e o principal bloqueio de clareza externa da v0.1.

### Ha imports internos expostos indevidamente?

Na API raiz, nao. Fora da raiz, ha risco: `etl_framework.utils.__all__` cria API avancada de fato sem inventario contratual, e o contrato cita modulos internos por caminho nominal (`etl_framework/utils/__init__.py:1-26`, `docs/v0.1-contract.md:277-279`).

## Oportunidades Priorizadas

### P1

- Corrigir imediatamente a identidade do pacote no README: distribuicao/projeto Poetry `spark-etl-framework`, modulo importavel `etl_framework`, repositorio `spark-etl-framework`.
- Criar matriz contratual de API publica com tres camadas: cotidiana, avancada/opcional e interna.
- Inventariar todos os exports de `etl_framework.utils` ou remover exports que nao devem ser estabilizados.

### P2

- Documentar o caminho suportado para uma pipeline concreta consumir a biblioteca fora do workspace do framework.
- Definir rota unica de observabilidade avancada e marcar explicitamente o restante de `etl_framework.infra.observability` como runtime interno.
- Substituir referencias nominais a `stage_metadata` e `observability_events` por descricao de comportamento ou aviso "interno, nao importar".

### P3

- Padronizar textos de links para evitar aparencia de caminho duplicado quando o destino relativo e correto.
- Reforcar no Quick Start que o import recomendado para uso comum e sempre pela raiz `etl_framework`.
- Adicionar uma tabela curta no README com "o que junior importa" versus "o que mantenedor pode importar".

## Conclusao

A v0.1 esta bem posicionada para manter uma API raiz pequena. O codigo respeita essa promessa. O problema e que a documentacao ativa ainda deixa bordas publicas demais sem classificacao formal: `utils`, observabilidade e nomes internos aparecem como se fossem igualmente consumiveis. Antes de tratar a v0.1 como contrato publico interno, a documentacao precisa corrigir o nome da distribuicao e congelar explicitamente o que e cotidiano, avancado e interno.
