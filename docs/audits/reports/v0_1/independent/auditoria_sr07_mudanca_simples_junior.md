# Auditoria SR07 - Mudanca Simples por Desenvolvedor Junior

## Escopo e metodo

Papel executado: especialista em DX junior e manutencao de mudancas simples.

Arquivos lidos conforme prompt: `README.md`, `QUICK_START.md`, `docs/v0.1-contract.md`, `etl_framework/`, `tests/fixtures/pipeline_author_journey/` e testes de jornada/validacao/dry-run relacionados. Nao foram lidos relatorios independentes existentes.

Validacao executada:

```text
poetry run pytest tests/test_pipeline_author_journey.py tests/test_validate_struct.py tests/test_dry_run.py
20 passed, 1 warning in 87.70s
warning: PytestCacheWarning: could not create cache path ...\.pytest_cache...\nodeids: [WinError 5] Acesso negado
```

## Veredito

O caminho feliz e relativamente facil quando o junior copia o `QUICK_START.md`: implementar `Extract._extract`, `Transform._transform`, `Load._load`, montar `EtlRunConfig` e chamar `Pipeline.run()`. A API publica e pequena (`Pipeline`, `EtlRunConfig`, `EtlExecutionContext`, `Extract`, `Transform`, `Load`) e o fluxo oficial esta claro.

Para manutencoes simples, porem, a autonomia ainda depende demais de ler contrato e testes. O framework reduz a ordem mental de ETL, mas desloca complexidade para convencoes declarativas em `StructType.metadata["checks"]`, politica global de colunas extras e colunas tecnicas persistidas por default. Isso e friccao criada pelo framework, nao dificuldade natural de PySpark.

## Matriz tarefa -> friccao -> evidencia

| Tarefa | Friccao para junior | Evidencia rastreavel |
| --- | --- | --- |
| Criar pipeline minima | Baixa no exemplo, media fora dele. O junior precisa declarar dois `StructType`, tres classes e um `EtlRunConfig` completo antes de rodar. | `QUICK_START.md:28`-`101` mostra o menor exemplo util. `Pipeline._preflight` falha antes do extract sem `source_struct` e `target_struct` em `etl_framework/contracts/pipeline.py:53`-`61`. Testes cobrem essa falha em `tests/test_pipeline_contract.py:322` e `tests/test_pipeline_contract.py:345`. |
| Adicionar regra simples no `target_struct` | Media. A regra e declarativa e curta, mas exige conhecer `metadata={"checks": [...]}`, severidade, SQL Spark e o detalhe de que `nullable=False` nao bloqueia nulos. | Exemplo de check em `QUICK_START.md:35`-`58`. Contrato explica que `nullable=False` nao bloqueia nulos em `docs/v0.1-contract.md:180`-`184`. Validacao adiciona `is_valid` em `etl_framework/utils/validate_struct.py:200`-`213`. Erro de regra SQL invalida inclui colunas disponiveis em `etl_framework/utils/validate_struct.py:226` e teste em `tests/test_validate_struct.py:387`. |
| Trocar politica de colunas extras | Media-baixa tecnicamente, media em descoberta. E uma flag simples, mas o default `ignore` aceita drift silencioso; `strict_schema=True` vira `warn`, nao `fail`. | Defaults em `etl_framework/models/config.py:40` e normalizacao `strict_schema` -> `warn` em `etl_framework/models/config.py:118`-`124`. Contrato documenta `ignore/warn/fail` em `docs/v0.1-contract.md:116`-`119` e default `ignore` em `docs/v0.1-contract.md:187`-`189`. Teste de bloqueio em `tests/test_validate_struct.py:116`. |
| Usar `dry_run` | Baixa para evitar escrita, media para custo. A flag funciona bem e pula `_load`/`_certify`, mas o limite so entra apos `_extract` e `auto_check`; isso surpreende quem espera amostra barata de origem. | `Extract.run` aplica limite apenas depois de `_run_check` em `etl_framework/contracts/extract.py:27`-`49` e `etl_framework/contracts/extract.py:91`-`99`. `Load.run` pula escrita em `etl_framework/contracts/load.py:27`-`41`. Contrato explicita custo em `docs/v0.1-contract.md:194`-`207`. Teste confirma input do transform limitado e load pulado em `tests/test_dry_run.py:1`. |
| Entender falha de schema | Media. Mensagens de coluna ausente/tipo/extra sao objetivas, e erros gerenciados carregam `pipeline_name`, `run_id` e `stage`. Falha por check invalido e boa. Falha por registros invalidos e menos acionavel porque nao mostra linhas nem valores, apenas contagem e checks. | Mensagens de schema em `etl_framework/utils/validate_struct.py:92`-`129`. Erro gerenciado em `etl_framework/infra/errors.py:14`-`46`. Diagnostico de invalidos em `etl_framework/utils/auto_quality.py:76`-`87`. Jornada cobre falha antes de transform/load em `tests/test_pipeline_author_journey.py:306`-`330`. |
| Evitar persistir colunas tecnicas no destino | Media-alta. O default persiste `is_valid`; para evitar isso o junior precisa descobrir `keep_technical_columns=False`. E uma convencao perigosa para destino real, apesar de documentada. | Default `keep_technical_columns=True` em `etl_framework/models/config.py:41`; filtro em `Load._persistable_df` em `etl_framework/contracts/load.py:132`. Teste prova o default com `is_valid` chegando ao load em `tests/test_pipeline_contract.py:656`-`661` e `tests/test_pipeline_contract.py:903`. Quick Start so menciona a mitigacao na lista final em `QUICK_START.md:151`-`152`. |

## Respostas obrigatorias

- O caminho feliz e facil de seguir? Sim, para quem copia o Quick Start. Fora dele, a primeira pipeline ainda exige entender quatro superficies: Spark, `StructType`, Template Method e `EtlRunConfig`.
- O junior precisa conhecer internals para fazer mudanca simples? Nao para rodar o exemplo; sim para diagnosticar efeitos de `is_valid`, ordem real do `dry_run`, diferenca entre `nullable=False` e checks SQL, e politica global de colunas extras.
- A documentacao aponta para o comportamento certo? Em geral sim. O ponto fraco e que comportamentos perigosos aparecem como notas: colunas tecnicas persistidas por default e `extra_columns_policy="ignore"`.
- Erros ajudam a corrigir o problema? Ajudam para schema e SQL invalido. Para registros invalidos, a mensagem e rastreavel mas pouco didatica: `Invalid records found during target validation: invalid_count=...; failed_checks=[...]`.
- Ha convencoes implicitas demais? Sim: checks ficam dentro de metadata Spark, `nullable=False` e apenas intencao, `strict_schema` nao e estrito, `dry_run` nao reduz custo de leitura, e `Load` recebe colunas tecnicas por default.
- A fixture de jornada ensina o uso real? Parcialmente. Ela ensina read-transform-load, dry-run e falha estrutural, mas nao ensina checks declarativos, troca de `extra_columns_policy` nem remocao de colunas tecnicas antes de destino real.

## Riscos de dependencia do autor principal

1. O junior tende a copiar a jornada de autor e acreditar que `nullable=False` protege nulos, porque `TARGET_STRUCT` da fixture nao tem checks em `tests/test_pipeline_author_journey.py:39`-`42`. O comportamento correto esta no contrato, nao no exemplo de jornada.
2. `strict_schema=True` parece semanticamente "estrito", mas no codigo vira warning quando a politica explicita continua `ignore`. Essa decisao exige conhecimento historico do framework.
3. A decisao de manter colunas tecnicas por default favorece diagnostico, mas aumenta risco de persistir `is_valid` por acidente em load real. Isso deveria ser decisao explicita de pipeline, nao detalhe lembrado no fim do Quick Start.
4. A fixture de jornada usa `inferSchema=True` em `tests/test_pipeline_author_journey.py:55`-`59`, o que facilita o exemplo, mas nao ensina um padrao robusto para evolucao controlada de schema.
5. A mensagem de invalidos evita expor dados, mas nao orienta o proximo passo para junior: qual regra alterar, como reproduzir localmente, como inspecionar amostra sem vazar dados.

## Oportunidades

### P1

- Tornar `keep_technical_columns=False` o padrao recomendado no Quick Start para qualquer exemplo que escreva destino, ou exigir que o `QuickLoad` selecione colunas de negocio. O estado atual ensina que `is_valid` chegar ao destino e normal.
- Adicionar cenario na fixture `pipeline_author_journey` com check declarativo real no `target_struct` e falha de registro invalido. Hoje a jornada nao cobre a manutencao mais comum: adicionar regra simples de qualidade.
- Renomear ou deprecar a leitura cotidiana de `strict_schema`; para junior, "strict" que apenas avisa e armadilha de manutencao.

### P2

- Criar uma secao curta "Receitas de mudanca simples" com snippets: adicionar check, bloquear coluna extra, dry-run seguro, dropar tecnicas. Hoje essas respostas estao espalhadas entre Quick Start, contrato e testes.
- Melhorar erro de invalidos com proxima acao segura, por exemplo indicar `failed_checks`, campo, regra e sugestao de rodar com amostra controlada. Nao precisa coletar linhas para ser mais didatico.
- Adicionar teste/documentacao que mostre explicitamente que `dry_run_limit` ocorre depois de extract/check, logo nao reduz custo de leitura.

### P3

- Incluir no README uma tabela "mudanca simples -> arquivo/flag" para reduzir busca: `target_struct.metadata["checks"]`, `extra_columns_policy`, `dry_run`, `keep_technical_columns`.
- Evitar `inferSchema=True` na fixture de jornada ou comentar por que ele foi escolhido apenas para fixture pequena.
- Corrigir friccao local de teste: o comando passou, mas o ambiente retornou warning de cache com `[WinError 5] Acesso negado` em `.pytest_cache`; isso polui a validacao de um junior.

## Distincao: PySpark vs framework

Dificuldade natural de PySpark: escrever transformacoes com `F.col`, casts, `when`, schemas `StructType`, regras SQL Spark e entender acoes como `count`.

Dificuldade criada pelo framework: obrigatoriedade de dois schemas antes do primeiro run, checks escondidos em metadata, `strict_schema` com semantica fraca, colunas tecnicas no load por default, e `dry_run` que protege escrita mas nao barateia leitura. Essas decisoes sao defensaveis tecnicamente, mas precisam de receitas mais diretas para um desenvolvedor junior manter mudancas pequenas sem consultar o autor principal.
