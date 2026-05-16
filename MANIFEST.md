# MANIFEST.md

## 1. Propósito

Este manifesto define o contrato arquitetural raiz do pacote `etl_framework`.

O framework existe para padronizar pipelines ETL em PySpark por meio de um fluxo
linear, explícito e previsível. O objetivo do núcleo é coordenar execução,
configuração, validação estrutural, logging, tratamento de erros gerenciados e
rastreabilidade mínima, sem assumir regras de negócio específicas de uma
pipeline concreta.

Este arquivo não é README, tutorial, guia de instalação ou documentação
comercial. Ele deve ser usado para avaliar se uma mudança proposta respeita a
arquitetura do framework.

## 2. Identidade Arquitetural

O pacote principal real é `etl_framework`.

A estrutura arquitetural atual está organizada em módulos pequenos:

- `etl_framework.contracts`: contratos de execução da pipeline.
- `etl_framework.models`: modelos de configuração e contexto de execução.
- `etl_framework.infra`: infraestrutura interna de logging e erros gerenciados.
- `etl_framework.utils`: utilitários de validação estrutural.

O framework é uma biblioteca interna para pipelines PySpark. Ele controla o ciclo
principal da execução, mas a lógica concreta de leitura, transformação, validação
de negócio mínima, escrita e certificação pertence às implementações concretas
dos contratos abstratos.

O núcleo deve permanecer compreensível para desenvolvedores juniores. Uma pessoa
deve conseguir entender a ordem da execução lendo `Pipeline`, `Extract`,
`Transform`, `Load`, `EtlRunConfig`, `EtlExecutionContext`, `EtlError`,
`log_event` e `validate_struct`, sem depender de comportamento implícito ou
frameworks externos.

## 3. Escopo Permitido

O framework pode:

- coordenar uma execução ETL linear em PySpark;
- padronizar o contrato entre extração, verificação, transformação, validação,
  carga e certificação;
- injetar explicitamente `SparkSession`, `EtlRunConfig` e
  `EtlExecutionContext` nas etapas concretas;
- validar configuração de execução antes da pipeline rodar;
- validar divergências estruturais entre `DataFrame` e `StructType`;
- executar checks declarativos simples definidos em metadados de `StructField`;
- registrar eventos técnicos de execução com `pipeline_name`, `run_id`, etapa,
  status, modo e duração;
- envolver exceções de etapas oficiais em erros gerenciados com contexto;
- oferecer `dry_run` para reduzir escrita, exposição de dados e custo de
  processamento após a extração;
- expor uma API pública pequena em `etl_framework.__init__`.

## 4. Escopo Proibido

O framework não deve:

- virar uma plataforma genérica de dados;
- assumir papel de orquestrador externo;
- substituir Airflow, Databricks Workflows, cron, schedulers ou ferramentas
  equivalentes;
- esconder a semântica essencial do Spark;
- substituir ferramentas completas de data quality;
- incorporar regras de negócio específicas de uma pipeline no núcleo;
- depender de configuração global espalhada por imports dentro das etapas;
- executar ações Spark desnecessárias no caminho normal de produção;
- gerar logs com dados sensíveis, amostras amplas ou payloads de registros;
- introduzir dependências externas de runtime além de PySpark;
- adicionar abstrações apenas para reorganizar código sem reduzir complexidade
  operacional real.

## 5. Contrato da Pipeline

O contrato oficial atual é:

```text
extract -> check -> transform -> validate -> load -> certify
```

`Pipeline.run()` controla a execução sequencial. A classe `Pipeline` chama os
métodos públicos `extract()`, `transform(df)` e `load(df)`. As etapas `check`,
`validate` e `certify` são executadas dentro dos contratos `Extract`,
`Transform` e `Load`.

Responsabilidades das etapas:

- `extract`: obter dados de origem por meio de `Extract._extract`.
- `check`: executar verificações preliminares antes da transformação por meio de
  `Extract._check`.
- `transform`: aplicar transformações de negócio por meio de
  `Transform._transform`.
- `validate`: validar a saída transformada por meio de `Transform._validate`.
- `load`: persistir o resultado por meio de `Load._load`, exceto quando
  `dry_run` estiver ativo.
- `certify`: produzir evidência final simples após carga bem-sucedida por meio
  de `Load._certify`.
- `run`: coordenar sequência, logging de execução, propagação de erro e retorno
  do `DataFrame` final.

Métodos obrigatórios para implementações concretas:

- `Extract._extract(spark, config, context) -> DataFrame`
- `Extract._check(df, spark, config, context) -> DataFrame`
- `Transform._transform(df, spark, config, context) -> DataFrame`
- `Transform._validate(df, spark, config, context) -> DataFrame`
- `Load._load(df, spark, config, context) -> None`
- `Load._certify(df, spark, config, context) -> None`

Cada etapa que declara retorno `DataFrame` deve retornar um
`pyspark.sql.DataFrame`. O utilitário interno `require_dataframe` formaliza esse
contrato e deve continuar gerando erro claro quando uma etapa retorna outro tipo.

Novas etapas só devem ser adicionadas quando houver justificativa arquitetural
forte, quando a ordem continuar fácil de ensinar e quando a mudança não tornar a
pipeline mais difícil de testar ou depurar. A adição de etapa não deve ser usada
para acomodar regra de negócio específica.

## 6. Princípios Obrigatórios

O fluxo principal deve ser explícito. A ordem das etapas deve estar visível no
código e nos logs.

Configuração deve ser injetada explicitamente. `EtlRunConfig` é o contrato de
configuração da execução e deve continuar sendo passado para as etapas em vez de
ser lido por constantes globais ou imports dispersos.

Contexto de execução deve ser pequeno. `EtlExecutionContext` deve representar
rastreabilidade, especialmente `run_id` e `started_at`, e não deve virar um
recipiente genérico de estado de pipeline.

Erros gerenciados devem preservar etapa, pipeline e `run_id` quando aplicável.
Exceções genéricas podem existir em implementações concretas, mas o framework
deve envolvê-las em erros específicos de etapa antes de expor a falha.

Validação estrutural deve proteger o contrato da pipeline contra drift silencioso
de schema. Ela não deve substituir validações completas de qualidade de dados.

Spark deve permanecer reconhecível. O usuário do framework deve continuar vendo
`SparkSession`, `DataFrame`, `StructType`, expressões SQL Spark e ações Spark
quando elas forem relevantes para custo ou semântica.

## 7. Regras de Simplicidade

Uma nova abstração só deve ser criada quando reduzir duplicação real, preservar o
contrato da pipeline e não exigir conhecimento avançado de Spark para uso básico.

Código do núcleo deve preferir funções pequenas, contratos explícitos e modelos
simples da standard library. O uso atual de `dataclass`, `abc`, `logging`,
`uuid`, `datetime`, `warnings` e tipos PySpark é compatível com essa regra.

O núcleo não deve criar DSLs, sistemas de plugin, registries dinâmicos ou
metaprogramação para casos que podem ser resolvidos por chamada direta de método.

Uma mudança que torna a execução mais conveniente, mas dificulta rastrear qual
etapa leu, alterou, validou ou persistiu dados, deve ser rejeitada.

Regras de negócio devem ficar nas classes concretas que implementam `Extract`,
`Transform` e `Load`. O núcleo só deve conter regras que sejam necessárias para o
contrato comum de todas as pipelines.

## 8. Regras de Extensão

Extensões devem respeitar a divisão atual de responsabilidade:

- `contracts` define ordem, assinaturas, retorno esperado, logging de etapas e
  wrapping de erros.
- `models` define dados mínimos de configuração e rastreabilidade.
- `infra` define mecanismos internos compartilhados, sem depender de serviços
  externos.
- `utils` contém funções auxiliares reutilizáveis, especialmente validação
  estrutural, sem assumir a ordem de execução da pipeline.

Novos campos em `EtlRunConfig` só devem ser aceitos quando forem necessários
para orientar comportamento comum do framework ou metadados de destino. Campos
específicos de uma pipeline concreta devem ficar fora do núcleo.

Novos dados em `EtlExecutionContext` só devem ser aceitos quando forem úteis para
rastreabilidade técnica de qualquer pipeline. O contexto não deve armazenar
`DataFrame`, parâmetros de regra de negócio ou resultados intermediários.

Novos erros gerenciados só devem ser adicionados quando corresponderem a uma
etapa oficial ou a uma responsabilidade comum do framework. Erros não devem
duplicar hierarquias específicas de uma pipeline.

Novos utilitários devem ser independentes, baratos de executar por padrão e
testáveis sem orquestração externa.

## 9. Dependências Permitidas

PySpark é a única dependência externa permitida no runtime de produção do
framework. O arquivo `pyproject.toml` declara `pyspark` como dependência de
produção e mantém ferramentas como `pytest`, `pytest-cov`, `black`, `isort`,
`commitizen` e `pre-commit` em dependências de desenvolvimento.

Bibliotecas da standard library podem ser usadas quando reduzirem complexidade
sem esconder comportamento relevante. Exemplos já usados no núcleo incluem
`dataclasses`, `abc`, `logging`, `time`, `datetime`, `uuid`, `warnings`, `re`,
`functools` e `typing`.

Dependências externas proibidas no runtime de produção:

- `pydantic`
- `pydantic-settings`
- `dynaconf`
- `hydra`
- `loguru`
- `pandera`
- `great-expectations`
- bibliotecas equivalentes que aumentem complexidade do núcleo, criem DSL
  paralela ou substituam contratos explícitos já existentes.

Uma nova dependência externa só pode ser aceita se for indispensável, se não
existir solução razoável com PySpark ou standard library e se houver justificativa
explícita documentando impacto em instalação, custo operacional, segurança,
testes e manutenção por desenvolvedores juniores.

Dependências transitivas de PySpark, como `py4j`, pertencem ao ecossistema de
Spark e não devem ser tratadas como ampliação deliberada do núcleo.

## 10. Validação Estrutural e Drift de Schema

Drift de schema não deve passar silenciosamente.

`EtlRunConfig` aceita `source_struct` e `target_struct` como `StructType`
opcionais. Esses schemas são validados por `SchemaMetadataValidator` na criação
da configuração. `target_struct` não pode declarar colunas técnicas reservadas em
`TECHNICAL_COLUMNS`, incluindo `inserted_at`, `updated_at`, `etl_run_at`,
`etl_run_id` e `is_valid`.

`validate_struct(df, schema, compute_summary=False, strict=False)` é o utilitário
central de validação estrutural atual. Ele deve manter as seguintes regras:

- `df` e `schema` não podem ser `None`;
- colunas obrigatórias ausentes devem gerar erro;
- tipos incompatíveis entre `DataFrame` e `StructType` devem gerar erro;
- colunas extras em modo `strict=True` devem gerar warning explícito;
- a coluna técnica `is_valid` não pode existir antes da validação estrutural;
- checks com severidade `error` determinam a coluna `is_valid`;
- checks com severidade `warning` não devem invalidar o registro;
- `compute_summary=False` deve continuar sendo o padrão, porque o resumo executa
  ações Spark;
- regras declarativas devem ser expressões SQL Spark simples em metadados de
  `StructField`.

Checks declarativos em metadados devem conter formato simples:

- `name`: string não vazia;
- `rule`: expressão SQL Spark não vazia;
- `severity`: `error` ou `warning`, com `warning` como comportamento padrão;
- `message`: string opcional.

Validações estruturais devem proteger o contrato mínimo da pipeline. Elas não
devem incorporar regras de negócio extensas, reconciliações entre sistemas,
deduplicação complexa, score de qualidade, profiling estatístico ou políticas de
observabilidade que pertençam a ferramentas externas.

Qualquer validação que exige ação Spark deve ser explícita no nome, parâmetro ou
documentação da função. O modo padrão deve evitar custo desnecessário.

## 11. Logging, Erros Gerenciados e Rastreabilidade

Logging é tratado por `etl_framework.infra.logger` usando `logging` da standard
library. O framework não deve introduzir `loguru` ou ferramenta equivalente no
runtime de produção.

`log_event` deve continuar emitindo eventos estruturados com, no mínimo:

- `event`;
- `pipeline_name`;
- `run_id`;
- `started_at`;
- `event_at`;
- `mode`;
- `stage`, quando aplicável;
- `status`, quando aplicável.

Logs devem permitir responder qual etapa iniciou, concluiu, falhou, foi limitada
ou foi ignorada. Logs não devem depender de ferramenta externa para serem úteis
em depuração local.

Logs não devem expor dados sensíveis. Mensagens de erro podem incluir contexto
técnico, mas não devem registrar linhas completas de dados, secrets, tokens,
credenciais, amostras amplas ou valores de colunas sensíveis.

Logs não devem gerar custo desnecessário. O caminho normal não deve executar
`count`, `collect`, `show`, profiling ou summaries sem solicitação explícita.

Erros gerenciados são definidos em `etl_framework.infra.errors`:

- `EtlError`
- `ExtractError`
- `CheckError`
- `TransformError`
- `ValidateError`
- `LoadError`
- `CertifyError`

`ensure_stage_error` deve continuar preservando erros gerenciados que já possuem
contexto e envolvendo exceções genéricas no erro específico da etapa. Sempre que
aplicável, o erro deve incluir `pipeline_name`, `stage`, `run_id` e `cause`.

Falhas não devem ser escondidas por logs. Depois de registrar o evento de falha,
o framework deve propagar a exceção para que o orquestrador externo, teste ou
chamador decida a política de retry, alerta ou interrupção.

## 12. Dry Run e Controle de Custo

`dry_run` existe para reduzir custo, exposição de dados e risco operacional.

O comportamento atual é configurado em `EtlRunConfig` por:

- `dry_run`;
- `dry_run_limit`;
- `dry_run_show_rows`.

Quando `dry_run=True`, `Pipeline._apply_dry_run_limit` aplica
`df.limit(dry_run_limit)` após o contrato de extração e verificação inicial. Em
seguida, a pipeline executa transformação e validação sobre o `DataFrame`
limitado. Na etapa de carga, `Pipeline.load` registra que a carga foi ignorada e
não chama `Load.run`, portanto `_load` e `_certify` não executam nesse modo.

`df.show()` automático só pode ocorrer quando `dry_run=True` e
`dry_run_show_rows > 0`. Fora de `dry_run`, o framework não deve chamar
`df.show()` automaticamente.

`dry_run` não deve alterar a semântica principal das transformações. Ele pode
reduzir volume, evitar escrita e permitir inspeção controlada, mas não deve
trocar regras de transformação, validação estrutural ou contrato de retorno.

Qualquer inspeção de dados deve ser explícita, limitada e rastreada em log. O
modo normal de produção deve evitar ações Spark desnecessárias.

Mudanças futuras em `dry_run` devem deixar claro em qual ponto da execução o
limite é aplicado. Alterar o ponto do limite pode mudar custo e semântica da
verificação inicial, portanto exige justificativa arquitetural e testes.

## 13. Critérios para Aceitar Novas Funcionalidades

Uma nova funcionalidade só deve ser aceita se:

- preservar o contrato oficial `extract -> check -> transform -> validate ->
  load -> certify`;
- manter `Pipeline.run()` como controlador explícito da execução;
- reduzir complexidade operacional real ou melhorar rastreabilidade, segurança,
  custo ou previsibilidade;
- não introduzir dependência externa desnecessária;
- continuar compreensível para desenvolvedores juniores;
- não esconder comportamento importante do Spark;
- não criar acoplamento desnecessário entre `contracts`, `models`, `infra` e
  `utils`;
- não transformar o framework em orquestrador;
- não transformar o framework em motor completo de data quality;
- ter testes proporcionais ao risco e ao contrato alterado;
- documentar qualquer ação Spark nova que possa gerar custo.

## 14. Critérios para Rejeitar Mudanças

Uma mudança deve ser rejeitada se:

- adiciona abstração apenas por organização estética;
- torna o fluxo da pipeline menos explícito;
- exige conhecimento avançado para uso básico;
- introduz dependência externa sem necessidade arquitetural;
- duplica responsabilidade de ferramentas externas de orquestração, catálogo,
  data quality ou observabilidade;
- mistura regras de negócio específicas com o núcleo do framework;
- cria comportamento implícito difícil de depurar;
- aumenta custo de execução sem ganho proporcional;
- dificulta manutenção por desenvolvedores juniores;
- transforma o framework em plataforma genérica;
- oculta `DataFrame`, `SparkSession`, `StructType` ou ações Spark quando esses
  elementos forem relevantes para custo e semântica;
- registra dados sensíveis ou executa inspeções amplas sem controle explícito.

## 15. Precedência Arquitetural

Quando houver conflito entre objetivos, a decisão deve seguir esta ordem:

1. Correção e previsibilidade do contrato.
2. Simplicidade de manutenção.
3. Baixo custo de execução.
4. Rastreabilidade e capacidade de depuração.
5. Extensibilidade controlada.
6. Conveniência de uso.
7. Sofisticação arquitetural.

Extensibilidade nunca deve justificar complexidade prematura. Uma solução menos
genérica e mais explícita deve ser preferida quando ela preservar o contrato,
reduzir custo de depuração e puder ser mantida por uma equipe júnior.

## 16. Responsabilidade de Manutenção

Toda alteração no núcleo deve ser avaliada contra este manifesto antes de ser
implementada.

Quem alterar `contracts` deve verificar se a ordem da pipeline, os métodos
obrigatórios, o wrapping de erros e os eventos de logging permanecem coerentes.

Quem alterar `models` deve verificar se `EtlRunConfig` continua pequeno,
imutável, explicitamente validado e livre de configuração específica de uma
pipeline. Também deve verificar se `EtlExecutionContext` continua restrito a
rastreabilidade técnica.

Quem alterar `infra` deve preservar o uso de standard library, a inclusão de
`run_id` quando aplicável e a propagação de exceções após logging.

Quem alterar `utils` deve preservar validação simples, explícita e barata por
padrão. Funções que executam ações Spark devem exigir opção explícita.

Quem adicionar exemplos, documentação ou testes deve refletir o comportamento
real do código. Não se deve documentar uma etapa, dependência ou modo de execução
como existente quando ele ainda for apenas uma diretriz futura.

Antes de aceitar uma mudança, a equipe deve conseguir responder objetivamente:

- a mudança preserva o contrato da pipeline?
- a mudança torna o framework mais simples de operar ou depurar?
- a mudança adiciona dependência ou custo?
- a mudança pertence ao núcleo ou à implementação concreta de uma pipeline?
- a mudança seria compreensível para um desenvolvedor júnior lendo o código?
- a mudança torna o Spark mais explícito ou mais escondido?
