# Padronização dos contratos `Extract`, `Transform` e `Load` — Template Method v0.1

Atue como GPT-5.5 com cognição extra-high.

Você está trabalhando no repositório `etl_framework`.

Sua tarefa é padronizar os contratos das etapas `Extract`, `Transform` e `Load` usando um padrão consistente de orquestração controlado pelo framework.

O objetivo é deixar claro que:

- o framework controla a ordem de execução;
- o framework controla logging, erro, contexto e validações obrigatórias;
- o desenvolvedor júnior implementa apenas a lógica específica da pipeline;
- controles críticos não dependem de chamada manual;
- os testes protegem regressão conforme o projeto crescer.

Não transforme a v0.1 em v1.
Não implemente arquitetura grande.
Não introduza dependências novas sem justificativa forte.
Não faça refatoração estética ampla.
Não altere o contrato público além do necessário para padronizar o fluxo.
Não esconda lacunas com documentação vaga.

---

# 1. Objetivo arquitetural

Padronize os contratos para este desenho:

```text
Extract.run()
  → _run_extract()
  → _run_check()
      → validate source_struct
      → optional custom check
  → return checked_df

Transform.run()
  → _run_transform()
  → _run_validate()
      → validate target_struct
      → optional custom validate
  → return valid_df

Load.run()
  → if dry_run: skip real load and produce dry-run evidence
  → _run_load()
  → _run_certify()
````

A intenção é aplicar o padrão Template Method:

```text
run()       = método público orquestrado pelo framework
_run_*()    = wrapper interno padronizado com erro/log/contexto
_*()        = hook implementado pela pipeline concreta
_custom_*() = hook opcional para regra específica da pipeline
```

---

# 2. Responsabilidades por etapa

## 2.1 Extract

`Extract.run()` deve:

1. chamar `_run_extract()`;
2. garantir que o retorno é um `DataFrame`;
3. chamar `_run_check()`;
4. aplicar validação estrutural obrigatória com `config.source_struct`;
5. executar check customizado opcional, se existir;
6. retornar `checked_df`.

Fluxo esperado:

```text
Extract.run()
  → _run_extract()
      → _extract()
      → require_dataframe(stage="extract")
      → erro gerenciado de extract
  → _run_check()
      → validação obrigatória de source_struct
      → _custom_check() opcional
      → erro gerenciado de check
  → return checked_df
```

A validação de `source_struct` deve pertencer ao framework.

O desenvolvedor júnior não deve precisar lembrar de chamar `check`.

A pipeline concreta pode adicionar regra específica, mas não deve conseguir remover silenciosamente a validação estrutural obrigatória.

Evite um desenho onde sobrescrever `_check()` substitui toda a proteção do framework.

Prefira separar:

```text
validação obrigatória do framework
+
hook opcional de customização
```

Exemplo conceitual:

```python
def _run_check(...):
    df = validate_struct(df, config.source_struct, ...)
    df = self._custom_check(df, spark, config, context)
    return require_dataframe(df, stage="check")
```

Se o projeto já possui nomes diferentes, preserve compatibilidade quando razoável, mas o comportamento final deve seguir esse contrato.

---

## 2.2 Transform

`Transform.run()` deve:

1. chamar `_run_transform()`;
2. garantir que o retorno é um `DataFrame`;
3. chamar `_run_validate()`;
4. aplicar validação estrutural obrigatória com `config.target_struct`;
5. executar validate customizado opcional, se existir;
6. retornar `valid_df`.

Fluxo esperado:

```text
Transform.run()
  → _run_transform()
      → _transform()
      → require_dataframe(stage="transform")
      → erro gerenciado de transform
  → _run_validate()
      → validação obrigatória de target_struct
      → _custom_validate() opcional
      → erro gerenciado de validate
  → return valid_df
```

A validação de `target_struct` deve pertencer ao framework.

O desenvolvedor júnior não deve precisar lembrar de chamar `validate`.

A pipeline concreta pode adicionar validação específica, mas não deve conseguir remover silenciosamente a validação estrutural obrigatória.

Evite um desenho onde sobrescrever `_validate()` substitui toda a proteção do framework.

---

## 2.3 Load

`Load.run()` deve:

1. garantir que a entrada é um `DataFrame`;
2. verificar `dry_run`;
3. se `dry_run=True`, não executar load real;
4. produzir evidência simples de dry-run;
5. se `dry_run=False`, executar `_run_load()`;
6. após load real, executar `_run_certify()`.

Fluxo esperado:

```text
Load.run()
  → require_dataframe(stage="load")
  → if dry_run:
        → _run_dry_run()
        → skip real load
        → do not call _run_load()
        → do not call _run_certify() as if load real happened
  → _run_load()
      → _load()
      → erro gerenciado de load
  → _run_certify()
      → _certify()
      → erro gerenciado de certify
```

Regra crítica:

```text
dry_run nunca pode executar efeito colateral real.
```

`certify` não deve fingir certificação real quando `dry_run=True`.

Se existir evidência de dry-run, ela deve ser nomeada claramente como dry-run evidence, dry-run summary ou equivalente.

---

# 3. Regras de erro, logging e contexto

Preserve ou implemente padronização por etapa.

Cada wrapper `_run_*()` deve garantir, quando já fizer parte do contrato atual:

* nome da pipeline;
* `run_id`;
* stage;
* erro gerenciado específico;
* propagação correta da exceção original;
* logging padronizado;
* ausência de exposição indevida de dados.

Use os erros existentes do projeto, por exemplo:

```text
ExtractError
CheckError
TransformError
ValidateError
LoadError
CertifyError
```

Se algum erro específico ainda não existir, não crie hierarquia complexa sem necessidade.

Prefira a solução mínima coerente com o padrão atual do repositório.

---

# 4. Restrições de design

Não permita que o usuário sobrescreva facilmente validações obrigatórias do framework.

Evite este desenho:

```text
_check() sobrescrito pelo usuário substitui tudo
_validate() sobrescrito pelo usuário substitui tudo
```

Prefira este desenho:

```text
_run_check()
  → validação obrigatória do framework
  → _custom_check() opcional

_run_validate()
  → validação obrigatória do framework
  → _custom_validate() opcional
```

Os hooks opcionais devem ter implementação padrão segura.

Exemplo conceitual:

```python
def _custom_check(...):
    return df

def _custom_validate(...):
    return df
```

Eles devem ser simples, explícitos e fáceis de testar.

---

# 5. Compatibilidade

Antes de alterar código:

1. inspecione a estrutura atual;
2. identifique classes existentes;
3. identifique nomes já usados;
4. identifique testes existentes;
5. identifique documentação que descreve o contrato.

Não quebre compatibilidade desnecessariamente.

Se for necessário renomear ou alterar comportamento público, documente:

* motivo;
* impacto;
* arquivos afetados;
* testes que protegem a mudança.

---

# 6. Implementação test-first

Para cada mudança comportamental, implemente primeiro o teste que prova o contrato.

A ordem obrigatória é:

1. criar ou ajustar teste;
2. confirmar que o teste falha pelo motivo correto, quando aplicável;
3. implementar código mínimo;
4. executar teste específico;
5. executar suíte completa;
6. atualizar documentação.

Não avance sem testes de regressão para os comportamentos críticos.

---

# 7. Testes obrigatórios

Crie ou ajuste testes para cobrir, no mínimo, os cenários abaixo.

## 7.1 Extract

Crie testes com nomes similares a:

```python
def test_extract_run_should_execute_extract_then_source_struct_check_automatically(...):
    ...

def test_extract_run_should_return_checked_dataframe(...):
    ...

def test_extract_run_should_raise_managed_error_when_extract_returns_non_dataframe(...):
    ...

def test_extract_run_should_raise_managed_error_when_source_struct_validation_fails(...):
    ...

def test_extract_run_should_execute_optional_custom_check_after_source_struct_validation(...):
    ...

def test_extract_run_should_preserve_run_id_in_managed_errors(...):
    ...
```

Esses testes devem provar que:

* `_extract` é executado;
* retorno é validado como `DataFrame`;
* `source_struct` é aplicado automaticamente;
* custom check é opcional;
* erro de extract/check é gerenciado;
* `run_id` aparece no erro quando isso fizer parte do contrato;
* o júnior não precisa chamar check manualmente.

---

## 7.2 Transform

Crie testes com nomes similares a:

```python
def test_transform_run_should_execute_transform_then_target_struct_validation_automatically(...):
    ...

def test_transform_run_should_return_valid_dataframe(...):
    ...

def test_transform_run_should_raise_managed_error_when_transform_returns_non_dataframe(...):
    ...

def test_transform_run_should_raise_managed_error_when_target_struct_validation_fails(...):
    ...

def test_transform_run_should_execute_optional_custom_validate_after_target_struct_validation(...):
    ...

def test_transform_run_should_preserve_run_id_in_managed_errors(...):
    ...
```

Esses testes devem provar que:

* `_transform` é executado;
* retorno é validado como `DataFrame`;
* `target_struct` é aplicado automaticamente;
* custom validate é opcional;
* erro de transform/validate é gerenciado;
* `run_id` aparece no erro quando isso fizer parte do contrato;
* o júnior não precisa chamar validate manualmente.

---

## 7.3 Load

Crie testes com nomes similares a:

```python
def test_load_run_should_skip_real_load_when_dry_run_is_enabled(...):
    ...

def test_load_run_should_produce_dry_run_evidence_when_dry_run_is_enabled(...):
    ...

def test_load_run_should_execute_load_then_certify_when_dry_run_is_disabled(...):
    ...

def test_load_run_should_not_execute_certify_as_real_load_when_dry_run_is_enabled(...):
    ...

def test_load_run_should_raise_managed_error_when_load_fails(...):
    ...

def test_load_run_should_raise_managed_error_when_certify_fails(...):
    ...

def test_load_run_should_preserve_run_id_in_managed_errors(...):
    ...
```

Esses testes devem provar que:

* load real não executa em `dry_run`;
* certify real não executa como se load tivesse ocorrido em `dry_run`;
* load executa antes de certify;
* erros são gerenciados;
* `run_id` aparece nos erros quando isso fizer parte do contrato.

---

## 7.4 Integração mínima

Crie ou ajuste um teste de integração mínimo do fluxo principal.

Nome sugerido:

```python
def test_pipeline_run_should_execute_extract_check_transform_validate_and_load_in_order(...):
    ...
```

Esse teste deve provar a ordem:

```text
extract
check
transform
validate
load
certify
```

E também deve existir uma variação para `dry_run`:

```python
def test_pipeline_run_should_execute_until_validate_and_skip_real_load_in_dry_run(...):
    ...
```

Esse teste deve provar que:

```text
extract
check
transform
validate
dry_run_evidence
```

ocorre, mas:

```text
load real
certify real
```

não ocorre.

---

# 8. Padrão de legibilidade dos testes

Todos os testes novos ou alterados devem ser fáceis de ler.

Use estrutura clara:

```python
# Arrange
...

# Act
...

# Assert
...
```

Use DataFrames pequenos e explícitos.

Evite fixtures globais complexas quando o cenário puder ser entendido localmente.

Mocks só são permitidos para fronteiras externas.

Não use mock para substituir o comportamento central que o teste deveria provar.

Exemplos de uso inadequado:

```text
mockar source_struct validation e dizer que source_struct foi validado
mockar target_struct validation e dizer que target_struct foi validado
mockar dry_run e dizer que load foi bloqueado
mockar check inteiro e dizer que check é automático
mockar validate inteiro e dizer que validate é automático
```

Os asserts devem validar comportamento observável.

---

# 9. Documentação obrigatória

Atualize a documentação pública do contrato.

A documentação deve explicar claramente:

```text
Extract.run()
  → _run_extract()
  → _run_check()
      → validate source_struct
      → optional custom check
  → return checked_df

Transform.run()
  → _run_transform()
  → _run_validate()
      → validate target_struct
      → optional custom validate
  → return valid_df

Load.run()
  → if dry_run: skip real load and produce dry-run evidence
  → _run_load()
  → _run_certify()
```

Também documente:

* o que o framework automatiza;
* o que a pipeline concreta implementa;
* quais hooks são opcionais;
* o que acontece em `dry_run`;
* que `certify` não significa idempotência, rollback ou garantia produtiva completa;
* que a v0.1 não promete load seguro universal se isso ainda depende da implementação concreta.

Não escreva documentação aspiracional como se fosse realidade.

---

# 10. Critérios de aceite

A tarefa só pode ser considerada concluída se todos os critérios abaixo forem atendidos.

## Código

* `Extract.run()` segue o padrão definido.
* `Transform.run()` segue o padrão definido.
* `Load.run()` segue o padrão definido.
* `source_struct` é validado automaticamente em `Extract`.
* `target_struct` é validado automaticamente em `Transform`.
* `dry_run` impede load real.
* Hooks customizados não removem validações obrigatórias silenciosamente.
* Erros continuam gerenciados.
* `run_id` continua rastreável quando aplicável.
* O contrato público permanece simples.

## Testes

* Existem testes de regressão para `Extract`.
* Existem testes de regressão para `Transform`.
* Existem testes de regressão para `Load`.
* Existe teste de integração mínimo do fluxo completo.
* Existe teste específico para `dry_run`.
* Os testes são legíveis.
* Os testes usam DataFrames pequenos.
* Os testes não dependem de mocks excessivos.
* A suíte completa executa com sucesso.

## Documentação

* O padrão final está documentado.
* A documentação não promete mais do que o código entrega.
* As limitações da v0.1 estão claras.
* O comportamento de `dry_run` está claro.
* O significado limitado de `certify` está claro.

---

# 11. Comandos obrigatórios

Execute os comandos aplicáveis.

```bash
pytest
```

Se existir cobertura:

```bash
pytest --cov
```

Se existir formatação:

```bash
black .
isort .
```

Se existir pre-commit:

```bash
pre-commit run --all-files
```

Se algum comando não puder ser executado, registre:

* comando;
* motivo;
* impacto na confiança da entrega.

---

# 12. Relatório final

Ao final, produza um resumo objetivo com:

## 1. Arquivos alterados

Liste arquivos de código, testes e documentação.

## 2. Contrato final implementado

Mostre o fluxo final de:

```text
Extract
Transform
Load
```

## 3. Testes criados ou ajustados

Tabela:

| Teste | Comportamento protegido | Tipo | Protege regressão? |
| ----- | ----------------------- | ---- | ------------------ |

## 4. Riscos removidos

Tabela:

| Risco | Como foi removido | Teste que protege |
| ----- | ----------------- | ----------------- |

## 5. Riscos restantes

Tabela:

| Risco | Gravidade | Motivo para aceitar na v0.1 | Documentação |
| ----- | --------- | --------------------------- | ------------ |

## 6. Comandos executados

Informe comando e resultado.

## 7. Veredito

Escolha uma opção:

* PRONTO PARA REVISÃO HUMANA;
* BLOQUEADO POR TESTES;
* BLOQUEADO POR DOCUMENTAÇÃO;
* BLOQUEADO POR CONTRATO;
* BLOQUEADO POR AMBIENTE.

Não declare pronto se a suíte de testes não foi executada com sucesso, salvo se houver limitação explícita de ambiente claramente documentada.

---

# 13. Regra final

O objetivo não é deixar o código mais sofisticado.

O objetivo é deixar o contrato mais claro, mais seguro, mais testável e mais difícil de quebrar no futuro.

Se alguma mudança aumentar complexidade sem proteger contrato ou regressão, não faça.
