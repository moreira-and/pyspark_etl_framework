# Execução final para entrega da v0.1 — `etl_framework`

Atue como GPT-5.5 com cognição extra-high.

Você está trabalhando no repositório `etl_framework`.

Este trabalho será executado em ciclo longo e pode utilizar subagentes especializados.  
Seu objetivo é transformar a auditoria severa da v0.1 em uma versão final, simples, testada, documentada e coerente com o que o projeto realmente entrega.

Este prompt é público, transparente e deve ser tratado como artefato de governança técnica do repositório.

---

# 1. Contexto obrigatório

Existe um prompt de auditoria severa salvo no repositório.

Antes de qualquer implementação, você deve localizar e executar a auditoria usando esse prompt como referência principal.

Prompt de auditoria a ser usado:

```text
docs\archive\dev-audit\V0_1\v0_1_severe_readiness_audit.md
````

A auditoria deve validar, entre outros pontos:

* aderência entre documentação, código e testes;
* confiabilidade extrema da suíte de testes;
* ausência de regressão conforme o projeto crescer;
* legibilidade dos testes;
* simplicidade para manutenção por desenvolvedor júnior;
* execução automática de controles críticos;
* `dry_run`;
* `run_id`;
* logging;
* erros gerenciados;
* `check`;
* `validate`;
* `source_struct`;
* `target_struct`;
* risco de promessa indevida para v0.1;
* risco de overengineering.

---

# 2. Missão central

Sua missão NÃO é apenas corrigir problemas pontuais.

Sua missão é entregar uma v0.1 tecnicamente honesta, controlada e sustentável.

A versão final deve garantir que:

1. O código entregue exatamente o que a documentação promete.
2. A documentação não prometa mais do que o código e os testes comprovam.
3. A suíte de testes funcione como barreira real de regressão.
4. Os testes sejam extremamente confiáveis, fáceis de ler e úteis como documentação viva.
5. O framework continue simples.
6. A carga cognitiva do desenvolvedor júnior fique concentrada no desenvolvimento da lógica de ETL, não na infraestrutura do framework.
7. O projeto possa crescer sem que regressões críticas passem silenciosamente.

---

# 3. Restrições absolutas

Não transforme a v0.1 em v1.

Não implemente funcionalidades grandes apenas para satisfazer ambição documental.

Não crie arquitetura complexa para parecer robusto.

Não crie abstrações prematuras.

Não esconda lacunas com documentação vaga.

Não use cobertura percentual como prova principal de qualidade.

Não use mocks para substituir justamente o comportamento que deveria ser testado.

Não aceite teste que passa, mas não prova contrato.

Não aceite documentação aspiracional escrita como se fosse realidade.

Não aceite comportamento crítico dependente de memória do desenvolvedor júnior.

Não aceite `dry_run` que possa executar efeito colateral real.

Não aceite `check` ou `validate` como promessa automática se dependem de chamada manual.

Não aceite sucesso produtivo, certificação ou resumo de execução como prova real de load seguro, idempotência ou rollback.

---

# 4. Definição de sucesso para v0.1

A v0.1 só pode ser considerada pronta se atender aos critérios abaixo.

## 4.1 Código

O código deve:

* ter fluxo claro;
* ter contrato público simples;
* ter baixo acoplamento;
* ser fácil de debugar;
* evitar comportamento implícito perigoso;
* evitar materialização desnecessária de DataFrames;
* evitar exposição de dados sensíveis;
* propagar `run_id` quando rastreabilidade for promessa;
* tratar erros por etapa quando isso fizer parte do contrato;
* garantir que `dry_run` não execute efeitos colaterais reais;
* aplicar automaticamente controles críticos quando isso for promessa pública.

## 4.2 Testes

Os testes devem:

* provar comportamento, não intenção;
* proteger contra regressão;
* cobrir caminho feliz;
* cobrir caminho triste;
* cobrir casos-limite críticos;
* usar DataFrames pequenos e explícitos;
* ter nomes claros;
* seguir Arrange / Act / Assert ou estrutura equivalente;
* ser legíveis para um desenvolvedor júnior;
* evitar mocks excessivos;
* validar efeitos observáveis;
* validar exceções gerenciadas;
* validar `run_id`, quando aplicável;
* validar logs, quando aplicável;
* validar `dry_run`;
* validar que load real não ocorre em `dry_run`;
* validar `source_struct`;
* validar `target_struct`;
* validar colunas faltantes;
* validar colunas extras;
* validar schema drift;
* validar `is_valid = NULL`, se fizer parte do contrato;
* validar que `check` e `validate` são executados automaticamente quando isso for promessa do framework.

## 4.3 Documentação

A documentação deve:

* vender apenas o que está implementado e testado;
* separar claramente realidade atual, limitação conhecida e roadmap;
* declarar limites da v0.1;
* não prometer v1;
* não prometer idempotência se não houver implementação e teste;
* não prometer rollback se não houver implementação e teste;
* não prometer certify real se existir apenas resumo/log;
* não prometer load seguro se o load ainda depende do desenvolvedor;
* explicar a experiência esperada para um desenvolvedor júnior;
* deixar claro o que o framework automatiza e o que ainda depende do usuário.

---

# 5. Equipe de subagentes obrigatória

Use subagentes especializados, se o ambiente permitir.

Cada subagente deve trabalhar de forma independente, registrar evidências e entregar recomendações objetivas.

## 5.1 Tech Lead da v0.1

Responsável por:

* orquestrar o trabalho;
* consolidar a auditoria;
* transformar achados em backlog;
* decidir escopo mínimo da v0.1;
* impedir overengineering;
* impedir promessas acima da entrega;
* revisar o resultado final.

## 5.2 QA Architect / Test Reliability Engineer

Responsável por:

* auditar todos os testes;
* identificar testes frágeis;
* identificar testes enganosos;
* propor testes de regressão;
* implementar ou orientar testes comportamentais;
* garantir legibilidade;
* garantir que cada promessa crítica tenha teste correspondente;
* criar matriz requisito → teste;
* rejeitar testes que aumentam cobertura sem aumentar confiança.

Este subagente tem poder de veto sobre a v0.1.

## 5.3 PySpark / Data Engineering Engineer

Responsável por:

* revisar fluxo real do pipeline;
* validar custo operacional;
* identificar ações perigosas como `show`, `collect`, `count` automático;
* validar comportamento de `dry_run`;
* validar uso de DataFrames pequenos nos testes;
* garantir que o framework não materialize dados indevidamente;
* revisar `check`, `validate`, `source_struct` e `target_struct`.

## 5.4 Framework / Software Architect

Responsável por:

* revisar contratos públicos;
* revisar acoplamento;
* revisar separação de responsabilidades;
* revisar uso de abstrações;
* revisar SOLID sem exagero;
* garantir simplicidade para v0.1;
* garantir que o framework seja entendível por desenvolvedor júnior.

## 5.5 Documentation / Governance Reviewer

Responsável por:

* revisar README;
* revisar docs;
* revisar exemplos;
* revisar roadmap;
* remover ou reclassificar promessas aspiracionais;
* separar claramente “entregue agora” de “futuro”;
* garantir transparência pública.

## 5.6 Final Reviewer / Release Gatekeeper

Responsável por:

* revisar todo o diff final;
* executar testes;
* validar documentação;
* validar se os P0/P1 foram resolvidos;
* validar se não foram introduzidas complexidades desnecessárias;
* emitir veredito final.

---

# 6. Ordem obrigatória de execução

Siga as fases abaixo exatamente nesta ordem.

---

## Fase 0 — Preparação segura

Antes de alterar qualquer arquivo:

1. Verifique o estado do repositório.

```bash
git status
```

2. Identifique branch atual.

```bash
git branch --show-current
```

3. Crie ou use uma branch específica para fechamento da v0.1.

Nome sugerido:

```text
release/v0.1-hardening
```

4. Liste estrutura principal do projeto.

5. Localize:

* código-fonte;
* testes;
* README;
* docs;
* prompt de auditoria severa;
* configuração de dependências;
* configuração de lint/test/pre-commit, se existir.

Não modifique nada antes de concluir essa fase.

---

## Fase 1 — Auditoria severa inicial

Execute a auditoria usando o prompt severo referenciado.

A auditoria deve produzir um relatório salvo em:

```text
docs/audits/reports/v0_1_readiness_audit_report.md
```

O relatório deve conter obrigatoriamente:

* veredito inicial;
* P0;
* P1;
* P2;
* P3;
* promessas vs realidade;
* fluxo real encontrado;
* lacunas de testes;
* testes frágeis;
* testes enganosos;
* documentação desalinhada;
* riscos operacionais;
* riscos de regressão;
* recomendação objetiva.

Não implemente correções durante essa fase.

---

## Fase 2 — Transformação da auditoria em backlog executável

A partir do relatório de auditoria, crie um backlog técnico salvo em:

```text
docs/audits/reports/v0_1_hardening_backlog.md
```

O backlog deve classificar cada item como:

* P0: bloqueia v0.1;
* P1: precisa corrigir antes da v0.1 ou documentar forte limitação;
* P2: backlog curto;
* P3: melhoria futura.

Para cada item, informe:

| ID | Prioridade | Problema | Evidência | Decisão | Arquivos prováveis | Teste obrigatório | Critério de aceite |
| -- | ---------- | -------- | --------- | ------- | ------------------ | ----------------- | ------------------ |

A coluna `Decisão` deve ser uma das opções:

* corrigir agora;
* testar agora;
* documentar limitação;
* remover promessa;
* manter como débito;
* rejeitar por escopo.

Regra crítica:

P0 não pode virar débito técnico.

P1 só pode virar débito técnico se:

* não violar contrato principal;
* não comprometer `dry_run`;
* não comprometer regressão;
* não comprometer rastreabilidade;
* não comprometer uso por júnior;
* estiver explicitamente documentado como limitação da v0.1.

---

## Fase 3 — Plano de implementação mínimo

Antes de alterar código, produza um plano salvo em:

```text
docs/audits/reports/v0_1_hardening_plan.md
```

O plano deve conter:

* objetivo da v0.1;
* escopo permitido;
* escopo proibido;
* lista de arquivos a alterar;
* lista de testes a criar ou ajustar;
* ordem de implementação;
* riscos;
* estratégia de rollback via Git;
* critérios de aceite final.

O plano deve priorizar:

1. testes de regressão;
2. correção de contratos críticos;
3. documentação honesta;
4. limpeza mínima;
5. simplicidade.

Não avance para implementação se o plano estiver genérico.

---

## Fase 4 — Implementação test-first

Para cada P0/P1 selecionado para correção:

1. Escreva ou ajuste primeiro o teste que expõe o problema.
2. Execute o teste e confirme que ele falha pelo motivo correto.
3. Corrija o código mínimo necessário.
4. Execute novamente o teste.
5. Execute a suíte completa.
6. Atualize documentação, se necessário.

Não faça refatoração ampla durante correção de bug.

Não misture mudança comportamental com limpeza estética.

Não altere API pública sem justificar.

Não introduza nova dependência sem justificativa forte.

Não corrija P2/P3 se isso ameaçar a estabilidade da entrega.

---

# 7. Padrão obrigatório para os testes

Todos os testes novos ou modificados devem seguir este padrão.

## 7.1 Nome

O nome do teste deve indicar comportamento e expectativa.

Use padrão semelhante a:

```python
def test_run_should_skip_load_when_dry_run_is_enabled(...):
    ...
```

Evite nomes vagos como:

```python
def test_pipeline(...):
    ...
```

## 7.2 Estrutura

Use Arrange / Act / Assert explícito ou equivalente.

```python
# Arrange
...

# Act
...

# Assert
...
```

## 7.3 Dados

Use dados pequenos, inline e legíveis.

Prefira:

```python
[
    {"id": 1, "name": "A", "is_valid": True},
    {"id": 2, "name": "B", "is_valid": False},
]
```

Evite fixtures globais complexas quando o cenário puder ser entendido localmente.

## 7.4 Mocks

Mocks são permitidos apenas para fronteiras externas.

Mocks não devem substituir o comportamento central que o teste deveria provar.

Exemplos de mau uso:

* mockar `check` e concluir que `check` funciona;
* mockar `validate` e concluir que `validate` funciona;
* mockar `dry_run` e concluir que `dry_run` protege load;
* mockar logging inteiro e concluir que logs têm `run_id`.

## 7.5 Asserts

Asserts devem validar efeitos observáveis.

Inclua, quando aplicável:

* exceção esperada;
* mensagem da exceção;
* tipo de erro gerenciado;
* `run_id`;
* stage;
* nome da pipeline;
* ausência de chamada de load em `dry_run`;
* schema esperado;
* colunas extras detectadas;
* colunas faltantes detectadas;
* comportamento com `is_valid = NULL`.

## 7.6 Testes como documentação viva

Ao ler um teste, um desenvolvedor júnior deve conseguir responder:

* qual comportamento está sendo protegido?
* qual erro seria perigoso?
* qual regressão esse teste impede?
* o que o framework faz automaticamente?
* o que o usuário ainda precisa implementar?

Se a resposta não for clara, o teste deve ser reescrito.

---

# 8. Matriz obrigatória requisito → teste

Crie ou atualize o arquivo:

```text
docs/audits/reports/v0_1_requirement_test_matrix.md
```

Formato obrigatório:

| Requisito | Promessa pública? | Arquivo de teste | Nome do teste | Tipo | Protege regressão? | Status |
| --------- | ----------------- | ---------------- | ------------- | ---- | ------------------ | ------ |

Tipos permitidos:

* unitário comportamental;
* integração mínima;
* contrato;
* regressão;
* documentação;
* inexistente.

Status permitido:

* protegido;
* parcialmente protegido;
* não protegido;
* promessa removida;
* limitação documentada.

Nenhuma promessa crítica da v0.1 pode ficar sem teste ou sem limitação explícita.

---

# 9. Gates de reprovação

A entrega final deve ser bloqueada se qualquer condição abaixo permanecer verdadeira.

## 9.1 Gate de testes

Bloqueie a v0.1 se:

* a suíte não executa;
* há teste falhando;
* `dry_run` não tem teste de proteção contra load real;
* `check` automático é promessa, mas não tem teste;
* `validate` automático é promessa, mas não tem teste;
* `source_struct` é promessa, mas não tem teste;
* `target_struct` é promessa, mas não tem teste;
* erros gerenciados são promessa, mas não têm teste;
* `run_id` é promessa, mas não tem teste;
* testes são majoritariamente baseados em mocks internos;
* não existe teste de integração mínimo do fluxo principal;
* não existe matriz requisito → teste.

## 9.2 Gate de documentação

Bloqueie a v0.1 se:

* README promete mais do que o código entrega;
* docs prometem v1 disfarçada de v0.1;
* idempotência é prometida sem implementação e teste;
* rollback é prometido sem implementação e teste;
* certify real é prometido sem implementação e teste;
* load seguro é prometido sem implementação e teste;
* limitações conhecidas não estão documentadas.

## 9.3 Gate de arquitetura

Bloqueie a v0.1 se:

* o fluxo principal é difícil de seguir;
* a API pública exige conhecimento interno excessivo;
* controles críticos dependem de memória do júnior;
* erro crítico pode passar silenciosamente;
* comportamento perigoso está implícito;
* houve overengineering para resolver problema simples.

## 9.4 Gate operacional

Bloqueie a v0.1 se:

* há `show`, `collect` ou `count` automático sem justificativa;
* há risco de exposição de dados em logs;
* `dry_run` pode gerar efeito colateral real;
* sucesso é registrado sem evidência verificável;
* falhas não carregam contexto mínimo de diagnóstico.

---

# 10. Estratégia de documentação

Após corrigir código e testes, revise documentação.

A documentação deve separar claramente:

## Entregue na v0.1

Liste apenas o que está implementado e testado.

## Limitações conhecidas da v0.1

Liste explicitamente o que ainda não está garantido.

Exemplos:

* idempotência não garantida;
* rollback não implementado;
* certify real de destino não implementado;
* proteção contra duplicidade não garantida;
* load seguro depende da implementação concreta;
* validações limitadas ao contrato atual.

## Roadmap futuro

Liste o que pode entrar depois da v0.1.

Não escreva roadmap como se fosse entrega atual.

---

# 11. Execução de qualidade

Ao final de cada bloco de mudança, execute o conjunto aplicável:

```bash
pytest
```

Se existir configuração de cobertura:

```bash
pytest --cov
```

Se existir configuração de formatação:

```bash
black .
isort .
```

Se existir pre-commit:

```bash
pre-commit run --all-files
```

Se algum comando não existir ou falhar por ambiente, registre isso no relatório final.

Não esconda falhas.

Não diga que a entrega está pronta se a suíte não foi executada.

---

# 12. Relatório final obrigatório

Ao final, crie ou atualize:

```text
docs/audits/reports/v0_1_final_release_report.md
```

O relatório deve conter exatamente estas seções.

## 1. Decisão final

Escolha uma:

* APROVADO PARA V0.1 CONTROLADA;
* APROVADO APENAS PARA PILOTO;
* NO-GO.

## 2. Resumo executivo técnico

Explique em até 15 linhas:

* o que foi auditado;
* o que foi corrigido;
* o que foi testado;
* o que foi documentado;
* o que permanece como limitação.

## 3. Itens P0 encontrados

| ID | Problema | Evidência inicial | Ação tomada | Teste criado/ajustado | Status |
| -- | -------- | ----------------- | ----------- | --------------------- | ------ |

## 4. Itens P1 encontrados

| ID | Problema | Evidência inicial | Ação tomada | Teste criado/ajustado | Status |
| -- | -------- | ----------------- | ----------- | --------------------- | ------ |

## 5. Itens mantidos como débito técnico

| ID | Prioridade | Débito | Justificativa | Risco | Onde está documentado |
| -- | ---------- | ------ | ------------- | ----- | --------------------- |

## 6. Matriz final requisito → teste

Inclua ou referencie:

```text
docs/audits/reports/v0_1_requirement_test_matrix.md
```

## 7. Testes executados

Informe:

* comando;
* resultado;
* quantidade de testes;
* falhas;
* limitações;
* cobertura, se disponível.

Não use cobertura como argumento principal.

## 8. Documentação alterada

| Arquivo | Mudança | Motivo |
| ------- | ------- | ------ |

## 9. Riscos restantes

| Risco | Gravidade | Motivo para aceitar ou rejeitar | Mitigação |
| ----- | --------- | ------------------------------- | --------- |

## 10. Veredito dos subagentes

Cada subagente deve registrar:

* aprovação;
* reprovação;
* ressalvas;
* evidências principais.

Formato:

| Subagente | Veredito | Ressalvas | Evidências |
| --------- | -------- | --------- | ---------- |

## 11. Recomendação final

Explique objetivamente se o projeto pode ou não ser marcado como v0.1.

Não use linguagem política.

---

# 13. Política de commit sugerida

Ao final, sugira commits separados por intenção.

Exemplos:

```text
test(etl-framework): add regression coverage for v0.1 contracts
fix(etl-framework): harden dry-run and validation flow
docs(etl-framework): align v0.1 documentation with implemented guarantees
chore(etl-framework): add release audit reports
```

Não faça um único commit gigante se houver mudanças conceitualmente separáveis.

---

# 14. Resultado esperado

Ao final da execução, o repositório deve conter:

* auditoria inicial;
* backlog de hardening;
* plano de implementação;
* matriz requisito → teste;
* testes novos ou ajustados;
* código corrigido apenas onde necessário;
* documentação alinhada;
* relatório final de release;
* recomendação objetiva para v0.1.

---

# 15. Regra final

A v0.1 não deve ser aprovada porque “parece robusta”.

A v0.1 só deve ser aprovada se:

* o código cumprir o contrato declarado;
* os testes provarem os comportamentos críticos;
* os testes forem legíveis e sustentáveis;
* a documentação for honesta;
* os riscos restantes estiverem explicitamente documentados;
* a experiência do desenvolvedor júnior for compatível com a promessa do framework.

Se houver dúvida relevante, classifique como:

> APROVADO APENAS PARA PILOTO

ou

> NO-GO

Nunca force aprovação.