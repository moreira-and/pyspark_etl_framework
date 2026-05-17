# Auditoria severa de prontidão v0.1 — `etl_framework`

Atue como GPT-5.5 com cognição extra-high.

Sua tarefa é executar uma auditoria técnica severa, cética, reproduzível e baseada em evidências no repositório `etl_framework`.

Este prompt será mantido publicamente no repositório. Portanto:

- escreva de forma profissional, objetiva e auditável;
- não use linguagem emocional, teatral ou informal;
- não exponha segredos, credenciais, dados sensíveis ou informações internas não necessárias;
- fundamente toda conclusão em evidência verificável;
- trate a auditoria como artefato público de governança técnica.

Você pode e deve invocar subagentes especializados se isso aumentar a qualidade da auditoria.

Não implemente correções.
Não refatore código.
Não altere testes.
Não suavize problemas.
Não aprove nada por simpatia ao projeto.
Não aceite documentação como prova de comportamento.
Não aceite cobertura percentual como prova de confiabilidade.
Não aceite intenção arquitetural como implementação real.
Não aceite teste existente como válido sem verificar se ele prova comportamento crítico.

A auditoria deve responder uma pergunta central:

> O que o projeto vende para uma v0.1 está realmente aderente ao código, aos testes e à experiência real de uso por um desenvolvedor júnior?

---

# 1. Contexto esperado do projeto

O `etl_framework` pretende ser um framework PySpark simples para padronizar pipelines ETL e reduzir a carga cognitiva de desenvolvedores júnior.

O objetivo vendido é que o desenvolvedor foque principalmente em:

- `extract`;
- `transform`;
- `load`, quando aplicável ao contrato atual.

Enquanto o framework deve garantir ou automatizar, quando isso estiver documentado como promessa:

- tratamento de erro;
- logs;
- rastreabilidade por `run_id`;
- `check`;
- validações;
- `dry_run`;
- certificações ou resumo de execução;
- validação estrutural via `source_struct`;
- validação estrutural via `target_struct`.

Atenção para v0.1:

Se a versão atual for v0.1, valide se o projeto NÃO está prometendo indevidamente:

- load seguro;
- idempotência;
- rollback;
- certify real de destino;
- leitura pós-load;
- proteção completa contra duplicidade;
- proteção completa contra carga parcial;
- segurança produtiva ampla;
- maturidade de v1.

Se esses pontos estiverem documentados como realidade, mas não existirem no código e nos testes, classifique como:

> PROMESSA NÃO ADERENTE AO CÓDIGO E AOS TESTES

---

# 2. Princípio central da auditoria de testes

A auditoria dos testes é o eixo principal desta avaliação.

A suíte de testes só deve ser considerada confiável se ela provar que o framework continuará respeitando seus contratos conforme o projeto crescer.

Não basta verificar se os testes passam.

Você deve verificar se os testes:

- são fáceis de ler;
- são fáceis de manter;
- testam comportamento, não detalhe acidental de implementação;
- falham quando o contrato do framework é quebrado;
- protegem contra regressão real;
- reduzem risco operacional;
- conseguem orientar um desenvolvedor júnior sobre o comportamento esperado;
- cobrem caminho feliz, caminho triste e casos-limite críticos;
- evitam excesso de mocks que criam falsa confiança;
- usam dados pequenos, explícitos e compreensíveis;
- têm nomes descritivos;
- têm relação clara com requisitos do framework;
- validam efeitos observáveis, não apenas chamadas internas.

Classifique como grave qualquer teste que:

- passe mesmo se o comportamento crítico for removido;
- use mock para substituir justamente a parte que deveria ser validada;
- valide apenas que uma função foi chamada, sem validar o resultado comportamental;
- seja tão acoplado à implementação que impeça refatoração segura;
- seja ilegível para um desenvolvedor júnior;
- esconda o cenário de negócio por excesso de fixture, factory ou abstração;
- aumente cobertura sem aumentar confiança.

---

# 3. Subagentes recomendados

Use subagentes, se possível, com estes papéis.

## 3.1 Auditor Principal de Testes e Regressão

Responsável por auditar a suíte como barreira de regressão da v0.1.

Deve verificar:

- se existe matriz requisito → comportamento → teste;
- se cada promessa pública possui pelo menos um teste comportamental;
- se os testes cobrem caminho feliz;
- se os testes cobrem caminho triste;
- se os testes cobrem casos-limite;
- se os testes falham quando o contrato principal é quebrado;
- se há teste para regressões em fluxo completo;
- se há teste de integração mínimo do pipeline;
- se há teste de contrato do fluxo `extract → check → transform → validate → load`;
- se há teste garantindo que `check` é automático quando prometido;
- se há teste garantindo que `validate` é automático quando prometido;
- se há teste garantindo que o júnior não precisa chamar controles críticos manualmente;
- se há teste garantindo que `dry_run` não executa efeitos colaterais indevidos;
- se há teste garantindo que load real não executa em `dry_run`;
- se há teste para `source_struct`;
- se há teste para `target_struct`;
- se há teste para coluna faltante;
- se há teste para coluna extra;
- se há teste para schema drift;
- se há teste para `is_valid = NULL`;
- se há teste para erro gerenciado;
- se há teste para propagação de `run_id`;
- se há teste para logs;
- se há teste para decorator `@stage`, caso exista;
- se há teste para exceção em cada etapa crítica;
- se há teste que diferencia erro esperado de erro inesperado.

Deve classificar a suíte como:

- CONFIÁVEL;
- ACEITÁVEL PARA V0.1 CONTROLADA;
- FRÁGIL;
- ENGANOSA;
- INSUFICIENTE PARA V0.1.

## 3.2 Auditor de Legibilidade dos Testes

Responsável por avaliar se os testes são compreensíveis e sustentáveis.

Deve verificar:

- nomes de arquivos;
- nomes de classes;
- nomes de funções de teste;
- padrão Arrange / Act / Assert ou equivalente;
- clareza dos dados de entrada;
- clareza das expectativas;
- mensagens de assert;
- excesso de fixtures globais;
- excesso de parametrização;
- excesso de mocks;
- testes que exigem conhecimento interno alto;
- duplicação aceitável versus abstração excessiva;
- se um júnior consegue entender o comportamento esperado lendo o teste;
- se o teste documenta o contrato real do framework.

Classifique cada grupo de testes como:

- CLARO;
- ACEITÁVEL;
- CONFUSO;
- ACOPLADO DEMAIS;
- ILEGÍVEL PARA MANUTENÇÃO JÚNIOR.

## 3.3 Auditor de Contrato e Código

Responsável por verificar o que o framework realmente executa.

Deve verificar:

- fluxo real do pipeline;
- ordem real das etapas;
- onde `check` é chamado;
- onde `validate` é chamado;
- se `source_struct` é usado automaticamente;
- se `target_struct` é usado automaticamente;
- se `run_id` é criado, propagado e anexado a erros/logs;
- se erros são gerenciados por etapa;
- se logs são gerados por decorator ou por chamadas manuais;
- se o contrato público é simples;
- se há comportamento implícito perigoso;
- se há acoplamento desnecessário;
- se há complexidade incompatível com v0.1.

## 3.4 Auditor de Documentação e Branding

Responsável por comparar o que o projeto promete com o que o código e os testes entregam.

Deve verificar:

- README;
- docs de contrato;
- roadmap;
- ADRs;
- exemplos;
- comentários relevantes;
- documentação de limitações;
- documentação de uso por júnior;
- documentação de segurança operacional;
- documentação de testes.

Deve classificar cada promessa como:

- COMPROVADA PELO CÓDIGO E PELOS TESTES;
- COMPROVADA PELO CÓDIGO, MAS NÃO PELOS TESTES;
- PARCIALMENTE COMPROVADA;
- NÃO COMPROVADA;
- CONTRADITÓRIA;
- ASPIRACIONAL DISFARÇADA DE REALIDADE.

## 3.5 Auditor de Experiência do Desenvolvedor Júnior

Responsável por avaliar a carga cognitiva real.

Deve responder:

- o júnior consegue criar uma pipeline sem entender a arquitetura interna?
- o júnior precisa lembrar manualmente de chamar `check`?
- o júnior precisa lembrar manualmente de chamar `validate`?
- o júnior precisa implementar tratamento de erro?
- o júnior precisa implementar logging?
- o júnior precisa propagar `run_id` manualmente?
- o júnior precisa conhecer detalhes internos para debugar?
- o júnior pode errar silenciosamente?
- o framework guia o uso correto ou apenas documenta o uso correto?
- os testes ajudam o júnior a entender o contrato?

## 3.6 Auditor de Risco Operacional

Responsável por avaliar riscos de produção.

Deve verificar:

- risco de carga duplicada;
- risco de carga parcial;
- risco de schema drift silencioso;
- risco de exposição de dados em logs;
- risco de `df.show`, `count` ou `collect` automático;
- risco de `dry_run` executar efeitos colaterais;
- risco de falsa certificação;
- risco de debugging difícil;
- risco de custo operacional alto;
- risco de erro mascarado por teste frágil;
- risco de confiança indevida por cobertura numérica.

---

# 4. Regras de evidência

Toda conclusão relevante deve apontar evidência.

Use, sempre que possível:

- arquivo;
- classe;
- método;
- função;
- teste;
- fixture;
- comando executado;
- saída observada;
- trecho de documentação;
- comportamento observado.

Não use frases como:

- “parece que”;
- “provavelmente”;
- “deve estar”;
- “aparentemente está coberto”;
- “a intenção é”;
- “o desenho sugere”.

Se não encontrar evidência, escreva:

> NÃO COMPROVADO

Se encontrar evidência parcial, escreva:

> PARCIALMENTE COMPROVADO

Se a documentação prometer algo que o código não comprova, escreva:

> PROMESSA NÃO ADERENTE AO CÓDIGO

Se o código existir, mas os testes não provarem o comportamento, escreva:

> IMPLEMENTADO, MAS NÃO PROTEGIDO CONTRA REGRESSÃO

Se o teste existir, mas não testar o comportamento crítico, escreva:

> TESTE EXISTENTE NÃO PROVA O CONTRATO

---

# 5. Critérios de reprovação imediata

Classifique como P0 ou bloqueador se encontrar qualquer item abaixo:

- documentação vende segurança produtiva que o código não entrega;
- documentação vende segurança produtiva que os testes não comprovam;
- `check` ou `validate` dependem de chamada manual do júnior, quando o projeto vende automação;
- `source_struct` ou `target_struct` existem, mas não são aplicados automaticamente no fluxo padrão;
- `dry_run` pode executar load real;
- sucesso produtivo é registrado sem evidência real;
- logs expõem dados sensíveis;
- erro gerenciado não contém contexto mínimo;
- erro gerenciado não contém `run_id` quando rastreabilidade é promessa;
- testes só validam caminho feliz;
- testes mascaram comportamento real com mocks excessivos;
- testes passam mesmo quando controles críticos são removidos;
- não existe teste de integração mínimo do fluxo completo;
- não existe evidência de proteção contra regressão dos contratos principais;
- fluxo exige conhecimento interno alto para uso comum;
- o projeto se vende como v1 produtivo sem cobrir load seguro, idempotência ou certify real;
- a documentação pública é mais ambiciosa que o comportamento testado.

---

# 6. Escopo obrigatório da auditoria

## 6.1 Auditoria do que é vendido

Liste as principais promessas do projeto.

Para cada promessa, classifique:

| Promessa | Onde é vendida | Evidência no código | Evidência nos testes | Status |
|---|---|---|---|---|

Status permitido:

- COMPROVADA;
- IMPLEMENTADA, MAS NÃO PROTEGIDA;
- PARCIAL;
- NÃO COMPROVADA;
- CONTRADITÓRIA;
- ASPIRACIONAL;
- PROMESSA ACIMA DA V0.1.

## 6.2 Auditoria do fluxo real

Mapeie o fluxo real executado pelo framework.

Responda:

- qual é a ordem real das etapas?
- `check` é automático ou manual?
- `validate` é automático ou manual?
- `run_id` é propagado automaticamente?
- logging é automático via decorator?
- erros são gerenciados por etapa?
- `dry_run` altera corretamente o comportamento?
- load continua legado/manual?
- existe algum certify real ou apenas resumo/log?
- qual parte do fluxo é garantida pelo framework?
- qual parte ainda depende do desenvolvedor?

## 6.3 Auditoria da carga cognitiva do júnior

Avalie se o júnior realmente precisa focar apenas em:

- `extract`;
- `transform`;
- `load` simples/legado, se aplicável.

Ou se ainda precisa entender manualmente:

- schema validation;
- check;
- validate;
- logging;
- erro gerenciado;
- run_id;
- dry_run;
- idempotência;
- certify;
- detalhes internos do framework;
- ordem correta de execução;
- limitações de produção.

Classifique:

- BAIXA CARGA COGNITIVA;
- MÉDIA CARGA COGNITIVA;
- ALTA CARGA COGNITIVA;
- CARGA COGNITIVA INCOMPATÍVEL COM JÚNIOR.

## 6.4 Auditoria severa dos testes

Audite os testes com rigor máximo.

Não se impressione com percentual de cobertura.

Verifique:

- quais comportamentos críticos são testados;
- quais comportamentos críticos não são testados;
- quais testes realmente protegem contrato público;
- quais testes só protegem implementação interna;
- quais testes são redundantes;
- quais testes são frágeis;
- quais testes são difíceis de ler;
- quais testes mascaram risco operacional;
- se os testes falham quando o framework deixa de cumprir o contrato;
- se há caminho triste suficiente;
- se há teste para erro gerenciado;
- se há teste para logs;
- se há teste para decorator `@stage`, caso exista;
- se há teste para `source_struct`;
- se há teste para `target_struct`;
- se há teste para coluna extra;
- se há teste para coluna faltante;
- se há teste para `is_valid = NULL`;
- se há teste para `dry_run`;
- se há teste garantindo que load real não executa em `dry_run`;
- se há teste de integração mínimo do fluxo completo;
- se há teste que provaria regressão se `check` fosse removido;
- se há teste que provaria regressão se `validate` fosse removido;
- se há teste que provaria regressão se `run_id` não fosse propagado;
- se há teste que provaria regressão se logs deixassem de registrar stage;
- se há teste que provaria regressão se schema drift passasse silenciosamente.

Para cada área crítica, informe:

| Área crítica | Teste existente | O que o teste prova | O que o teste não prova | Risco de regressão | Status |
|---|---|---|---|---|---|

Status permitido:

- PROTEGIDO;
- PARCIALMENTE PROTEGIDO;
- TESTADO SUPERFICIALMENTE;
- NÃO PROTEGIDO;
- TESTE ENGANOSO.

Depois classifique a suíte:

- CONFIÁVEL;
- ACEITÁVEL PARA V0.1 CONTROLADA;
- FRÁGIL;
- ENGANOSA;
- INSUFICIENTE PARA V0.1.

## 6.5 Auditoria de legibilidade dos testes

Avalie a legibilidade da suíte.

Para cada grupo relevante de testes, verifique:

- o nome do teste comunica o comportamento esperado?
- o cenário é claro?
- os dados de entrada são pequenos e explícitos?
- o resultado esperado é fácil de entender?
- o teste usa asserts relevantes?
- há mensagens de erro úteis?
- há excesso de fixture?
- há excesso de mock?
- há setup obscuro?
- há acoplamento excessivo à implementação?
- um júnior conseguiria entender por que o teste existe?

Classifique:

| Arquivo ou grupo de testes | Clareza do cenário | Clareza do assert | Dependência de mocks/fixtures | Valor como documentação viva | Status |
|---|---|---|---|---|---|

Status permitido:

- EXCELENTE;
- BOM;
- ACEITÁVEL;
- CONFUSO;
- FRÁGIL;
- ILEGÍVEL.

## 6.6 Auditoria de logging e erros

Verifique:

- se logging é centralizado;
- se logging é implementado via decorator `@stage` ou equivalente;
- se há logs manuais espalhados;
- se logs têm `run_id`;
- se logs têm nome da pipeline;
- se logs têm stage;
- se logs têm duração;
- se falhas são logadas;
- se exceções são propagadas;
- se DataFrames são materializados indevidamente;
- se há risco de exposição de dados;
- se os testes protegem esses comportamentos.

## 6.7 Auditoria de simplicidade arquitetural

Avalie:

- se o framework está simples;
- se existem camadas demais;
- se existem classes sem valor claro;
- se há abstrações prematuras;
- se há acoplamento desnecessário;
- se o fluxo é fácil de seguir;
- se debugging é direto;
- se a estrutura é adequada para manutenção por júnior;
- se os testes facilitam ou dificultam refatoração segura.

Classifique:

- SIMPLES E SUSTENTÁVEL;
- ACEITÁVEL COM DÉBITOS;
- COMPLEXO DEMAIS;
- OVERENGINEERED.

## 6.8 Auditoria de prontidão v0.1

Dê um veredito:

- APROVADO PARA V0.1 CONTROLADA;
- APROVADO APENAS PARA PILOTO;
- NO-GO.

Não aprove como v0.1 controlada se:

- os testes não provam o contrato principal;
- não há teste de regressão para fluxo completo;
- `dry_run` não está protegido por teste;
- `check` e `validate` não estão protegidos por teste;
- documentação promete comportamento não testado;
- o júnior ainda precisa lembrar manualmente de controles críticos;
- há risco de efeito colateral real em execução supostamente segura.

Não aprove como v1 produtiva se:

- load seguro não existe;
- idempotência não existe;
- certify real não existe;
- proteção contra duplicidade depende do júnior;
- documentação promete mais do que entrega;
- testes não provam o contrato principal.

---

# 7. Comandos e reprodutibilidade

Antes do veredito, informe:

- quais comandos foram executados;
- quais comandos não puderam ser executados;
- qual foi o resultado observado;
- se a auditoria dependeu apenas de inspeção estática;
- se os testes foram executados;
- se algum teste falhou;
- se alguma falha foi ignorada.

Se não executar a suíte de testes, declare explicitamente:

> A suíte de testes não foi executada. O veredito sobre testes está limitado à inspeção estática.

Se executar a suíte, informe o comando exato, por exemplo:

```bash
pytest
pytest --cov
````

Não use resultado de cobertura como prova principal de qualidade.

---

# 8. Formato obrigatório da resposta

Responda exatamente nas seções abaixo.

## 1. Veredito final

Informe:

* decisão;
* nível de confiança;
* justificativa em até 10 linhas.

## 2. Resumo brutalmente honesto

Explique:

* o que está sólido;
* o que parece sólido, mas não está;
* o que está sendo vendido acima da evidência;
* qual o maior risco técnico;
* qual o maior risco operacional;
* qual o maior risco de regressão.

## 3. Promessas vs realidade

| Promessa | Onde é vendida | Evidência no código | Evidência nos testes | Status | Comentário |
| -------- | -------------- | ------------------- | -------------------- | ------ | ---------- |

## 4. Fluxo real encontrado

Mostre o fluxo real em texto ou diagrama simples.

Depois diga:

* o que é automático;
* o que ainda é manual;
* o que depende do júnior;
* o que depende de revisão sênior;
* o que está documentado, mas não comprovado.

## 5. Auditoria severa dos testes

| Área crítica | Teste existente | O que o teste prova | O que o teste não prova | Risco de regressão | Status |
| ------------ | --------------- | ------------------- | ----------------------- | ------------------ | ------ |

Depois classifique a suíte:

* CONFIÁVEL;
* ACEITÁVEL PARA V0.1 CONTROLADA;
* FRÁGIL;
* ENGANOSA;
* INSUFICIENTE PARA V0.1.

## 6. Legibilidade e manutenibilidade dos testes

| Arquivo ou grupo de testes | Clareza do cenário | Clareza do assert | Uso de mocks/fixtures | Valor como documentação viva | Status |
| -------------------------- | ------------------ | ----------------- | --------------------- | ---------------------------- | ------ |

Depois responda:

* os testes são fáceis de ler?
* os testes ajudam um júnior a entender o contrato?
* os testes protegem refatorações futuras?
* os testes são adequados como documentação viva?

## 7. Mapa requisito → teste

| Requisito ou promessa | Teste que protege | Tipo de teste | Protege regressão? | Status |
| --------------------- | ----------------- | ------------- | ------------------ | ------ |

Tipo de teste permitido:

* unitário comportamental;
* unitário estrutural;
* integração mínima;
* contrato;
* regressão;
* documentação;
* inexistente.

## 8. Carga cognitiva do júnior

| Responsabilidade | Está no framework? | Está protegida por teste? | Ainda depende do júnior? | Risco |
| ---------------- | ------------------ | ------------------------- | ------------------------ | ----- |

Depois classifique a carga cognitiva.

## 9. Logging, erros e rastreabilidade

| Item | Evidência no código | Evidência nos testes | Status | Risco |
| ---- | ------------------- | -------------------- | ------ | ----- |

## 10. Riscos críticos

| Risco | Gravidade | Evidência | Mitigação mínima |
| ----- | --------- | --------- | ---------------- |

Use gravidade:

* P0: bloqueia aprovação;
* P1: exige correção antes ou uso extremamente controlado;
* P2: backlog curto;
* P3: melhoria.

## 11. Documentação desalinhada

| Documento | Promessa problemática | Por que está desalinhada | Ação recomendada |
| --------- | --------------------- | ------------------------ | ---------------- |

## 12. O que deve ser corrigido antes de aprovar

Liste apenas itens realmente bloqueadores.

Separe em:

* bloqueadores de código;
* bloqueadores de teste;
* bloqueadores de documentação;
* bloqueadores de experiência do desenvolvedor.

## 13. O que pode virar débito técnico

Liste apenas itens aceitáveis para v0.1 controlada.

Não coloque como débito técnico algo que comprometa:

* regressão;
* segurança operacional;
* rastreabilidade;
* `dry_run`;
* contrato principal;
* documentação pública.

## 14. Perguntas que ainda precisam de resposta

Liste perguntas objetivas que impedem uma aprovação mais ampla.

## 15. Comandos executados e limitações da auditoria

Informe:

* comandos executados;
* arquivos principais inspecionados;
* limitações;
* pontos que exigem validação posterior.

## 16. Recomendação final

Escolha uma:

1. Aprovar para v0.1 controlada.
2. Aprovar apenas para piloto.
3. Não aprovar.

Explique sem linguagem política.

---

# 9. Regra final

Se a auditoria encontrar que o framework ainda depende de o júnior lembrar manualmente de executar controles críticos, a conclusão não pode ser “pronto para v1”.

Se a documentação vender garantias que o código e os testes não comprovam, classifique isso como risco de governança técnica.

Se os testes parecerem bons numericamente, mas não provarem os comportamentos críticos, classifique a suíte como frágil ou enganosa.

Se os testes forem difíceis de ler, excessivamente mockados ou incapazes de proteger regressão, classifique a suíte como insuficiente para sustentar crescimento seguro do projeto.

Audite como se uma falha de regressão pudesse virar incidente real em produção.