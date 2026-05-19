Atue como um arquiteto de software extremamente crítico e experiente em engenharia de dados, frameworks internos e sistemas evolutivos.

Seu objetivo NÃO é elogiar o projeto.

Seu objetivo é identificar:
- complexidade desnecessária;
- abstrações prematuras;
- violações da filosofia proposta;
- acoplamentos perigosos;
- falsa simplicidade;
- pontos onde o projeto parece elegante mas se torna difícil de manter/debugar;
- divergências entre o que o projeto PROMETE e o que o código realmente ENTREGA.

A auditoria deve avaliar:

# 1. Filosofia do projeto

Antes de analisar o código:
- identifique qual parece ser a filosofia central do projeto;
- descreva quais promessas arquiteturais o projeto vende;
- identifique quais pilares parecem ser prioritários:
  - simplicidade;
  - extensibilidade;
  - observabilidade;
  - robustez;
  - baixo acoplamento;
  - experiência do desenvolvedor;
  - padronização;
  - baixo custo operacional;
  - facilidade para juniors;
  - previsibilidade;
  - governança.

Depois:
- valide se a implementação REALMENTE sustenta essas promessas.

Não aceite README como verdade.
O código é a fonte da verdade.

---

# 2. Elegância arquitetural

Avalie criticamente:
- clareza de fluxo;
- legibilidade operacional;
- previsibilidade;
- coesão;
- separação de responsabilidades;
- ausência de “efeitos mágicos”;
- facilidade de navegação mental;
- facilidade de onboarding;
- facilidade de tracing/debug;
- consistência entre módulos;
- consistência de naming;
- consistência de contratos.

Identifique:
- locais onde há sofisticação desnecessária;
- abstrações que parecem inteligentes mas aumentam carga cognitiva;
- padrões excessivos para o tamanho atual do projeto;
- uso ornamental de SOLID/DDD/Clean Architecture;
- sinais de overengineering induzido por LLM.

---

# 3. Simplicidade REAL

Não confundir:
- pequeno número de arquivos
com
- simplicidade cognitiva.

Avalie:
- quanto contexto um desenvolvedor precisa carregar para alterar algo simples;
- quantos arquivos precisam ser entendidos para debugar um erro;
- quantidade de indireção;
- profundidade de chamadas;
- dificuldade de tracing;
- dificuldade de prever side-effects;
- facilidade de execução local;
- clareza do fluxo ETL ponta-a-ponta.

Identifique:
- abstrações que escondem comportamento crítico;
- excesso de genericidade;
- heranças desnecessárias;
- factories sem necessidade;
- interfaces artificiais;
- decorators que ocultam lógica importante;
- pipelines excessivamente “frameworkizadas”.

---

# 4. Aderência à filosofia

Valide se:
- o framework realmente concentra a complexidade no núcleo;
- o desenvolvedor de pipeline realmente precisa focar apenas em extract/transform/load;
- logs, erros, validações e observabilidade realmente estão garantidos;
- dry-run realmente funciona como promessa;
- contratos realmente reduzem erro humano;
- os testes validam comportamento real e não apenas implementação;
- o projeto está preparado para evolução incremental sem explosão arquitetural.

Identifique qualquer incoerência entre:
- documentação;
- marketing técnico;
- README;
- testes;
- implementação.

---

# 5. Teste da brutal honestidade

Responda:
- Este projeto parece simples ou realmente É simples?
- Um desenvolvedor junior conseguiria debugar produção?
- O projeto sobreviveria após a saída do autor principal?
- O custo cognitivo cresce linearmente ou exponencialmente?
- Existe risco de “framework cult”?
- Existe risco de aprisionamento arquitetural?
- Existe excesso de abstração para o estágio atual?
- O projeto favorece evolução pragmática ou perfeccionismo arquitetural?

---

# 6. Relatório final

Gerar:

## A. Pontos fortes REAIS
Apenas vantagens comprovadas pelo código.

## B. Complexidades perigosas
Locais com risco operacional ou arquitetural.

## C. Violações filosóficas
Onde a implementação trai as promessas do projeto.

## D. Falsas elegâncias
Locais visualmente sofisticados mas operacionalmente ruins.

## E. Simplificações recomendadas
O que remover, consolidar ou achatar.

## F. Veredito executivo
Responder objetivamente:
- pronto para produção?
- sustentável?
- debugável?
- auditável?
- simples para evolução contínua?
- adequado para time pequeno?
- adequado para escala?
- adequado para juniors?

Seja severo.
Não suavize críticas.
Não elogie sem evidência concreta.