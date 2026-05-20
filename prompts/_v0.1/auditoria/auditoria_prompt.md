Siga com a execução da bateria de auditorias planejada anteriormente.

## Estratégia de execução

Cada auditoria deve ser executada por um subagente independente e isolado.

Objetivo do isolamento:
- evitar contaminação de contexto;
- evitar viés entre auditorias;
- permitir avaliações críticas independentes;
- maximizar profundidade técnica em cada domínio.

## Regras obrigatórias

### 1. Isolamento entre auditorias

Cada subagente:
- NÃO deve conhecer os resultados das outras auditorias;
- NÃO deve reutilizar conclusões anteriores;
- NÃO deve assumir que outra auditoria validará determinado aspecto;
- deve atuar como se fosse o único responsável pela avaliação daquele domínio.

Exceção:
- auditorias `99_*` podem acessar os resultados das demais para consolidação e geração do relatório executivo final.

---

### 2. Especialização dos subagentes

Cada auditoria deve utilizar um subagente especializado no domínio avaliado.

Exemplos:
- arquitetura;
- SOLID;
- testes;
- observabilidade;
- performance;
- DX (developer experience);
- carga cognitiva;
- contratos;
- robustez operacional;
- documentação;
- packaging;
- rastreabilidade;
- pipeline ETL;
- qualidade de código.

O perfil do especialista deve ser coerente com a auditoria executada.

---

### 3. Profundidade da auditoria

As auditorias devem ser:
- severas;
- céticas;
- técnicas;
- orientadas a evidência;
- orientadas a produção;
- orientadas a manutenção de longo prazo.

O objetivo NÃO é aprovar o projeto.
O objetivo é identificar riscos reais, inconsistências, acoplamentos ocultos, complexidade desnecessária e divergências entre a filosofia prometida e a implementação real.

---

### 4. Critérios obrigatórios de avaliação

Cada auditoria deve analisar:

- aderência à filosofia do projeto;
- simplicidade arquitetural;
- baixo acoplamento;
- separação de responsabilidades;
- aderência ao SOLID;
- legibilidade;
- previsibilidade;
- debugabilidade;
- rastreabilidade;
- robustez operacional;
- experiência de manutenção por desenvolvedores júnior;
- redução de carga cognitiva;
- aderência entre documentação e código;
- risco operacional em produção;
- clareza dos contratos;
- risco de crescimento descontrolado de complexidade.

---

### 5. Evidência obrigatória

Toda conclusão deve conter:
- evidência concreta;
- trecho de código, fluxo ou comportamento observado;
- justificativa técnica;
- impacto real;
- recomendação objetiva.

Não aceite:
- opiniões genéricas;
- sugestões vagas;
- frases sem comprovação;
- “best practices” sem contextualização.

---

### 6. Estrutura obrigatória de saída por auditoria

Cada auditoria deve gerar:

# Nome da Auditoria

## Resumo Executivo

## Principais Riscos Identificados

## Oportunidades Identificadas

Para cada oportunidade:

- severidade: P1 / P2 / P3;
- contexto;
- evidência;
- impacto técnico;
- impacto operacional;
- impacto cognitivo;
- divergência da filosofia;
- comportamento atual;
- comportamento ideal;
- recomendação;
- risco de não corrigir.

## Conclusão Técnica

---

### 7. Consolidação final (`99_*`)

As auditorias `99_*` devem:

- consolidar os resultados;
- remover duplicidades;
- agrupar problemas relacionados;
- identificar padrões sistêmicos;
- identificar conflitos entre auditorias;
- priorizar ações;
- gerar um relatório executivo final orientado à release v0.1.

O consolidado final deve responder objetivamente (mais o proposto em [auditoria_sr99_consolidacao_executiva.md](prompts/_v0.1/auditoria/auditoria_sr99_consolidacao_executiva.md)): 

- O projeto está pronto para produção?
- O projeto está simples ou apenas organizado?
- A complexidade atual é sustentável?
- Um desenvolvedor júnior conseguiria manter o sistema?
- Os testes realmente protegem o sistema?
- A observabilidade é suficiente para debug real?
- O framework entrega o que promete?
- Existem sinais de sobreengenharia?
- Existem pontos de fragilidade estrutural?