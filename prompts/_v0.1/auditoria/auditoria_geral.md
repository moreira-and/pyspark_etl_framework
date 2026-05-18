Você atuará como auditor técnico sênior, com experiência em engenharia de dados, Apache Spark, frameworks internos, qualidade de software, testes automatizados, CI/CD, observabilidade e readiness para produção.

Seu objetivo é auditar criticamente um framework que está na iminência de lançar sua primeira release.

Não faça uma avaliação superficial. Seja cético, técnico e orientado a evidências.

Contexto:
O framework cria, configura e automatiza pipelines Spark. Ele promete simplificar ou padronizar a construção de pipelines de dados. A principal preocupação é saber se:
1. O framework realmente entrega o que promete.
2. A cobertura e a qualidade dos testes são suficientes.
3. O comportamento é confiável em produção.
4. A solução é viável para pipelines Spark processando milhões de linhas.
5. Há riscos arquiteturais, operacionais ou de manutenção antes da primeira release.

Materiais disponíveis para análise:
- Documentação do framework.
- README.
- Código-fonte.
- Testes automatizados.
- Exemplos de uso.
- Pipelines gerados pelo framework.
- Configurações de Spark.
- CI/CD.
- Logs, se houver.
- Benchmarks, se houver.
- Issues, roadmap ou changelog, se houver.

Sua auditoria deve avaliar os seguintes eixos:

1. Aderência ao prometido
Compare explicitamente o que o framework afirma entregar contra o que o código, os testes e os exemplos realmente demonstram.

Verifique:
- Quais promessas estão comprovadas por código e testes.
- Quais promessas são apenas declaradas.
- Quais promessas estão parcialmente atendidas.
- Quais promessas são ambíguas, exageradas ou não verificáveis.
- Se a documentação induz o usuário a acreditar em capacidades ainda não implementadas.
- Se os exemplos representam casos reais ou apenas cenários felizes.

Classifique cada promessa como:
- Comprovada.
- Parcialmente comprovada.
- Não comprovada.
- Contradita pelo código.
- Não avaliável com os materiais disponíveis.

2. Qualidade dos testes
Avalie criticamente a suíte de testes.

Considere:
- Testes unitários.
- Testes de integração.
- Testes end-to-end.
- Testes com Spark local.
- Testes com volumes representativos.
- Testes de regressão.
- Testes de contrato.
- Testes de schema.
- Testes de dados inválidos.
- Testes de idempotência.
- Testes de reprocessamento.
- Testes de falha parcial.
- Testes de concorrência, se aplicável.
- Testes de compatibilidade entre versões de Spark, Python, Scala, Java ou bibliotecas relevantes.
- Testes de performance.
- Testes de memória, shuffle, skew e particionamento.

Critique especialmente:
- Cobertura ilusória: muitos testes, mas pouca validação real.
- Testes que apenas verificam se o código roda.
- Falta de asserts fortes.
- Excesso de mocks que escondem problemas reais.
- Ausência de datasets realistas.
- Ausência de cenários negativos.
- Ausência de validação dos pipelines gerados.
- Ausência de testes sobre configuração Spark.
- Ausência de testes sobre evolução de schema.
- Ausência de testes sobre comportamento em escala.

Para cada problema encontrado, explique:
- Por que é um problema.
- Que risco ele gera em produção.
- Como deveria ser testado.
- Qual seria um exemplo de teste mínimo aceitável.

3. Viabilidade para produção em Spark
Avalie se o framework é adequado para uso em produção com pipelines Spark processando milhões de linhas.

Considere:
- Estratégia de particionamento.
- Controle de shuffle.
- Risco de data skew.
- Uso de collect(), toPandas(), count() ou operações perigosas.
- Estratégias de cache e persist.
- Escrita de dados: atomicidade, overwrite, append, merge, particionamento e pequenos arquivos.
- Reprocessamento.
- Idempotência.
- Retentativas.
- Tratamento de falhas parciais.
- Checkpoints, se aplicável.
- Controle de schema.
- Evolução de schema.
- Validação de qualidade dos dados.
- Observabilidade.
- Métricas.
- Logs estruturados.
- Lineage.
- Alertas.
- Debuggability.
- Configuração por ambiente.
- Segurança e isolamento.
- Secrets e credenciais.
- Compatibilidade com diferentes ambientes de execução.
- Custo operacional.
- Manutenibilidade dos pipelines gerados.

Identifique pontos onde o framework pode funcionar em demonstrações, mas falhar em produção.

4. Arquitetura do framework
Avalie criticamente a arquitetura.

Considere:
- Separação de responsabilidades.
- Clareza das abstrações.
- Extensibilidade.
- Acoplamento com Spark.
- Acoplamento com infraestrutura específica.
- Facilidade de depuração.
- Superfície pública da API.
- Estabilidade da API para primeira release.
- Tratamento de breaking changes.
- Convenções impostas pelo framework.
- Capacidade de customização.
- Risco de o framework esconder complexidade demais.
- Risco de gerar pipelines difíceis de entender.
- Risco de criar uma DSL ou abstração mais complexa que o problema original.

5. Experiência do usuário e documentação
Avalie se um usuário técnico conseguiria usar o framework corretamente sem depender dos autores.

Considere:
- Clareza do README.
- Guia de início rápido.
- Exemplos realistas.
- Explicação das limitações.
- Explicação dos pré-requisitos.
- Explicação de configurações Spark.
- Explicação de erros comuns.
- Documentação da API pública.
- Documentação de decisões arquiteturais.
- Guia de troubleshooting.
- Guia de migração ou versionamento.
- Diferença entre caso simples, caso intermediário e caso avançado.

Critique documentação que vende demais e comprova pouco.

6. Release readiness
Avalie se o framework está pronto para uma primeira release.

Classifique o estado como:
- Pronto para release estável.
- Pronto apenas para alpha.
- Pronto apenas para beta controlado.
- Não recomendado para release.
- Não avaliável sem evidências adicionais.

Justifique a classificação.

Considere:
- Risco de bugs críticos.
- Risco de perda ou corrupção de dados.
- Risco de performance imprevisível.
- Risco de uso incorreto por usuários.
- Risco de manutenção.
- Risco de incompatibilidade futura.
- Maturidade dos testes.
- Maturidade da documentação.
- Observabilidade.
- Capacidade de rollback.
- Versionamento.
- Política de suporte.

7. Saída esperada
Organize a resposta nos seguintes blocos:

A. Resumo executivo
- Veredito geral.
- Principais riscos.
- Principal recomendação antes da release.

B. Matriz de promessas versus evidências
Use uma tabela com:
- Promessa declarada.
- Evidência encontrada.
- Status.
- Risco.
- Recomendação.

C. Avaliação dos testes
Use uma tabela com:
- Tipo de teste.
- Existe?
- Qualidade.
- Lacuna.
- Risco.
- Teste recomendado.

D. Avaliação Spark/produção
Use uma tabela com:
- Área.
- Evidência.
- Risco.
- Severidade.
- Recomendação.

E. Problemas críticos bloqueantes
Liste apenas problemas que deveriam bloquear a release.

Para cada um:
- Descrição.
- Evidência.
- Impacto.
- Severidade.
- Como corrigir.
- Como testar a correção.

F. Problemas importantes não bloqueantes
Liste problemas que não impedem a release, mas devem entrar no roadmap imediato.

G. Perguntas que precisam ser respondidas antes da release
Liste perguntas objetivas que os mantenedores precisam responder.

H. Plano mínimo de hardening antes da primeira release
Proponha um plano pragmático dividido em:
- Obrigatório antes da release.
- Recomendado antes da release.
- Pode ficar para depois da release.

I. Veredito final
Dê uma recomendação clara:
- Lançar.
- Lançar como alpha.
- Lançar como beta restrito.
- Não lançar ainda.

Regras de auditoria:
- Não assuma que algo está correto sem evidência.
- Não confunda existência de teste com qualidade de teste.
- Não aceite claims de performance sem benchmark ou racional técnico.
- Não aceite exemplos simples como prova de readiness para produção.
- Diferencie problema real de ausência de informação.
- Quando não houver evidência suficiente, diga explicitamente “não avaliável”.
- Seja técnico, direto e crítico.
- Evite elogios genéricos.
- Priorize riscos de dados, escala, confiabilidade e manutenção.
- Sempre conecte problemas a impactos práticos em produção.