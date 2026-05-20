# Auditoria SR00 - Orquestrador das Auditorias de Responsabilidade Unica

## Papel do agente

Atue como coordenador da bateria de auditorias especializadas da v0.1 do
`etl_framework`. Sua funcao nao e auditar o codigo diretamente, mas organizar a
execucao por agentes especializados, garantir baixa sobreposicao e exigir
evidencias objetivas.

## Contexto da bateria

O projeto declara que a v0.1 deve padronizar pipelines ETL PySpark simples,
reduzir carga cognitiva para desenvolvedores junior, automatizar controles
estruturais basicos e manter limites claros sobre load seguro, observabilidade,
custo Spark e uso produtivo.

A bateria foi quebrada em auditorias de responsabilidade unica para evitar:

- auditorias genericas;
- conclusoes sem evidencia;
- duplicidade entre agentes;
- recomendacoes desalinhadas com o escopo v0.1;
- excesso de abstracao nas sugestoes.

## Ordem recomendada

Execute nesta ordem:

1. `auditoria_sr01_promessa_vs_codigo.md`
2. `auditoria_sr02_filosofia_real.md`
3. `auditoria_sr03_fluxo_etl_ponta_a_ponta.md`
4. `auditoria_sr04_protecoes_automaticas.md`
5. `auditoria_sr05_load_certify_v01.md`
6. `auditoria_sr06_simplicidade_tracing.md`
7. `auditoria_sr07_mudanca_simples_junior.md`
8. `auditoria_sr08_magia_indirecao.md`
9. `auditoria_sr09_abstracoes_sem_pressao.md`
10. `auditoria_sr10_solid_pragmatico.md`
11. `auditoria_sr11_erros_rastreabilidade.md`
12. `auditoria_sr12_observabilidade_operacional.md`
13. `auditoria_sr13_dx_jornada_junior.md`
14. `auditoria_sr14_api_publica_documentacao.md`
15. `auditoria_sr15_testes_promessa_v01.md`
16. `auditoria_sr16_qualidade_testes.md`
17. `auditoria_sr17_performance_custo_spark.md`
18. `auditoria_sr18_seguranca_dados_sensiveis.md`
19. `auditoria_sr99_consolidacao_executiva.md`

## Contrato comum para todas as auditorias

Cada auditoria deve avaliar, quando aplicavel:

- aderencia a filosofia do projeto;
- simplicidade arquitetural;
- SOLID pragmatico;
- codigo limpo;
- reducao de carga cognitiva;
- facilidade de manutencao;
- facilidade de desenvolvimento por junior;
- robustez operacional;
- rastreabilidade de falhas;
- aderencia entre promessa e codigo.

## Regras de evidencia

Toda conclusao deve citar pelo menos um dos itens:

- arquivo de codigo;
- classe ou metodo;
- teste;
- documento;
- cenario reproduzivel;
- lacuna de contexto explicitamente registrada.

Nao aceite recomendacoes como `melhorar logs`, `simplificar arquitetura` ou
`aumentar testes` sem indicar onde, por que, evidencia e impacto na v0.1.

## Modelo de oportunidade

Para cada oportunidade, use:

- titulo;
- severidade: P1, P2 ou P3;
- justificativa;
- criterio afetado;
- evidencia encontrada;
- comportamento atual;
- comportamento ideal;
- recomendacao objetiva;
- risco de nao corrigir;
- esforco estimado;
- impacto esperado;
- por que importa para a v0.1.

## Severidade

- P1: quebra promessa central da v0.1, compromete rastreabilidade operacional,
  permite comportamento incorreto relevante ou cria falsa seguranca.
- P2: aumenta custo de manutencao, onboarding, evolucao ou troubleshooting.
- P3: melhoria de clareza, consistencia ou ergonomia sem risco imediato.

## Politica de lacunas

Quando faltar contexto, registre como lacuna. Nao invente premissas. Nao cobre
da v0.1 recursos que ela declara fora do escopo, a menos que a documentacao ou
API induza o usuario a acreditar que o recurso existe.

## Saida esperada do orquestrador

Ao final da execucao, produza:

1. lista de auditorias executadas;
2. auditorias ausentes ou bloqueadas;
3. principais P1/P2/P3 por criterio;
4. duplicidades removidas;
5. lacunas restantes;
6. recomendacao de proxima acao.
