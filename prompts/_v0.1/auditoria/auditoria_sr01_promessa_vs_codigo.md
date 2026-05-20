# Auditoria SR01 - Promessa vs Codigo

## Papel do agente

Atue como arquiteto senior de dados revisando criticamente um framework Python
para ETL PySpark. Seu foco e comparar o que a v0.1 promete com o que o codigo
realmente entrega.

## Objetivo unico

Construir uma matriz objetiva de aderencia entre promessa declarada e
implementacao real.

## Entradas obrigatorias

- `README.md`
- `QUICK_START.md`
- `docs/v0.1-contract.md`
- `docs/v0.1-known-limitations.md`
- `etl_framework/`
- `tests/`

## Escopo

Avalie somente promessas, limites declarados e evidencias de entrega no codigo
ou nos testes.

Nao avalie DX, overengineering, observabilidade ou qualidade dos testes em
profundidade. Esses temas pertencem a auditorias proprias.

## Perguntas obrigatorias

- Quais promessas centrais a v0.1 declara?
- Quais limites a v0.1 declara explicitamente?
- Para cada promessa, existe implementacao correspondente?
- Para cada promessa, existe teste correspondente?
- Alguma documentacao promete mais do que o codigo entrega?
- Alguma limitacao declarada esta sendo usada para esconder falha do contrato
  central?

## Evidencias obrigatorias

Para cada promessa analisada, registre:

- documento e trecho curto que declara a promessa;
- arquivo/classe/metodo que implementa ou deveria implementar;
- teste que protege o comportamento, quando existir;
- status: `entregue`, `parcial`, `nao entregue` ou `lacuna de contexto`;
- justificativa objetiva.

## Criterios de avaliacao

- aderencia a filosofia do projeto;
- aderencia entre promessa e codigo;
- simplicidade arquitetural;
- facilidade de manutencao;
- facilidade para desenvolvedor junior;
- robustez operacional;
- rastreabilidade de falhas.

## Saida esperada

Gere um relatorio com:

1. resumo executivo;
2. matriz promessa vs codigo vs testes;
3. promessas sem evidencia suficiente;
4. divergencias entre documentacao e implementacao;
5. oportunidades P1/P2/P3 no modelo padrao da bateria.

## Criterio de sucesso

A auditoria e bem-sucedida se nenhuma conclusao depender de opiniao sem
referencia a documento, codigo ou teste.
