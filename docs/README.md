# Documentacao

Este diretorio guarda documentos operacionais e materiais de auditoria do
`spark-etl-framework`.

## Guias Ativos

- [quick_start_explained.md](quick_start_explained.md): leitura humana do Quick
  Start, explicando o papel de cada classe e os cuidados antes de evoluir para
  producao.
- [production_readiness.md](production_readiness.md): checklist de uso produtivo,
  load seguro, observabilidade, data quality, custo Spark e comandos oficiais de
  validacao.
- [../prompts/notebook_parser/00_overview.md](../prompts/notebook_parser/00_overview.md): workflow
  auditavel para converter `.ipynb` desformatado em notebook limpo e `cfg_*.py`
  compativel com `etl_framework`.

## Materiais De Auditoria

- [audit_prompt.md](audit_prompt.md): prompt usado para auditar o estado do
  framework. Ele e historico de avaliacao, nao substitui o guia produtivo.

## Ordem De Leitura

1. [README.md](../README.md): visao geral do projeto.
2. [QUICK_START.md](../QUICK_START.md): primeira pipeline local.
3. [quick_start_explained.md](quick_start_explained.md): explicacao do exemplo.
4. [../prompts/notebook_parser/00_overview.md](../prompts/notebook_parser/00_overview.md):
   parser de notebook para pipeline.
5. [production_readiness.md](production_readiness.md): checklist antes de go-live.
6. [MANIFEST.md](../MANIFEST.md): regras arquiteturais para evoluir o nucleo.
