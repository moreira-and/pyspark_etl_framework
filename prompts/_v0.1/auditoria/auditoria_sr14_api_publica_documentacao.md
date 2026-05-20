# Auditoria SR14 - API Publica e Documentacao Ativa

## Papel do agente

Atue como maintainer de biblioteca Python interna.

## Objetivo unico

Verificar se a API publica documentada e a documentacao ativa sao consistentes,
pequenas e adequadas para a v0.1.

## Entradas obrigatorias

- `etl_framework/__init__.py`
- `pyproject.toml`
- `README.md`
- `QUICK_START.md`
- `docs/v0.1-contract.md`
- `docs/v0.1-known-limitations.md`
- `docs/development/testing.md`
- `CHANGELOG.md`

## Escopo

Foque em exports publicos, instalacao, comandos oficiais, documentacao ativa,
links, exemplos e consistencia de nomes.

Nao audite empacotamento profundamente alem do necessario para validar uso da
biblioteca.

## Perguntas obrigatorias

- A API raiz contem apenas o esperado?
- A documentacao ensina o caminho suportado?
- Existem docs desatualizadas ou contraditorias?
- Os comandos oficiais batem com o projeto?
- O pacote publicado e claro para o usuario?
- Ha imports internos expostos indevidamente?

## Evidencias obrigatorias

- exports reais;
- API esperada na documentacao;
- links e comandos;
- divergencias de nomes;
- lacunas de documentacao.

## Criterios de avaliacao

- aderencia promessa-codigo;
- developer experience;
- manutencao;
- simplicidade;
- facilidade para junior.

## Saida esperada

Gere:

1. matriz API/documentacao;
2. inconsistencias;
3. lacunas;
4. oportunidades P1/P2/P3.

## Criterio de sucesso

O relatorio deve deixar claro o que e API cotidiana, API avancada e detalhe
interno.
