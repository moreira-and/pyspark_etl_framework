# ADR 0001 - Adiar SafeLoad Para v0.2

## Status

Aceita.

## Contexto

O objetivo da v0.1 e reduzir a carga cognitiva de desenvolvedores junior sem
transformar o framework em plataforma generica de dados.

A v0.1 automatiza controles estruturais:

- `auto_check` com `source_struct`;
- `auto_validate` com `target_struct`;
- criacao e bloqueio de `is_valid=False`.

Load seguro depende do destino concreto: formato de tabela, suporte a transacao,
semantica de overwrite, particionamento, retry, rollback e estrategia de
commit.

Implementar `SafeLoad`, `LoadStrategy` e certificacao real na v0.1 criaria um
contrato publico antes de haver evidencia suficiente sobre os destinos reais.

## Decisao

Adiar para a v0.2:

- `SafeLoad`;
- `LoadStrategy`;
- staging padronizado;
- leitura pos-load;
- idempotencia;
- rollback;
- certificacao real baseada no destino.

Na v0.1, `Load._load` e `Load._certify` continuam como responsabilidade da
pipeline concreta.

## Consequencias

Positivas:

- A v0.1 permanece pequena e ensinavel.
- O contrato atual nao promete seguranca produtiva falsa.
- O framework entrega valor real em `extract`, `transform`, check e validate.
- A equipe evita cristalizar uma abstracao de load errada.

Negativas:

- A v0.1 nao pode ser chamada de pronta para producao completa.
- Pipelines produtivas ainda exigem revisao senior.
- Rerun, duplicidade, carga parcial e certify real seguem fora do nucleo.
- A documentacao precisa manter essa limitacao explicita.

## Criterio Para Revisitar

Revisitar na v0.2 quando houver pelo menos um destino real ou padrao interno com:

- semantica de escrita conhecida;
- staging por `run_id`;
- regra de commit;
- criterio de idempotencia;
- teste de falha antes e depois do commit;
- teste de rerun;
- evidencia persistida no destino;
- certificacao lendo o destino real.
