# Auditoria SR00 - Orquestrador e Responsabilidade Unica

## Resumo executivo

O artefato SR00 declara corretamente a intencao de ser um coordenador da bateria
de auditorias, nao um auditor direto do codigo. A responsabilidade proposta e
coerente com a filosofia da v0.1: reduzir ambiguidade, evitar auditorias
genericas, exigir evidencia objetiva e organizar a execucao por especialistas.

Entretanto, SR00 nao entrega um protocolo operacional verificavel para cumprir
essa responsabilidade unica. Ele lista auditorias, regras comuns e uma saida
esperada, mas nao define estado de execucao, contrato de entrada/saida dos
agentes especializados, matriz de ownership, criterio de aceite de evidencia,
criterio para remover duplicidades, nem mecanismo para registrar bloqueios. O
resultado e um orquestrador textual que pode induzir falsa seguranca: parece
coordenar uma bateria independente, mas nao torna a coordenacao auditavel.

A comparacao com o proprio framework reforca a divergencia. O `Pipeline` do
codigo delimita responsabilidade unica com execucao linear explicita,
preflight, injecao de dependencias e testes de ordem/falha. SR00 deveria fazer o
equivalente no dominio das auditorias: coordenar sem auditar, mas com contrato
objetivo de execucao e consolidacao.

Auditorias executadas nesta avaliacao: SR00 somente.

Auditorias ausentes ou bloqueadas: SR01 a SR18 e SR99 nao foram executadas nesta
auditoria independente, por escopo e por restricao explicita de leitura.

Duplicidades removidas: nenhuma duplicidade real foi removida, pois nao houve
execucao dos agentes especializados nem leitura de seus relatórios.

Lacunas restantes: nao ha evidencia de um executor automatizado de auditorias,
de um schema de relatorio comum, de uma matriz de escopo por auditoria, nem de
um registro consolidado de status independente de relatorios finais.

Recomendacao de proxima acao: transformar SR00 de lista orientativa em contrato
de orquestracao verificavel, com matriz de responsabilidade por SR, checklist de
execucao, schema minimo de resultado, criterio de bloqueio e regra objetiva de
deduplicacao.

## Principais riscos

- P1 por governanca: SR00 se apresenta como coordenador, mas nao define como
  executar, validar, bloquear ou consolidar a bateria.
- P1 por falsa seguranca: a saida esperada exige sumarizacao de P1/P2/P3 e
  duplicidades removidas sem garantir que os relatórios especializados foram
  executados, lidos ou comparados sob um contrato comum.
- P2 por sobreposicao: o contrato comum repete criterios amplos para todas as
  auditorias, mas nao aloca ownership por criterio; isso incentiva conclusoes
  duplicadas e disputas de escopo.
- P2 por rastreabilidade: as regras de evidencia exigem arquivo/classe/metodo,
  mas nao exigem linha, trecho, comando, cenario ou status de reproducao.
- P3 por ergonomia: a ordem recomendada lista nomes de prompts, mas nao indica
  dependencia, entrada esperada, saida esperada por auditoria ou criterio de
  continuidade em caso de falha.

## Oportunidades

### 1. Definir um contrato executavel de orquestracao

Severidade: P1.

Contexto: SR00 declara que o agente deve coordenar a bateria de auditorias
especializadas e que sua funcao "nao e auditar o codigo diretamente".

Evidencia encontrada: `prompts/_v0.1/auditoria/auditoria_sr00_orquestrador_responsabilidade_unica.md:5`
a `:7` define o papel de coordenador; `:25` inicia a ordem recomendada; `:108`
inicia a saida esperada do orquestrador. Nao ha no prompt um protocolo de
estado, formato de despacho, criterio de sucesso por auditoria ou estrutura de
registro de execucao.

Impactos: a bateria pode ser considerada executada apenas porque existe uma
lista ordenada, sem prova de que cada agente produziu evidencias, sem bloqueio
formal quando uma auditoria falha e sem trilha objetiva para SR99 consolidar.

Divergencia: o codigo do framework mostra um padrao melhor para orquestracao.
`etl_framework/contracts/pipeline.py:45` a `:50` executa preflight, extract,
transform e load em ordem explicita; `:53` a `:84` concentra preflight antes de
efeitos externos; `tests/test_pipeline_contract.py:295` e `:304` verificam a
ordem oficial. SR00, em contraste, lista uma ordem, mas nao define preflight da
bateria nem asserts equivalentes.

Comportamento atual: SR00 orienta a execucao em texto livre e deixa a
coordenacao depender da disciplina do agente.

Comportamento ideal: SR00 deveria conter uma tabela operacional com, no minimo,
id da auditoria, dono de criterio, entradas permitidas, arquivos proibidos,
saida obrigatoria, status permitido (`executada`, `bloqueada`, `nao aplicavel`),
evidencias minimas e condicao de parada.

Recomendacao objetiva: adicionar ao SR00 um "Contrato de execucao" com schema
fixo para cada auditoria especializada e um "Preflight da bateria" exigindo
verificacao de existencia dos prompts SR01-SR18/SR99, diretorio de saida,
restricoes de leitura e criterio de bloqueio antes de iniciar a coordenacao.

Risco de nao corrigir: relatorios podem divergir em formato, profundidade e
criterio; SR99 pode consolidar percepcoes incompatíveis; a v0.1 ganha uma camada
de governanca que aparenta rigor, mas nao e reproduzivel.

Esforco estimado: medio.

Impacto esperado: alto, porque transforma SR00 de orientacao narrativa em
controle auditavel.

Por que importa para a v0.1: a v0.1 vende previsibilidade e reducao de carga
cognitiva; a propria auditoria da v0.1 precisa ser previsivel.

### 2. Separar coordenacao de consolidacao tecnica

Severidade: P1.

Contexto: SR00 afirma que nao deve auditar o codigo diretamente, mas sua saida
final exige "principais P1/P2/P3 por criterio", "duplicidades removidas" e
"lacunas restantes".

Evidencia encontrada: `prompts/_v0.1/auditoria/auditoria_sr00_orquestrador_responsabilidade_unica.md:5`
a `:7` limita o papel a coordenador; `:108` a `:116` exige uma saida
consolidada. O prompt nao define se essa consolidacao deve ler relatorios
especializados, memoria de execucao, artefatos intermediarios ou respostas de
agentes.

Impactos: o orquestrador pode invadir a responsabilidade de SR99 ou dos agentes
especializados para preencher lacunas. Tambem pode produzir uma consolidacao sem
base material, principalmente quando nao ha acesso ou quando as auditorias ainda
nao foram executadas.

Divergencia: o contrato v0.1 separa dono por etapa em
`docs/v0.1-contract.md:55`; extract, transform, load e certify pertencem a
pipeline concreta, enquanto check e validate pertencem ao framework. SR00 nao
faz separacao equivalente entre coordenar execucao, validar formato dos
resultados e consolidar achados tecnicos.

Comportamento atual: SR00 mistura papel de gerente de execucao com sumario
executivo tecnico, sem explicitar a fronteira com SR99.

Comportamento ideal: SR00 deveria produzir status da bateria, qualidade dos
artefatos e conflitos de escopo; SR99 deveria ser o dono explicito da
consolidacao tecnica final, consumindo relatorios validados.

Recomendacao objetiva: alterar a saida esperada de SR00 para distinguir
"estado da bateria" de "consolidacao tecnica". SR00 deve listar achados por
criterio apenas como metadados recebidos dos agentes, com origem obrigatoria
(`SRxx`, arquivo, titulo do achado, severidade). A analise executiva final deve
ser delegada a SR99.

Risco de nao corrigir: o orquestrador se torna um auditor generico, exatamente o
antiobjetivo descrito no contexto da bateria.

Esforco estimado: baixo a medio.

Impacto esperado: alto, por reduzir acoplamento de responsabilidades e conflito
entre SR00 e SR99.

Por que importa para a v0.1: a responsabilidade unica e um criterio central da
auditoria; o primeiro prompt da bateria nao deveria violar essa fronteira.

### 3. Criar matriz de ownership para evitar sobreposicao entre agentes

Severidade: P2.

Contexto: o prompt declara que a bateria foi quebrada para evitar duplicidade
entre agentes e exige baixa sobreposicao.

Evidencia encontrada: `prompts/_v0.1/auditoria/auditoria_sr00_orquestrador_responsabilidade_unica.md:21`
cita duplicidade entre agentes; `:49` a `:62` aplica praticamente todos os
criterios a todas as auditorias; `:75` a `:77` rejeita recomendacoes genericas,
mas nao define ownership primario/secundario por criterio.

Impactos: SR04, SR05, SR11, SR12, SR17 e SR18 podem disputar robustez
operacional; SR06, SR08, SR09, SR10, SR13 e SR14 podem disputar simplicidade,
DX e abstracao; SR01, SR02 e SR99 podem repetir promessa versus realidade. Sem
matriz, a remocao de duplicidade fica subjetiva.

Divergencia: `tests/test_runtime_observability_architecture.py` usa regras AST
claras para impedir responsabilidades indevidas nos contratos, como nao importar
infraestrutura de observabilidade e nao definir builders de metadata. SR00 nao
tem regra comparavel para impedir que cada auditoria cubra o mesmo criterio.

Comportamento atual: todos os agentes recebem quase os mesmos criterios
transversais e a lista de prompts so diferencia o titulo.

Comportamento ideal: cada SR deveria ter criterio primario, criterios
secundarios permitidos, criterios explicitamente fora de escopo e regra de
transferencia de achado para outro SR.

Recomendacao objetiva: incluir em SR00 uma matriz `SR -> criterio primario ->
criterios secundarios -> fora de escopo -> sinais de escalonamento para SR99`.
Exigir que cada relatorio declare "achados transferidos" quando encontrar tema
fora do seu ownership.

Risco de nao corrigir: relatorios longos, repetitivos e dificeis de consolidar;
perda de foco; aumento da carga cognitiva para quem precisa decidir o backlog da
v0.1.

Esforco estimado: medio.

Impacto esperado: medio a alto.

Por que importa para a v0.1: a bateria existe justamente para reduzir auditorias
genericas; sem ownership, a decomposicao em SRs vira apenas particionamento
nominal.

### 4. Endurecer a politica de evidencia para rastreabilidade reproduzivel

Severidade: P2.

Contexto: SR00 exige evidencia objetiva e rejeita recomendacoes genericas.

Evidencia encontrada: `prompts/_v0.1/auditoria/auditoria_sr00_orquestrador_responsabilidade_unica.md:64`
a `:73` permite citar arquivo, classe, metodo, teste, documento, cenario ou
lacuna; `:75` a `:77` rejeita recomendacoes sem onde/por que/evidencia/impacto.
Nao ha exigencia de linha, comando, versao de arquivo, status de reproducao ou
distincao entre evidencia direta e inferencia.

Impactos: dois auditores podem citar o mesmo arquivo com conclusoes opostas sem
que o revisor consiga localizar rapidamente o trecho. A consolidacao tambem
perde capacidade de distinguir achado reproduzido de inferencia arquitetural.

Divergencia: os testes do framework sao concretos sobre comportamento:
`tests/test_pipeline_contract.py:322` e `:345` demonstram falha de preflight
antes de extract; `:382` cobre eventos obrigatorios de observabilidade; `:791`
cobre empacotamento de falhas por etapa; `:907` cobre parada de etapas
downstream. SR00 aceita evidencia menos precisa que a propria suite exige do
codigo.

Comportamento atual: a regra de evidencia e necessaria, mas permissiva demais
para uma bateria independente.

Comportamento ideal: toda oportunidade deveria citar arquivo e linha quando
aplicavel, mais tipo de evidencia (`codigo`, `teste`, `doc`, `cenario`,
`lacuna`), e indicar se o achado foi observado diretamente ou inferido.

Recomendacao objetiva: expandir o modelo de oportunidade de SR00 com campos
obrigatorios `tipo_de_evidencia`, `referencias`, `status_de_reproducao` e
`nivel_de_inferencia`. Para lacunas, exigir o arquivo/area onde a informacao foi
procurada e nao encontrada.

Risco de nao corrigir: backlog gerado por auditoria fica dificil de validar,
priorizar e transformar em mudanca objetiva.

Esforco estimado: baixo.

Impacto esperado: medio.

Por que importa para a v0.1: rastreabilidade operacional e promessa central da
v0.1; a auditoria precisa seguir o mesmo padrao.

### 5. Registrar bloqueios e lacunas como dados de primeira classe

Severidade: P3.

Contexto: SR00 menciona politica de lacunas, mas nao especifica formato de
registro, severidade, dono ou proxima acao.

Evidencia encontrada: `prompts/_v0.1/auditoria/auditoria_sr00_orquestrador_responsabilidade_unica.md:100`
a `:105` orienta registrar lacunas e nao inventar premissas; `:108` a `:116`
exige listar lacunas restantes. Nao ha um modelo para lacunas equivalente ao
modelo de oportunidade de `:79` a `:93`.

Impactos: lacunas podem virar notas soltas sem acao. Em uma bateria
independente, isso e perigoso porque ausencia de contexto pode ser tao relevante
quanto um bug confirmado.

Divergencia: a documentacao v0.1 e explicita sobre limites conhecidos em
`docs/v0.1-known-limitations.md`, separando entrega, nao entrega, riscos
remanescentes e casos que exigem revisao senior. SR00 deveria tratar lacunas de
auditoria com granularidade semelhante.

Comportamento atual: lacunas sao mencionadas, mas nao normalizadas.

Comportamento ideal: lacuna deveria ter id, origem, escopo afetado, evidencia de
busca, impacto, responsavel provavel, severidade e decisao esperada.

Recomendacao objetiva: adicionar um "Modelo de lacuna" ao SR00 e exigir que
lacunas bloqueantes possam impedir SR99 de consolidar uma conclusao.

Risco de nao corrigir: lacunas criticas podem ser diluidas no texto final e
tratadas como detalhe editorial.

Esforco estimado: baixo.

Impacto esperado: medio.

Por que importa para a v0.1: a v0.1 explicita seus limites; a bateria de
auditoria deve explicitar os seus.

## Conclusao tecnica

SR00 tem a intencao correta, mas ainda nao tem a forma tecnica de um
orquestrador confiavel. Ele define papel, ordem, criterios comuns, severidade e
saida esperada, mas deixa a execucao real em texto livre. Isso compromete a
responsabilidade unica porque o agente SR00 acaba precisando decidir sozinho
como despachar, validar, deduplicar, bloquear e consolidar.

O codigo do framework fornece uma referencia interna mais madura: `Pipeline`
coordena, nao implementa regras de negocio; executa preflight antes de efeitos;
injeta contratos; para downstream em falha; e tem testes cobrindo essas
fronteiras. SR00 deveria adotar o mesmo nivel de contrato no dominio das
auditorias.

Minha recomendacao tecnica e tratar SR00 como backlog prioritario de
governanca: antes de executar uma bateria nova, endurecer o prompt com contrato
de execucao, matriz de ownership, schema de evidencias, modelo de lacunas e
fronteira explicita com SR99. Sem isso, a bateria pode produzir relatorios
aparentemente rigorosos, mas com baixa reprodutibilidade e alto risco de
sobreposicao.
