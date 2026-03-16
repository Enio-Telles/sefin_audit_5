# Proposta: Vetorizacao Opcional para Sugestoes de Agregacao e Manutencao Desagregada

## Objetivo

Adicionar uma camada opcional de sugestoes para identificar:

- provaveis produtos a agregar;
- provaveis produtos a manter desagregados.

Essa camada deve ser:

- opcional e desligada por padrao;
- executada apenas quando o usuario selecionar a opcao no bloco `Sistema Atual de Produtos`;
- isolada do pipeline principal, sem alterar `produtos_agregados_{cnpj}.parquet` por padrao;
- cacheada por CNPJ e por hash da base atual para evitar recomputacao desnecessaria.

## Diretriz Principal

O fluxo deterministico atual continua sendo a verdade operacional:

1. normalizar;
2. agregar descricoes exatamente equivalentes;
3. separar codigos reaproveitados;
4. gerar a tabela final;
5. revisar visualmente.

A vetorizacao entra apenas como motor de triagem e prioridade para revisao humana, nunca como substituta da regra documental.

## Onde Encaixar no Sistema Atual

O projeto ja possui pontos de extensao adequados:

- backend:
  - [produto_runtime.py](C:/Users/eniot/.gemini/antigravity/scratch/sefin_audit_5/server/python/core/produto_runtime.py)
  - [produto_unid.py](C:/Users/eniot/.gemini/antigravity/scratch/sefin_audit_5/server/python/routers/produto_unid.py)
- frontend:
  - [AuditResultView.tsx](C:/Users/eniot/.gemini/antigravity/scratch/sefin_audit_5/client/src/components/auditoria/AuditResultView.tsx)
  - [pythonApi.ts](C:/Users/eniot/.gemini/antigravity/scratch/sefin_audit_5/client/src/lib/pythonApi.ts)

O backend ainda preserva a familia de endpoints de pares similares:

- `GET /api/python/produtos/pares-grupos-similares`
- `GET /api/python/produtos/vectorizacao-status`
- `POST /api/python/produtos/vectorizacao-clear-cache`

A proposta e reutilizar essa superficie, mas mudar o papel dela:

- antes: tentativa de classificacao ampla;
- agora: sugestao opcional para revisao, fora do fluxo padrao.

## Proposta de Arquitetura

### Modo padrao

- `metodo = documental`
- nenhum calculo vetorial
- nenhum impacto de CPU adicional
- tela principal continua lendo apenas os artefatos atuais

### Modo opcional leve

Usar uma abordagem mais barata do que embeddings densos:

- texto de entrada:
  - `descricao`
  - `lista_descr_compl`
  - tokens estruturados de embalagem, volume, marca e quantidade
- representacao:
  - `TF-IDF` de caracteres `3-5 grams`
  - mais robusto a abreviacoes e erros de digitacao do que tokenizacao simples
- busca:
  - `top-k cosine similarity` em matriz esparsa
  - sem FAISS
- bibliotecas sugeridas:
  - `scikit-learn`
  - opcionalmente `sparse_dot_topn` se houver necessidade

Esse modo deve ser o primeiro opcional recomendado.

Motivo:

- instalacao mais simples;
- menor custo operacional;
- boa aderencia a descricoes fiscais curtas e ruidosas;
- suficiente para ranquear candidatos provaveis.

### Modo opcional semantico

Usar embeddings densos multilingues:

- modelo sugerido:
  - `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2`
- indice:
  - `FAISS` quando disponivel
  - fallback para `numpy` quando FAISS nao estiver instalado

Esse modo entra como segunda opcao, para bases em que o modo leve nao recuperar casos suficientes.

## Recomendacao Tecnica

Adotar 3 niveis de operacao:

1. `desligado`
   - sem custo adicional;
   - comportamento atual.
2. `leve`
   - `TF-IDF char n-gram + cosine top-k`;
   - melhor relacao custo/beneficio.
3. `semantico`
   - embeddings + FAISS;
   - maior cobertura para sinonimos e textos mais heterogeneos.

O modo `hibrido` pode existir internamente, mas nao precisa ser exposto primeiro na interface. Para o usuario operacional, `leve` e `semantico` bastam.

## Saidas Esperadas

A vetorizacao nao deve gerar decisao final. Ela deve produzir uma tabela de sugestoes com colunas como:

- `chave_produto_a`
- `descricao_a`
- `lista_codigos_a`
- `ncm_a`
- `cest_a`
- `gtin_a`
- `chave_produto_b`
- `descricao_b`
- `lista_codigos_b`
- `ncm_b`
- `cest_b`
- `gtin_b`
- `score_textual`
- `score_semantico`
- `score_estrutural`
- `score_final`
- `recomendacao`
- `motivo_recomendacao`

Onde:

- `recomendacao = PROVAVEL_AGREGAR`
- `recomendacao = PROVAVEL_MANTER_DESAGREGADO`
- `recomendacao = REVISAR`

## Regra de Composicao de Score

O score final deve combinar texto com sinais fiscais e estruturais.

### Score textual

- similaridade da descricao principal;
- similaridade de `descr_compl`;
- coincidencia de tokens fortes:
  - marca
  - volume
  - concentracao
  - sabor
  - material
  - modelo

### Score estrutural

Sinais de reforco:

- `NCM` igual ou prefixo forte do `NCM`
- `CEST` igual
- `GTIN` igual
- unidade compativel
- lista de codigos sem sobreposicao

Sinais de bloqueio:

- `GTIN` valido diferente
- `NCM` claramente diferente
- `CEST` diferente quando ambos preenchidos
- tokens contraditorios de apresentacao:
  - `200ML` x `2L`
  - `CX 12` x `UN`
  - `SABOR MORANGO` x `SABOR UVA`

### Regra sugerida

- `GTIN` igual e valido:
  - priorizar `PROVAVEL_AGREGAR`
- `GTIN` diferente e valido:
  - priorizar `PROVAVEL_MANTER_DESAGREGADO`
- `NCM` e `CEST` iguais, score textual alto:
  - `PROVAVEL_AGREGAR`
- texto proximo, mas com divergencia material relevante:
  - `PROVAVEL_MANTER_DESAGREGADO`

## Faixas de Acao Sugeridas

- `score_final >= 0.92`
  - `PROVAVEL_AGREGAR`
- `0.75 <= score_final < 0.92`
  - `REVISAR`
- `score_final < 0.75`
  - nao exibir por padrao

Com bloqueios:

- `GTIN` valido diferente:
  - classificar como `PROVAVEL_MANTER_DESAGREGADO`, mesmo com texto proximo
- `NCM` diferente nos 4 primeiros digitos:
  - bloquear sugestao de agregacao

## Fluxo Operacional Proposto

### No bloco `Sistema Atual de Produtos`

Adicionar um painel opcional:

- `Sugestoes por similaridade`
- seletor:
  - `Desligado`
  - `Leve`
  - `Semantico`
- parametros avancados:
  - `Top K`
  - `Score minimo`
  - `Forcar recalculo`

### Comportamento

- se `Desligado`, nada muda no sistema;
- se `Leve` ou `Semantico`, o usuario clica em `Gerar sugestoes`;
- o backend calcula e grava parquet separado;
- a tela abre uma fila de revisao de pares sugeridos.

## Artefatos Novos Sugeridos

- `pares_sugeridos_agregacao_leve_{cnpj}.parquet`
- `pares_sugeridos_agregacao_semantico_{cnpj}.parquet`
- `pares_sugeridos_agregacao_{cnpj}.json`

Esses artefatos devem ficar fora da tabela final e fora do runtime principal.

## Por Que Nao Impacta o Desempenho Padrao

Porque:

- o fluxo principal continua deterministicamente identico;
- a vetorizacao so roda sob comando explicito do usuario;
- a leitura da tela principal continua usando `produtos_agregados_{cnpj}.parquet`;
- os resultados vetorizados ficam em cache por:
  - `cnpj`
  - hash do parquet base
  - metodo
  - parametros

## Vantagens da Abordagem Leve

- baixo acoplamento;
- sem dependencia obrigatoria de FAISS;
- mais simples para distribuir e manter;
- bom desempenho em descricoes curtas;
- menor risco de regressao operacional.

## Fragilidades e Cuidados

- similaridade textual pode aproximar produtos parentes, mas distintos;
- volumes e embalagens precisam virar sinais negativos fortes;
- `GTIN`, `NCM` e `CEST` nao podem ser tratados como mero detalhe;
- o resultado deve ser sempre sugestao, nunca acao automatica cega.

## Proposta de Implementacao por Fases

### Fase 1

Implementar apenas o modo `leve`:

- `TF-IDF char n-gram`
- cosine similarity
- cache por parquet
- nova fila de sugestoes

### Fase 2

Adicionar enriquecimento estrutural:

- extracao de volume
- embalagem
- marca
- quantidade por caixa

### Fase 3

Adicionar modo `semantico`:

- embeddings
- FAISS opcional
- fallback simples quando FAISS nao existir

## Recomendacao Final

Para este projeto, a melhor proposta inicial nao e FAISS como padrao.

A melhor relacao entre custo, robustez e facilidade de manutencao e:

1. manter o fluxo documental como motor oficial;
2. adicionar um modo opcional `leve` para sugestoes;
3. deixar `semantico/FAISS` como expansao ativada somente quando necessario.

Isso preserva desempenho, reduz risco e aproveita bem a arquitetura que o sistema ja tem preparada para pares similares e cache de sugestoes.
