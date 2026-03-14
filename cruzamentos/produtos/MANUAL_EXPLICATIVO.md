# Manual Explicativo do Modulo de Produtos

## Objetivo

Este modulo consolida produtos vindos de multiplas fontes fiscais e cadastrais do projeto.

A regra operacional atual e:

- a `descricao` e a chave principal do produto
- produtos com a mesma `descricao` ficam no mesmo grupo
- `tipo_item`, `ncm`, `cest` e `gtin` sao harmonizados por consenso dentro da descricao
- divergencias nesses campos nao separam automaticamente o produto
- a revisao manual fica concentrada, principalmente, nos casos em que:
  - um mesmo `codigo` aparece com mais de uma `descricao`
  - o usuario decide unir ou manter separados grupos de descricao diferentes

## Estrutura dos Arquivos

### Arquivos do modulo

- `produto_unid.py`
  - fachada publica do modulo
  - ponto de entrada usado pelo restante do projeto

- `_produto_unid_shared.py`
  - schema padrao
  - mapeamento das fontes
  - normalizacao
  - utilitarios comuns

- `_produto_unid_analytics.py`
  - consenso por descricao
  - agrupamentos
  - tabelas analiticas

- `_produto_unid_manual.py`
  - aplicacao de mapas manuais
  - regras por item
  - regras por descricao
  - geracao de auditoria

- `_produto_unid_pipeline.py`
  - orquestracao do fluxo
  - leitura das fontes
  - gravacao dos parquets finais

## Fontes de Entrada

O pipeline tenta carregar, quando existirem:

- `NFe_{cnpj}.parquet`
- `NFCe_{cnpj}.parquet`
- `c170_simplificada_{cnpj}.parquet`
- `reg_0200_{cnpj}.parquet`
- `bloco_h_{cnpj}.parquet`

Cada fonte passa por mapeamento de colunas e normalizacao para chegar a uma base unica.

## Campos Principais da Base

Depois da normalizacao, a base detalhada trabalha principalmente com:

- `codigo`
- `descricao`
- `descricao_ori`
- `descr_compl`
- `tipo_item`
- `ncm`
- `cest`
- `gtin`
- `unid`
- `fonte`

## Ordem de Processamento

O fluxo principal e este:

1. carregar as fontes disponiveis
2. aplicar o mapa manual por descricao
3. aplicar o mapa manual por item
4. aplicar auto-consenso por descricao
5. gravar a base detalhada
6. gerar tabelas analiticas
7. gerar parquets de auditoria

## Mapas Manuais

Existem dois niveis de mapa manual.

### 1. Mapa manual por item

Arquivo:

- `mapa_manual_unificacao_{cnpj}.parquet`

Uso:

- alterar codigo, descricao ou atributos de linhas especificas
- agregar ou desagregar itens individualmente

Chave de aplicacao:

- `fonte`
- `codigo_original`
- `descricao_original`
- `tipo_item_original`

Essa chave e consolidada em `hash_manual_key`.

### 2. Mapa manual por descricao

Arquivo:

- `mapa_manual_descricoes_{cnpj}.parquet`

Uso:

- unir grupos inteiros de descricao
- registrar pares de descricao que devem permanecer separados

Tipos de regra:

- `UNIR_GRUPOS`
  - faz `descricao_origem` passar a ser `descricao_destino`
  - efeito aplicado antes do agrupamento final

- `MANTER_SEPARADO`
  - bloqueia unificacoes entre duas descricoes
  - tambem bloqueia convergencia indireta para o mesmo destino

Exemplo:

- se existe `MANTER_SEPARADO(A, B)`, entao:
  - `A -> B` e bloqueado
  - `A -> C` e `B -> C` tambem nao podem acabar no mesmo grupo final

## Consenso Automatico

O consenso automatico e calculado por `descricao` para os campos:

- `tipo_item`
- `ncm`
- `cest`
- `gtin`

Regra de escolha:

1. prefere valor preenchido
2. escolhe o valor mais frequente
3. em empate, usa prioridade de fonte

Prioridade de fonte:

1. `EFD_0200`
2. `Bloco_H`
3. `EFD_C170`
4. `NFe`
5. `NFCe`

Campos definidos manualmente por item nao sao sobrescritos pelo consenso.

## Quando um Grupo Vai para Revisao Manual

Hoje o gatilho principal de `requer_revisao_manual` e o caso em que um mesmo `codigo` aparece com mais de uma `descricao`.

Em termos praticos:

- se um mesmo `codigo` aparece ligado a mais de uma `descricao` na base
- o grupo de `descricao` que contem esse codigo e marcado para revisao

Isso permite concentrar a revisao nos casos mais arriscados.

## Tabelas Geradas

### 1. `base_detalhes_produtos_{cnpj}.parquet`

Base linha a linha, ja com:

- normalizacao
- aplicacao de mapas manuais
- auto-consenso por descricao

Uso:

- rastrear cada item original
- inspecionar fonte, codigo, descricao e atributos finais

### 2. `variacoes_produtos_{cnpj}.parquet`

Tabela de combinacoes distintas encontradas por produto.

Uso:

- ver quais variacoes existem por codigo e descricao
- analisar listas de unidades e fontes

### 3. `produtos_indexados_{cnpj}.parquet`

Tabela de rastreabilidade fina por combinacao original de atributos.

Uma linha por combinacao distinta de:

- `chave_produto`
- `codigo`
- `descricao`
- `descr_compl`
- `tipo_item`
- `ncm`
- `cest`
- `gtin`

Com:

- `lista_unidades`
- `lista_fontes`
- `qtd_linhas`

Uso:

- enxergar todas as combinacoes originais do grupo sem precisar desagregar manualmente

### 4. `codigos_multidescricao_{cnpj}.parquet`

Tabela residual de revisao.

Uma linha por `codigo` que aparece com mais de uma `descricao`.

Campos principais:

- `codigo`
- `qtd_descricoes`
- `lista_descricoes`
- `lista_ncm`
- `lista_cest`
- `lista_gtin`
- `lista_tipo_item`
- `lista_chave_produto`
- `qtd_grupos_descricao_afetados`

### 5. `produtos_agregados_{cnpj}.parquet`

Tabela principal de visao consolidada.

Uma linha por `descricao`.

Campos importantes:

- `chave_produto`
- `descricao`
- `lista_codigo`
- `lista_descricao`
- `qtd_descricoes`
- `qtd_codigos`
- `tipo_item_consenso`
- `ncm_consenso`
- `cest_consenso`
- `gtin_consenso`
- `unid_consenso`
- `requer_revisao_manual`

## Arquivos de Auditoria

### Auditoria por item

- `mapa_auditoria_agregados_{cnpj}.parquet`
- `mapa_auditoria_desagregados_{cnpj}.parquet`

Uso:

- rastrear decisoes manuais por item

### Auditoria por descricao

- `mapa_auditoria_descricoes_{cnpj}.parquet`
  - mapa bruto normalizado por descricao

- `mapa_auditoria_descricoes_aplicadas_{cnpj}.parquet`
  - unificacoes por descricao que realmente foram aplicadas

- `mapa_auditoria_descricoes_bloqueadas_{cnpj}.parquet`
  - unificacoes `UNIR_GRUPOS` bloqueadas por `MANTER_SEPARADO`

Campos importantes no arquivo de bloqueadas:

- `descricao_origem`
- `descricao_destino`
- `descricao_destino_resolvido`
- `motivo_bloqueio`
- `descricao_bloqueante`

Motivos possiveis:

- `MANTER_SEPARADO_DIRETO`
- `MANTER_SEPARADO_CONVERGENCIA_GRUPO`

## Telas Relacionadas

### Revisao Manual

Tela principal para revisar grupos com `requer_revisao_manual = true`.

Permite:

- agregar por linha
- desagregar por linha
- navegar para a revisao de pares e grupos

### Decisao de Pares e Grupos

Tela para selecionar grupos pendentes e:

- unir grupos inteiros de descricao
- manter grupos separados

Essa tela grava regras no `mapa_manual_descricoes_{cnpj}.parquet`.

## Endpoint Importante

Para gravar regras por descricao:

- `POST /api/python/produtos/resolver-manual-descricoes`

Payload esperado:

```json
{
  "cnpj": "37671507000187",
  "regras": [
    {
      "tipo_regra": "UNIR_GRUPOS",
      "descricao_origem": "DESC A",
      "descricao_destino": "DESC B",
      "descricao_par": "DESC B",
      "chave_grupo_a": "ID_0001",
      "chave_grupo_b": "ID_0002",
      "score_origem": "0.91",
      "acao_manual": "AGREGAR"
    }
  ]
}
```

## Como Executar

Execucao direta:

```bash
python cruzamentos/produtos/produto_unid.py 37671507000187
```

Fluxo normal no sistema:

- rodar auditoria do CNPJ
- abrir `Revisao Manual`
- se necessario, usar `Decisao de Pares e Grupos`
- reprocessar e consultar os parquets de auditoria

## Leitura Rapida para Operacao

Se a pergunta for "onde vejo o que aconteceu?", use:

- base final linha a linha:
  - `base_detalhes_produtos_{cnpj}.parquet`

- visao consolidada:
  - `produtos_agregados_{cnpj}.parquet`

- decisoes por item:
  - `mapa_auditoria_agregados_{cnpj}.parquet`
  - `mapa_auditoria_desagregados_{cnpj}.parquet`

- decisoes por descricao:
  - `mapa_auditoria_descricoes_{cnpj}.parquet`
  - `mapa_auditoria_descricoes_aplicadas_{cnpj}.parquet`
  - `mapa_auditoria_descricoes_bloqueadas_{cnpj}.parquet`
