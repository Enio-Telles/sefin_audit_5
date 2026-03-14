# Manual de Uso do Fluxo de Produtos

## Objetivo

Este manual descreve o uso operacional do fluxo de produtos.

A regra atual do sistema e:

- mesma `descricao` = mesmo produto
- diferencas em `ncm`, `cest`, `gtin` e `tipo_item` sao harmonizadas por consenso
- a revisao humana fica concentrada no residual em que um mesmo `codigo` aparece com descricoes diferentes

## Onde o fluxo aparece no sistema

As telas principais sao:

- `Auditar CNPJ`
- `Revisao residual`
- `Decisao entre grupos`
- `Consolidacao por selecao`
- `Visualizar Tabelas`

## Fluxo recomendado de uso

### 1. Rodar a auditoria do CNPJ

Na tela `Auditar CNPJ`:

1. informar o CNPJ
2. iniciar a auditoria
3. aguardar a geracao dos parquets de produtos

Os arquivos ficam em:

- `CNPJ/{cnpj}/analises/`

### 2. Verificar o resultado automatico

Os arquivos mais importantes apos o processamento sao:

- `produtos_agregados_{cnpj}.parquet`
  - visao consolidada final por `descricao`

- `produtos_indexados_{cnpj}.parquet`
  - visao detalhada por combinacao original de atributos

- `codigos_multidescricao_{cnpj}.parquet`
  - fila residual de revisao

### 3. Trabalhar primeiro na revisao residual

Na tela `Revisao residual` aparecem apenas os casos em que:

- um mesmo `codigo` foi encontrado com mais de uma `descricao`

Essa e a fila principal de decisao manual.

### 4. Usar `Consolidar` ou `Separar`

Para cada `codigo` residual, existem duas acoes principais:

- `Consolidar`
- `Separar`

## Quando usar cada acao

### `Consolidar`

Use `Consolidar` quando:

- o codigo representa o mesmo produto
- as descricoes sao variacoes de escrita
- as diferencas sao apenas de cadastro, abreviacao, complemento ou padrao textual

Efeito:

- o codigo e mantido
- voce escolhe a descricao canonica
- o sistema aplica atributos finais de consenso

### `Separar`

Use `Separar` quando:

- o mesmo codigo foi reutilizado para produtos diferentes
- as descricoes indicam produtos realmente distintos
- e necessario criar novos produtos a partir das descricoes desse codigo

Efeito:

- o codigo original e dividido em novos grupos
- cada grupo recebe `codigo`, `descricao`, `ncm`, `cest` e `gtin` finais

### `Decisao entre grupos`

Use essa tela quando a decisao precisa ocorrer entre grupos inteiros de `descricao`.

Acoes disponiveis:

- `Unir grupos selecionados`
- `Manter grupos separados`
- `Desfazer regras entre selecionados`

## Tela `Consolidar codigo`

Essa tela mostra:

- o codigo em revisao
- quantas descricoes existem
- as opcoes de descricao canonica
- as opcoes de `ncm`, `cest` e `gtin`

### Como usar

1. escolher a `descricao canonica`
2. escolher `ncm`, `cest` e `gtin`, quando houver mais de uma opcao
3. confirmar a consolidacao

O sistema vai:

- carregar os detalhes brutos do codigo
- gravar a decisao manual por item
- reprocessar os produtos
- atualizar a tela de origem

### Como desfazer

Existe o botao:

- `Desfazer decisao`

Efeito:

- remove as decisoes manuais por item ligadas ao `codigo_original`
- reprocessa o fluxo
- atualiza a tela residual

Use quando uma consolidacao manual foi aplicada de forma incorreta.

## Tela `Separar codigo`

Essa tela mostra:

- as descricoes encontradas para o codigo
- os grupos de destino
- os campos editaveis de cada novo produto

### Como usar

1. selecionar uma ou mais descricoes na coluna da esquerda
2. clicar `Mover` no grupo de destino
3. repetir ate montar os grupos finais
4. ajustar `codigo`, `descricao`, `ncm`, `cest` e `gtin` de cada grupo
5. confirmar a separacao

### Regra importante

E possivel manter duas ou mais descricoes no mesmo grupo e deixar as demais separadas.

Exemplo:

- `STEAK BURGER DE COSTELA 210G CX 420G - BLACK ANGUS`
- `HB SEM TEMPERO - COSTELA 210G CX 420G - BLACK ANGUS`

Essas duas podem ser movidas para o mesmo grupo, enquanto as demais descricoes do codigo permanecem em grupos separados.

### Como desfazer

Existe o botao:

- `Desfazer decisao`

Efeito:

- remove as decisoes manuais por item ligadas ao `codigo_original`
- reprocessa o fluxo
- atualiza a tela residual

Use quando a separacao manual foi montada incorretamente.

## Tela `Decisao entre grupos`

Essa tela trabalha com grupos inteiros de `descricao`.

### `Unir grupos selecionados`

Use quando dois ou mais grupos diferentes devem virar um unico grupo final.

Efeito:

- grava regras `UNIR_GRUPOS`
- `descricao_origem` passa a usar `descricao_destino`
- o pipeline e reprocessado

### `Manter grupos separados`

Use quando grupos de descricao nao podem convergir para o mesmo destino.

Efeito:

- grava regras `MANTER_SEPARADO`
- futuras unificacoes conflitantes ficam bloqueadas

### `Desfazer regras entre selecionados`

Use quando uma decisao anterior entre grupos precisa ser revertida.

Efeito:

- o sistema reconstrui o estado anterior com base no historico do mapa manual de descricoes
- nao faz apenas exclusao cega de regra
- o pipeline e reprocessado

## Tela `Consolidacao por selecao`

Essa tela serve para consolidar manualmente varios grupos escolhidos pelo usuario.

Use quando:

- a selecao foi feita diretamente sobre `produtos_agregados`
- voce quer juntar grupos de forma explicita

Comportamento atual:

- a grade principal recarrega o parquet real depois da conclusao
- o resultado refletido e o estado verdadeiro do backend
- nao e mais apenas uma unificacao visual local

## Arquivos principais e quando usar

### `produtos_agregados_{cnpj}.parquet`

Use para:

- ver a consolidacao final por `descricao`
- consultar `ncm_consenso`, `cest_consenso`, `gtin_consenso`
- verificar `requer_revisao_manual`

### `produtos_indexados_{cnpj}.parquet`

Use para:

- ver todas as combinacoes originais do produto
- auditar `codigo`, `descricao`, `descr_compl`, `tipo_item`, `ncm`, `cest`, `gtin`
- verificar unidades e fontes sem precisar separar manualmente

### `codigos_multidescricao_{cnpj}.parquet`

Use para:

- localizar o residual de revisao
- identificar codigos com mais de uma descricao

### `base_detalhes_produtos_{cnpj}.parquet`

Use para:

- rastrear cada linha da base final
- conferir como o item ficou depois de mapa manual e consenso

## Arquivos de auditoria

### Auditoria por item

- `mapa_auditoria_agregados_{cnpj}.parquet`
- `mapa_auditoria_desagregados_{cnpj}.parquet`

Use para:

- ver decisoes manuais linha a linha
- conferir `codigo_original`, `descricao_original`, `codigo_novo`, `descricao_nova` e `acao_manual`

Observacao:

- esses mapas sao arquivos de auditoria
- em `Visualizar Tabelas`, eles devem ser lidos como parquet comum
- nao devem ser confundidos com acoes operacionais da tela residual

### Auditoria por descricao

- `mapa_manual_descricoes_{cnpj}.parquet`
- `mapa_manual_descricoes_historico_{cnpj}.parquet`
- `mapa_auditoria_descricoes_{cnpj}.parquet`
- `mapa_auditoria_descricoes_aplicadas_{cnpj}.parquet`
- `mapa_auditoria_descricoes_bloqueadas_{cnpj}.parquet`

Use para:

- ver regras de uniao por descricao
- ver regras bloqueadas por `MANTER_SEPARADO`
- entender porque uma unificacao nao foi aplicada
- reconstruir historico de decisao entre grupos

## Como decidir mais rapido

### Regra pratica 1

Se o problema e so variacao textual do mesmo produto:

- use `Consolidar`

### Regra pratica 2

Se o mesmo codigo representa itens diferentes:

- use `Separar`

### Regra pratica 3

Se dois grupos diferentes devem convergir para a mesma descricao final:

- use `Decisao entre grupos` -> `Unir grupos selecionados`

### Regra pratica 4

Se dois grupos nao podem convergir:

- use `Decisao entre grupos` -> `Manter grupos separados`

### Regra pratica 5

Se uma decisao anterior foi errada:

- use `Desfazer decisao` nas telas de `Consolidar` ou `Separar`
- ou `Desfazer regras entre selecionados` na tela `Decisao entre grupos`

## Ordem de consulta recomendada

Quando houver duvida, consulte nesta ordem:

1. `codigos_multidescricao_{cnpj}.parquet`
2. `produtos_indexados_{cnpj}.parquet`
3. `base_detalhes_produtos_{cnpj}.parquet`
4. `mapa_auditoria_descricoes_bloqueadas_{cnpj}.parquet`
5. `mapa_manual_descricoes_historico_{cnpj}.parquet`
6. `mapa_auditoria_agregados_{cnpj}.parquet`
7. `mapa_auditoria_desagregados_{cnpj}.parquet`

## Resumo operacional

- o sistema consolida automaticamente por `descricao`
- a revisao humana ocorre apenas no residual relevante
- `Consolidar` corrige codigos com descricoes equivalentes
- `Separar` corrige codigos reutilizados para produtos distintos
- `Decisao entre grupos` controla unioes, bloqueios e reversoes entre grupos inteiros
- `produtos_indexados` e a principal tabela de rastreabilidade fina
- o historico de regras por descricao permite desfazer decisoes reconstruindo o estado anterior
