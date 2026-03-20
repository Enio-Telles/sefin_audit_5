# Criar `tabela_itens_caracteristicas.parquet`

### Mapeamento de Colunas por Fonte

| Campo destino   | NFe / NFCe                         | C170 (c170_simplificada) | bloco_h (via reg_0200) |
| --------------- | ---------------------------------- | ------------------------ | ---------------------- |
| `codigo`      | `prod_cprod`                     | `cod_item`             | `codigo_produto`     |
| `descricao`   | `prod_xprod`                     | `descr_item`           | `descricao_produto`  |
| `descr_compl` | —                                 | `descr_compl`          | —                     |
| `tipo_item`   | —                                 | `tipo_item`            | `tipo_item`          |
| `ncm`         | `prod_ncm`                       | `cod_ncm`              | `cod_ncm`            |
| `cest`        | `prod_cest`                      | `cest`                 | `cest`               |
| `gtin`        | `prod_ceantrib` OU `prod_cean` | `cod_barra`            | `cod_barra`          |
| `unidade`     | `prod_ucom`                      | `unid`                 | `unidade_medida`     |


> **NFe/NFCe**: somente linhas onde `co_emitente = cnpj`
> **C170**: arquivo `c170_simplificada_<cnpj>.parquet`
> **bloco_h**: arquivo `bloco_h_<cnpj>.parquet` 
>
> compras = preço de compra
>
> venda = preço de venda

### Lógica de `chave_item_individualizado`

A chave é o hash `MD5` dos campos `[codigo, descricao, descr_compl, tipo_item, ncm, cest, gtin]` após normalização (strip, upper). Isso permite identificar unicamente uma combinação de características, independente da fonte.

### `lista_unidades`

Após a agregação dos registros com mesma `chave_item_individualizado`, as diferentes unidades de medida encontradas são consolidadas numa lista ordenada e deduplicada no campo `lista_unidades` (ex: `["KG", "PC"]`).

---

### Estrutura de pastase arquivo

#### [NEW] [tabela_itens_caracteristicas.py](file:///c:/funcoes/funcoes_tabelas/tabela_produtos/tabela_itens_caracteristicas.py)

Função principal: `gerar_tabela_itens_caracteristicas(cnpj: str, pasta_cnpj: Path) -> bool`

#### Diretório de saída

Os arquivos são salvos em `c:\funcoes\CNPJ\<cnpj>\analises\tabela_itens_caracteristicas_<cnpj>.parquet`

---

### Estratégia Polars (performance)

1. **Leitura lazy**: `pl.scan_parquet(...)` para cada fonte — zero custo antes de materializar.
2. **Projeção antecipada**: selecionar apenas as colunas necessárias antes de qualquer join/union (`select([...])` na query lazy).
3. **Filtragem early**: `filter(pl.col("co_emitente") == cnpj)` em NFe/NFCe antes de qualquer operação.
4. **União vertical**: `pl.concat([df_nfe, df_nfce, df_c170, df_h], how="diagonal_relaxed")` — aceita schemas diferentes, alinhando por nome de coluna.
5. **Chave via hashing**: `pl.concat_str` + `map_elements(hashlib.md5)` para gerar `chave_item_individualizado`.
6. **Deduplicação + agregação**: `group_by("chave_item_individualizado").agg(...)` com `pl.first()` para os campos fixos e `pl.col("unidade").drop_nulls().unique().sort()` para a lista de unidades.
7. **Coleta e gravação**: `.collect()` + [salvar_para_parquet](file:///c:/funcoes/funcoes_auxiliares/salvar_para_parquet.py#5-78).

## Verification Plan

### Teste Manual

```powershell
python c:\funcoes\funcoes_tabelas\tabela_produtos\tabela_itens_caracteristicas.py 37671507000187
```

Verificar que o arquivo `analises\tabela_itens_caracteristicas_37671507000187.parquet` é criado corretamente com as colunas esperadas e sem linhas duplicadas.

SSS

***No projeto localizado em:***

C:\Users\03002693901\OneDrive - SECRETARIA DE ESTADO DE FINANCAS\Desenvolvimento\sistema_monitoramento

reaproveite a interface gráfica já existente como base principal da solução. Expanda essa interface para que ela permita executar consultas, visualizar resultados e exportar as tabelas geradas para Excel.

Além disso, implemente um mecanismo para extrair automaticamente os parâmetros das consultas SQL, exibi-los ao usuário e permitir seu preenchimento de forma amigável na interface.

Para isso, aproveite funções e conceitos do @beautifulMention , ou solução equivalente já compatível com a arquitetura do projeto, para:

identificar parâmetros presentes no texto SQL, como :CNPJ, :data_inicial, :data_final, :data_limite_processamento;

destacar visualmente esses parâmetros na consulta;

listar os parâmetros extraídos em uma área lateral, painel ou formulário dinâmico;

permitir edição dos valores dos parâmetros antes da execução da consulta;

sincronizar os parâmetros detectados no SQL com os campos exibidos na interface;

atualizar automaticamente a lista de parâmetros quando o texto da consulta for alterado.

A interface deve permitir:

Selecionar ou abrir consultas SQL já existentes no projeto;

Visualizar a consulta SQL em área apropriada;

Extrair automaticamente os parâmetros da consulta;

Exibir os parâmetros encontrados em formulário dinâmico;

Permitir preenchimento e edição desses parâmetros pelo usuário;

Executar a consulta com os parâmetros informados;

Visualizar os resultados em tabela, com:

rolagem,

ordenação,

filtro,

busca textual,

ajuste de largura das colunas;

Selecionar qual tabela gerada deseja visualizar;

Exportar os resultados para Excel (.xlsx);

Exibir mensagens claras de:

carregamento,

sucesso,

erro,

ausência de resultados.

Requisitos técnicos

Reaproveitar ao máximo a GUI já existente;

Separar claramente:

camada de interface,

camada de leitura/interpretação do SQL,

camada de execução da consulta,

camada de exportação para Excel;

Criar uma função ou serviço específico para parse de parâmetros SQL;

Garantir que o parser reconheça parâmetros no padrão Oracle com :nome_parametro;

Evitar duplicidade de parâmetros quando o mesmo nome aparecer mais de uma vez na consulta;

Permitir definição de tipo de campo conforme o nome do parâmetro, por exemplo:

datas → campo de data,

CNPJ → campo textual ou mascarado,

valores numéricos → campo numérico;

Manter a interface responsiva durante consultas demoradas;

Garantir que a exportação funcione para tabelas grandes;

Preparar a arquitetura para futura inclusão de novos tipos de consultas e novos filtros.

Objetivo final

Transformar a interface gráfica existente em um painel operacional completo, no qual o usuário possa:

abrir ou selecionar consultas SQL,

visualizar o SQL,

identificar automaticamente seus parâmetros,

informar os valores desses parâmetros,

executar a consulta,

visualizar os resultados,

exportar os resultados para Excel.

Versão mais técnica para Codex

No projeto sistema_monitoramento, reutilize a interface gráfica existente como frontend principal da aplicação. Implemente suporte completo para execução de consultas SQL parametrizadas, visualização de resultados e exportação para Excel.

A solução deve incluir um módulo de extração de parâmetros SQL a partir do texto da consulta. Esse módulo deve identificar parâmetros no padrão :nome_parametro, eliminar duplicidades e retornar uma estrutura utilizável pela GUI.

A interface deve usar recursos inspirados em @beautifulMention ou integrá-lo diretamente, se fizer sentido na stack atual, para enriquecer a experiência de edição/visualização do SQL, permitindo destacar visualmente os parâmetros detectados e sincronizá-los com um formulário dinâmico de entrada de valores.

Funcionalidades esperadas

carregar ou selecionar consultas SQL existentes;

exibir o texto SQL em componente de edição ou visualização;

detectar automaticamente parâmetros nomeados no SQL;

destacar os parâmetros dentro do editor;

gerar automaticamente formulário com os parâmetros extraídos;

permitir edição dos valores antes da execução;

executar a consulta com bind parameters;

exibir o resultado em tabela com ordenação, filtro, busca e rolagem;

exportar o resultado atual para .xlsx.

Requisitos de implementação

criar serviço dedicado para parse de parâmetros SQL;

usar regex ou parser simples confiável para capturar tokens no padrão :parametro;

ignorar repetições do mesmo parâmetro;

possibilitar metadados por parâmetro, como:

nome,

valor,

tipo inferido,

obrigatório/opcional;

manter separação entre:

UI,

parser de SQL,

executor de consulta,

exportador Excel;

evitar travamento da interface em operações pesadas;

reaproveitar componentes já existentes no projeto sempre que possível.

Resultado esperado

A GUI existente deve se tornar a interface principal para:

selecionar consultas,

enxergar os parâmetros que a consulta exige,

preencher esses parâmetros de forma amigável,

executar a consulta,

visualizar os resultados,

exportar os dados para Excel.

Trecho extra para deixar o Codex ainda mais assertivo

Implemente também uma função semelhante a:

extract_sql_parameters(sql: str) -> list[dict]

Que retorne algo como:

[

  {"name": "CNPJ", "type": "text", "required": True},

  {"name": "data_inicial", "type": "date", "required": False},

  {"name": "data_final", "type": "date", "required": False},

  {"name": "data_limite_processamento", "type": "date", "required": False}

]

A inferência de tipo pode ser baseada no nome do parâmetro.
