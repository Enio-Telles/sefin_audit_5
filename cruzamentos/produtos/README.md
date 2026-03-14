# Processamento de Produtos e Unificação (SEFIN Audit Tool)

Este módulo realiza a consolidação de dados de produtos provenientes de múltiplas fontes fiscais (Bloco H, Registro 0200, NFe e NFCe), utilizando a **Descrição** como chave primária de unidificação.

## Arquivos Principais

- **`produto_unid.py`**: Fachada pública do módulo e ponto de entrada compatível com o restante do projeto.
- **`_produto_unid_shared.py`**: Schemas, mapeamentos, normalização e utilitários compartilhados.
- **`_produto_unid_analytics.py`**: Consensos, agrupamentos e geração das tabelas analíticas.
- **`_produto_unid_manual.py`**: Aplicação do mapa manual e geração dos parquets de auditoria manual.
- **`_produto_unid_pipeline.py`**: Orquestração do fluxo de leitura, processamento e persistência.
- **`README.md`**: Documentação técnica do módulo.

## Conceitos Chave

### 1. Assinatura de Identidade do Produto

Para o sistema, um "Produto" único é definido pelo conjunto das seguintes características:

- Descrição Normalizada
- NCM
- CEST
- GTIN (EAN)
- Tipo de Item (SPED)
- Descrição Complementar (C170)

### 2. Ambiguidade de Código (`requer_revisao_manual`)

O script identifica se um `codigo` (original do item) é "ambíguo", ou seja, se ele é utilizado para produtos com características ou descrições diferentes em qualquer ponto da base de dados. Quando uma `descricao` agrupada possui pelo menos um código ambíguo, ela é marcada com `requer_revisao_manual = True`.

## Fluxo de Processamento

1. **Extração e Normalização**: Leitura de Parquets (Bloco H, C170, NFe, NFCe) com limpeza de acentos e caracteres especiais.
2. **Cálculo de Ambiguidade**: Identifica códigos que referenciam múltiplos perfis de itens distintos.
3. **Agrupamento e Consenso**: Agrupa os dados pela `descricao`. Calcula a **Moda** (valor mais frequente) para definir os campos de consenso (NCM, CEST, GTIN, Unidade, etc.).
4. **Persistência**: O resultado consolidado é salvo no diretório de análises do CNPJ.

## Tabelas e Campos Gerados

### 1. Master de Produtos (`base_detalhes_produtos_{cnpj}.parquet`)

Localizado em: `CNPJ/{cnpj}/analises/`

| Campo | Descrição |
| :--- | :--- |
| **`chave_produto_unica`** | Código de consenso utilizado como identificador do grupo. |
| **`descricao`** | A descrição normalizada que serviu de base para o agrupamento. |
| **`requer_revisao_manual`** | `True` se houver ambiguidade nos códigos associados a esta descrição. |
| **`lista_codigo`** | Todos os códigos originais associados a este grupo. |
| **`lista_descricao`** | Histórico das descrições brutas (originais). |
| **`lista_ncm`** / **`lista_cest`** ... | Listas completas de todos os valores encontrados nas fontes originais. |
| **`codigo_consenso`** | Valor mais frequente entre os códigos do grupo. |
| **`ncm_consenso`** | NCM sugerido (moda). |
| **`gtin_consenso`** | GTIN sugerido (moda). |
| **`unid_consenso`** | Unidade de medida sugerida (moda). |
| **`lista_fonte`** | Fontes onde o produto foi identificado (NFe, NFCe, EFD). |

## Como Executar

O script pode ser executado passando o CNPJ como argumento:

```bash
python cruzamentos/produtos/produto_unid.py <CNPJ>
```

Ele buscará automaticamente os dados em `CNPJ/{cnpj}/arquivos_parquet/`.

