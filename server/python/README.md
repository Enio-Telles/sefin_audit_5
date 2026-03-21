# Python Backend - Data Processing Layer 🐍

Este diretório contém a lógica de alta performance para processamento de dados fiscais da **SEFIN-RO**, utilizando **FastAPI** e **Polars**.

## 🚀 Tecnologias

- **FastAPI**: Servidor web assíncrono para os endpoints de dados.
- **Polars**: Engine de processamento colunar de dados extremamente rápida, otimizada para multithreading.
- **OracleDB**: Driver robusto para conexão com o Data Warehouse da SEFIN.
- **PyArrow**: Manipulação de arquivos Parquet e suporte a tipos de dados complexos.
- **Python-Docx**: Geração dinâmica de relatórios Word em papel timbrado.

## 🛠️ Instalação das Dependências

Certifique-se de ter o Python 3.11 instalado e o Oracle Instant Client configurado no sistema.

```bash
cd server/python
pip install -r requirements.txt
```

> [!NOTE]
> Se o arquivo `requirements.txt` não existir, as principais bibliotecas necessárias são:
> `fastapi uvicorn polars oracledb pyarrow openpyxl xlsxwriter python-docx pydantic-settings`

## Dependências opcionais de vetorização

O módulo de produtos consegue rodar em três modos:

- `lexical`: sem dependências extras
- `light`: não requer dependências extras, vetorização por char n-grams usando string matching
- `faiss`: requer `sentence-transformers` e `faiss-cpu` para busca semântica

Para o ambiente Conda `audit`, instale assim:

```bash
conda run -n audit pip install -r server/python/requirements-vectorizacao.txt
```

Se `faiss-cpu` não estiver disponível no ambiente, o sistema ainda funciona com:

```bash
conda run -n audit pip install sentence-transformers
```

Nesse caso, a busca semântica entra em `NUMPY fallback`, com custo maior de CPU.

## 📡 Endpoints Principais

| Rota | Descrição |
| :--- | :--- |
| `POST /api/python/oracle/test-connection` | Valida credenciais e conectividade Oracle. |
| `POST /api/python/oracle/extract` | Executa SQL e salva resultado em Parquet por CNPJ. |
| `POST /api/python/parquet/read` | Lê arquivos Parquet com paginação, filtros e ordenação. |
| `POST /api/python/parquet/write-cell` | Edição inline de dados no arquivo Parquet. |
| `POST /api/python/reports/timbrado` | Gera relatório Word baseado em modelo institucional. |
| `POST /api/python/export/excel` | Exporta Parquet para Excel (.xlsx). |

## 🧬 Lógica de Negócio (api.py)

O arquivo `api.py` é o coração deste backend:
1. **Validação**: Verificação rigorosa de CNPJ e formatação de dados.
2. **Normalização**: Converte nomes de colunas do Oracle para minúsculas uniformes.
3. **Persistência**: Garante que os dados sejam salvos de forma eficiente em arquivos `.parquet`.
4. **Performance**: Utiliza o Polars para operações de filtragem e janelamento de dados sem travar a thread principal.

---
*SEFIN-RO: Eficiência e Tecnologia na Auditoria Fiscal.*
