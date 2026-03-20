# Fiscal Parquet Analyzer

Sistema para extração, análise e agregação de dados fiscais (Oracle -> Parquet -> UI).

## 🚀 Como Iniciar

### Pré-requisitos
- Python 3.10+
- Oracle Client / VPN ativa (para extração)
- Dependências: `pip install polars oracledb PySide6 rich python-dotenv openpyxl python-docx`

### Executando a Interface Gráfica
```bash
python src/interface_grafica/main.py
```

### Executando o Pipeline (Linha de Comando)
```bash
python -m src.orquestrador --cnpj <CNPJ>
```

## 📁 Estrutura do Projeto

- `src/`: Código fonte principal.
  - `extracao/`: Módulos de conexão Oracle e execução SQL.
  - `transformacao/`: Lógica de processamento e consolidação de dados.
    - `analise_produtos/`: Itens, Produtos e Enriquecimento.
  - `interface_grafica/`: Componentes UI (PySide6).
  - `servicos/`: Camada de serviços e regras de negócio.
  - `utilitarios/`: Funções auxiliares, validações e exportação.
- `sql/`: Consultas SQL para extração de dados brutos.
- `dados/`: Armazenamento de dados (Parquet).
  - `CNPJ/`: Dados extraídos organizados por CNPJ.
  - `referencias/`: Tabelas de referência (NCM, SEFIN, CFOP).
- `docs/`: Documentação técnica e planos de projeto.
- `workspace/`: Arquivos temporários e estado da aplicação.

## 🛠️ Tecnologias
- **Polars**: Processamento ultra-rápido de dados.
- **OracleDB**: Conectividade robusta com banco de dados.
- **PySide6**: Interface gráfica moderna.
- **Parquet**: Armazenamento eficiente de grandes volumes de dados.

## 📄 Documentação
Veja a pasta `docs/` para detalhes sobre o mapeamento de colunas e o plano de refatoração.
