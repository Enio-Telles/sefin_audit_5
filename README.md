# Fiscal Parquet Analyzer

Projeto em Python com interface gráfica usando PySide6, voltado para manipulação, extração de dados e processamento de arquivos de alta performance (Parquet) e banco de dados Oracle.

## Funcionalidades Principais

- **Visualização de Parquet e Integração Banco de Dados**: Consulta arquivos Parquet localizados ou no banco de dados, lidando com NFe, NFCe, C170, e outros dados fiscais.
- **Parametrização Dinâmica de SQL**: Extração e identificação automática de parâmetros (`:parametro`) a partir de arquivos e textos de consultas SQL, permitindo preenchimento através da interface gráfica com validação.
- **Exportação para Excel**: Geração de relatórios através de Pandas e Polars para arquivos Excel formatados.
- **Análise de Fator de Conversão e Códigos Mercadoria**: Mecanismos customizados de hashing (`MD5`) e tratativas de deduplicação usando as funções otimizadas do pacote `polars`.

## Instalação e Execução

O ambiente e as dependências estão gerenciados e documentados (via `.env.example`).
Execute a aplicação via terminal usando:

```bash
python3 app.py
```

## Execução dos Testes

O projeto utiliza o `pytest` para rodar os testes unitários.
Certifique-se de executar no diretório raiz do projeto definindo a variável `PYTHONPATH`:

```bash
PYTHONPATH=./funcoes_auxiliares:. python3 -m pytest
```

Isso garante que todos os módulos (incluindo testes que fazem mock e testam funções auxiliares de extração e tabelas) sejam corretamente resolvidos.
