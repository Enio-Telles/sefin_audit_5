# SEFIN Audit Tool 🔍

Uma ferramenta técnica de auditoria fiscal desenvolvida para a **Secretaria de Estado de Finanças (SEFIN)**. Projetada com uma arquitetura de alta performance focada na extração, manipulação e análise vetorial/volumétrica de dados fiscais (Oracle/SPED) combinando o ecossistema Node.js (orquestração e interface) com Python (processamento de dados brutos e Machine Learning).

## 📊 Visão Geral

O sistema permite a extração de dados fiscais diretamente do DW Corporativo da SEFIN e realiza o processamento pesado desses dados, estruturando-os de forma eficiente em arquivos Parquet organizados por CNPJ. Através de uma interface web, auditores podem configurar extrações, visualizar análises e gerar relatórios.

> **⚠️ Status do Projeto em Transição:** O repositório encontra-se em uma transição de arquitetura. O sistema original era uma aplicação desktop construída com PySide6 e `orquestrador.py` CLI (ainda presente na pasta `src/`). A **arquitetura principal atual e ativa** é a aplicação web híbrida (descrita abaixo), que substitui a interface gráfica antiga.

## 🏗️ Arquitetura do Sistema

O sistema utiliza uma arquitetura híbrida de múltiplos serviços otimizada para lidar com grandes volumes de dados sem onerar a Thread Pool do Node.js:

1. **Frontend (Client)**:
   - SPA construída com **React 19**, **TypeScript**, **Vite**, **Tailwind CSS v4** e **shadcn/ui**.
   - Gerenciamento de estado e cache com **TanStack React Query**; comunicação via **tRPC**.
   - Design System "Night Ledger" focado na ergonomia do auditor (tema escuro nativo).

2. **Backend Gateway e Orquestrador (Node.js)**:
   - Servidor **Express.js** atuando como API Gateway (`server/_core/index.ts`).
   - Gerenciamento de metadados de autenticação e estado da sessão utilizando **Drizzle ORM** (limitado ao escopo de metadados, armazenado em SQLite local `sefin_audit.db`).
   - Proxy reverso (Reverse Proxy) nativo em stream: roteia requisições pesadas (`/api/python/*`) diretamente para o backend Python.

3. **Backend de Processamento de Dados (Python/FastAPI)**:
   - Microsserviço robusto em **Python 3.12+** rodando **FastAPI** / **Uvicorn** (`server/python/api.py`).
   - **Polars**: Engine principal para data wrangling ultra-rápido, cruzamentos sem FFI-bounds e persistência eficiente utilizando o formato `.parquet`.
   - Conectividade direta com DW Corporativo via **oracledb**.
   - *Opcional*: Geração vetorial de similaridade semântica utilizando modelo local `SentenceTransformer` / `FAISS` (se as dependências opcionais estiverem configuradas).

## 🔄 Fluxo de Execução (Reverse Proxy)

Para isolar o processamento pesado de dados operacionais do Node.js, foi implementada uma ponte via Proxy Streaming:

1. O cliente (Frontend) requisita uma operação de dados (ex: `/api/python/extract-sped`).
2. O middleware no `server/_core/index.ts` intercepta requests com prefixo `/api/python/*`.
3. O Node.js atua como Proxy: faz o repasse transparente (convertendo buffers de stream) para a porta interna do FastAPI (padrão: `8001`).
4. A resposta (JSON ou um Blob binário como Excel gerado sob demanda) é retornada mantendo o content-type.

## 📂 Estrutura Principal de Diretórios

- `client/`: Código Frontend React (Componentes, Páginas, Cliente tRPC).
- `server/`: Código Backend unificado.
  - `server/_core/`: Orquestrador Node.js Express, gateway proxy e autenticação.
  - `server/routers/`: Controladores do tRPC (para a camada Node).
  - `server/python/`: Microsserviço FastAPI e lógica de processamento Polars.
- `shared/`: Tipos TypeScript dinâmicos e esquemas (Zod) compartilhados.
- `drizzle/`: Definições de Schemas do banco de metadados SQLite.
- `CNPJ/`: Diretório persistente local para armazenamento em cache e output de arquivos Parquet e análises geradas, estruturado por número de CNPJ.
- `src/`: *(Legado)* Contém o código da antiga versão desktop PySide6 e orquestrador CLI (em processo de descontinuação).

## 📦 Requisitos e Dependências Reais

- **Node.js**: v18.0.0+
- **pnpm**: Gerenciador de pacotes para o frontend e Node backend (`npm install -g pnpm`)
- **Python**: 3.11+ (Recomendado 3.12)
- **Oracle Instant Client**: Obrigatório e deve estar configurado na variável de ambiente de sistema (`PATH`) para o driver `oracledb` conectar ao DW corporativo da SEFIN.

## 🚀 Instalação e Execução (Desenvolvimento)

A aplicação possui um script para orquestrar a inicialização do ecossistema de forma autônoma.

### 1. Clonar o repositório
```bash
git clone <url-do-repositorio>
cd sefin_audit_5
```

### 2. Inicialização Completa Automática
O script raiz gerencia a verificação de requisitos, instalação de dependências Node/Python (`pnpm` e `requirements.txt`), criação do `.env` padrão e execução dos servidores Node (Porta `3000`) e FastAPI (Porta `8001`).

```bash
# Inicia toda a stack: Node.js gateway e Python backend
node start.js
```

> Alternativamente, utilizando scripts do pacote:
> ```bash
> pnpm run start:all
> ```

### 3. Execução Manual / Separada (Caso necessário)
Se preferir rodar os serviços isolados:
1. Instale as dependências: `pnpm install` e `python -m pip install -r requirements.txt`.
2. Configure o banco SQLite: `pnpm db:push`
3. Inicie o Frontend + Node Gateway: `pnpm dev`
4. Inicie o Python API (em outro terminal): `cd server/python && uvicorn api:app --port 8001`

## ⚠️ Observações Importantes

- **Drizzle e SQLite**: São utilizados **exclusivamente** para controle de sessão, perfis de usuários (auth) e pequenos metadados da ferramenta. Não armazenam os dados operacionais ou fiscais brutos das consultas (estes ficam em arquivos `.parquet` dentro da pasta `CNPJ/`).
- **Análises Vetoriais**: O uso do `SentenceTransformer` e `faiss` é opcional e modular. Caso as dependências (`numpy`, `faiss-cpu`) não estejam presentes no ambiente ou o hardware não suporte bem, o sistema tem *fallbacks* para similaridade por sequências textuais comuns (`difflib`).
- **Tratamento de Arquivos**: O diretório `CNPJ/` pode crescer consideravelmente de acordo com o volume de extrações Oracle realizadas. Monitore o disco local.
