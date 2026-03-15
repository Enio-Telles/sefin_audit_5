# SEFIN Audit Tool 🔍

Uma ferramenta técnica avançada de auditoria fiscal desenvolvida para a **Secretaria de Estado de Finanças (SEFIN)**. Projetada com uma arquitetura de alta performance focada na extração, manipulação e análise vetorial/volumétrica de dados fiscais (Oracle/SPED) combinando o ecossistema Node.js (orquestração e interface) com Python (processamento de dados brutos e Machine Learning).

## 🏗️ Arquitetura do Sistema

O sistema utiliza uma arquitetura híbrida de múltiplos serviços otimizada para lidar com grandes volumes de dados sem onerar a Thread Pool do Node.js:

1. **Frontend (Client)**:
   - SPA construída com **React 19**, **TypeScript**, **Vite**, **Tailwind CSS v4** e **shadcn/ui** (baseado em Radix UI).
   - Gerenciamento de estado global e cache com **TanStack React Query**, comunicação fortemente tipada via **tRPC**.
   - Design System Minimalista ("Night Ledger") focado na ergonomia do auditor, com tema escuro nativo e `framer-motion` para micro-interações.

2. **Backend Gateway e Orquestrador (Node.js)**:
   - Servidor **Express.js** que atua como API Gateway, servindo rotas tRPC sob `/api/trpc`.
   - Gerenciamento de metadados transacionais (perfil de usuários, configurações de extração, relatórios) utilizando **Drizzle ORM** com suporte nativo a SQLite/MySQL.
   - Proxy reverso (Reverse Proxy) nativo em stream para rotas que exigem processamento pesado (`/api/python/*`).

3. **Backend de Processamento de Dados (Python/FastAPI)**:
   - Microsserviço robusto em **Python 3.12+** rodando **FastAPI** / **Uvicorn**.
   - **Polars**: Engine principal (substituindo o Pandas) para data wrangling, cruzamentos FFI-boundless e conversão de DataFrames em arquivos `.parquet` para máxima eficiência I/O.
   - Conectividade direta com DW Corporativo usando **oracledb** e geração vetorial de similaridade semântica utilizando o modelo local `SentenceTransformer`.

---

## 🔄 Fluxo Node → Python (Reverse Proxy)

Para isolar o processamento pesado de dados operacionais do Event Loop do Node.js, foi implementada uma ponte em Proxy Streaming:

1. O cliente requisita um processamento (ex: `/api/python/extract-sped`) via fetch/Axios.
2. O middleware no `server/_core/index.ts` intercepta qualquer request com prefixo `/api/python/*`.
3. O Node.js atua como um Proxy transparente: clona os headers (removendo `host`), converte verbos HTTP e faz um *Pipe* do buffer de stream `req` do Express para um `fetch()` apontando para a porta interna do FastAPI (padrão: `8001`).
4. A resposta (seja um JSON de status ou um Blob de arquivo Excel gerado sob demanda) é parseada via `response.arrayBuffer()` e retornada via `res.send()`, mantendo o content-type intacto.

---

## 📦 Dependências Necessárias

Para configurar o ambiente de desenvolvimento, você precisará dos seguintes artefatos instalados e em seu `PATH`:

- **Node.js** (`v18.0.0` ou superior)
- **pnpm** (Gerenciador de pacotes otimizado: `npm install -g pnpm`)
- **Python** (`3.11` ou superior, recomendado `3.12`)
- **Oracle Instant Client** (configurado na variável de ambiente de sistema para o driver `oracledb` conectar ao banco SEFIN).

---

## 🚀 Instruções Completas de Instalação

A aplicação foi desenhada para orquestrar sua própria inicialização de forma autônoma através do `start.js`.

### 1. Clone o repositório
```bash
git clone <url-do-repositorio>
cd sefin-audit-tool
```

### 2. Inicialização Automática (Start Script)
O script automatiza a instalação das dependências Node e Python, criação do arquivo `.env` e inicialização do SQLite via Drizzle.

```bash
# Executa as validações, instala dependências e inicia os serviços em paralelo
node start.js

# Ou alternativamente, via package.json:
pnpm run start:all
```

*Nota: Em ambientes controlados, você pode instalar manualmente via `pnpm install` e `python -m pip install -r requirements.txt`, rodando as migrações via `pnpm db:push` e os servidores separadamente (`pnpm dev` e `uvicorn api:app --port 8001`).*

---

## 📂 Estrutura de Diretórios

O *monorepo* está organizado nas seguintes camadas:

- `client/`: Todo o código Frontend React. Contém `src/components` (shadcn/ui), `src/pages` (rotas wouter), e `src/lib` (utilitários e tRPC client).
- `server/`: Backend unificado.
  - `server/_core/`: Orquestrador Express, proxy Node->Python, integração Vite (para modo dev) e provedores OAuth.
  - `server/routers/`: Controladores e definições de rotas do tRPC.
  - `server/python/`: Microsserviço FastAPI (`api.py`), manipulação de DB Oracle e lógica vetorial.
- `shared/`: Tipos TypeScript (`*.ts`) e esquemas de validação (Zod) compartilhados dinamicamente entre Client e Server.
- `drizzle/`: Definição de Schemas (`schema.ts`) e arquivos de migração relacional.
- `cruzamentos/`: Diretório persistente local destinado ao armazenamento em cache e output de arquivos gerados (arquivos Parquet de grandes extrações, logs).

---

## ⚙️ Variáveis de Ambiente Necessárias

Crie um arquivo `.env` na raiz do projeto contendo as variáveis obrigatórias. *Caso não exista, o `start.js` criará um default automaticamente.*

```env
# Configurações do Banco de Dados (Metadados/SQLite/MySQL)
DATABASE_URL=file:./sefin_audit.db

# Configurações de Rede
PORT=3000                     # Porta do Gateway Node.js/Frontend
PYTHON_API_PORT=8001          # Porta interna do microsserviço Python

# Autenticação e Criptografia
JWT_SECRET=sua_chave_secreta_aqui # Obrigatório em produção para cookies seguros
OAUTH_SERVER_URL=http://localhost:3000/mock-oauth
VITE_OAUTH_PORTAL_URL=http://localhost:3000/mock-oauth

# Configurações do Frontend
VITE_APP_ID=sefin-audit-tool

# Opcional - Credenciais Oracle (Dependente de configuração do OS)
# ORACLE_USER=...
# ORACLE_PASSWORD=...
# ORACLE_DSN=...
```
