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

## 🔄 Fluxo Node → Python (Reverse Proxy)

Para isolar o processamento pesado de dados operacionais do Event Loop do Node.js, foi implementada uma ponte em Proxy Streaming:

1. O cliente requisita um processamento pesado via fetch/Axios.
2. O middleware no `server/_core/index.ts` intercepta qualquer request com prefixo `/api/python/*`.
3. O Node.js atua como um Proxy transparente, fazendo um *Pipe* do buffer de stream `req` do Express para um `fetch()` apontando para a porta interna do FastAPI (padrão: `8001`).
4. A resposta é parseada e retornada via `res.send()`, mantendo o content-type intacto.

## 📦 Pré-requisitos e Instalação

Para configurar o ambiente de desenvolvimento, você precisará dos seguintes artefatos instalados e em seu `PATH`:

- **Node.js** (`v18.0.0` ou superior)
- **pnpm** (`npm install -g pnpm`)
- **Python** (`3.11` ou superior, recomendado `3.12`)
- **Oracle Instant Client** (configurado no sistema para o driver `oracledb` conectar ao banco SEFIN).

### Instalação e Execução (Desenvolvimento)

A aplicação foi desenhada para orquestrar sua própria inicialização de forma autônoma através do `start.js`.

1. **Clone o repositório:**
```bash
git clone <url-do-repositorio>
cd sefin-audit-tool
```

2. **Inicialização Automática (Start Script):**
O script automatiza a instalação das dependências Node e Python, criação do arquivo `.env` e inicialização do banco SQLite local via Drizzle. **Este é o setup OFICIAL do projeto**, projetado para garantir máxima reprodutibilidade e isolamento.

```bash
# Executa as validações, instala dependências e inicia os serviços em paralelo
node start.js

# Ou alternativamente, via package.json:
pnpm run start:all
```

*Nota: Em ambientes controlados, você pode instalar manualmente via `pnpm install` e `python -m pip install -r requirements.txt`, rodando as migrações via `pnpm db:push` e os servidores separadamente (`pnpm dev` para o Node/Vite e `uvicorn api:app --port 8001` no diretório `server/python/`).*

## ⚙️ Configuração de Ambiente

Crie um arquivo `.env` na raiz do projeto contendo as variáveis obrigatórias. **Se o arquivo não existir, o script `start.js` criará um `.env` mínimo automaticamente com os valores padrão.**

Este é o setup oficial gerado:

```env
# ==============================================================================
# SEFIN Audit Tool - Configuração de Ambiente
# ==============================================================================
# Este é o setup OFICIAL do projeto.
# As variáveis nesta seção são geradas automaticamente pelo `start.js`.
# Para configurações avançadas/opcionais, consulte o arquivo `.env.example`.

DATABASE_URL=file:./sefin_audit.db
PYTHON_API_PORT=8001
PORT=3000
OAUTH_SERVER_URL=http://localhost:3000/mock-oauth
VITE_OAUTH_PORTAL_URL=http://localhost:3000/mock-oauth
VITE_APP_ID=sefin-audit-tool
VITE_ANALYTICS_ENDPOINT=mock-endpoint
VITE_ANALYTICS_WEBSITE_ID=mock-id
JWT_SECRET=local_dev_secret_12345678
```

> **Quando editar o `.env` manualmente?**
> Você só precisa editar este arquivo se precisar de configurações avançadas (como configurar o `NODE_ENV=production`), se quiser usar um `JWT_SECRET` forte para segurança extra em produção, ou para alterar portas caso as padrões (3000 e 8001) já estejam em uso. O ambiente lida com as credenciais do banco Oracle dinamicamente via interface Web.

> **Variáveis Legadas**: Versões anteriores do projeto usavam variáveis fixas como `ORACLE_HOST`, `DB_USER` ou `PG_HOST` no arquivo `.env`. Essas variáveis **não fazem mais parte do fluxo principal Web** e foram removidas do arquivo oficial e do `.env.example` para simplificar a inicialização e evitar confusões de ambiente. Se você precisar do contexto legado para rodar scripts avulsos antigos (`src/extracao/`), consulte o histórico do git para recuperar essas configurações.

## 📂 Estrutura de Diretórios

O projeto está organizado nas seguintes camadas principais:

- `client/`: Todo o código Frontend React (shadcn/ui, tRPC client, páginas).
- `server/`: Backend unificado.
  - `server/_core/`: Orquestrador Express, proxy Node->Python e integração Vite.
  - `server/routers/`: Controladores e definições de rotas do tRPC.
  - `server/python/`: Microsserviço FastAPI (`api.py`), manipulação de DB Oracle e lógica com Polars.
- `shared/`: Tipos TypeScript (`*.ts`) e esquemas de validação (Zod) compartilhados entre Client e Server.
- `drizzle/`: Definição de Schemas e migrações do banco de metadados.
- `cruzamentos/`: Armazenamento em cache e output de arquivos (Parquet, resultados de extrações).
- `src/`: *Contexto de Legado* - Contém código antigo (como interfaces em PySide6 e scripts CLI autônomos). Esta arquitetura transicional não faz parte do fluxo web principal e é mantida apenas para referência ou migração pontual de rotinas.

## ⚠️ Observações e Limitações

- **Múltiplos Bancos de Dados:** Lembre-se que dados de negócio e extrações pesadas provêm do Oracle e são salvos como Parquet, enquanto os metadados do sistema (logins, histórico de relatórios) residem no SQLite gerido via Drizzle.
- **Isolamento de Performance:** Operações que travam a CPU e manipulam DataFrames **devem** ser alocadas no backend Python e chamadas assincronamente (ex: usando `BackgroundTasks` no FastAPI) para não impactar o tempo de resposta da API principal.
- **Legado:** O antigo "Fiscal Parquet Analyzer" (executado via terminal ou PySide6) encontra-se deprecado na arquitetura atual de microserviços baseada em web. O fluxo oficial de uso é acessar a aplicação pelo navegador em `http://localhost:3000`.
