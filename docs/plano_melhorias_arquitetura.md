# Plano de melhorias arquiteturais do SEFIN Audit 5

## Objetivo

Evoluir o sistema de uma ferramenta funcional de auditoria para uma plataforma auditável, rastreável, modular e segura para extração, processamento e análise fiscal.

## Entregas já iniciadas

### 1. Rastreabilidade de execução

Arquivo: `server/python/core/execution_trace.py`

Inclui:
- geração de `execution_id`
- trilha por etapas
- captura de artefatos gerados
- persistência em `execution_summary.json` e `execution_events.jsonl`

### 2. Catálogo de consultas SQL

Arquivo: `server/python/services/query_catalog.py`

Inclui:
- varredura recursiva de arquivos `.sql`
- enumeração de parâmetros
- classificação por categoria
- resumo consolidado para uso pela API

### 3. Metadados de auditoria no banco

Arquivo: `drizzle/0002_audit_execution.sql`

Inclui:
- `audit_executions`
- `audit_execution_events`
- `audit_artifacts`

## Próximos passos recomendados

### Fase 1 — Integração mínima

1. Integrar `execution_trace.py` às rotas de extração Oracle.
2. Integrar o catálogo às rotas de filesystem e listagem de consultas.
3. Executar a migração SQL no bootstrap do sistema.
4. Registrar status, parâmetros e artefatos no SQLite.

### Fase 2 — Modularização Python

Criar camadas:
- `routers/` para HTTP
- `services/` para casos de uso
- `repositories/` para Oracle, Parquet, filesystem e SQLite
- `domain/` para regras fiscais

Prioridades:
- `oracle.py`
- `filesystem.py`
- `parquet.py`

### Fase 3 — Governança de artefatos

Cada execução deve registrar:
- CNPJ
- usuário
- parâmetros
- nome e hash da consulta SQL
- arquivos gerados
- duração por etapa
- erros
- versão do código

### Fase 4 — Catálogo lógico de dados fiscais

Formalizar estruturas canônicas para:
- item fiscal
- documento fiscal
- produto consolidado
- unidade e fator de conversão

### Fase 5 — Testes

Adicionar:
- testes unitários de utilitários e serviços
- testes de integração das rotas Python
- massa de teste fiscal mínima com parquets sintéticos

## Resultado esperado

Ao final da implementação, o sistema deverá:
- permitir rastreabilidade completa das execuções
- reduzir acoplamento entre rotas e lógica de negócio
- facilitar auditoria técnica e manutenção
- dar base para evolução do módulo de produtos, unidades e cruzamentos fiscais
