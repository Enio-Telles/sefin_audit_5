# Analise da `versao_11` e incorporacoes no projeto atual

## Escopo analisado
- `C:\Users\eniot\OneDrive - SECRETARIA DE ESTADO DE FINANCAS\Desenvolvimento\versao_11\agrupamento.py`
- `C:\Users\eniot\OneDrive - SECRETARIA DE ESTADO DE FINANCAS\Desenvolvimento\versao_11\conversao.py`
- `C:\Users\eniot\OneDrive - SECRETARIA DE ESTADO DE FINANCAS\Desenvolvimento\versao_11\services\analise_service.py`
- `C:\Users\eniot\OneDrive - SECRETARIA DE ESTADO DE FINANCAS\Desenvolvimento\versao_11\docs\GUIA_TECNICO_AGRUPAMENTO.md`

## Pontos fortes da `versao_11`
- A logica de agrupamento usa duas fases bem definidas:
  - Fase vetorizada para matches exatos de alta confianca.
  - Fase fuzzy dentro de blocos fiscais menores.
- O conceito de `GTIN Golden Key` reduz falsos positivos quando GTIN, NCM e CEST convergem.
- A separacao entre `discrepancias`, `duplicidades` e `coincidencias` melhora a priorizacao operacional.
- O modulo de conversao nao so calcula fatores; ele tambem diagnostica fragilidades:
  - fatores extremos;
  - fatores invalidos;
  - excesso de unidades por produto;
  - alta variacao entre fatores;
  - inconsistencias estatisticas.

## Fragilidades observadas na `versao_11`
- Parte relevante da regra de negocio ainda fica misturada com fluxos de interface e operacao manual.
- O estado de caches e regras ignoradas e bastante disperso, o que dificulta previsibilidade.
- Algumas heuristicas fortes dependem do contexto da tela e nem sempre ficam reutilizaveis em API/servico.
- O diagnostico de fatores e rico, mas pouco desacoplado para reaproveitamento externo.

## Situacao do projeto atual
- O projeto atual ja tem uma base melhor desacoplada em `server/python/core/produto_runtime.py`.
- A classificacao de pares similares existia, mas ainda sem explicitar a fase "match exato forte" da `versao_11`.
- O tratamento de fatores estava focado em importacao/atualizacao de parquet, sem um diagnostico formal de fragilidade.

## O que foi incorporado aqui
- Reforco da heuristica de unificacao em `server/python/core/produto_runtime.py`:
  - `GTIN Golden Key` agora promove uniao automatica elegivel quando GTIN valido, NCM e CEST sao identicos.
  - `NCM + CEST` identicos agora elevam o score e a recomendacao de unificacao sugerida.
- Criacao de diagnostico de fatores em `server/python/core/factor_diagnostics.py` com regras inspiradas na `versao_11`:
  - fator invalido;
  - fator extremo alto;
  - fator extremo baixo;
  - unidade de origem vazia;
  - muitas unidades por produto/ano;
  - alta variacao entre fatores do mesmo produto/ano.
- Exposicao do diagnostico via API:
  - `GET /api/python/fatores/diagnostico?cnpj=...`
- Tipagem e cliente adicionados em `client/src/lib/pythonApi.ts`.

## Resultado pratico esperado
- Menos pares "bons" caindo em revisao manual quando ha convergencia fiscal forte.
- Melhor capacidade de identificar fragilidades estruturais em fatores antes de recalcular estoque/custo medio.
- Base mais preparada para futuras telas de auditoria de fatores sem depender de heuristicas escondidas na UI.
