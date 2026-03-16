# Especificacao Operacional: Unificacao em Lote

Status: proposta operacional para implementacao incremental na revisao final de produtos.

## Objetivo

Criar um mecanismo de unificacao em lote sobre a tabela final desagregada (`produtos_agregados_{cnpj}.parquet`), com regras explicitas, thresholds auditaveis e suporte a botoes de lote na interface.

O lote deve:
- usar a tabela final como base unica de decisao;
- respeitar o fluxo documental atual;
- usar similaridade textual apenas como apoio, nunca como criterio isolado;
- permitir regras mais conservadoras e regras mais agressivas;
- registrar trilha de auditoria da acao aplicada.

## Escopo

Esta especificacao cobre:
- matriz de regras de unificacao em lote;
- comparadores fiscais com tratamento explicito de nulos;
- construcao de propostas de lote;
- payload esperado entre frontend e backend;
- proposta de botoes em lote na revisao final.

Nao cobre nesta etapa:
- implementacao definitiva do endpoint;
- mudancas no motor de consolidacao manual;
- geracao automatica sem confirmacao do usuario.

## Base de Trabalho

- Tabela principal: `produtos_agregados_{cnpj}.parquet`
- Tela principal: `client/src/pages/RevisaoFinalProdutos.tsx`
- Motor documental atual: `server/python/core/produto_classification.py`
- Fluxo documental: `documentacao/Fluxo de Consolidacao de Produtos.md`

Observacao importante:
- O motor atual usa `metric_score()` e retorna `0.5` quando um ou ambos os campos estao vazios.
- Para unificacao em lote, isso nao e suficiente.
- O lote deve usar um comparador novo, com semantica explicita de `igual preenchido`, `igual nulo`, `conflito` e `incompleto`.

## Comparadores Fiscais

Cada comparacao entre `NCM`, `CEST` e `GTIN` deve resultar em um destes estados:

| Estado | Definicao |
|---|---|
| `EQUAL_FILLED` | Ambos preenchidos e iguais |
| `EQUAL_NULL` | Ambos vazios ou nulos |
| `CONFLICT` | Ambos preenchidos e diferentes |
| `INCOMPLETE` | Um vazio e outro preenchido |

### Funcoes base

`compare_nullable_equal(a, b)`
- `EQUAL_FILLED` quando ambos preenchidos e iguais
- `EQUAL_NULL` quando ambos vazios
- `CONFLICT` quando ambos preenchidos e diferentes
- `INCOMPLETE` quando apenas um esta preenchido

`is_equal_nullable(state)`
- verdadeiro para `EQUAL_FILLED` e `EQUAL_NULL`

`is_conflict(state)`
- verdadeiro apenas para `CONFLICT`

`filled_evidence_count(ncm_state, cest_state, gtin_state)`
- conta quantos campos estao em `EQUAL_FILLED`

## Similaridade Textual

O lote pode usar:
- `DOCUMENTAL`: similaridade textual do motor atual;
- `LIGHT`: apoio por `char n-grams`;
- `FAISS`: apoio semantico opcional.

Regra geral:
- `FAISS` e `LIGHT` priorizam e agrupam candidatos;
- a aprovacao do lote depende sempre da regra fiscal do botao.

### Campos textuais considerados

1. `descricao`
2. `lista_descricao`
3. `lista_descr_compl` como reforco, nunca como criterio isolado

### Scores sugeridos

- `score_descricao`: principal
- `score_descr_compl`: auxiliar
- `score_final_regra`: score final da proposta de lote

## Matriz de Regras

### Regra R1: Alta Confianca Fiscal Completa

Botao sugerido:
- `Unificar alta confianca`

Uso:
- regra padrao e mais segura para lote

Criterios:
- `score_descricao >= 0.78`
- `NCM` em `EQUAL_FILLED` ou `EQUAL_NULL`
- `CEST` em `EQUAL_FILLED` ou `EQUAL_NULL`
- `GTIN` em `EQUAL_FILLED` ou `EQUAL_NULL`
- `filled_evidence_count >= 2` ou `GTIN == EQUAL_FILLED`
- nenhum campo fiscal em `CONFLICT`
- nao pode haver compartilhamento de codigo entre grupos que torne a fusao ambigua

Interpretacao:
- aceita `nulo = nulo`
- bloqueia `vazio x preenchido`
- evita unir casos onde tudo e nulo

Resultado esperado:
- proposta de unificacao com confianca `HIGH`

### Regra R2: NCM + CEST Compativeis

Botao sugerido:
- `Unificar NCM + CEST`

Uso:
- segunda regra padrao, ainda conservadora

Criterios:
- `score_descricao >= 0.74`
- `NCM == EQUAL_FILLED`
- `CEST` em `EQUAL_FILLED` ou `EQUAL_NULL`
- `GTIN` em `EQUAL_FILLED` ou `EQUAL_NULL`
- `GTIN` nao pode estar em `INCOMPLETE`
- nenhum campo fiscal em `CONFLICT`

Interpretacao:
- exige `NCM` preenchido e igual
- aceita `CEST` nulo em ambos
- se um `GTIN` existir e o outro nao, sai do lote

Resultado esperado:
- proposta de unificacao com confianca `HIGH` ou `MEDIUM_HIGH`

### Regra R3: GTIN + NCM Compativeis

Botao sugerido:
- `Unificar GTIN + NCM`

Uso:
- forte quando `GTIN` e confiavel, mesmo sem `CEST`

Criterios:
- `score_descricao >= 0.68`
- `GTIN == EQUAL_FILLED`
- `NCM` em `EQUAL_FILLED` ou `EQUAL_NULL`
- `CEST` em `EQUAL_FILLED`, `EQUAL_NULL` ou `INCOMPLETE`
- nenhum campo em `CONFLICT`
- se `NCM` estiver `INCOMPLETE`, a proposta cai para revisao assistida

Interpretacao:
- `GTIN` preenchido e igual pesa muito
- `CEST` nao pode conflitar

Resultado esperado:
- proposta de unificacao com confianca `MEDIUM_HIGH`

### Regra R4: Descricao Muito Similar + NCM Igual

Botao sugerido:
- `Unificar NCM apenas`

Uso:
- regra mais agressiva, nao deve aparecer como CTA primario

Criterios:
- `score_descricao >= 0.90`
- `NCM == EQUAL_FILLED`
- `CEST` nao pode estar em `CONFLICT`
- `GTIN` nao pode estar em `CONFLICT`
- pelo menos um entre `CEST` e `GTIN` deve estar em `EQUAL_NULL`

Interpretacao:
- destinada a casos textualmente muito proximos
- boa para fila assistida, nao para lote cego

Resultado esperado:
- proposta de unificacao com confianca `MEDIUM`

### Regra R5: Revisao Assistida

Botao sugerido:
- `Mandar para revisao`

Uso:
- quando ha alta semelhanca textual, mas evidencia fiscal insuficiente para lote

Criterios:
- `score_descricao >= 0.72`
- `NCM == EQUAL_FILLED`
- sem `CONFLICT` fiscal forte
- existe ao menos um `INCOMPLETE`

Resultado esperado:
- nao unifica
- so marca o grupo como candidato para revisao humana

### Regra R6: Manter Desagregado

Botao sugerido:
- `Manter separados`

Uso:
- lote de bloqueio

Criterios:
- qualquer um dos seguintes:
  - `GTIN == CONFLICT`
  - `NCM == CONFLICT`
  - `CEST == CONFLICT` em regra dependente de `CEST`
  - conflito fiscal em 2 ou mais campos

Resultado esperado:
- grava status `MANTIDO_SEPARADO`
- impede reoferta nos botoes de unificacao conservadora

## Resumo dos Botoes de Lote

### Botoes principais

1. `Unificar alta confianca`
2. `Unificar NCM + CEST`
3. `Mandar para revisao`
4. `Manter separados`

### Botoes secundarios

1. `Unificar GTIN + NCM`
2. `Unificar NCM apenas`
3. `Ver somente elegiveis`
4. `Ver somente bloqueados`

### Recomendacao de UX

Na revisao final:
- mostrar so os 4 botoes principais por padrao;
- esconder os botoes mais agressivos em `Mais opcoes`;
- exibir contadores por regra ao lado dos botoes.

Exemplo:
- `Unificar alta confianca (42)`
- `Unificar NCM + CEST (17)`
- `Mandar para revisao (28)`
- `Manter separados (13)`

## Montagem das Propostas de Lote

### Entrada

As propostas devem ser construidas sobre:
- linhas visiveis na revisao final;
- filtros atuais de `descricao`, `NCM` e `CEST`;
- modo de agrupamento atual (`flat` ou `faiss`);
- opcionalmente, cache `LIGHT` ou `FAISS`.

### Etapas

1. Ler grupos visiveis da revisao final.
2. Construir pares candidatos.
3. Aplicar a matriz de regras nos pares.
4. Agrupar pares em componentes de unificacao.
5. Validar o componente inteiro.
6. Emitir propostas elegiveis por regra.

### Regra anti-efeito-corrente

Se `A` combina com `B` e `B` combina com `C`, o lote so pode unir `A+B+C` quando:
- todos os pares do componente forem compativeis com a mesma regra; ou
- o componente passar por checagem de compatibilidade global.

Se houver conflito interno:
- o componente nao entra como proposta unica;
- os itens seguem para revisao assistida.

### Limites iniciais sugeridos

- `max_component_size = 12`
- `require_all_pairs_compatible = true`
- `top_k = 8` para apoio `FAISS` ou `LIGHT`

## Payload Backend

### 1. Preview de propostas

Endpoint proposto:
- `POST /api/python/produtos/unificacao-lote/propostas`

Request:

```json
{
  "cnpj": "37671507000187",
  "source_context": "REVISAO_FINAL",
  "filters": {
    "descricao_contains": "whisky",
    "ncm_contains": "2208",
    "cest_contains": "0201",
    "show_verified": false
  },
  "grouping_mode": "faiss",
  "similarity_source": {
    "engine": "FAISS",
    "use_cache": true,
    "top_k": 8,
    "min_score": 0.62
  },
  "rule_ids": [
    "R1_HIGH_CONFIDENCE_FULL_FISCAL",
    "R2_NCM_CEST",
    "R6_MANTER_SEPARADO"
  ],
  "options": {
    "only_visible": true,
    "require_all_pairs_compatible": true,
    "max_component_size": 12
  }
}
```

Response:

```json
{
  "success": true,
  "cnpj": "37671507000187",
  "generated_at_utc": "2026-03-15T15:30:00Z",
  "dataset_hash": "abc123",
  "resumo": {
    "total_rows_considered": 814,
    "total_candidate_pairs": 2883,
    "total_components": 96,
    "total_proposals": 58,
    "by_rule": [
      {
        "rule_id": "R1_HIGH_CONFIDENCE_FULL_FISCAL",
        "button_label": "Unificar alta confianca",
        "proposal_count": 24,
        "group_count": 56
      },
      {
        "rule_id": "R2_NCM_CEST",
        "button_label": "Unificar NCM + CEST",
        "proposal_count": 19,
        "group_count": 41
      },
      {
        "rule_id": "R6_MANTER_SEPARADO",
        "button_label": "Manter separados",
        "proposal_count": 15,
        "group_count": 29
      }
    ]
  },
  "proposals": [
    {
      "proposal_id": "LOT_R1_0001",
      "rule_id": "R1_HIGH_CONFIDENCE_FULL_FISCAL",
      "button_label": "Unificar alta confianca",
      "confidence_band": "HIGH",
      "status": "ELEGIVEL",
      "source_method": "FAISS",
      "component_size": 3,
      "chaves_produto": ["ID_0001", "ID_0002", "ID_0045"],
      "descricao_canonica_sugerida": "WHISKY JW RED LABEL 12 1000ML",
      "lista_descricoes": [
        "WHISKY JW RED LABEL 12/1000ML",
        "WHISKY JW RED LABEL 12/1000ML - SC",
        "WHISKY JOHNNIE WALKER RED LABEL 1L"
      ],
      "fiscal_signature": {
        "ncm_values": ["22083020"],
        "cest_values": ["0201600"],
        "gtin_values": ["5000267014277"]
      },
      "relation_summary": {
        "ncm": "EQUAL_FILLED",
        "cest": "EQUAL_FILLED",
        "gtin": "EQUAL_FILLED"
      },
      "metrics": {
        "score_descricao_min": 0.82,
        "score_descricao_avg": 0.89,
        "score_descr_compl_avg": 0.0,
        "filled_evidence_count": 3,
        "score_final_regra": 0.96
      },
      "blocked": false,
      "blocked_reason": null
    }
  ]
}
```

### 2. Aplicacao do lote

Endpoint proposto:
- `POST /api/python/produtos/unificacao-lote/aplicar`

Request:

```json
{
  "cnpj": "37671507000187",
  "source_context": "REVISAO_FINAL",
  "action": "UNIFICAR",
  "rule_id": "R1_HIGH_CONFIDENCE_FULL_FISCAL",
  "proposal_ids": ["LOT_R1_0001", "LOT_R1_0007", "LOT_R1_0010"],
  "context_snapshot": {
    "grouping_mode": "faiss",
    "filters": {
      "descricao_contains": "whisky",
      "ncm_contains": "2208",
      "cest_contains": ""
    },
    "similarity_source": {
      "engine": "FAISS",
      "top_k": 8,
      "min_score": 0.62
    }
  },
  "options": {
    "mark_status": true,
    "status_after_apply": "CONSOLIDADO",
    "dry_run": false
  }
}
```

Response:

```json
{
  "success": true,
  "cnpj": "37671507000187",
  "action": "UNIFICAR",
  "rule_id": "R1_HIGH_CONFIDENCE_FULL_FISCAL",
  "applied_count": 3,
  "affected_groups_count": 8,
  "skipped_count": 1,
  "skipped": [
    {
      "proposal_id": "LOT_R1_0010",
      "reason": "grupo alterado desde o preview"
    }
  ],
  "status_updates_count": 7,
  "mapa_manual_path": "CNPJ/37671507000187/analises/mapa_manual_descricoes_37671507000187.parquet",
  "status_path": "CNPJ/37671507000187/analises/status_analise_produtos_37671507000187.parquet"
}
```

### 3. Acao de lote para manter separado

O mesmo endpoint pode receber:

```json
{
  "cnpj": "37671507000187",
  "source_context": "REVISAO_FINAL",
  "action": "MANTER_SEPARADO",
  "rule_id": "R6_MANTER_SEPARADO",
  "proposal_ids": ["LOT_R6_0012", "LOT_R6_0013"],
  "options": {
    "mark_status": true,
    "status_after_apply": "MANTIDO_SEPARADO"
  }
}
```

## Tipos Esperados no Frontend

### Enumeracoes

```ts
export type BatchRuleId =
  | "R1_HIGH_CONFIDENCE_FULL_FISCAL"
  | "R2_NCM_CEST"
  | "R3_GTIN_NCM"
  | "R4_NCM_ONLY_STRICT"
  | "R5_REVIEW_ASSISTED"
  | "R6_MANTER_SEPARADO";

export type FiscalRelationState =
  | "EQUAL_FILLED"
  | "EQUAL_NULL"
  | "CONFLICT"
  | "INCOMPLETE";

export type BatchAction = "UNIFICAR" | "MANTER_SEPARADO" | "MANDAR_PARA_REVISAO";
```

### Preview

```ts
export interface BatchProposalItem {
  proposal_id: string;
  rule_id: BatchRuleId;
  button_label: string;
  confidence_band: "HIGH" | "MEDIUM_HIGH" | "MEDIUM" | "LOW";
  status: "ELEGIVEL" | "BLOQUEADO" | "REVISAR";
  source_method: "DOCUMENTAL" | "LIGHT" | "FAISS";
  component_size: number;
  chaves_produto: string[];
  descricao_canonica_sugerida: string;
  lista_descricoes: string[];
  fiscal_signature: {
    ncm_values: string[];
    cest_values: string[];
    gtin_values: string[];
  };
  relation_summary: {
    ncm: FiscalRelationState;
    cest: FiscalRelationState;
    gtin: FiscalRelationState;
  };
  metrics: {
    score_descricao_min: number;
    score_descricao_avg: number;
    score_descr_compl_avg: number;
    filled_evidence_count: number;
    score_final_regra: number;
  };
  blocked: boolean;
  blocked_reason: string | null;
}
```

### Apply

```ts
export interface BatchApplyResponse {
  success: boolean;
  cnpj: string;
  action: BatchAction;
  rule_id: BatchRuleId;
  applied_count: number;
  affected_groups_count: number;
  skipped_count: number;
  skipped: Array<{ proposal_id: string; reason: string }>;
  status_updates_count: number;
  mapa_manual_path?: string;
  status_path?: string;
}
```

## Comportamento Esperado na UI

### Faixa de lote na revisao final

Na `RevisaoFinalProdutos.tsx`, acima da tabela:
- chips de contagem por regra;
- botoes principais de lote;
- filtro `somente elegiveis`;
- modal de confirmacao com resumo do impacto.

### Confirmacao

Antes de aplicar:
- mostrar quantidade de propostas;
- quantidade de grupos afetados;
- codigo padrao sugerido por componente;
- regra usada;
- aviso de que a operacao grava mapa manual e status.

### Mensagens

Exemplos:
- `24 propostas elegiveis para Unificar alta confianca`
- `17 propostas elegiveis para Unificar NCM + CEST`
- `13 propostas movidas para Mantido separado`

## Trilhas e Auditoria

Cada aplicacao deve registrar:
- `cnpj`
- `rule_id`
- `action`
- `proposal_id`
- `chaves_produto`
- `timestamp`
- `source_context`
- `grouping_mode`
- `similarity_source.engine`
- filtros usados na hora do preview

Destino sugerido:
- parquet de auditoria de lote dedicado; ou
- extensao do `status_analise_produtos_{cnpj}.parquet` com metadados de acao.

## Ordem Recomendada de Implementacao

### Fase A
- novo comparador fiscal com estados explicitos
- endpoint de preview com `R1`, `R2` e `R6`
- chips de contagem e filtro `somente elegiveis`

### Fase B
- endpoint de aplicacao do lote
- gravacao de status e auditoria
- confirmacao de lote com preview resumido

### Fase C
- `R3` e `R4` como botoes avancados
- integracao mais forte com `FAISS`
- fila assistida para `R5`

## Recomendacao Final

Comecar em producao com:
- `R1_HIGH_CONFIDENCE_FULL_FISCAL`
- `R2_NCM_CEST`
- `R6_MANTER_SEPARADO`

E deixar inicialmente fora do CTA principal:
- `R3_GTIN_NCM`
- `R4_NCM_ONLY_STRICT`
- `R5_REVIEW_ASSISTED`

Assim a primeira entrega cobre lote conservador, com baixo risco e boa explicabilidade para o usuario.
