# Plano de Implementacao dos Demais Botoes de Agregacao em Lote

Status: planejamento operacional das regras restantes de agregacao em lote na revisao final de produtos.

Base de referencia:
- [documentacao/PRODUTOS/ESPECIFICACAO_UNIFICACAO_LOTE.md](c:/Users/eniot/.gemini/antigravity/scratch/sefin_audit_5/documentacao/PRODUTOS/ESPECIFICACAO_UNIFICACAO_LOTE.md)
- [client/src/pages/RevisaoFinalProdutos.tsx](c:/Users/eniot/.gemini/antigravity/scratch/sefin_audit_5/client/src/pages/RevisaoFinalProdutos.tsx)
- [server/python/core/produto_batch_lote.py](c:/Users/eniot/.gemini/antigravity/scratch/sefin_audit_5/server/python/core/produto_batch_lote.py)
- [server/python/routers/produto_unid.py](c:/Users/eniot/.gemini/antigravity/scratch/sefin_audit_5/server/python/routers/produto_unid.py)

## Objetivo

Expandir a agregacao em lote da tabela final desagregada com novos botoes de acao, mantendo:
- seguranca fiscal como criterio principal;
- similaridade textual apenas como apoio;
- trilha de auditoria clara;
- comportamento previsivel para o usuario.

## Estado Atual

Ja implementado:
- `Unificar alta confianca` (`R1_HIGH_CONFIDENCE_FULL_FISCAL`)
- `Unificar NCM + CEST` (`R2_NCM_CEST`)
- `Manter separados` (`R6_MANTER_SEPARADO`)

Ja existe no sistema:
- preview de propostas por regra;
- aplicacao em lote sem depender da janela de consolidacao;
- suporte a `DOCUMENTAL`, `LIGHT` e `FAISS`;
- comparador fiscal com `EQUAL_FILLED`, `EQUAL_NULL`, `CONFLICT` e `INCOMPLETE`.

## Meta dos Proximos Botoes

Implementar os botoes restantes:
- `Unificar GTIN + NCM`
- `Unificar NCM apenas`
- `Mandar para revisao`

Esses botoes devem aparecer como extensoes do painel `Lote sugerido`, sem poluir a tela principal.

## Matriz Resumida dos Novos Botoes

| Regra | Botao | Papel | Risco | Acao |
|---|---|---|---|---|
| `R3_GTIN_NCM` | `Unificar GTIN + NCM` | consolidacao forte com peso grande de GTIN | medio | unifica |
| `R4_NCM_ONLY_HIGH_TEXT` | `Unificar NCM apenas` | consolidacao assistida/agressiva | medio-alto | unifica |
| `R5_REVIEW_QUEUE` | `Mandar para revisao` | nao consolida; prioriza fila humana | baixo | marca grupos |

## Regras Operacionais

### 1. Botao `Unificar GTIN + NCM`

Quando usar:
- descricoes similares;
- `GTIN` preenchido e igual;
- `NCM` igual ou ambos nulos;
- `CEST` sem conflito.

Criterios sugeridos:
- `score_descricao >= 0.68`
- `gtin_state == EQUAL_FILLED`
- `ncm_state in {EQUAL_FILLED, EQUAL_NULL}`
- `cest_state in {EQUAL_FILLED, EQUAL_NULL, INCOMPLETE}`
- nenhum campo com `CONFLICT`

Bloqueios:
- `GTIN` diferente
- `NCM` diferente
- grupo com mais de um `GTIN` preenchido e conflitante dentro do componente

UX:
- botao secundario, abaixo de `Unificar NCM + CEST`
- precisa badge deixando claro: `GTIN forte`

### 2. Botao `Unificar NCM apenas`

Quando usar:
- texto muito parecido;
- `NCM` preenchido e igual;
- `CEST` e `GTIN` sem conflito, mas com evidencia fiscal incompleta.

Criterios sugeridos:
- `score_descricao >= 0.90`
- `ncm_state == EQUAL_FILLED`
- `cest_state != CONFLICT`
- `gtin_state != CONFLICT`
- ao menos um entre `CEST` e `GTIN` em `EQUAL_NULL`

Bloqueios:
- qualquer conflito fiscal forte
- componentes maiores que `max_component_size`
- divergencia de familias textuais internas acima do threshold de seguranca

UX:
- botao recolhido em `Mais opcoes`
- antes de aplicar, exibir confirmacao reforcada:
  - `Regra mais agressiva`
  - `Revise os filtros antes de continuar`

### 3. Botao `Mandar para revisao`

Quando usar:
- texto semelhante;
- evidencia fiscal insuficiente para unir automaticamente;
- nenhum conflito forte, mas existe `INCOMPLETE`.

Criterios sugeridos:
- `score_descricao >= 0.72`
- `ncm_state == EQUAL_FILLED`
- pelo menos um campo fiscal em `INCOMPLETE`
- nenhum `CONFLICT` bloqueante

Acao:
- nao altera mapa manual de unificacao;
- grava status operacional por grupo ou componente;
- retira os itens da fila de sugestao automatica;
- reapresenta na fila principal como `revisao prioritaria`.

UX:
- botao de baixa friccao;
- ideal para lotes maiores;
- pode ser acompanhado por chip `Revisao assistida`.

## Requisitos de Backend

### Fase B1: Expandir o motor de propostas

Arquivo principal:
- [server/python/core/produto_batch_lote.py](c:/Users/eniot/.gemini/antigravity/scratch/sefin_audit_5/server/python/core/produto_batch_lote.py)

Implementar:
- `R3_GTIN_NCM`
- `R4_NCM_ONLY_HIGH_TEXT`
- `R5_REVIEW_QUEUE`

Necessario:
- adicionar novos `rule_id`;
- calcular `button_label`, `confidence`, `blocked_reason`;
- manter prevencao contra efeito-corrente;
- manter exclusao de grupos ja decididos.

### Fase B2: Expandir o endpoint de aplicacao

Arquivo principal:
- [server/python/routers/produto_unid.py](c:/Users/eniot/.gemini/antigravity/scratch/sefin_audit_5/server/python/routers/produto_unid.py)

Implementar suporte a:
- `UNIFICAR` para `R3` e `R4`
- `MARCAR_REVISAO` para `R5`

Persistencia esperada:
- `R3` e `R4`: gravar `UNIR_GRUPOS` no mapa manual
- `R5`: gravar status dedicado, por exemplo `REVISAR_EM_LOTE`

Resposta esperada:
- `applied_count`
- `affected_groups_count`
- `status_updates_count`
- `rule_id`
- `action`
- `message`

### Fase B3: Testes

Adicionar testes em:
- [server/python/tests/test_produto_batch_lote.py](c:/Users/eniot/.gemini/antigravity/scratch/sefin_audit_5/server/python/tests/test_produto_batch_lote.py)

Cobrir:
- proposta valida de `R3`
- bloqueio de `R3` com `GTIN` conflitante
- proposta valida de `R4`
- bloqueio de `R4` por conflito fiscal
- proposta valida de `R5`
- aplicacao de `MARCAR_REVISAO`

## Requisitos de Frontend

### Fase F1: Chips e contadores

Arquivo principal:
- [client/src/pages/RevisaoFinalProdutos.tsx](c:/Users/eniot/.gemini/antigravity/scratch/sefin_audit_5/client/src/pages/RevisaoFinalProdutos.tsx)

Adicionar:
- chips para `R3`, `R4` e `R5`
- contagem por regra
- ordenacao visual:
  1. `Alta confianca`
  2. `NCM + CEST`
  3. `GTIN + NCM`
  4. `NCM apenas`
  5. `Mandar para revisao`
  6. `Manter separados`

### Fase F2: Acoes rapidas

Adicionar botoes:
- `Consolidar GTIN + NCM`
- `Consolidar NCM apenas`
- `Mandar para revisao`

Comportamento:
- `GTIN + NCM`: botao visivel por padrao
- `NCM apenas`: escondido em `Mais opcoes`
- `Mandar para revisao`: visivel por padrao

### Fase F3: Confirmacoes

Confirmacoes sugeridas:
- `R3`: confirmacao simples
- `R4`: confirmacao reforcada com texto de risco
- `R5`: confirmacao discreta ou aplicacao direta

### Fase F4: Mensagens de retorno

Toast de sucesso deve deixar claro:
- quantas propostas foram aplicadas
- quantos grupos foram afetados
- qual regra foi usada

Exemplos:
- `12 propostas consolidadas pela regra GTIN + NCM.`
- `18 grupos enviados para revisao assistida.`

## Ordem Recomendada de Implementacao

### Etapa 1
- implementar `R3_GTIN_NCM`
- adicionar chip e botao correspondente
- validar em bases com `GTIN` confiavel

### Etapa 2
- implementar `R5_REVIEW_QUEUE`
- adicionar persistencia de status operacional
- permitir limpar essa fila depois

### Etapa 3
- implementar `R4_NCM_ONLY_HIGH_TEXT`
- esconder em `Mais opcoes`
- acompanhar com confirmacao reforcada

## Proposta de Layout dos Botoes

Faixa principal:
- `Consolidar alta confianca`
- `Consolidar NCM + CEST`
- `Consolidar GTIN + NCM`
- `Mandar para revisao`
- `Manter separados`

Faixa secundaria:
- `Mais opcoes`
  - `Consolidar NCM apenas`
  - `Selecionar bloqueados`
  - `Ver somente elegiveis`

## Riscos e Cuidados

- `R3` pode ficar forte demais se `GTIN` vier reciclado ou sujo na origem.
- `R4` nao deve virar CTA principal.
- `R5` precisa status claro para nao sumir com grupos sem explicacao.
- agrupamentos por `FAISS` e `LIGHT` devem continuar opcionais e nunca superar a validacao fiscal.

## Criterios de Aceite

- o preview mostra contagens para `R3`, `R4` e `R5`;
- cada botao aplica a regra correta via endpoint;
- grupos decididos saem do preview apos a aplicacao;
- `R5` nao consolida nada, apenas muda o status operacional;
- a UI continua estavel em `DOCUMENTAL`, `LIGHT` e `FAISS`.

## Resultado Esperado

Ao final dessas etapas, a revisao final passa a ter um conjunto completo de botoes de agregacao em lote:
- conservadores para ganho rapido;
- assistidos para casos intermediarios;
- agressivos, mas claramente isolados;
- com fila humana como alternativa segura quando a evidencia ainda nao basta.
