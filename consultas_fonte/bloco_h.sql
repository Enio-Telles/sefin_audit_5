WITH PARAMETROS AS (
    SELECT 
        :CNPJ                                         AS cnpj_filtro,
        NVL(TO_DATE(:data_limite_processamento, 'DD/MM/YYYY'), TRUNC(SYSDATE)) AS dt_corte
    FROM dual
),

-- 1. ARQUIVOS: Pega o arquivo EFD mais recente por período
ARQUIVOS_RANKING AS (
    SELECT
        r.id AS reg_0000_id,
        r.cnpj,
        r.dt_ini,
        ROW_NUMBER() OVER (
            PARTITION BY r.cnpj, r.dt_ini 
            ORDER BY r.data_entrega DESC, r.id DESC
        ) AS rn
    FROM sped.reg_0000 r
    INNER JOIN PARAMETROS p ON r.cnpj = p.cnpj_filtro
    WHERE r.data_entrega <= p.dt_corte
),

-- 2. H005: Cabeçalho do Inventário (Nível 1)
CTE_H005 AS (
    SELECT 
        id AS reg_h005_id, -- Chave Primária do Inventário
        reg_0000_id,
        TO_DATE(dt_inv, 'DDMMYYYY') AS dt_inv,
        NVL(CAST(vl_inv AS NUMBER), 0) AS vl_inv_total,
        mot_inv AS cod_mot_inv,
        DECODE(mot_inv, 
               '01', '01 - No final do período',
               '02', '02 - Mudança de tributação (ICMS)',
               '03', '03 - Baixa cadastral/paralisação temporária',
               '04', '04 - Alteração de regime de pagamento',
               '05', '05 - Por determinação dos fiscos',
               '06', '06 - Controle ST - restituição/ressarcimento',
               mot_inv) AS mot_inv_desc
    FROM sped.reg_h005
    WHERE reg_0000_id IN (SELECT reg_0000_id FROM ARQUIVOS_RANKING WHERE rn = 1)
),

-- 3. H010: Itens do Inventário (Nível 2 - Vinculado ao H005)
CTE_H010 AS (
    SELECT 
        id AS reg_h010_id,       -- Chave Primária do Item
        reg_h005_id,             -- Chave Estrangeira para o Cabeçalho H005
        reg_0000_id,
        cod_item AS codigo_produto_original,
        REGEXP_REPLACE(cod_item, '[^[:alnum:]]', '') AS codigo_produto_limpo,
        unid AS unidade_medida,
        NVL(CAST(qtd AS NUMBER), 0) AS quantidade,
        NVL(CAST(vl_unit AS NUMBER), 0) AS valor_unitario,
        NVL(CAST(vl_item AS NUMBER), 0) AS valor_item,
        ind_prop,                -- 0=Próprio em poder, 1=Próprio em terceiros, 2=Terceiros em poder
        cod_part,                -- Preenchido se ind_prop = 1 ou 2
        txt_compl                -- Descrição complementar (se houver)
    FROM sped.reg_h010
    WHERE reg_0000_id IN (SELECT reg_0000_id FROM ARQUIVOS_RANKING WHERE rn = 1)
),

-- 4. H020: Tributação do Item (Nível 3 - Vinculado ao H010)
CTE_H020 AS (
    SELECT 
        reg_h010_id,             -- Chave Estrangeira para o Item H010
        reg_0000_id,
        cst_icms,
        NVL(CAST(bc_icms AS NUMBER), 0) AS bc_icms,
        NVL(CAST(vl_icms AS NUMBER), 0) AS vl_icms
    FROM sped.reg_h020
    WHERE reg_0000_id IN (SELECT reg_0000_id FROM ARQUIVOS_RANKING WHERE rn = 1)
),

-- 5. 0200: Cadastro de Produtos (Dimensão)
CTE_0200 AS (
    SELECT 
        reg_0000_id,
        cod_item,
        descr_item AS descricao_produto,
        tipo_item,               -- 00=Mercadoria p/ Revenda, 01=Matéria-Prima, etc.
        cod_ncm,
        cest,
        cod_barra
    FROM sped.reg_0200
    WHERE reg_0000_id IN (SELECT reg_0000_id FROM ARQUIVOS_RANKING WHERE rn = 1)
)

-- ==========================================
-- CONSULTA FINAL: Montagem do Relatório
-- ==========================================
SELECT 
    
    arq.cnpj,
    
    -- Dados do Cabeçalho (H005)
    h005.dt_inv,
    h005.cod_mot_inv,
    h005.mot_inv_desc,
    h005.vl_inv_total AS valor_total_inventario_h005,
    
    -- Dados do Produto (0200)
    h010.codigo_produto_original AS codigo_produto,
    r0200.descricao_produto,
    r0200.cod_ncm,
    r0200.cest,
    r0200.cod_barra,
    r0200.tipo_item,
    
    -- Dados do Item no Inventário (H010)
    h010.unidade_medida,
    h010.quantidade,
    h010.valor_unitario,
    h010.valor_item,
    h010.ind_prop AS indicador_propriedade,
    h010.cod_part AS participante_terceiro,
    h010.txt_compl AS obs_complementar,
    
    -- Dados de Tributação (H020)
    h020.cst_icms,
    h020.bc_icms,
    h020.vl_icms

FROM ARQUIVOS_RANKING arq

-- 1. Traz o cabeçalho do inventário (Arquivo -> H005)
INNER JOIN CTE_H005 h005 
    ON h005.reg_0000_id = arq.reg_0000_id

-- 2. Traz os itens do inventário vinculados EXATAMENTE àquele cabeçalho (H005 -> H010)
INNER JOIN CTE_H010 h010 
    ON h010.reg_h005_id = h005.reg_h005_id 
   AND h010.reg_0000_id = arq.reg_0000_id

-- 3. Traz a tributação vinculada EXATAMENTE àquele item (H010 -> H020)
LEFT JOIN CTE_H020 h020 
    ON h020.reg_h010_id = h010.reg_h010_id 
   AND h020.reg_0000_id = arq.reg_0000_id

-- 4. Traz o cadastro do produto (H010 -> 0200)
LEFT JOIN CTE_0200 r0200 
    ON r0200.reg_0000_id = arq.reg_0000_id 
   AND r0200.cod_item = h010.codigo_produto_original

WHERE arq.rn = 1
ORDER BY 
    h005.dt_inv DESC, 
    h010.codigo_produto_limpo;