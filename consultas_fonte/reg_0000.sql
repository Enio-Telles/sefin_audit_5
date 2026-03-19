WITH PARAMETROS AS (
    SELECT 
        :CNPJ AS cnpj_filtro,
        NVL(TO_DATE(:data_limite_processamento, 'DD/MM/YYYY'), TRUNC(SYSDATE)) AS dt_corte
    FROM dual
),
ARQUIVOS_PROCESSADOS AS (
    SELECT
        r.id AS reg_0000_id,
        r.reg,
        r.cod_ver,
        r.cod_fin,
        r.dt_ini,
        r.dt_fin,
        r.nome,
        r.cnpj,
        r.cpf,
        r.uf,
        r.ie,
        r.cod_mun,
        r.im,
        r.suframa,
        r.ind_perfil,
        r.ind_ativ,
        r.arquivo_nome,
        r.data_entrega,
        r.created_at,
        r.updated_at,
        r.reg_1,
        r.reg_c,
        r.reg_d,
        r.reg_e,
        r.reg_g,
        r.reg_h,
        r.reg_k,
        r.arquivo_tamanho,
        p.dt_corte,

        ROW_NUMBER() OVER (
            PARTITION BY r.cnpj, r.dt_ini
            ORDER BY r.data_entrega DESC, r.id DESC
        ) AS ordem,

        COUNT(*) OVER (
            PARTITION BY r.cnpj, r.dt_ini
        ) AS qtd_envios

    FROM sped.reg_0000 r
    CROSS JOIN PARAMETROS p
    WHERE r.cnpj = p.cnpj_filtro
      AND r.data_entrega < p.dt_corte + 1
)
SELECT 
    reg_0000_id,
    reg,
    cod_ver,
    cod_fin,
    CASE 
       WHEN cod_fin = 0 THEN '0 - Remessa do arquivo original'
       WHEN cod_fin = 1 THEN '1 - Remessa do arquivo substituto'
       ELSE 'Outros'
    END AS finalidade_arquivo,
    dt_ini,
    dt_fin,
    nome,
    cnpj,
    cpf,
    uf,
    ie,
    cod_mun,
    im,
    suframa,
    CASE 
       WHEN ind_perfil = 'A' THEN 'Perfil A'
       WHEN ind_perfil = 'B' THEN 'Perfil B'
       WHEN ind_perfil = 'C' THEN 'Perfil C'
       ELSE 'Outros'
    END AS ind_perfil,
    CASE 
       WHEN ind_ativ = 0 THEN '0 – Industrial ou equiparado a industrial'
       WHEN ind_ativ = 1 THEN '1 – Outros.'
       ELSE 'Outros'
    END AS ind_ativ,
    arquivo_nome,
    created_at,
    updated_at,
    reg_1,
    reg_c,
    reg_d,
    reg_e,
    reg_g,
    reg_h,
    reg_k,
    arquivo_tamanho,
    TO_CHAR(dt_ini, 'MM/YYYY') AS periodo_efd,
    data_entrega,
    ordem,
    qtd_envios,
    dt_corte AS data_limite_processamento
FROM ARQUIVOS_PROCESSADOS
ORDER BY dt_ini, ordem;